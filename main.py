from __future__ import annotations

import argparse
import asyncio
from contextlib import AsyncExitStack, suppress
from typing import Any

from pyrogram import Client

from app.admin_bot import run_admin_bot
from app.config import AppConfig, load_config
from app.control import clear_stop, watch_stop_request, write_status
from app.db import Database
from app.destination_manager import add_destination, list_destinations, remove_destination
from app.logging import setup_logging
from app.queue import MessageQueue
from app.scanner import Scanner
from app.telegram_client import (
    TelegramLimiter,
    install_stop_handlers,
    interactive_login,
    make_bot_client,
    make_user_client,
    resolve_chat,
    update_account_cache,
)
from app.upload import Uploader
from app.worker import Verifier, Worker


COMMANDS = (
    "login",
    "admin",
    "scan",
    "process",
    "verify",
    "run",
    "stats",
    "recover",
    "list-destinations",
    "add-destination",
    "remove-destination",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Telegram migration with reusable bot file_id cache"
    )
    parser.add_argument("command", choices=COMMANDS, help="Command to run")
    parser.add_argument("values", nargs="*", help="Arguments for destination commands")
    parser.add_argument("--config", default="config.yaml", help="Path to YAML config")
    parser.add_argument("--session", help="Session name for login command")
    return parser.parse_args()


def handle_destination_command(args: argparse.Namespace) -> bool:
    if args.command == "list-destinations":
        destinations = list_destinations(args.config)
        if not destinations:
            print("No destinations configured")
            return True
        print("Destinations:")
        for index, destination in enumerate(destinations, start=1):
            text = f"{index}. {destination['chat']}"
            if destination.get("topic_id") is not None:
                text += f" (topic {destination['topic_id']})"
            print(text)
        return True

    if args.command == "add-destination":
        if not 1 <= len(args.values) <= 2:
            raise SystemExit(
                "Usage: python3 main.py add-destination @channel [topic_id]"
            )
        topic_id = int(args.values[1]) if len(args.values) == 2 else None
        added = add_destination(args.values[0], topic_id, args.config)
        print(f"Added destination: {added}")
        return True

    if args.command == "remove-destination":
        if len(args.values) != 1:
            raise SystemExit("Usage: python3 main.py remove-destination <number>")
        removed = remove_destination(int(args.values[0]), args.config)
        print(f"Removed destination: {removed}")
        return True

    return False


def _configured_destinations(config: AppConfig) -> list[Any]:
    return [
        spec
        for spec in config.destinations
        if spec.chat
        and "destination_channel_or_-100_id" not in str(spec.chat).lower()
    ]


async def choose_writer_for_destinations(
    config: AppConfig,
    reader: Client,
    writer: Client,
    limiter: TelegramLimiter,
    logger: Any | None = None,
) -> tuple[Client, bool]:
    """Use the bot when it knows every destination; otherwise safely use the user session."""
    destinations = _configured_destinations(config)
    if writer is reader or not destinations:
        return writer, True

    for spec in destinations:
        try:
            await resolve_chat(writer, limiter, spec)
            continue
        except Exception as bot_error:
            try:
                await resolve_chat(reader, limiter, spec)
            except Exception as reader_error:
                if logger:
                    logger.warning(
                        "Destination %s cannot be resolved by writer or reader: writer=%s reader=%s",
                        spec.chat,
                        bot_error,
                        reader_error,
                    )
                return writer, False

            if logger:
                logger.warning(
                    "Writer bot cannot resolve private destination %s; using user session for this cycle",
                    spec.chat,
                )
            return reader, True

    return writer, True


async def run_with_clients(config: AppConfig, command: str) -> None:
    logger = setup_logging(config.logging)
    limiter = TelegramLimiter(config, logger)
    stop_event = asyncio.Event()
    install_stop_handlers(stop_event)

    db = Database(config.queue.db_path)
    db.initialize()
    queue = MessageQueue(db, config)
    stop_watcher: asyncio.Task[None] | None = None

    try:
        if command == "stats":
            print_counts(queue.counts_by_status())
            print(f"cached_file_id: {queue.media_cache_count()}")
            return
        if command == "recover":
            recovered = queue.recover_in_progress()
            print(f"Recovered {recovered} in-progress jobs to pending")
            return

        clear_stop(config)
        write_status(config, "starting", message="Menyambung ke Telegram...")
        stop_watcher = asyncio.create_task(watch_stop_request(config, stop_event))

        async with AsyncExitStack() as stack:
            reader = make_user_client(config)
            await stack.enter_async_context(reader)
            me = await limiter.call("read", reader.get_me)
            update_account_cache(config, config.telegram.user_session, me)
            logger.info("Reader session: %s (%s)", me.first_name, me.id)

            bot = make_bot_client(config)
            writer = reader
            if bot and config.telegram.use_bot_for_uploads:
                writer = bot
                await stack.enter_async_context(writer)
                bot_me = await limiter.call("read", writer.get_me)
                logger.info("Writer bot: %s (%s)", bot_me.first_name, bot_me.id)

            writer, destinations_ready = await choose_writer_for_destinations(
                config,
                reader,
                writer,
                limiter,
                logger,
            )
            if writer is reader and bot is not None:
                write_status(
                    config,
                    "starting",
                    message="Destination private dikesan. Menggunakan user session untuk penghantaran.",
                    reader_id=me.id,
                )

            if destinations_ready:
                revived = queue.requeue_peer_id_errors()
                if revived:
                    logger.info("Returned %s PEER_ID_INVALID job(s) to pending", revived)

            write_status(
                config,
                "starting",
                message="Telegram connected. Menyediakan migration cycle.",
                reader_id=me.id,
                writer="user" if writer is reader else "bot",
            )

            if config.telegram.load_dialogs_on_start:
                logger.info(
                    "Dialog cache warmup skipped; chats are resolved directly through the limiter"
                )

            if command in {"scan", "run"}:
                scanner = Scanner(
                    config,
                    queue,
                    reader,
                    limiter,
                    writer=writer,
                    logger=logger,
                )
                await scanner.scan(stop_event)

            if command in {"process", "run"} and not stop_event.is_set():
                uploader = Uploader(
                    config,
                    reader,
                    writer,
                    limiter,
                    queue=queue,
                    logger=logger,
                )
                worker = Worker(config, queue, uploader, logger=logger)
                await worker.run(stop_event)

            if command == "verify" and not stop_event.is_set():
                verifier = Verifier(config, queue, writer, limiter, logger=logger)
                await verifier.run(stop_event)

            if stop_event.is_set():
                write_status(
                    config,
                    "stopped",
                    message="Migration dihentikan dengan selamat.",
                    **queue.counts_by_status(),
                )
    except Exception as exc:
        write_status(
            config,
            "error",
            message="Migration cycle berhenti kerana ralat.",
            error=f"{exc.__class__.__name__}: {exc}"[:1000],
            **queue.counts_by_status(),
        )
        raise
    finally:
        if stop_watcher is not None:
            stop_watcher.cancel()
            with suppress(asyncio.CancelledError):
                await stop_watcher
        clear_stop(config)
        db.close()


def print_counts(counts: dict[str, int]) -> None:
    if not counts:
        print("Queue is empty")
        return
    for status in ("pending", "downloading", "uploading", "copied", "failed", "skipped"):
        print(f"{status}: {counts.get(status, 0)}")


async def async_main() -> None:
    args = parse_args()
    if handle_destination_command(args):
        return

    config = load_config(args.config)
    config.ensure_directories()

    if args.command == "login":
        await interactive_login(config, args.session)
        return

    if args.command == "admin":
        await run_admin_bot(config, args.config)
        return

    await run_with_clients(config, args.command)


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
