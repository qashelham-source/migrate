from __future__ import annotations

import argparse
import asyncio
from contextlib import AsyncExitStack

from app.admin_bot import run_admin_bot
from app.config import AppConfig, load_config
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


async def run_with_clients(config: AppConfig, command: str) -> None:
    logger = setup_logging(config.logging)
    limiter = TelegramLimiter(config, logger)
    stop_event = asyncio.Event()
    install_stop_handlers(stop_event)

    db = Database(config.queue.db_path)
    db.initialize()
    queue = MessageQueue(db, config)

    try:
        if command == "stats":
            print_counts(queue.counts_by_status())
            print(f"cached_file_id: {queue.media_cache_count()}")
            return
        if command == "recover":
            recovered = queue.recover_in_progress()
            print(f"Recovered {recovered} in-progress jobs to pending")
            return

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
    finally:
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
