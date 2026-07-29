from __future__ import annotations

import argparse
import asyncio
from contextlib import AsyncExitStack, suppress
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyrogram import Client

from app.admin_bot import run_admin_bot
from app.config import AppConfig, load_config
from app.control import clear_stop, watch_stop_request, write_status
from app.db import Database
from app.destination_manager import add_destination, list_destinations, remove_destination
from app.health import run_health_check
from app.live import LiveTrigger
from app.logging import setup_logging
from app.queue import MessageQueue
from app.release3_uploader import Release3Uploader
from app.scanner import Scanner
from app.source_registry import refresh_source_registry
from app.telegram_client import (
    TelegramLimiter,
    install_stop_handlers,
    interactive_login,
    make_bot_client,
    make_user_client,
    resolve_chat,
    update_account_cache,
)
from app.worker import Verifier, Worker


COMMANDS = (
    "login",
    "admin",
    "health",
    "scan",
    "sync",
    "process",
    "verify",
    "run",
    "serve",
    "stats",
    "recover",
    "list-destinations",
    "add-destination",
    "remove-destination",
)


@dataclass(frozen=True)
class CycleOutcome:
    """The point where a sequential source queue may safely pause."""

    state: str
    source_chat_id: int | None = None
    source_index: int | None = None
    source_total: int | None = None
    next_retry_at: str | None = None
    retry_after_seconds: float | None = None
    review_items: int = 0
    review_job_id: int | None = None
    review_summary: str | None = None
    message: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verified live Telegram migration with reusable bot file_id cache"
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
        for spec in getattr(config, "destinations", [])
        if spec.chat
        and "destination_channel_or_-100_id" not in str(spec.chat).lower()
    ]


def _configured_sources(config: AppConfig) -> list[Any]:
    return [
        spec
        for spec in getattr(config, "sources", [])
        if spec.chat and "source_channel_or_-100_id" not in str(spec.chat).lower()
    ]


def _configured_numeric_peer_ids(config: AppConfig) -> set[int]:
    result: set[int] = set()
    for spec in [*getattr(config, "sources", []), *getattr(config, "destinations", [])]:
        value = str(spec.chat or "").strip()
        if value.lstrip("-").isdigit():
            result.add(int(value))
    return result


def _source_status_details(
    state: dict[str, Any],
    *,
    source: str,
    source_chat_id: int,
    source_index: int,
    source_total: int,
) -> dict[str, Any]:
    return {
        "source": source,
        "source_chat": source_chat_id,
        "source_index": source_index,
        "source_total": source_total,
        "source_pending": state["pending_jobs"],
        "source_active": state["active_jobs"],
        "source_delayed": state["delayed_jobs"],
        "source_paused": state["paused_jobs"],
        "source_failed": state["failed_jobs"],
        "source_skipped": state["skipped_issue_jobs"],
        "source_review_items": state["review_items"],
        "review_job_id": state.get("review_job_id"),
        "review_kind": state.get("review_kind"),
        "review_summary": state.get("last_error"),
        "source_verification_pending": state["verification_pending_jobs"],
        "source_verification_failed": state["verification_failed_jobs"],
        "source_verification_repairing": state["verification_repairing_jobs"],
    }


def _source_outcome(
    state: dict[str, Any],
    *,
    source_chat_id: int,
    source_index: int,
    source_total: int,
) -> CycleOutcome:
    """Keep later sources waiting until this source is safe to advance."""
    common = {
        "source_chat_id": source_chat_id,
        "source_index": source_index,
        "source_total": source_total,
    }
    primary_failed_jobs = int(state.get("primary_failed_jobs", state["failed_jobs"]))
    primary_skipped_issue_jobs = int(
        state.get("primary_skipped_issue_jobs", state["skipped_issue_jobs"])
    )
    terminal_issues = primary_failed_jobs + primary_skipped_issue_jobs
    if terminal_issues:
        return CycleOutcome(
            "blocked",
            message="A source upload failed and needs manual review before it can be retried safely.",
            **common,
        )
    if int(state["paused_jobs"]):
        return CycleOutcome(
            "blocked",
            message="A destination for this source is paused and must be fixed first.",
            **common,
        )
    if int(state["active_jobs"]):
        return CycleOutcome(
            "blocked",
            message="An older job is still marked as in progress. It is held to prevent a duplicate upload.",
            **common,
        )
    if int(state["delayed_jobs"]):
        return CycleOutcome(
            "retry",
            next_retry_at=state.get("next_retry_at"),
            message="An automatic retry has been scheduled for this source.",
            **common,
        )
    if int(state["verification_pending_jobs"]):
        return CycleOutcome(
            "retry",
            retry_after_seconds=60,
            message="Waiting to retry verification before moving to the next source.",
            **common,
        )
    if int(state["runnable_jobs"]):
        return CycleOutcome(
            "retry",
            retry_after_seconds=2,
            message=(
                "Queued repair work will continue automatically before moving to the next source."
                if int(state["verification_repairing_jobs"])
                else "Runnable work remains; the worker will continue automatically."
            ),
            **common,
        )
    if int(state["verification_repairing_jobs"]):
        return CycleOutcome(
            "retry",
            retry_after_seconds=5,
            message="Repair verification is still running and will continue automatically.",
            **common,
        )
    review_items = int(state.get("review_items", state["verification_failed_jobs"]))
    if review_items:
        review_job_id = state.get("review_job_id")
        return CycleOutcome(
            "complete",
            review_items=review_items,
            review_job_id=int(review_job_id) if review_job_id is not None else None,
            review_summary=str(state.get("last_error") or "") or None,
            message=(
                f"{review_items} review item(s) were saved in Issue Center. "
                "The next source can run safely."
            ),
            **common,
        )
    return CycleOutcome("complete", **common)


def _retry_wait_seconds(outcome: CycleOutcome) -> float:
    if outcome.retry_after_seconds is not None:
        return max(0.5, float(outcome.retry_after_seconds))
    if outcome.next_retry_at:
        try:
            value = str(outcome.next_retry_at).replace("Z", "+00:00")
            deadline = datetime.fromisoformat(value)
            if deadline.tzinfo is None:
                deadline = deadline.replace(tzinfo=timezone.utc)
            return max(0.5, (deadline - datetime.now(timezone.utc)).total_seconds())
        except (TypeError, ValueError):
            pass
    return 30.0


async def warm_dialog_cache(
    config: AppConfig,
    reader: Client,
    stop_event: asyncio.Event,
    logger: Any | None = None,
) -> tuple[int, set[int]]:
    """Load the user session's dialogs so private peer IDs and access hashes are cached."""
    if not config.telegram.load_dialogs_on_start:
        return 0, set()

    targets = _configured_numeric_peer_ids(config)
    found: set[int] = set()
    loaded = 0
    write_status(
        config,
        "starting",
        message="Loading Telegram dialog and channel cache...",
        target_peers=len(targets),
    )
    if logger:
        logger.info(
            "Warming Telegram dialog cache before peer resolution; target_peer_ids=%s",
            sorted(targets),
        )

    try:
        async for dialog in reader.get_dialogs():
            if stop_event.is_set():
                break
            loaded += 1
            chat = getattr(dialog, "chat", None)
            chat_id = getattr(chat, "id", None)
            if chat_id is not None:
                numeric_id = int(chat_id)
                if numeric_id in targets:
                    found.add(numeric_id)
            if targets and found == targets:
                break
    except Exception as exc:
        if logger:
            logger.warning("Telegram dialog cache warmup stopped early: %s", exc)

    missing = targets - found
    if logger:
        logger.info(
            "Dialog cache warmup complete: loaded=%s matched=%s missing=%s",
            loaded,
            sorted(found),
            sorted(missing),
        )
    write_status(
        config,
        "starting",
        message="Telegram dialog cache loaded.",
        dialogs_loaded=loaded,
        matched_peers=len(found),
        missing_peers=len(missing),
    )
    return loaded, found


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


async def _resume_healthy_destinations(
    config: AppConfig,
    queue: MessageQueue,
    client: Client,
    limiter: TelegramLimiter,
    logger: Any | None = None,
) -> None:
    for spec in _configured_destinations(config):
        try:
            resolved = await resolve_chat(client, limiter, spec)
            queue.resume_destination(resolved.chat_id)
        except Exception as exc:
            if logger:
                logger.debug("Destination %s remains unavailable: %s", spec.chat, exc)


async def _resolved_source_ids(
    config: AppConfig,
    queue: MessageQueue,
    reader: Client,
    limiter: TelegramLimiter,
    logger: Any | None = None,
) -> set[int]:
    source_ids: set[int] = set()
    for spec in _configured_sources(config):
        try:
            resolved = await resolve_chat(reader, limiter, spec)
            source_ids.add(int(resolved.chat_id))
            queue.set_live_watch(resolved.chat_id, True)
        except Exception as exc:
            if logger:
                logger.warning("Live watcher could not resolve source %s: %s", spec.chat, exc)
    return source_ids


async def _write_initial_wait_status(
    config: AppConfig,
    queue: MessageQueue,
    reader: Client,
    limiter: TelegramLimiter,
    *,
    watched_sources: int,
    reconciliation_seconds: float,
    logger: Any | None = None,
) -> CycleOutcome:
    """Report queued work and return whether a safe queue should resume on boot."""
    sources = _configured_sources(config)
    destinations = _configured_destinations(config)
    if not sources or not destinations:
        write_status(
            config,
            "waiting",
            message="Set at least one source and one destination first.",
            source="set" if sources else "missing",
            destination="set" if destinations else "missing",
        )
        return CycleOutcome("blocked", message="The source or destination is not configured.")

    for source_index, source in enumerate(sources, start=1):
        try:
            resolved = await resolve_chat(reader, limiter, source)
        except Exception as exc:
            write_status(
                config,
                "blocked",
                message="The source cannot be accessed. The queue will not run automatically.",
                source=source.chat,
                source_index=source_index,
                source_total=len(sources),
                error=f"{exc.__class__.__name__}: {exc}"[:1000],
            )
            return CycleOutcome(
                "blocked",
                source_index=source_index,
                source_total=len(sources),
                message="The source cannot be accessed.",
            )
        source_chat_id = int(resolved.chat_id)
        state = queue.source_work_state(source_chat_id)
        queue.recompute_source_state(source_chat_id)
        outcome = _source_outcome(
            state,
            source_chat_id=source_chat_id,
            source_index=source_index,
            source_total=len(sources),
        )
        if outcome.state == "complete":
            continue

        details = _source_status_details(
            state,
            source=str(resolved.title or source.chat),
            source_chat_id=source_chat_id,
            source_index=source_index,
            source_total=len(sources),
        )
        if outcome.state == "blocked":
            write_status(
                config,
                "blocked",
                message=(
                    "This source queue needs attention. "
                    "Later sources remain in the waiting list."
                ),
                last_error=state.get("last_error"),
                **details,
            )
        else:
            write_status(
                config,
                "waiting_retry",
                message=(
                    "Safe queued work will resume automatically. "
                    "Later sources remain in the waiting list until it is complete."
                ),
                retry_at=outcome.next_retry_at,
                last_error=state.get("last_error"),
                **details,
            )
        return outcome

    write_status(
        config,
        "watching",
        message="Live Watcher is active. Waiting for a new post or an admin command before migration starts.",
        live_watcher=True,
        watched_sources=watched_sources,
        reconciliation_seconds=reconciliation_seconds,
        **queue.counts_by_status(),
    )
    return CycleOutcome("complete")


async def _execute_cycle(
    config: AppConfig,
    command: str,
    *,
    config_path: str | Path | None = None,
    reader: Client,
    bot: Client | None,
    limiter: TelegramLimiter,
    queue: MessageQueue,
    stop_event: asyncio.Event,
    reader_me: Any,
    writer_me: Any,
    logger: Any | None,
    trigger_reason: str | None = None,
) -> CycleOutcome:
    queue.config = config
    writer = bot if bot is not None and config.telegram.use_bot_for_uploads else reader
    writer, destinations_ready = await choose_writer_for_destinations(
        config,
        reader,
        writer,
        limiter,
        logger,
    )
    if destinations_ready:
        await _resume_healthy_destinations(config, queue, writer, limiter, logger)
        revived = queue.requeue_peer_id_errors()
        if revived and logger:
            logger.info("Returned %s PEER_ID_INVALID job(s) to pending", revived)

    cycle_mode = "incremental" if command == "sync" else "full" if command in {"scan", "run"} else command
    write_status(
        config,
        "starting",
        message="Telegram connected. Preparing migration cycle.",
        reader_id=reader_me.id,
        writer="user" if writer is reader else "bot",
        cycle_mode=cycle_mode,
        live_trigger=trigger_reason,
    )

    if command == "health":
        report = await run_health_check(
            config,
            reader,
            writer,
            limiter,
            queue,
            reader_me=reader_me,
            writer_me=writer_me,
            logger=logger,
        )
        print(f"Health check: {report['overall']}")
        return CycleOutcome("complete")

    sources = _configured_sources(config)
    destinations = _configured_destinations(config)
    if not sources or not destinations:
        write_status(
            config,
            "waiting",
            message="Set at least one source and one destination first.",
            source="set" if sources else "missing",
            destination="set" if destinations else "missing",
        )
        return CycleOutcome(
            "blocked",
            message="The source or destination is not configured.",
        )
    if not destinations_ready:
        write_status(
            config,
            "blocked",
            message="The destination cannot be accessed. The source queue is safely paused.",
            destination_count=len(destinations),
        )
        return CycleOutcome(
            "blocked",
            message="The destination is not ready for upload.",
        )

    source_total = len(sources)
    for source_index, source in enumerate(sources, start=1):
        if stop_event.is_set():
            break
        if config_path is not None:
            try:
                live_sources = _configured_sources(load_config(config_path))
            except Exception as exc:
                if logger:
                    logger.warning("Could not refresh source queue before source %s: %s", source.chat, exc)
            else:
                still_configured = any(
                    str(item.chat) == str(source.chat)
                    and getattr(item, "topic_id", None) == getattr(source, "topic_id", None)
                    for item in live_sources
                )
                if not still_configured:
                    if logger:
                        logger.info("Skipping blacklisted source %s", source.chat)
                    continue

        try:
            resolved_source = await resolve_chat(reader, limiter, source)
        except Exception as exc:
            write_status(
                config,
                "blocked",
                message="The source cannot be accessed. Later sources remain in the waiting list.",
                source=source.chat,
                source_index=source_index,
                source_total=source_total,
                error=f"{exc.__class__.__name__}: {exc}"[:1000],
            )
            return CycleOutcome(
                "blocked",
                source_index=source_index,
                source_total=source_total,
                message="The source cannot be accessed.",
            )

        source_chat_id = int(resolved_source.chat_id)
        source_title = str(resolved_source.title or source.chat)
        source_config = replace(config, sources=[source])
        queue.config = source_config

        if command in {"scan", "sync", "run"}:
            scanner = Scanner(
                source_config,
                queue,
                reader,
                limiter,
                writer=writer,
                logger=logger,
                scan_mode="incremental" if command == "sync" else "full",
                source_index_offset=source_index - 1,
                source_total_override=source_total,
                config_path=config_path,
            )
            await scanner.scan(stop_event)

        if command == "scan":
            state = queue.source_work_state(source_chat_id)
            queue.recompute_source_state(source_chat_id)
            if state["pending_jobs"] or state["active_jobs"] or state["verification_pending_jobs"]:
                write_status(
                    config,
                    "queued",
                    message="Scan complete. This source queue is waiting for a Process/Run command.",
                    **_source_status_details(
                        state,
                        source=source_title,
                        source_chat_id=source_chat_id,
                        source_index=source_index,
                        source_total=source_total,
                    ),
                )
                return CycleOutcome(
                    "queued",
                    source_chat_id=source_chat_id,
                    source_index=source_index,
                    source_total=source_total,
                    message="Scan built the queue but did not start the worker.",
                )
            continue

        if command in {"process", "sync", "run"} and not stop_event.is_set():
            uploader = Release3Uploader(
                source_config,
                reader,
                writer,
                limiter,
                queue=queue,
                logger=logger,
            )
            worker = Worker(
                source_config,
                queue,
                uploader,
                logger=logger,
                source_chat_id=source_chat_id,
                source_index=source_index,
                source_total=source_total,
                source_label=source_title,
            )
            await worker.run(stop_event)

        if command in {"verify", "process", "sync", "run"} and not stop_event.is_set():
            verifier = Verifier(
                source_config,
                queue,
                reader,
                writer,
                limiter,
                logger=logger,
                source_chat_id=source_chat_id,
            )
            await verifier.run(stop_event)

        if stop_event.is_set():
            break

        state = queue.source_work_state(source_chat_id)
        queue.recompute_source_state(source_chat_id)
        outcome = _source_outcome(
            state,
            source_chat_id=source_chat_id,
            source_index=source_index,
            source_total=source_total,
        )
        if outcome.state == "complete":
            if source_index < source_total:
                completion_message = "Source complete. Moving to the next source in the queue."
                if outcome.review_items:
                    completion_message = (
                        f"Source complete. {outcome.review_items} item(s) were saved in Issue Center; "
                        "moving to the next source."
                    )
                write_status(
                    config,
                    "source_complete",
                    message=completion_message,
                    review_items=outcome.review_items,
                    review_job_id=outcome.review_job_id,
                    review_summary=outcome.review_summary,
                    **_source_status_details(
                        state,
                        source=source_title,
                        source_chat_id=source_chat_id,
                        source_index=source_index,
                        source_total=source_total,
                    ),
                )
            continue

        details = _source_status_details(
            state,
            source=source_title,
            source_chat_id=source_chat_id,
            source_index=source_index,
            source_total=source_total,
        )
        if outcome.state == "retry":
            write_status(
                config,
                "waiting_retry",
                message=outcome.message,
                retry_at=outcome.next_retry_at,
                last_error=state.get("last_error"),
                **details,
            )
        else:
            write_status(
                config,
                "blocked",
                message=outcome.message,
                last_error=state.get("last_error"),
                **details,
            )
        return outcome

    if stop_event.is_set():
        return CycleOutcome("blocked", message="Migration was stopped.")

    write_status(
        config,
        "source_complete",
        message="All sources in the queue are complete for this cycle.",
        source_total=source_total,
        cycle_mode=cycle_mode,
    )
    return CycleOutcome("complete", source_total=source_total)


async def _run_live_service(
    initial_config: AppConfig,
    config_path: str | Path,
    *,
    reader: Client,
    bot: Client | None,
    limiter: TelegramLimiter,
    queue: MessageQueue,
    stop_event: asyncio.Event,
    reader_me: Any,
    writer_me: Any,
    logger: Any | None,
) -> None:
    await refresh_source_registry(initial_config, queue, reader, stop_event, logger)
    source_ids = await _resolved_source_ids(initial_config, queue, reader, limiter, logger)
    trigger = LiveTrigger(source_ids)
    reader.add_handler(trigger.handler)
    command: str | None = None
    reason: str | None = None
    cycle_number = 0
    try:
        while not stop_event.is_set():
            config = load_config(config_path)
            config.ensure_directories()
            queue.config = config
            trigger.source_ids = await _resolved_source_ids(config, queue, reader, limiter, logger)

            if command is None:
                initial_outcome = await _write_initial_wait_status(
                    config,
                    queue,
                    reader,
                    limiter,
                    watched_sources=len(trigger.source_ids),
                    reconciliation_seconds=trigger.settings.reconcile_interval_seconds,
                    logger=logger,
                )
                if isinstance(initial_outcome, CycleOutcome) and initial_outcome.state == "retry":
                    command, reason = "process", "automatic_resume"
                    continue
                next_trigger = await trigger.wait(
                    config,
                    stop_event,
                    allow_reconciliation=False,
                )
                if next_trigger is None:
                    break
                command, reason = next_trigger
                continue

            cycle_number += 1
            write_status(
                config,
                "starting",
                message="Live Watcher is active. Running migration cycle.",
                live_watcher=True,
                watched_sources=len(trigger.source_ids),
                reconciliation_seconds=trigger.settings.reconcile_interval_seconds,
                live_trigger=reason,
                cycle_number=cycle_number,
            )
            outcome = await _execute_cycle(
                config,
                command,
                config_path=config_path,
                reader=reader,
                bot=bot,
                limiter=limiter,
                queue=queue,
                stop_event=stop_event,
                reader_me=reader_me,
                writer_me=writer_me,
                logger=logger,
                trigger_reason=reason,
            )
            if stop_event.is_set():
                break
            if cycle_number % 12 == 0:
                await refresh_source_registry(config, queue, reader, stop_event, logger)
            if not isinstance(outcome, CycleOutcome):
                outcome = CycleOutcome("complete")
            if outcome.state == "retry":
                next_trigger = await trigger.wait_for_retry(
                    config,
                    stop_event,
                    _retry_wait_seconds(outcome),
                )
                if next_trigger is None:
                    break
                command, reason = next_trigger
                continue
            if outcome.state == "blocked":
                next_trigger = await trigger.wait(
                    config,
                    stop_event,
                    allow_reconciliation=False,
                )
                if next_trigger is None:
                    break
                command, reason = next_trigger
                continue
            write_status(
                config,
                "watching",
                message="Live Watcher is waiting for new posts; all active sources are complete.",
                live_watcher=True,
                watched_sources=len(trigger.source_ids),
                reconciliation_seconds=trigger.settings.reconcile_interval_seconds,
                **queue.counts_by_status(),
            )
            next_trigger = await trigger.wait(config, stop_event)
            if next_trigger is None:
                break
            command, reason = next_trigger
    finally:
        reader.remove_handler(trigger.handler)

async def run_with_clients(config: AppConfig, command: str, config_path: str | Path) -> None:
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
            print(f"scan_checkpoints: {len(db.list_scan_checkpoints())}")
            print(f"source_registry: {len(queue.list_registered_sources())}")
            return
        if command == "recover":
            recovery = queue.recover_in_progress()
            print(f"Recovered {recovery.requeued_downloads} interrupted download job(s) to pending")
            print(
                f"Held {recovery.held_uploads} interrupted upload job(s) for manual destination verification"
            )
            return

        recovery = queue.recover_in_progress()
        if recovery.total and logger:
            logger.warning(
                "Recovered interrupted queue state: downloads=%s uploads_held=%s",
                recovery.requeued_downloads,
                recovery.held_uploads,
            )

        clear_stop(config)
        write_status(
            config,
            "starting",
            message="Connecting to Telegram...",
            cycle_mode="service" if command == "serve" else command,
        )
        stop_watcher = asyncio.create_task(watch_stop_request(config, stop_event))

        async with AsyncExitStack() as stack:
            reader = make_user_client(config)
            await stack.enter_async_context(reader)
            me = await limiter.call("read", reader.get_me)
            update_account_cache(config, config.telegram.user_session, me)
            logger.info("Reader session: %s (%s)", me.first_name, me.id)

            await warm_dialog_cache(config, reader, stop_event, logger)

            bot = make_bot_client(config)
            writer_me = me
            if bot and config.telegram.use_bot_for_uploads:
                await stack.enter_async_context(bot)
                writer_me = await limiter.call("read", bot.get_me)
                logger.info("Writer bot: %s (%s)", writer_me.first_name, writer_me.id)
            else:
                bot = None

            if command == "serve":
                await _run_live_service(
                    config,
                    config_path,
                    reader=reader,
                    bot=bot,
                    limiter=limiter,
                    queue=queue,
                    stop_event=stop_event,
                    reader_me=me,
                    writer_me=writer_me,
                    logger=logger,
                )
            else:
                await _execute_cycle(
                    config,
                    command,
                    config_path=config_path,
                    reader=reader,
                    bot=bot,
                    limiter=limiter,
                    queue=queue,
                    stop_event=stop_event,
                    reader_me=me,
                    writer_me=writer_me,
                    logger=logger,
                )

            if stop_event.is_set():
                write_status(
                    config,
                    "stopped",
                    message="Migration stopped safely.",
                    **queue.counts_by_status(),
                )
    except Exception as exc:
        write_status(
            config,
            "error",
            message="Migration cycle stopped because of an error.",
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

    await run_with_clients(config, args.command, args.config)


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
