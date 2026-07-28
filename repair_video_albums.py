from __future__ import annotations

import argparse
import asyncio
import json
from contextlib import AsyncExitStack
from typing import Any

from pyrogram import Client
from pyrogram.types import Message

from app.config import load_config
from app.db import Database, utc_now
from app.logging import setup_logging
from app.queue import MessageJob, MessageQueue
from app.telegram_client import TelegramLimiter, make_bot_client, make_user_client, message_file_size, message_media_type
from app.upload import Uploader
from main import choose_writer_for_destinations, warm_dialog_cache


REPAIR_ACTION = "replace_large_video_album"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Safely replace migrated albums containing large videos")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--min-video-mb", type=int, default=100)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--confirm", action="store_true")
    parser.add_argument(
        "--force-repair",
        action="store_true",
        help="Include albums already recorded as successfully repaired",
    )
    return parser.parse_args()


def candidate_jobs(db: Database, limit: int, *, include_repaired: bool = False) -> list[MessageJob]:
    repaired_filter = "" if include_repaired else """
          AND NOT EXISTS (
              SELECT 1
              FROM repair_actions ra
              WHERE ra.job_id = messages.id
                AND ra.action = ?
                AND ra.outcome = 'replaced'
          )
    """
    params: tuple[Any, ...]
    if include_repaired:
        params = (max(1, int(limit)),)
    else:
        params = (REPAIR_ACTION, max(1, int(limit)))
    rows = db.query(
        f"""
        SELECT * FROM messages
        WHERE status = 'copied'
          AND media_group_id IS NOT NULL
          AND dest_message_ids IS NOT NULL
          {repaired_filter}
        ORDER BY id ASC
        LIMIT ?
        """,
        params,
    )
    jobs = [MessageJob.from_row(row) for row in rows]
    return [job for job in jobs if len(job.source_message_ids) > 1 and job.dest_message_ids]


def video_metadata(message: Message) -> tuple[int, int, int, int]:
    video = getattr(message, "video", None)
    if video is None:
        return (0, 0, 0, 0)
    return (
        int(getattr(video, "file_size", 0) or 0),
        int(getattr(video, "duration", 0) or 0),
        int(getattr(video, "width", 0) or 0),
        int(getattr(video, "height", 0) or 0),
    )


def replacement_matches(source: list[Message], destination: list[Message]) -> tuple[bool, str]:
    if len(source) != len(destination):
        return False, f"item count differs: source={len(source)} destination={len(destination)}"
    for index, (src, dst) in enumerate(zip(source, destination), start=1):
        src_type = message_media_type(src)
        dst_type = message_media_type(dst)
        if src_type != dst_type:
            return False, f"item {index} media type differs: {src_type} != {dst_type}"
        if src_type == "video":
            source_values = video_metadata(src)
            destination_values = video_metadata(dst)
            labels = ("size", "duration", "width", "height")
            for label, source_value, destination_value in zip(labels, source_values, destination_values):
                if source_value and destination_value and source_value != destination_value:
                    return False, f"item {index} {label} differs: {source_value} != {destination_value}"
    return True, "count, media types and video metadata match"


async def delete_messages(client: Client, limiter: TelegramLimiter, chat_id: int | str, ids: list[int]) -> None:
    await limiter.call("delete", client.delete_messages, chat_id=chat_id, message_ids=ids)


async def rollback_replacement(writer: Client, reader: Client, limiter: TelegramLimiter, job: MessageJob, new_ids: list[int]) -> None:
    for client in (writer, reader):
        try:
            await delete_messages(client, limiter, job.dest_chat_id, new_ids)
            return
        except Exception:
            continue


async def replace_job(
    job: MessageJob,
    uploader: Uploader,
    reader: Client,
    writer: Client,
    limiter: TelegramLimiter,
    queue: MessageQueue,
    min_video_bytes: int,
    dry_run: bool,
) -> str:
    source = await uploader._load_source_messages(job)
    source = [message for message in source if uploader._message_should_process(message)]
    large_videos = [
        message for message in source
        if message_media_type(message) == "video" and message_file_size(message) >= min_video_bytes
    ]
    if not large_videos:
        return "skipped: no video meets size threshold"
    if len(source) != len(job.source_message_ids):
        return f"skipped: source album incomplete ({len(source)}/{len(job.source_message_ids)})"
    if dry_run:
        largest = max(message_file_size(message) for message in large_videos)
        return f"matched: {len(source)} items, largest video={largest} bytes"

    queue.delete_media_cache(job.file_unique_key)
    stop_event = asyncio.Event()

    async def phase(_: str) -> None:
        return None

    result = await uploader._download_and_upload(job, source, stop_event, phase)
    new_ids = result.dest_message_ids
    if len(new_ids) != len(source):
        await rollback_replacement(writer, reader, limiter, job, new_ids)
        raise RuntimeError(f"replacement upload incomplete ({len(new_ids)}/{len(source)})")

    fetched = await limiter.call("read", writer.get_messages, job.dest_chat_id, new_ids)
    destination = fetched if isinstance(fetched, list) else [fetched]
    destination = [message for message in destination if message and not getattr(message, "empty", False)]
    verified, detail = replacement_matches(source, destination)
    if not verified:
        await rollback_replacement(writer, reader, limiter, job, new_ids)
        raise RuntimeError(f"replacement verification failed: {detail}")

    delete_error: Exception | None = None
    for client in (writer, reader):
        try:
            await delete_messages(client, limiter, job.dest_chat_id, job.dest_message_ids)
            delete_error = None
            break
        except Exception as exc:
            delete_error = exc
    if delete_error is not None:
        await rollback_replacement(writer, reader, limiter, job, new_ids)
        raise RuntimeError(f"old album deletion failed; replacement rolled back: {delete_error}")

    now = utc_now()
    with queue.db.conn:
        queue.db.conn.execute(
            """
            UPDATE messages
            SET dest_message_ids = ?, verified_at = ?, last_error = NULL, updated_at = ?
            WHERE id = ?
            """,
            (json.dumps(new_ids), now, now, job.id),
        )
    queue.log_repair(
        action=REPAIR_ACTION,
        job=job,
        reason="Previously migrated album contained a large video",
        outcome="replaced",
        details={"old_dest_message_ids": job.dest_message_ids, "new_dest_message_ids": new_ids, "verification": detail},
    )
    return f"replaced: old={job.dest_message_ids} new={new_ids}"


async def run(args: argparse.Namespace) -> None:
    if not args.dry_run and not args.confirm:
        raise SystemExit("Refusing destructive repair without --confirm. Run --dry-run first.")

    config = load_config(args.config)
    config.ensure_directories()
    logger = setup_logging(config.logging)
    limiter = TelegramLimiter(config, logger)
    db = Database(config.queue.db_path)
    db.initialize()
    queue = MessageQueue(db, config)
    jobs = candidate_jobs(db, args.limit, include_repaired=args.force_repair)
    print(f"Candidate album jobs: {len(jobs)}")
    if args.force_repair:
        print("WARNING: --force-repair includes albums already repaired successfully")
    if not jobs:
        print("No unrepaired candidate albums remain.")
        db.close()
        return

    try:
        async with AsyncExitStack() as stack:
            reader = make_user_client(config)
            await stack.enter_async_context(reader)
            await warm_dialog_cache(config, reader, asyncio.Event(), logger)
            bot = make_bot_client(config)
            if bot and config.telegram.use_bot_for_uploads:
                await stack.enter_async_context(bot)
            else:
                bot = None
            initial_writer = bot or reader
            writer, ready = await choose_writer_for_destinations(config, reader, initial_writer, limiter, logger)
            if not ready:
                raise RuntimeError("Destination cannot be resolved safely")
            uploader = Uploader(config, reader, writer, limiter, queue, logger=logger)
            threshold = max(0, int(args.min_video_mb)) * 1024 * 1024
            repaired = 0
            failed = 0
            for job in jobs:
                try:
                    outcome = await replace_job(job, uploader, reader, writer, limiter, queue, threshold, args.dry_run)
                    print(f"job {job.id}: {outcome}")
                    if outcome.startswith("replaced:"):
                        repaired += 1
                except Exception as exc:
                    failed += 1
                    queue.log_repair(
                        action=REPAIR_ACTION,
                        job=job,
                        reason=f"{exc.__class__.__name__}: {exc}",
                        outcome="failed",
                    )
                    print(f"job {job.id}: FAILED: {exc}")
            print(f"Done. repaired={repaired} failed={failed} dry_run={args.dry_run}")
    finally:
        db.close()


def main() -> None:
    asyncio.run(run(parse_args()))


if __name__ == "__main__":
    main()
