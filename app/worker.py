from __future__ import annotations

import asyncio
from typing import Any

from pyrogram import Client
from pyrogram.errors import BadRequest, ChannelInvalid, ChannelPrivate, ChatWriteForbidden, MediaEmpty

from app.config import AppConfig
from app.control import write_status
from app.errors import PermanentJobError, RetryableJobError, compact_error
from app.queue import MessageJob, MessageQueue
from app.telegram_client import (
    TelegramLimiter,
    message_caption,
    message_file_size,
    message_is_empty,
    message_media_type,
)
from app.telemetry import StoragePolicy, storage_snapshot
from app.upload import Uploader


class Worker:
    def __init__(
        self,
        config: AppConfig,
        queue: MessageQueue,
        uploader: Uploader,
        logger: Any | None = None,
    ) -> None:
        self.config = config
        self.queue = queue
        self.uploader = uploader
        self.logger = logger
        self.storage_policy = StoragePolicy.from_environment()

    def _status_details(self, job: MessageJob | None = None, **extra: Any) -> dict[str, Any]:
        counts = self.queue.counts_by_status()
        details: dict[str, Any] = {
            "pending": counts.get("pending", 0),
            "downloading": counts.get("downloading", 0),
            "uploading": counts.get("uploading", 0),
            "copied": counts.get("copied", 0),
            "failed": counts.get("failed", 0),
            "skipped": counts.get("skipped", 0),
        }
        try:
            details.update(storage_snapshot(self.config.downloads.root, self.storage_policy).as_status())
        except OSError:
            pass
        if job is not None:
            details.update(
                {
                    "job_id": job.id,
                    "source_chat": job.source_chat_id,
                    "destination_chat": job.dest_chat_id,
                    "source_message_id": job.source_message_id,
                    "media_type": job.media_type,
                    "media_size_bytes": job.file_size,
                }
            )
            details.update(self.queue.telemetry_for_job(job.id))
        details.update(extra)
        return details

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            jobs = self.queue.claim_due(self.config.batch.size)
            if not jobs:
                if self.logger:
                    self.logger.info("No due pending jobs")
                write_status(
                    self.config,
                    "idle",
                    message="Tiada job pending yang boleh dijalankan. Migration sedang idle.",
                    **self._status_details(),
                )
                return

            for index, job in enumerate(jobs, start=1):
                if stop_event.is_set():
                    break
                await self._process_one(
                    job,
                    stop_event,
                    batch_index=index,
                    batch_total=len(jobs),
                )

            if stop_event.is_set():
                break
            if len(jobs) >= self.config.batch.size:
                pause = self.config.batch.pause_between_batches_seconds
                if self.logger:
                    self.logger.info("Batch complete; pausing %ss before next batch", pause)
                write_status(
                    self.config,
                    "batch_pause",
                    message=f"Batch selesai. Rehat {pause} saat sebelum sambung.",
                    pause_seconds=pause,
                    **self._status_details(),
                )
                await sleep_or_stop(stop_event, pause)
            else:
                write_status(
                    self.config,
                    "idle",
                    message="Semua job untuk cycle ini selesai.",
                    **self._status_details(),
                )
                return

        if self.logger:
            self.logger.info("Stopping after current safe operation")
        write_status(
            self.config,
            "stopped",
            message="Migration dihentikan dengan selamat. Job belum selesai kekal dalam queue.",
            **self._status_details(),
        )

    async def _process_one(
        self,
        job: MessageJob,
        stop_event: asyncio.Event,
        *,
        batch_index: int,
        batch_total: int,
    ) -> None:
        attempts = job.attempts
        if self.logger:
            self.logger.info("Processing job %s attempt %s", job.id, attempts)

        common = {
            "batch_index": batch_index,
            "batch_total": batch_total,
            "attempt": attempts,
        }
        write_status(
            self.config,
            "processing",
            message="Menyediakan job migration.",
            **self._status_details(job, **common),
        )

        async def set_phase(status: str) -> None:
            self.queue.set_phase(job.id, status)
            message = {
                "downloading": "Sedang download media restricted.",
                "uploading": "Sedang hantar ke destination.",
            }.get(status, "Sedang proses job.")
            write_status(
                self.config,
                status,
                message=message,
                **self._status_details(job, **common),
            )

        try:
            result = await self.uploader.process(job, stop_event, set_phase)
            if result.status == "copied":
                self.queue.mark_copied(job.id, result.dest_message_ids)
                if self.logger:
                    self.logger.info("Job %s copied to %s", job.id, result.dest_message_ids or "destination")
                write_status(
                    self.config,
                    "processing",
                    message=f"Job #{job.id} selesai dan menunggu verification.",
                    last_result="copied",
                    **self._status_details(job, **common),
                )
            elif result.status == "skipped":
                self.queue.mark_skipped(job.id, result.reason)
                if self.logger:
                    self.logger.info("Job %s skipped: %s", job.id, result.reason)
                write_status(
                    self.config,
                    "processing",
                    message=f"Job #{job.id} diskip.",
                    last_result="skipped",
                    last_error=result.reason,
                    **self._status_details(job, **common),
                )
            else:
                raise RuntimeError(f"Unknown upload result status: {result.status}")
        except PermanentJobError as exc:
            self.queue.mark_skipped(job.id, compact_error(exc))
            self.queue.log_repair(
                action="permanent_skip",
                job=job,
                reason=compact_error(exc),
                outcome="skipped",
            )
            if self.logger:
                self.logger.warning("Job %s skipped permanently: %s", job.id, exc)
            write_status(
                self.config,
                "processing",
                message=f"Job #{job.id} diskip kerana ralat kekal.",
                last_result="skipped",
                last_error=compact_error(exc),
                **self._status_details(job, **common),
            )
        except RetryableJobError as exc:
            if stop_event.is_set():
                self.queue.set_phase(job.id, "pending")
                status = "pending"
                message = f"Job #{job.id} dikembalikan ke pending."
            else:
                status = self.queue.mark_failure(job, compact_error(exc), attempts)
                message = f"Job #{job.id} akan dicuba semula."
            self.queue.log_repair(
                action="retry_with_backoff",
                job=job,
                reason=compact_error(exc),
                outcome=status,
                details={"attempt": attempts},
            )
            if self.logger:
                self.logger.warning("Job %s %s after retryable failure: %s", job.id, status, exc)
            write_status(
                self.config,
                "stopping" if stop_event.is_set() else "processing",
                message=message,
                last_result=status,
                last_error=compact_error(exc),
                **self._status_details(job, **common),
            )
        except (ChannelPrivate, ChannelInvalid, ChatWriteForbidden) as exc:
            error = compact_error(exc)
            self.queue.pause_destination(job, "Destination access/permission failed", error)
            self.queue.mark_skipped(job.id, error)
            self.queue.log_repair(
                action="pause_destination",
                job=job,
                reason=error,
                outcome="paused",
            )
            if self.logger:
                self.logger.warning("Destination %s paused by Telegram error: %s", job.dest_chat_id, exc)
            write_status(
                self.config,
                "processing",
                message="Destination bermasalah dipause; destination lain akan diteruskan.",
                last_result="destination_paused",
                last_error=error,
                paused_destination=job.dest_chat_id,
                **self._status_details(job, **common),
            )
        except (MediaEmpty, BadRequest) as exc:
            error = compact_error(exc)
            status = self.queue.mark_failure(job, error, attempts)
            self.queue.log_repair(
                action="telegram_media_retry",
                job=job,
                reason=error,
                outcome=status,
                details={"attempt": attempts},
            )
            if self.logger:
                self.logger.warning("Job %s %s after media error: %s", job.id, status, exc)
            write_status(
                self.config,
                "processing",
                message=f"Job #{job.id} mengalami ralat media dan akan melalui recovery.",
                last_result=status,
                last_error=error,
                **self._status_details(job, **common),
            )
        except Exception as exc:
            error = compact_error(exc)
            status = self.queue.mark_failure(job, error, attempts)
            self.queue.log_repair(
                action="automatic_retry",
                job=job,
                reason=error,
                outcome=status,
                details={"attempt": attempts},
            )
            if self.logger:
                self.logger.exception("Job %s %s after failure", job.id, status)
            write_status(
                self.config,
                "processing",
                message=f"Job #{job.id} gagal dan recovery direkodkan.",
                last_result=status,
                last_error=error,
                **self._status_details(job, **common),
            )


class Verifier:
    def __init__(
        self,
        config: AppConfig,
        queue: MessageQueue,
        reader: Client,
        destination_client: Client,
        limiter: TelegramLimiter,
        logger: Any | None = None,
    ) -> None:
        self.config = config
        self.queue = queue
        self.reader = reader
        self.destination_client = destination_client
        self.limiter = limiter
        self.logger = logger

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            jobs = self.queue.fetch_for_verification(self.config.batch.size)
            if not jobs:
                if self.logger:
                    self.logger.info("No copied jobs need verification")
                return
            for job in jobs:
                if stop_event.is_set():
                    break
                try:
                    await self._verify_one(job)
                except Exception as exc:
                    self.queue.log_repair(
                        action="verification_retry",
                        job=job,
                        reason=compact_error(exc),
                        outcome="pending",
                    )
                    if self.logger:
                        self.logger.warning("Verification deferred for job %s: %s", job.id, exc)

            if len(jobs) < self.config.batch.size:
                return
            await sleep_or_stop(stop_event, self.config.batch.pause_between_batches_seconds)

    async def _verify_one(self, job: MessageJob) -> None:
        source_result = await self.limiter.call(
            "verify",
            self.reader.get_messages,
            job.source_chat_id,
            job.source_message_ids,
        )
        destination_result = await self.limiter.call(
            "verify",
            self.destination_client.get_messages,
            job.dest_chat_id,
            job.dest_message_ids,
        )
        source_messages = source_result if isinstance(source_result, list) else [source_result]
        destination_messages = destination_result if isinstance(destination_result, list) else [destination_result]

        expected_count = len(job.source_message_ids)
        present_count = sum(1 for message in destination_messages if not message_is_empty(message))
        missing_indexes = [
            index
            for index in range(expected_count)
            if index >= len(destination_messages) or message_is_empty(destination_messages[index])
        ]
        missing_source_ids = [
            job.source_message_ids[index]
            for index in missing_indexes
            if index < len(job.source_message_ids)
        ]

        comparable_pairs = [
            (source_messages[index], destination_messages[index])
            for index in range(min(len(source_messages), len(destination_messages)))
            if not message_is_empty(source_messages[index]) and not message_is_empty(destination_messages[index])
        ]
        media_match = bool(comparable_pairs) and all(
            message_media_type(source) == message_media_type(destination)
            for source, destination in comparable_pairs
        )
        size_match = bool(comparable_pairs) and all(
            self._size_matches(message_file_size(source), message_file_size(destination))
            for source, destination in comparable_pairs
        )
        expected_caption = "" if self.config.transfer.drop_caption else self._first_caption(source_messages)
        actual_caption = self._first_caption(destination_messages)
        caption_match = self._normalize_caption(expected_caption) == self._normalize_caption(actual_caption)

        details = {
            "source_message_ids": job.source_message_ids,
            "dest_message_ids": job.dest_message_ids,
            "missing_indexes": missing_indexes,
            "missing_source_message_ids": missing_source_ids,
            "source_media_types": [
                message_media_type(message) for message in source_messages if not message_is_empty(message)
            ],
            "destination_media_types": [
                message_media_type(message) for message in destination_messages if not message_is_empty(message)
            ],
        }

        complete = (
            present_count == expected_count
            and not missing_source_ids
            and media_match
            and size_match
            and caption_match
        )
        if complete:
            self.queue.record_verification(
                job_id=job.id,
                status="verified",
                expected_count=expected_count,
                present_count=present_count,
                media_match=True,
                caption_match=True,
                size_match=True,
                details=details,
            )
            self.queue.mark_verified(job.id)
            if self.logger:
                self.logger.info("Strong verification passed for job %s", job.id)
            return

        if missing_source_ids:
            repair_jobs = [
                self.queue.enqueue_repair_item(job, source_message_id)
                for source_message_id in missing_source_ids
            ]
            self.queue.record_verification(
                job_id=job.id,
                status="repairing",
                expected_count=expected_count,
                present_count=present_count,
                media_match=media_match,
                caption_match=caption_match,
                size_match=size_match,
                missing_source_message_ids=missing_source_ids,
                details={**details, "repair_job_ids": [value for value in repair_jobs if value]},
            )
            if self.logger:
                self.logger.warning(
                    "Verification queued item-only repair for job %s: missing source ids=%s",
                    job.id,
                    missing_source_ids,
                )
            return

        self.queue.record_verification(
            job_id=job.id,
            status="failed",
            expected_count=expected_count,
            present_count=present_count,
            media_match=media_match,
            caption_match=caption_match,
            size_match=size_match,
            details=details,
        )
        self.queue.log_repair(
            action="verification_mismatch",
            job=job,
            reason="Destination media failed strict verification",
            outcome="issue",
            details={
                "media_match": media_match,
                "caption_match": caption_match,
                "size_match": size_match,
                **details,
            },
        )
        self.queue.recompute_source_state(job.source_chat_id)
        if self.logger:
            self.logger.warning(
                "Strong verification failed for job %s: media=%s caption=%s size=%s",
                job.id,
                media_match,
                caption_match,
                size_match,
            )

    @staticmethod
    def _normalize_caption(value: str | None) -> str:
        return "\n".join(line.rstrip() for line in str(value or "").strip().splitlines())

    @staticmethod
    def _first_caption(messages: list[Any]) -> str:
        for message in messages:
            if not message_is_empty(message):
                return message_caption(message) or ""
        return ""

    @staticmethod
    def _size_matches(source_size: int | None, destination_size: int | None) -> bool:
        if not source_size and not destination_size:
            return True
        if not source_size or not destination_size:
            return False
        tolerance = max(1024, int(source_size * 0.01))
        return abs(int(source_size) - int(destination_size)) <= tolerance


async def sleep_or_stop(stop_event: asyncio.Event, seconds: int | float) -> None:
    if seconds <= 0:
        return
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        return
