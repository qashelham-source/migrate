from __future__ import annotations

import asyncio
import errno
from contextlib import suppress
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


JOB_HEARTBEAT_SECONDS = 30


class Worker:
    def __init__(
        self,
        config: AppConfig,
        queue: MessageQueue,
        uploader: Uploader,
        logger: Any | None = None,
        source_chat_id: int | str | None = None,
        source_index: int | None = None,
        source_total: int | None = None,
        source_label: str | None = None,
    ) -> None:
        self.config = config
        self.queue = queue
        self.uploader = uploader
        self.logger = logger
        self.source_chat_id = source_chat_id
        self.source_index = source_index
        self.source_total = source_total
        self.source_label = source_label
        self.storage_policy = StoragePolicy.from_environment()

    @staticmethod
    def _is_transport_interruption(exc: OSError) -> bool:
        """Return whether an OS error represents a broken network connection."""
        return isinstance(exc, (ConnectionError, TimeoutError)) or getattr(exc, "errno", None) in {
            errno.EPIPE,
            errno.ECONNABORTED,
            errno.ECONNRESET,
            errno.ETIMEDOUT,
            errno.ENETDOWN,
            errno.ENETUNREACH,
            errno.EHOSTUNREACH,
            errno.ECONNREFUSED,
        }

    def _status_details(self, job: MessageJob | None = None, **extra: Any) -> dict[str, Any]:
        counts = self.queue.counts_by_status(self.source_chat_id)
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
        if self.source_index is not None and self.source_total is not None:
            details.update(
                {
                    "source_index": self.source_index,
                    "source_total": self.source_total,
                    "source": self.source_label or self.source_chat_id,
                }
            )
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
            jobs = self.queue.claim_due(self.config.batch.size, self.source_chat_id)
            if not jobs:
                if self.logger:
                    self.logger.info("No due pending jobs")
                write_status(
                    self.config,
                    "idle",
                    message="No runnable pending jobs. Migration is idle.",
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
                if not self.queue.fetch_due(1, self.source_chat_id):
                    write_status(
                        self.config,
                        "idle",
                        message="All runnable jobs for this source are complete.",
                        **self._status_details(),
                    )
                    return
                pause = self.config.batch.pause_between_batches_seconds
                if self.logger:
                    self.logger.info("Batch complete; pausing %ss before next batch", pause)
                write_status(
                    self.config,
                    "batch_pause",
                    message=f"Batch complete. Pausing for {pause} seconds before continuing.",
                    pause_seconds=pause,
                    **self._status_details(),
                )
                await sleep_or_stop(stop_event, pause)
            else:
                write_status(
                    self.config,
                    "idle",
                    message="All jobs for this cycle are complete.",
                    **self._status_details(),
                )
                return

        if self.logger:
            self.logger.info("Stopping after current safe operation")
        write_status(
            self.config,
            "stopped",
            message="Migration stopped safely. Unfinished jobs remain in the queue.",
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
        phase = job.status if job.status in {"downloading", "uploading"} else "downloading"
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
            message="Preparing migration job.",
            **self._status_details(job, **common),
        )

        async def set_phase(status: str) -> None:
            nonlocal phase
            phase = status
            self.queue.set_phase(job.id, status)
            message = {
                "downloading": "Downloading restricted media.",
                "uploading": "Uploading to the destination.",
            }.get(status, "Processing job.")
            write_status(
                self.config,
                status,
                message=message,
                **self._status_details(job, **common),
            )

        async def heartbeat() -> None:
            while True:
                await asyncio.sleep(JOB_HEARTBEAT_SECONDS)
                if not self.queue.touch_active_job(job.id):
                    return
                message = {
                    "downloading": "Downloading restricted media.",
                    "uploading": "Uploading to the destination.",
                }.get(phase, "Processing migration job.")
                write_status(
                    self.config,
                    phase,
                    message=message,
                    **self._status_details(job, **common),
                )

        heartbeat_task = asyncio.create_task(heartbeat())
        try:
            result = await self.uploader.process(job, stop_event, set_phase)
            if result.status == "copied":
                self.queue.mark_copied(job.id, result.dest_message_ids)
                if self.logger:
                    self.logger.info("Job %s copied to %s", job.id, result.dest_message_ids or "destination")
                write_status(
                    self.config,
                    "processing",
                    message=f"Job #{job.id} is complete and awaiting verification.",
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
                    message=f"Job #{job.id} was skipped.",
                    last_result="skipped",
                    last_error=result.reason,
                    **self._status_details(job, **common),
                )
            else:
                raise RuntimeError(f"Unknown upload result status: {result.status}")
        except PermanentJobError as exc:
            error = compact_error(exc)
            if self.queue.is_repair_job(job):
                self.queue.mark_skipped(job.id, error)
                outcome = "skipped"
            else:
                error = self.queue.cancel_job(job, error)
                outcome = "cancelled"
            self.queue.log_repair(
                action="permanent_skip",
                job=job,
                reason=error,
                outcome=outcome,
            )
            if self.logger:
                self.logger.warning("Job %s %s after permanent error: %s", job.id, outcome, exc)
            write_status(
                self.config,
                "processing",
                message=f"Job #{job.id} was {outcome} after a permanent error.",
                last_result=outcome,
                last_error=error,
                **self._status_details(job, **common),
            )
        except RetryableJobError as exc:
            if stop_event.is_set():
                self.queue.set_phase(job.id, "pending")
                status = "pending"
                message = f"Job #{job.id} was returned to pending."
            else:
                status = self.queue.mark_failure(job, compact_error(exc), attempts)
                message = (
                    f"Job #{job.id} was cancelled after its final failed attempt."
                    if status == "cancelled"
                    else f"Job #{job.id} will be retried."
                )
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
            if self.queue.is_repair_job(job):
                self.queue.mark_skipped(job.id, error)
            else:
                error = self.queue.cancel_job(job, error)
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
                message="This destination is paused; other destinations will continue.",
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
                message=(
                    f"Job #{job.id} was cancelled after its final failed attempt."
                    if status == "cancelled"
                    else f"Job #{job.id} had a media error and will go through recovery."
                ),
                last_result=status,
                last_error=error,
                **self._status_details(job, **common),
            )
        except OSError as exc:
            error = compact_error(exc)
            if self._is_transport_interruption(exc):
                if phase == "uploading":
                    status = "failed"
                    reason = self.queue.hold_uncertain_upload(job, error)
                    action = "hold_uncertain_upload"
                    message = (
                        f"Job #{job.id} lost its upload connection and was held to prevent a duplicate."
                    )
                    status_phase = "blocked"
                elif stop_event.is_set():
                    status = "pending"
                    reason = error
                    self.queue.set_phase(job.id, "pending")
                    action = "stop_during_download"
                    message = f"Job #{job.id} was returned to pending."
                    status_phase = "stopping"
                else:
                    status = "pending"
                    reason = self.queue.defer_download_transport_failure(job, error, attempts)
                    action = "retry_download_transport"
                    message = f"Job #{job.id} lost its download connection and will retry automatically."
                    status_phase = "waiting_retry"
                self.queue.log_repair(
                    action=action,
                    job=job,
                    reason=reason,
                    outcome=status,
                    details={"attempt": attempts, "phase": phase},
                )
                if self.logger:
                    self.logger.warning(
                        "Job %s %s after transport interruption during %s: %s",
                        job.id,
                        status,
                        phase,
                        exc,
                    )
                write_status(
                    self.config,
                    status_phase,
                    message=message,
                    last_result=status,
                    last_error=reason,
                    **self._status_details(job, **common),
                )
                return

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
                message=(
                    f"Job #{job.id} was cancelled after its final failed attempt."
                    if status == "cancelled"
                    else f"Job #{job.id} failed and recovery was recorded."
                ),
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
                message=(
                    f"Job #{job.id} was cancelled after its final failed attempt."
                    if status == "cancelled"
                    else f"Job #{job.id} failed and recovery was recorded."
                ),
                last_result=status,
                last_error=error,
                **self._status_details(job, **common),
            )
        finally:
            heartbeat_task.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat_task


class Verifier:
    def __init__(
        self,
        config: AppConfig,
        queue: MessageQueue,
        reader: Client,
        destination_client: Client,
        limiter: TelegramLimiter,
        logger: Any | None = None,
        source_chat_id: int | str | None = None,
    ) -> None:
        self.config = config
        self.queue = queue
        self.reader = reader
        self.destination_client = destination_client
        self.limiter = limiter
        self.logger = logger
        self.source_chat_id = source_chat_id

    def _status_details(self, job: MessageJob) -> dict[str, Any]:
        counts = self.queue.counts_by_status(self.source_chat_id)
        return {
            "pending": counts.get("pending", 0),
            "downloading": counts.get("downloading", 0),
            "uploading": counts.get("uploading", 0),
            "copied": counts.get("copied", 0),
            "failed": counts.get("failed", 0),
            "skipped": counts.get("skipped", 0),
            "job_id": job.id,
            "source_chat": job.source_chat_id,
            "destination_chat": job.dest_chat_id,
            "source_message_id": job.source_message_id,
            "media_type": job.media_type,
            "media_size_bytes": job.file_size,
        }

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            jobs = self.queue.fetch_for_verification(
                self.config.batch.size,
                self.source_chat_id,
            )
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

    async def _verification_heartbeat(self, job: MessageJob) -> None:
        while True:
            await asyncio.sleep(JOB_HEARTBEAT_SECONDS)
            if not self.queue.touch_active_job(job.id):
                return
            write_status(
                self.config,
                "verifying",
                message="Verifying destination media.",
                **self._status_details(job),
            )

    async def _verify_one(self, job: MessageJob) -> None:
        self.queue.begin_verification(job.id)
        write_status(
            self.config,
            "verifying",
            message="Verifying destination media.",
            **self._status_details(job),
        )
        heartbeat_task = asyncio.create_task(self._verification_heartbeat(job))
        try:
            await self._verify_one_inner(job)
        finally:
            heartbeat_task.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat_task
            self.queue.clear_activity_phase(job.id)

    async def _verify_one_inner(self, job: MessageJob) -> None:
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
