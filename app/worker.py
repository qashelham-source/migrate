from __future__ import annotations

import asyncio
from typing import Any

from pyrogram import Client
from pyrogram.errors import BadRequest, ChannelInvalid, ChannelPrivate, ChatWriteForbidden, MediaEmpty

from app.config import AppConfig
from app.control import write_status
from app.errors import PermanentJobError, RetryableJobError, compact_error
from app.queue import MessageJob, MessageQueue
from app.telegram_client import TelegramLimiter, message_is_empty
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
        if job is not None:
            details.update(
                {
                    "job_id": job.id,
                    "source_chat": job.source_chat_id,
                    "destination_chat": job.dest_chat_id,
                    "source_message_id": job.source_message_id,
                    "media_type": job.media_type,
                }
            )
        details.update(extra)
        return details

    async def run(self, stop_event: asyncio.Event) -> None:
        recovered = self.queue.recover_in_progress()
        if recovered and self.logger:
            self.logger.info("Recovered %s interrupted jobs back to pending", recovered)

        while not stop_event.is_set():
            jobs = self.queue.fetch_due(self.config.batch.size)
            if not jobs:
                if self.logger:
                    self.logger.info("No due pending jobs")
                write_status(
                    self.config,
                    "idle",
                    message="Tiada job pending. Migration sedang idle.",
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
        attempts = self.queue.start_attempt(job)
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
                    message=f"Job #{job.id} selesai.",
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
                if self.logger:
                    self.logger.info("Job %s returned to pending after stop request", job.id)
                write_status(
                    self.config,
                    "stopping",
                    message=f"Job #{job.id} dikembalikan ke pending.",
                    last_result="pending",
                    **self._status_details(job, **common),
                )
            else:
                status = self.queue.mark_failure(job, compact_error(exc), attempts)
                if self.logger:
                    self.logger.warning("Job %s %s after retryable failure: %s", job.id, status, exc)
                write_status(
                    self.config,
                    "processing",
                    message=f"Job #{job.id} akan dicuba semula.",
                    last_result=status,
                    last_error=compact_error(exc),
                    **self._status_details(job, **common),
                )
        except (ChannelPrivate, ChannelInvalid, ChatWriteForbidden, MediaEmpty, BadRequest) as exc:
            self.queue.mark_skipped(job.id, compact_error(exc))
            if self.logger:
                self.logger.warning("Job %s skipped by Telegram error: %s", job.id, exc)
            write_status(
                self.config,
                "processing",
                message=f"Job #{job.id} diskip kerana ralat Telegram.",
                last_result="skipped",
                last_error=compact_error(exc),
                **self._status_details(job, **common),
            )
        except Exception as exc:
            status = self.queue.mark_failure(job, compact_error(exc), attempts)
            if self.logger:
                self.logger.exception("Job %s %s after failure", job.id, status)
            write_status(
                self.config,
                "processing",
                message=f"Job #{job.id} gagal dan direkodkan.",
                last_result=status,
                last_error=compact_error(exc),
                **self._status_details(job, **common),
            )


class Verifier:
    def __init__(
        self,
        config: AppConfig,
        queue: MessageQueue,
        client: Client,
        limiter: TelegramLimiter,
        logger: Any | None = None,
    ) -> None:
        self.config = config
        self.queue = queue
        self.client = client
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
                await self._verify_one(job)

            if len(jobs) < self.config.batch.size:
                return
            await sleep_or_stop(stop_event, self.config.batch.pause_between_batches_seconds)

    async def _verify_one(self, job: MessageJob) -> None:
        result = await self.limiter.call("verify", self.client.get_messages, job.dest_chat_id, job.dest_message_ids)
        messages = result if isinstance(result, list) else [result]
        present = [msg for msg in messages if not message_is_empty(msg)]
        if len(present) == len(job.dest_message_ids):
            self.queue.mark_verified(job.id)
            if self.logger:
                self.logger.info("Verified job %s", job.id)
        elif self.logger:
            self.logger.warning(
                "Verification incomplete for job %s: expected=%s present=%s",
                job.id,
                len(job.dest_message_ids),
                len(present),
            )


async def sleep_or_stop(stop_event: asyncio.Event, seconds: int | float) -> None:
    if seconds <= 0:
        return
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        return
