from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any

from pyrogram.types import Message

from app.control import write_status
from app.errors import RetryableJobError
from app.queue import MessageJob
from app.telemetry import ProgressMeter, StoragePolicy, storage_snapshot
from app.upload import UploadResult, Uploader


class Release3Uploader(Uploader):
    """Uploader instrumentation that leaves the proven transfer routes intact."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._active_job: MessageJob | None = None
        self._meters: dict[str, ProgressMeter] = {}
        self._last_progress_write = 0.0
        self._storage_policy = StoragePolicy.from_environment()

    async def process(
        self,
        job: MessageJob,
        stop_event: asyncio.Event,
        on_phase: Any,
    ) -> UploadResult:
        self._active_job = job
        self._meters.clear()
        try:
            return await super().process(job, stop_event, on_phase)
        finally:
            self._active_job = None
            self._meters.clear()

    async def _download_and_upload(
        self,
        job: MessageJob,
        messages: list[Message],
        stop_event: asyncio.Event,
        on_phase: Any,
    ) -> UploadResult:
        snapshot = storage_snapshot(self.config.downloads.root, self._storage_policy)
        if not snapshot.has_capacity(job.file_size):
            reason = (
                f"Storage guard blocked job: required={job.file_size or 'unknown'} "
                f"usable={snapshot.usable_bytes} free={snapshot.free_bytes}"
            )
            self.queue.log_repair(
                action="storage_safe_stop",
                job=job,
                reason=reason,
                outcome="deferred",
                details=snapshot.as_status(),
            )
            write_status(
                self.config,
                "storage_guard",
                message="Storage server tidak cukup untuk memulakan media baharu. Job dikekalkan dalam queue.",
                job_id=job.id,
                source_message_id=job.source_message_id,
                **snapshot.as_status(),
            )
            raise RetryableJobError(reason)
        self.queue.log_repair(
            action="download_upload_fallback",
            job=job,
            reason="Native/cached transfer unavailable; using restricted-media download/upload route",
            outcome="started",
            details=snapshot.as_status(),
        )
        return await super()._download_and_upload(job, messages, stop_event, on_phase)

    async def _download_one(self, message: Message, job_dir: Path) -> Path:
        path = job_dir / self._file_name_for(message)
        result = await self.limiter.call(
            "download",
            message.download,
            file_name=str(path),
            progress=self._progress,
            progress_args=("downloading",),
        )
        return Path(result or path)

    async def _upload_downloaded_individually(
        self,
        job: MessageJob,
        downloaded: list[tuple[Message, Path]],
    ) -> list[Message]:
        self.queue.log_repair(
            action="album_individual_fallback",
            job=job,
            reason="Telegram returned MEDIA_EMPTY for media group",
            outcome="started",
            details={"items": len(downloaded)},
        )
        return await super()._upload_downloaded_individually(job, downloaded)

    async def _progress(self, current: int, total: int, stage: str) -> None:
        job = self._active_job
        if job is None:
            return
        meter = self._meters.get(stage)
        if meter is None or (meter.total and total and meter.total != int(total)):
            meter = ProgressMeter(total=int(total) if total else None)
            self._meters[stage] = meter
        values = meter.update(int(current), int(total) if total else None)
        now = time.monotonic()
        if now - self._last_progress_write < 1.0 and current < total:
            return
        self._last_progress_write = now
        self.queue.update_telemetry(
            job.id,
            stage=stage,
            bytes_processed=int(values["bytes_processed"] or 0),
            bytes_total=int(values["bytes_total"] or 0) or None,
            speed_bps=float(values["speed_bps"] or 0.0),
            eta_seconds=float(values["eta_seconds"]) if values["eta_seconds"] is not None else None,
        )
        snapshot = storage_snapshot(self.config.downloads.root, self._storage_policy)
        write_status(
            self.config,
            stage,
            message="Sedang download media restricted." if stage == "downloading" else "Sedang upload media.",
            job_id=job.id,
            source_chat=job.source_chat_id,
            destination_chat=job.dest_chat_id,
            source_message_id=job.source_message_id,
            media_type=job.media_type,
            bytes_processed=values["bytes_processed"],
            bytes_total=values["bytes_total"],
            speed_bps=values["speed_bps"],
            media_eta_seconds=values["eta_seconds"],
            **snapshot.as_status(),
        )
