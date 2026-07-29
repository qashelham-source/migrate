from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any

from pyrogram.types import Message

from app.control import write_status
from app.errors import PermanentJobError, RetryableJobError
from app.queue import MessageJob
from app.telegram_client import message_media_type
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
        self._install_pause_guard()

    @staticmethod
    def _directory_usage(path: Path) -> tuple[int, int]:
        files = 0
        total_bytes = 0
        if not path.exists():
            return files, total_bytes
        try:
            for item in path.rglob("*"):
                if not item.is_file():
                    continue
                files += 1
                try:
                    total_bytes += item.stat().st_size
                except OSError:
                    pass
        except OSError:
            pass
        return files, total_bytes

    def _audit(self, level: str, event: str, **details: Any) -> None:
        if not self.logger:
            return
        payload = " ".join(f"{key}={value}" for key, value in details.items())
        log_method = getattr(self.logger, level, self.logger.info)
        log_method("%s%s", event, f" {payload}" if payload else "")

    def _install_pause_guard(self) -> None:
        """Prevent automatic resolve checks from clearing a real permission pause."""
        self.queue.db.conn.executescript(
            """
            CREATE TRIGGER IF NOT EXISTS trg_keep_permission_destination_paused
            BEFORE UPDATE OF paused ON destination_health
            WHEN OLD.paused = 1
             AND NEW.paused = 0
             AND EXISTS (
                SELECT 1 FROM messages m
                WHERE m.dest_chat_id = OLD.dest_chat_id
                  AND m.status IN ('failed', 'skipped')
                  AND (
                       LOWER(COALESCE(m.last_error, '')) LIKE '%chatwriteforbidden%'
                    OR LOWER(COALESCE(m.last_error, '')) LIKE '%chat_write_forbidden%'
                    OR LOWER(COALESCE(m.last_error, '')) LIKE '%channelprivate%'
                    OR LOWER(COALESCE(m.last_error, '')) LIKE '%channel_private%'
                    OR LOWER(COALESCE(m.last_error, '')) LIKE '%channelinvalid%'
                    OR LOWER(COALESCE(m.last_error, '')) LIKE '%channel_invalid%'
                    OR LOWER(COALESCE(m.last_error, '')) LIKE '%not enough rights%'
                    OR LOWER(COALESCE(m.last_error, '')) LIKE '%forbidden%'
                  )
             )
            BEGIN
                SELECT RAISE(IGNORE);
            END;
            """
        )
        self.queue.db.conn.commit()

    async def process(
        self,
        job: MessageJob,
        stop_event: asyncio.Event,
        on_phase: Any,
    ) -> UploadResult:
        if self.queue.release3.is_destination_paused(job.dest_chat_id):
            raise RetryableJobError(
                f"Destination {job.dest_chat_id} is paused after an access or permission failure"
            )
        self._active_job = job
        self._meters.clear()
        self._audit(
            "info",
            "RELEASE3_PROCESS_ENTER",
            job=job.id,
            source=job.source_message_id,
            destination=job.dest_chat_id,
            media=job.media_type,
        )
        try:
            try:
                result = await super().process(job, stop_event, on_phase)
                self._audit(
                    "info",
                    "RELEASE3_PROCESS_EXIT",
                    job=job.id,
                    status=result.status,
                    destination_messages=result.dest_message_ids,
                )
                return result
            except PermanentJobError as exc:
                error = f"{exc.__class__.__name__}: {exc}"
                lowered = error.lower()
                if any(
                    marker in lowered
                    for marker in (
                        "chatwriteforbidden",
                        "chat_write_forbidden",
                        "channelprivate",
                        "channel_private",
                        "channelinvalid",
                        "channel_invalid",
                        "not enough rights",
                        "forbidden",
                    )
                ):
                    self.queue.pause_destination(
                        job,
                        "Destination access/permission failed",
                        error,
                    )
                    self.queue.log_repair(
                        action="pause_destination",
                        job=job,
                        reason=error,
                        outcome="paused",
                    )
                raise
        except BaseException as exc:
            self._audit(
                "exception",
                "RELEASE3_PROCESS_ERROR",
                job=job.id,
                error=f"{exc.__class__.__name__}:{exc}",
            )
            raise
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
                message="Server storage is too low to start new media. The job remains in the queue.",
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
        job_dir = self.config.downloads.active_dir / f"job-{job.id}"
        self._audit(
            "info",
            "RELEASE3_CALLSITE_ENTER",
            job=job.id,
            path=job_dir,
            messages=len(messages),
        )
        try:
            result = await super()._download_and_upload(job, messages, stop_event, on_phase)
            self._audit(
                "info",
                "RELEASE3_UPLOAD_ROUTE_FINISHED",
                job=job.id,
                status=result.status,
                destination_messages=result.dest_message_ids,
            )
            return result
        except BaseException as exc:
            self._audit(
                "exception",
                "RELEASE3_CALLSITE_ERROR",
                job=job.id,
                path=job_dir,
                error=f"{exc.__class__.__name__}:{exc}",
            )
            raise
        finally:
            files, total_bytes = self._directory_usage(job_dir)
            self._audit(
                "info" if not job_dir.exists() else "warning",
                "RELEASE3_CALLSITE_EXIT",
                job=job.id,
                path=job_dir,
                exists=job_dir.exists(),
                files=files,
                bytes=total_bytes,
            )

    async def _download_one(self, message: Message, job_dir: Path) -> Path:
        path = job_dir / self._file_name_for(message)
        self._audit(
            "info",
            "RELEASE3_DOWNLOAD_START",
            job=self._active_job.id if self._active_job else None,
            message=message.id,
            path=path,
        )
        try:
            result = await self.limiter.call(
                "download",
                message.download,
                file_name=str(path),
                progress=self._progress,
                progress_args=("downloading",),
            )
        except BaseException as exc:
            self._audit(
                "exception",
                "RELEASE3_DOWNLOAD_FAILED",
                job=self._active_job.id if self._active_job else None,
                message=message.id,
                path=path,
                error=f"{exc.__class__.__name__}:{exc}",
            )
            raise
        final_path = Path(result or path)
        size = final_path.stat().st_size if final_path.exists() else -1
        self._audit(
            "info",
            "RELEASE3_DOWNLOAD_FINISHED",
            job=self._active_job.id if self._active_job else None,
            message=message.id,
            path=final_path,
            bytes=size,
        )
        return final_path

    async def _upload_downloaded(
        self,
        job: MessageJob,
        downloaded: list[tuple[Message, Path]],
    ) -> tuple[UploadResult, list[Message]]:
        self._audit(
            "info",
            "RELEASE3_UPLOAD_START",
            job=job.id,
            files=len(downloaded),
            bytes=sum(path.stat().st_size for _, path in downloaded if path.exists()),
        )
        try:
            result = await super()._upload_downloaded(job, downloaded)
        except BaseException as exc:
            self._audit(
                "exception",
                "RELEASE3_UPLOAD_FAILED",
                job=job.id,
                files=len(downloaded),
                error=f"{exc.__class__.__name__}:{exc}",
            )
            raise
        self._audit("info", "RELEASE3_UPLOAD_FINISHED", job=job.id, files=len(downloaded))
        return result

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

    async def _send_downloaded_item(
        self,
        job: MessageJob,
        message: Message,
        path: Path,
        *,
        caption: str | None,
    ) -> Message:
        media_type = message_media_type(message)
        common = dict(
            chat_id=job.dest_chat_id,
            caption=caption,
            progress=self._progress,
            progress_args=("uploading",),
            **self._destination_kwargs(job),
        )
        if media_type == "photo":
            return await self.limiter.call(
                "upload",
                self.writer.send_photo,
                photo=str(path),
                **common,
            )
        if media_type == "video":
            return await self.limiter.call(
                "upload",
                self.writer.send_video,
                video=str(path),
                supports_streaming=True,
                **common,
            )
        return await self.limiter.call(
            "upload",
            self.writer.send_document,
            document=str(path),
            **common,
        )

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
            message="Downloading restricted media." if stage == "downloading" else "Uploading media.",
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
