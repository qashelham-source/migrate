from __future__ import annotations

import asyncio
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable

from pyrogram import Client
from pyrogram.errors import (
    BadRequest,
    ChannelInvalid,
    ChannelPrivate,
    ChatForwardsRestricted,
    ChatWriteForbidden,
    MediaEmpty,
)
from pyrogram.types import InputMediaDocument, InputMediaPhoto, InputMediaVideo, Message

from app.config import AppConfig
from app.errors import PermanentJobError, RetryableJobError
from app.queue import MessageJob
from app.telegram_client import TelegramLimiter, message_file_size, message_is_empty, message_media_type


PhaseCallback = Callable[[str], Awaitable[None]]


@dataclass(frozen=True)
class UploadResult:
    status: str
    dest_message_ids: list[int] = field(default_factory=list)
    reason: str = ""


class Uploader:
    def __init__(
        self,
        config: AppConfig,
        reader: Client,
        writer: Client,
        limiter: TelegramLimiter,
        logger: Any | None = None,
    ) -> None:
        self.config = config
        self.reader = reader
        self.writer = writer
        self.limiter = limiter
        self.logger = logger

    async def process(
        self,
        job: MessageJob,
        stop_event: asyncio.Event,
        on_phase: PhaseCallback,
    ) -> UploadResult:
        messages = await self._load_source_messages(job)
        messages = [msg for msg in messages if self._message_should_process(msg)]

        if not messages:
            return UploadResult(status="skipped", reason="Source messages missing or filtered out")

        text_only = all(message_media_type(message) == "text" for message in messages)

        if self._should_use_native_copy(job):
            try:
                await on_phase("uploading")
                return await self._copy_or_forward(job, messages)
            except ChatForwardsRestricted as exc:
                if self.config.transfer.forwarding_only:
                    return UploadResult(status="skipped", reason=f"Forward/copy restricted: {exc}")
                if self.logger:
                    self.logger.warning("Native copy failed for job %s; falling back to download/upload", job.id)
            except (ChannelPrivate, ChannelInvalid, ChatWriteForbidden, MediaEmpty) as exc:
                raise PermanentJobError(str(exc)) from exc
            except BadRequest as exc:
                if self.config.transfer.forwarding_only:
                    raise PermanentJobError(str(exc)) from exc
                if self.logger:
                    self.logger.warning("Native copy failed for job %s; falling back: %s", job.id, exc)

        if self.config.transfer.forwarding_only:
            return UploadResult(status="skipped", reason="forwarding_only is enabled and native copy was unavailable")

        if text_only:
            await on_phase("uploading")
            return await self._send_text(job, messages[0])

        return await self._download_and_upload(job, messages, stop_event, on_phase)

    async def _send_text(self, job: MessageJob, message: Message) -> UploadResult:
        text = message.text or message.caption or ""
        if not text:
            return UploadResult(status="skipped", reason="Text message was empty")
        result = await self.limiter.call(
            "upload",
            self.writer.send_message,
            chat_id=job.dest_chat_id,
            text=text,
            entities=message.entities or message.caption_entities,
            **self._destination_kwargs(job),
        )
        return UploadResult(status="copied", dest_message_ids=self._result_message_ids(result))

    async def _load_source_messages(self, job: MessageJob) -> list[Message]:
        result = await self.limiter.call(
            "read",
            self.reader.get_messages,
            job.source_chat_id,
            job.source_message_ids,
        )
        if not isinstance(result, list):
            result = [result]
        return [msg for msg in result if not message_is_empty(msg)]

    def _should_use_native_copy(self, job: MessageJob) -> bool:
        if not self.config.transfer.prefer_copy:
            return False
        if self.writer is not self.reader:
            return False
        if not self.config.transfer.hide_sender and job.dest_topic_id:
            return False
        return True

    async def _copy_or_forward(self, job: MessageJob, messages: list[Message]) -> UploadResult:
        kwargs = self._destination_kwargs(job)
        first = messages[0]

        if self.config.transfer.hide_sender:
            if len(messages) > 1 and first.media_group_id:
                result = await self.limiter.call(
                    "copy",
                    self.writer.copy_media_group,
                    chat_id=job.dest_chat_id,
                    from_chat_id=job.source_chat_id,
                    message_id=first.id,
                    captions="" if self.config.transfer.drop_caption else None,
                    **kwargs,
                )
            else:
                result = await self.limiter.call(
                    "copy",
                    self.writer.copy_message,
                    chat_id=job.dest_chat_id,
                    from_chat_id=job.source_chat_id,
                    message_id=first.id,
                    caption="" if self.config.transfer.drop_caption else None,
                    **kwargs,
                )
        else:
            result = await self.limiter.call(
                "copy",
                self.writer.forward_messages,
                chat_id=job.dest_chat_id,
                from_chat_id=job.source_chat_id,
                message_ids=[msg.id for msg in messages],
            )

        return UploadResult(status="copied", dest_message_ids=self._result_message_ids(result))

    async def _download_and_upload(
        self,
        job: MessageJob,
        messages: list[Message],
        stop_event: asyncio.Event,
        on_phase: PhaseCallback,
    ) -> UploadResult:
        job_dir = self.config.downloads.active_dir / f"job-{job.id}"
        job_dir.mkdir(parents=True, exist_ok=True)
        downloaded: list[tuple[Message, Path]] = []
        success = False

        try:
            await on_phase("downloading")
            for message in messages:
                self._validate_bot_upload_size(message)
                path = await self._download_one(message, job_dir)
                downloaded.append((message, path))

            if stop_event.is_set():
                raise RetryableJobError("Stop requested after download; leaving job for retry")

            await on_phase("uploading")
            result = await self._upload_downloaded(job, downloaded)
            success = True
            return result
        finally:
            self._cleanup_job_dir(job_dir, success)

    async def _download_one(self, message: Message, job_dir: Path) -> Path:
        path = job_dir / self._file_name_for(message)
        result = await self.limiter.call("download", message.download, file_name=str(path))
        return Path(result or path)

    async def _upload_downloaded(
        self,
        job: MessageJob,
        downloaded: list[tuple[Message, Path]],
    ) -> UploadResult:
        kwargs = self._destination_kwargs(job)

        if len(downloaded) > 1:
            media_group = []
            caption_used = False
            for message, path in downloaded:
                caption = self._caption_for(message) if not caption_used else None
                caption_entities = message.caption_entities if caption else None
                caption_used = caption_used or bool(caption)
                media_type = message_media_type(message)

                if media_type == "photo":
                    media_group.append(InputMediaPhoto(str(path), caption=caption, caption_entities=caption_entities))
                elif media_type == "video":
                    media_group.append(
                        InputMediaVideo(
                            str(path),
                            caption=caption,
                            caption_entities=caption_entities,
                            supports_streaming=True,
                        )
                    )
                else:
                    media_group.append(InputMediaDocument(str(path), caption=caption, caption_entities=caption_entities))

            result = await self.limiter.call(
                "upload",
                self.writer.send_media_group,
                chat_id=job.dest_chat_id,
                media=media_group,
                **kwargs,
            )
            return UploadResult(status="copied", dest_message_ids=self._result_message_ids(result))

        message, path = downloaded[0]
        caption = self._caption_for(message)
        media_type = message_media_type(message)

        if media_type == "photo":
            result = await self.limiter.call(
                "upload",
                self.writer.send_photo,
                chat_id=job.dest_chat_id,
                photo=str(path),
                caption=caption,
                caption_entities=message.caption_entities if caption else None,
                **kwargs,
            )
        elif media_type == "video":
            result = await self.limiter.call(
                "upload",
                self.writer.send_video,
                chat_id=job.dest_chat_id,
                video=str(path),
                caption=caption,
                caption_entities=message.caption_entities if caption else None,
                supports_streaming=True,
                **kwargs,
            )
        elif media_type == "text":
            result = await self.limiter.call(
                "upload",
                self.writer.send_message,
                chat_id=job.dest_chat_id,
                text=message.text or message.caption or "",
                entities=message.entities or message.caption_entities,
                **kwargs,
            )
        else:
            result = await self.limiter.call(
                "upload",
                self.writer.send_document,
                chat_id=job.dest_chat_id,
                document=str(path),
                caption=caption,
                caption_entities=message.caption_entities if caption else None,
                **kwargs,
            )

        return UploadResult(status="copied", dest_message_ids=self._result_message_ids(result))

    def _destination_kwargs(self, job: MessageJob) -> dict[str, Any]:
        if job.dest_topic_id:
            return {"reply_to_message_id": job.dest_topic_id}
        return {}

    def _caption_for(self, message: Message) -> str | None:
        if self.config.transfer.drop_caption:
            return None
        return message.caption or message.text or None

    def _message_should_process(self, message: Message) -> bool:
        media_type = message_media_type(message)
        if media_type == "video":
            return self.config.transfer.include_videos
        if media_type == "photo":
            return self.config.transfer.include_photos
        if media_type == "document":
            return self.config.transfer.include_documents
        if media_type == "text":
            return self.config.transfer.include_text
        return False

    def _validate_bot_upload_size(self, message: Message) -> None:
        if self.writer is self.reader:
            return
        size = message_file_size(message)
        if size and size > self.config.transfer.max_bot_upload_bytes:
            raise PermanentJobError(
                f"File is {size} bytes, above configured bot upload limit "
                f"{self.config.transfer.max_bot_upload_bytes}"
            )

    def _cleanup_job_dir(self, job_dir: Path, success: bool) -> None:
        if not job_dir.exists():
            return

        keep_completed = self.config.downloads.keep_completed or self.config.transfer.save_to_local
        if success and keep_completed:
            self._move_directory(job_dir, self.config.downloads.completed_dir / job_dir.name)
            return
        if not success and self.config.downloads.keep_failed:
            self._move_directory(job_dir, self.config.downloads.failed_dir / job_dir.name)
            return
        shutil.rmtree(job_dir, ignore_errors=True)

    def _move_directory(self, source: Path, dest: Path) -> None:
        if dest.exists():
            shutil.rmtree(dest, ignore_errors=True)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source), str(dest))

    def _file_name_for(self, message: Message) -> str:
        media = None
        for attr in ("video", "photo", "document", "animation", "audio", "voice", "video_note"):
            media = getattr(message, attr, None)
            if media:
                break

        original = getattr(media, "file_name", None) if media else None
        if not original:
            extension = {
                "photo": ".jpg",
                "video": ".mp4",
                "animation": ".mp4",
                "audio": ".mp3",
                "voice": ".ogg",
                "video_note": ".mp4",
                "document": ".bin",
            }.get(message_media_type(message), ".bin")
            original = f"{message.id}{extension}"

        safe = re.sub(r"[^A-Za-z0-9._-]+", "_", original).strip("._")
        return f"{message.id}_{safe or 'media.bin'}"

    def _result_message_ids(self, result: Any) -> list[int]:
        if isinstance(result, list):
            return [int(item.id) for item in result if getattr(item, "id", None)]
        if getattr(result, "id", None):
            return [int(result.id)]
        return []
