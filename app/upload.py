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
from app.queue import MediaCacheEntry, MessageJob, MessageQueue
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
        queue: MessageQueue,
        logger: Any | None = None,
    ) -> None:
        self.config = config
        self.reader = reader
        self.writer = writer
        self.limiter = limiter
        self.queue = queue
        self.logger = logger

    @staticmethod
    def _cache_matches_job(job: MessageJob, cached: MediaCacheEntry) -> bool:
        """Only reuse a bot file_id when Telegram media types match exactly."""
        cached_types = [str(media_type).lower() for media_type in cached.media_types]
        supported_types = {"photo", "video", "document"}
        if not cached_types or any(media_type not in supported_types for media_type in cached_types):
            return False
        if job.media_type == "album":
            return len(cached_types) > 1
        return all(media_type == job.media_type for media_type in cached_types)

    @staticmethod
    def _is_cached_file_id_type_mismatch(exc: ValueError) -> bool:
        message = str(exc).casefold()
        return (
            message.startswith("expected ")
            and " got " in message
            and " file id instead" in message
        )

    def _discard_cached_media(self, job: MessageJob, reason: str) -> None:
        self.queue.delete_media_cache(job.file_unique_key)
        if self.logger:
            self.logger.warning("Discarded cached bot file_id for job %s: %s", job.id, reason)

    async def process(
        self,
        job: MessageJob,
        stop_event: asyncio.Event,
        on_phase: PhaseCallback,
    ) -> UploadResult:
        cached = self.queue.get_media_cache(job.file_unique_key)
        if cached and job.media_type != "text":
            if not self._cache_matches_job(job, cached):
                self._discard_cached_media(job, "media type does not match the queued source")
            else:
                try:
                    await on_phase("uploading")
                    result = await self._send_cached(job, cached)
                    if self.logger:
                        self.logger.info("Job %s reused cached bot file_id", job.id)
                    return result
                except (BadRequest, MediaEmpty) as exc:
                    self._discard_cached_media(job, f"Telegram rejected cached file_id: {exc}")
                except ValueError as exc:
                    if not self._is_cached_file_id_type_mismatch(exc):
                        raise
                    self._discard_cached_media(job, f"cached file_id type mismatch: {exc}")

        messages = await self._load_source_messages(job)
        messages = [message for message in messages if self._message_should_process(message)]
        if not messages:
            return UploadResult(status="skipped", reason="Source messages missing or filtered out")

        text_only = all(message_media_type(message) == "text" for message in messages)
        if self.config.transfer.prefer_copy:
            try:
                await on_phase("uploading")
                return await self._copy_or_forward(job, messages)
            except ChatForwardsRestricted as exc:
                if self.config.transfer.forwarding_only:
                    return UploadResult(status="skipped", reason=f"Forward/copy restricted: {exc}")
                if self.logger:
                    self.logger.warning("Restricted copy for job %s; downloading once", job.id)
            except ChatWriteForbidden as exc:
                if self.writer is self.reader:
                    raise PermanentJobError(str(exc)) from exc
                if self.logger:
                    self.logger.warning("User cannot post job %s; uploader bot fallback", job.id)
            except MediaEmpty as exc:
                if self.config.transfer.forwarding_only:
                    raise PermanentJobError(str(exc)) from exc
                if self.logger:
                    self.logger.warning(
                        "Native copy returned MEDIA_EMPTY for job %s; downloading for upload fallback",
                        job.id,
                    )
            except (ChannelPrivate, ChannelInvalid) as exc:
                raise PermanentJobError(str(exc)) from exc
            except BadRequest as exc:
                if self.config.transfer.forwarding_only:
                    raise PermanentJobError(str(exc)) from exc
                if self.logger:
                    self.logger.warning("Native copy failed for job %s: %s", job.id, exc)

        if self.config.transfer.forwarding_only:
            return UploadResult(status="skipped", reason="forwarding_only is enabled")
        if text_only:
            await on_phase("uploading")
            return await self._send_text(job, messages[0])
        return await self._download_and_upload(job, messages, stop_event, on_phase)

    async def _send_cached(self, job: MessageJob, cached: MediaCacheEntry) -> UploadResult:
        caption = None if self.config.transfer.drop_caption else job.caption
        kwargs = self._destination_kwargs(job)
        if len(cached.bot_file_ids) > 1:
            media: list[Any] = []
            caption_used = False
            for file_id, media_type in zip(cached.bot_file_ids, cached.media_types):
                item_caption = caption if not caption_used else None
                caption_used = caption_used or bool(item_caption)
                if media_type == "photo":
                    media.append(InputMediaPhoto(file_id, caption=item_caption))
                elif media_type == "video":
                    media.append(InputMediaVideo(file_id, caption=item_caption, supports_streaming=True))
                else:
                    media.append(InputMediaDocument(file_id, caption=item_caption))
            result = await self.limiter.call(
                "upload",
                self.writer.send_media_group,
                chat_id=job.dest_chat_id,
                media=media,
                **kwargs,
            )
        else:
            file_id = cached.bot_file_ids[0]
            media_type = cached.media_types[0]
            if media_type == "photo":
                result = await self.limiter.call(
                    "upload",
                    self.writer.send_photo,
                    chat_id=job.dest_chat_id,
                    photo=file_id,
                    caption=caption,
                    **kwargs,
                )
            elif media_type == "video":
                result = await self.limiter.call(
                    "upload",
                    self.writer.send_video,
                    chat_id=job.dest_chat_id,
                    video=file_id,
                    caption=caption,
                    supports_streaming=True,
                    **kwargs,
                )
            else:
                result = await self.limiter.call(
                    "upload",
                    self.writer.send_document,
                    chat_id=job.dest_chat_id,
                    document=file_id,
                    caption=caption,
                    **kwargs,
                )
        return UploadResult(status="copied", dest_message_ids=self._result_message_ids(result))

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
        return [message for message in result if not message_is_empty(message)]

    async def _copy_or_forward(self, job: MessageJob, messages: list[Message]) -> UploadResult:
        kwargs = self._destination_kwargs(job)
        first = messages[0]
        if self.config.transfer.hide_sender:
            if len(messages) > 1 and job.media_group_id and first.media_group_id:
                result = await self.limiter.call(
                    "copy",
                    self.reader.copy_media_group,
                    chat_id=job.dest_chat_id,
                    from_chat_id=job.source_chat_id,
                    message_id=first.id,
                    captions="" if self.config.transfer.drop_caption else None,
                    **kwargs,
                )
            elif len(messages) > 1:
                # A filtered album must not call copy_media_group: Telegram would
                # copy disabled members from the original album as well.
                result = [
                    await self.limiter.call(
                        "copy",
                        self.reader.copy_message,
                        chat_id=job.dest_chat_id,
                        from_chat_id=job.source_chat_id,
                        message_id=message.id,
                        caption="" if self.config.transfer.drop_caption else None,
                        **kwargs,
                    )
                    for message in messages
                ]
            else:
                result = await self.limiter.call(
                    "copy",
                    self.reader.copy_message,
                    chat_id=job.dest_chat_id,
                    from_chat_id=job.source_chat_id,
                    message_id=first.id,
                    caption="" if self.config.transfer.drop_caption else None,
                    **kwargs,
                )
        else:
            result = await self.limiter.call(
                "copy",
                self.reader.forward_messages,
                chat_id=job.dest_chat_id,
                from_chat_id=job.source_chat_id,
                message_ids=[message.id for message in messages],
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
        try:
            await on_phase("downloading")
            for message in messages:
                self._validate_bot_upload_size(message)
                downloaded.append((message, await self._download_one(message, job_dir)))
            if stop_event.is_set():
                raise RetryableJobError("Stop requested after download; job will retry")

            await on_phase("uploading")
            result, sent_messages = await self._upload_downloaded(job, downloaded)
            bot_file_ids = [self._message_file_id(message) for message in sent_messages]
            media_types = [message_media_type(message) for message in sent_messages]
            cache_entry = MediaCacheEntry(
                file_unique_key=job.file_unique_key,
                bot_file_ids=bot_file_ids,
                media_types=media_types,
            )
            if bot_file_ids and all(bot_file_ids) and self._cache_matches_job(job, cache_entry):
                self.queue.save_media_cache(job.file_unique_key, bot_file_ids, media_types)
                if self.logger:
                    self.logger.info("Saved %s reusable bot file_id value(s)", len(bot_file_ids))
            elif bot_file_ids:
                self.queue.delete_media_cache(job.file_unique_key)
                if self.logger:
                    self.logger.warning(
                        "Did not cache job %s because Telegram changed its media type", job.id
                    )
            return result
        finally:
            shutil.rmtree(job_dir, ignore_errors=True)

    async def _download_one(self, message: Message, job_dir: Path) -> Path:
        path = job_dir / self._file_name_for(message)
        result = await self.limiter.call("download", message.download, file_name=str(path))
        return Path(result or path)

    async def _upload_downloaded(
        self,
        job: MessageJob,
        downloaded: list[tuple[Message, Path]],
    ) -> tuple[UploadResult, list[Message]]:
        kwargs = self._destination_kwargs(job)
        if len(downloaded) > 1:
            media: list[Any] = []
            caption_used = False
            for message, path in downloaded:
                caption = self._caption_for(message) if not caption_used else None
                caption_used = caption_used or bool(caption)
                media_type = message_media_type(message)
                if media_type == "photo":
                    media.append(InputMediaPhoto(str(path), caption=caption))
                elif media_type == "video":
                    media.append(
                        InputMediaVideo(
                            str(path),
                            caption=caption,
                            **self._video_metadata(message),
                        )
                    )
                else:
                    media.append(InputMediaDocument(str(path), caption=caption))
            try:
                result = await self.limiter.call(
                    "upload",
                    self.writer.send_media_group,
                    chat_id=job.dest_chat_id,
                    media=media,
                    **kwargs,
                )
                sent = result if isinstance(result, list) else [result]
            except MediaEmpty:
                if self.logger:
                    self.logger.warning(
                        "Media group upload returned MEDIA_EMPTY for job %s; sending %s item(s) individually",
                        job.id,
                        len(downloaded),
                    )
                sent = await self._upload_downloaded_individually(job, downloaded)
                result = sent
        else:
            message, path = downloaded[0]
            result = await self._send_downloaded_item(
                job,
                message,
                path,
                caption=self._caption_for(message),
            )
            sent = [result]
        return UploadResult(status="copied", dest_message_ids=self._result_message_ids(result)), sent

    async def _upload_downloaded_individually(
        self,
        job: MessageJob,
        downloaded: list[tuple[Message, Path]],
    ) -> list[Message]:
        sent: list[Message] = []
        caption_used = False
        for message, path in downloaded:
            caption = self._caption_for(message) if not caption_used else None
            caption_used = caption_used or bool(caption)
            sent.append(await self._send_downloaded_item(job, message, path, caption=caption))
        return sent

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
                **self._video_metadata(message),
                **common,
            )
        return await self.limiter.call(
            "upload",
            self.writer.send_document,
            document=str(path),
            **common,
        )

    @staticmethod
    def _video_metadata(message: Message) -> dict[str, Any]:
        video = getattr(message, "video", None)
        if not video:
            return {"supports_streaming": True}
        metadata: dict[str, Any] = {
            "supports_streaming": bool(getattr(video, "supports_streaming", True)),
        }
        for key in ("duration", "width", "height"):
            value = getattr(video, key, None)
            if value is not None:
                metadata[key] = int(value)
        return metadata

    def _destination_kwargs(self, job: MessageJob) -> dict[str, Any]:
        return {"reply_to_message_id": job.dest_topic_id} if job.dest_topic_id else {}

    def _caption_for(self, message: Message) -> str | None:
        return None if self.config.transfer.drop_caption else (message.caption or message.text or None)

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

    @staticmethod
    def _message_file_id(message: Message) -> str:
        for attribute in ("video", "photo", "document", "animation", "audio", "voice", "video_note"):
            media = getattr(message, attribute, None)
            if media:
                return str(getattr(media, "file_id", "") or "")
        return ""

    @staticmethod
    def _file_name_for(message: Message) -> str:
        media = None
        for attribute in ("video", "photo", "document", "animation", "audio", "voice", "video_note"):
            media = getattr(message, attribute, None)
            if media:
                break
        original = getattr(media, "file_name", None) if media else None
        if not original:
            extension = {"photo": ".jpg", "video": ".mp4", "document": ".bin"}.get(
                message_media_type(message), ".bin"
            )
            original = f"{message.id}{extension}"
        safe = re.sub(r"[^A-Za-z0-9._-]+", "_", original).strip("._")
        return f"{message.id}_{safe or 'media.bin'}"

    @staticmethod
    def _result_message_ids(result: Any) -> list[int]:
        if isinstance(result, list):
            return [int(item.id) for item in result if getattr(item, "id", None)]
        return [int(result.id)] if getattr(result, "id", None) else []
