from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Any

from pyrogram import Client
from pyrogram.types import Message

from app.config import AppConfig, ChatSpec
from app.queue import MessageQueue
from app.telegram_client import (
    ResolvedChat,
    TelegramLimiter,
    message_caption,
    message_file_size,
    message_is_empty,
    message_media_type,
    message_unique_key,
    resolve_chat,
)


class Scanner:
    def __init__(
        self,
        config: AppConfig,
        queue: MessageQueue,
        reader: Client,
        limiter: TelegramLimiter,
        writer: Client | None = None,
        logger: Any | None = None,
    ) -> None:
        self.config = config
        self.queue = queue
        self.reader = reader
        self.writer = writer
        self.limiter = limiter
        self.logger = logger

    async def scan(self, stop_event: asyncio.Event) -> None:
        destinations = await self._resolve_destinations()
        if not destinations:
            raise ValueError("No valid destinations configured")

        for source in self.config.sources:
            if stop_event.is_set():
                break
            await self._scan_source(source, destinations, stop_event)

    async def _resolve_destinations(self) -> list[ResolvedChat]:
        resolved: list[ResolvedChat] = []
        for spec in self.config.destinations:
            try:
                resolved.append(await resolve_chat(self.reader, self.limiter, spec))
                continue
            except Exception as exc:
                if self.logger:
                    self.logger.warning("Reader could not resolve destination %s: %s", spec.chat, exc)

            if self.writer and self.writer is not self.reader:
                try:
                    resolved.append(await resolve_chat(self.writer, self.limiter, spec))
                    continue
                except Exception as exc:
                    if self.logger:
                        self.logger.warning("Writer could not resolve destination %s: %s", spec.chat, exc)

            resolved.append(ResolvedChat(chat_id=str(spec.chat), topic_id=spec.topic_id, title=str(spec.chat)))

        return resolved

    async def _scan_source(
        self,
        source: ChatSpec,
        destinations: list[ResolvedChat],
        stop_event: asyncio.Event,
    ) -> None:
        if source.start_id is None or source.end_id is None:
            raise ValueError(f"Source {source.chat} needs message_range.start and message_range.end")

        resolved_source = await resolve_chat(self.reader, self.limiter, source)
        start_id = min(source.start_id, source.end_id)
        end_id = max(source.start_id, source.end_id)
        chunk_size = max(1, self.config.limits.get_messages_chunk_size)

        if self.logger:
            self.logger.info(
                "Scanning %s (%s) message ids %s-%s",
                resolved_source.title,
                resolved_source.chat_id,
                start_id,
                end_id,
            )

        messages: list[Message] = []
        for chunk_start in range(start_id, end_id + 1, chunk_size):
            if stop_event.is_set():
                break
            chunk_end = min(chunk_start + chunk_size - 1, end_id)
            ids = list(range(chunk_start, chunk_end + 1))
            result = await self.limiter.call("read", self.reader.get_messages, resolved_source.chat_id, ids)
            if not isinstance(result, list):
                result = [result]
            messages.extend([msg for msg in result if not message_is_empty(msg)])

        grouped = self._group_messages(messages)
        added = skipped = existing = 0

        for group in grouped:
            first = group[0]
            processable = self._group_should_process(group)
            status = "pending" if processable else "skipped"
            if not processable and not self.config.queue.record_skipped:
                continue

            unique_key = self._group_unique_key(resolved_source.chat_id, group)
            media_type = self._group_media_type(group)
            source_message_ids = [msg.id for msg in group]
            file_size = sum(message_file_size(msg) or 0 for msg in group) or None
            caption = message_caption(first)
            if caption and len(caption) > 1000:
                caption = caption[:1000]

            for dest in destinations:
                inserted = self.queue.enqueue(
                    source_chat_id=resolved_source.chat_id,
                    source_message_id=first.id,
                    dest_chat_id=dest.chat_id,
                    file_unique_key=unique_key,
                    source_message_ids=source_message_ids,
                    source_topic_id=resolved_source.topic_id,
                    dest_topic_id=dest.topic_id,
                    media_group_id=first.media_group_id,
                    media_type=media_type,
                    file_size=file_size,
                    caption=caption,
                    status=status,
                    last_error=None if processable else "Filtered out by config",
                )
                if inserted and status == "pending":
                    added += 1
                elif inserted:
                    skipped += 1
                else:
                    existing += 1

        if self.logger:
            self.logger.info("Scan complete: added=%s skipped=%s already_queued=%s", added, skipped, existing)

    def _group_messages(self, messages: list[Message]) -> list[list[Message]]:
        groups: dict[str, list[Message]] = defaultdict(list)
        for message in messages:
            key = str(message.media_group_id) if message.media_group_id else f"single:{message.id}"
            groups[key].append(message)
        return [sorted(group, key=lambda msg: msg.id) for group in groups.values()]

    def _group_should_process(self, messages: list[Message]) -> bool:
        return any(self._message_should_process(message) for message in messages)

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

    def _group_media_type(self, messages: list[Message]) -> str:
        types = {message_media_type(message) for message in messages}
        if len(types) == 1:
            return next(iter(types))
        return "album"

    def _group_unique_key(self, source_chat_id: str, messages: list[Message]) -> str:
        keys = [message_unique_key(message) for message in messages]
        keys = [key for key in keys if key]
        if keys:
            return "album:" + "|".join(keys) if len(keys) > 1 else keys[0]
        return "messages:" + source_chat_id + ":" + ",".join(str(message.id) for message in messages)

