from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from pyrogram import Client
from pyrogram.types import Message

from app.config import AppConfig, ChatSpec
from app.control import write_status
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


SCAN_MODES = {"full", "incremental"}


def _is_placeholder(chat: str) -> bool:
    value = str(chat).lower()
    return "source_channel_or_-100_id" in value or "destination_channel_or_-100_id" in value


@dataclass(frozen=True)
class ScanPlan:
    start_id: int
    end_id: int
    baseline: int | None
    bootstrapped_from_queue: bool

    @property
    def has_work(self) -> bool:
        return self.start_id <= self.end_id


def build_scan_plan(
    *,
    configured_start: int,
    configured_end: int | None,
    latest_message_id: int | None,
    checkpoint: int | None,
    queue_highwater: int | None,
    scan_mode: str,
) -> ScanPlan | None:
    """Calculate a full or incremental ID range without touching Telegram."""
    normalized = str(scan_mode).strip().lower()
    if normalized not in SCAN_MODES:
        raise ValueError(f"Unsupported scan mode: {scan_mode}")

    end_candidate = configured_end if configured_end is not None else latest_message_id
    if end_candidate is None:
        return None

    lower = min(int(configured_start), int(end_candidate))
    upper = max(int(configured_start), int(end_candidate))
    baseline: int | None = None
    bootstrapped = False
    if normalized == "incremental":
        if checkpoint is not None:
            baseline = int(checkpoint)
        elif queue_highwater is not None:
            baseline = int(queue_highwater)
            bootstrapped = True

    start_id = lower if baseline is None else max(lower, baseline + 1)
    return ScanPlan(
        start_id=start_id,
        end_id=upper,
        baseline=baseline,
        bootstrapped_from_queue=bootstrapped,
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
        scan_mode: str = "full",
        source_index_offset: int = 0,
        source_total_override: int | None = None,
    ) -> None:
        normalized_mode = str(scan_mode).strip().lower()
        if normalized_mode not in SCAN_MODES:
            raise ValueError(f"Unsupported scan mode: {scan_mode}")
        self.config = config
        self.queue = queue
        self.reader = reader
        self.writer = writer
        self.limiter = limiter
        self.logger = logger
        self.scan_mode = normalized_mode
        self.source_index_offset = max(0, int(source_index_offset))
        self.source_total_override = (
            max(1, int(source_total_override))
            if source_total_override is not None
            else None
        )

    async def scan(self, stop_event: asyncio.Event) -> None:
        destinations = await self._resolve_destinations()
        sources = [source for source in self.config.sources if source.chat and not _is_placeholder(source.chat)]

        if not sources or not destinations:
            if self.logger:
                self.logger.info(
                    "Migration waiting for Telegram bot settings: source=%s destination=%s",
                    "set" if sources else "missing",
                    "set" if destinations else "missing",
                )
            write_status(
                self.config,
                "waiting",
                message="Tetapkan source dan destination dalam bot.",
                source="set" if sources else "missing",
                destination="set" if destinations else "missing",
                scan_mode=self.scan_mode,
            )
            return

        source_total = self.source_total_override or len(sources)
        for local_index, source in enumerate(sources, start=1):
            if stop_event.is_set():
                break
            source_index = self.source_index_offset + local_index
            await self._scan_source(
                source,
                destinations,
                stop_event,
                source_index=source_index,
                source_total=source_total,
            )

    async def _resolve_destinations(self) -> list[ResolvedChat]:
        resolved: list[ResolvedChat] = []
        sending_client = self.writer or self.reader

        for spec in self.config.destinations:
            if not spec.chat or _is_placeholder(spec.chat):
                continue

            try:
                destination = await resolve_chat(sending_client, self.limiter, spec)
                resolved.append(destination)
                continue
            except Exception as exc:
                if self.logger:
                    client_name = "Writer" if sending_client is not self.reader else "Reader"
                    self.logger.warning(
                        "%s could not resolve destination %s: %s",
                        client_name,
                        spec.chat,
                        exc,
                    )

            write_status(
                self.config,
                "error",
                message="Destination tidak dapat dikenal oleh bot uploader.",
                destination_chat=spec.chat,
                error=(
                    "Pastikan bot MANAGER sudah menjadi admin destination dan cuba "
                    "tambah destination semula melalui forward post atau -100 ID."
                ),
                scan_mode=self.scan_mode,
            )

        return resolved

    async def _latest_message_id(self, chat_id: int | str) -> int | None:
        async for message in self.reader.get_chat_history(chat_id, limit=1):
            if not message_is_empty(message):
                return int(message.id)
        return None

    async def _scan_source(
        self,
        source: ChatSpec,
        destinations: list[ResolvedChat],
        stop_event: asyncio.Event,
        *,
        source_index: int,
        source_total: int,
    ) -> None:
        write_status(
            self.config,
            "scanning",
            message="Resolving source channel...",
            source=source.chat,
            source_index=source_index,
            source_total=source_total,
            scan_mode=self.scan_mode,
        )
        resolved_source = await resolve_chat(self.reader, self.limiter, source)

        configured_start = source.start_id if source.start_id is not None else 1
        latest_message_id: int | None = None
        if source.end_id is None:
            latest_message_id = await self.limiter.call(
                "read", self._latest_message_id, resolved_source.chat_id
            )

        checkpoint = self.queue.get_scan_checkpoint(
            resolved_source.chat_id,
            resolved_source.topic_id,
        )
        queue_highwater = (
            self.queue.source_queue_highwater(resolved_source.chat_id)
            if self.scan_mode == "incremental" and checkpoint is None
            else None
        )
        plan = build_scan_plan(
            configured_start=configured_start,
            configured_end=source.end_id,
            latest_message_id=latest_message_id,
            checkpoint=checkpoint,
            queue_highwater=queue_highwater,
            scan_mode=self.scan_mode,
        )
        if plan is None:
            if self.logger:
                self.logger.info("Source %s has no messages", resolved_source.title)
            write_status(
                self.config,
                "scan_complete",
                message="Source tiada mesej.",
                source=resolved_source.title,
                source_index=source_index,
                source_total=source_total,
                scan_mode=self.scan_mode,
            )
            return

        if not plan.has_work:
            checkpoint_target = max(plan.baseline or 0, plan.end_id)
            if checkpoint_target > 0 and checkpoint != checkpoint_target:
                self.queue.set_scan_checkpoint(
                    resolved_source.chat_id,
                    resolved_source.topic_id,
                    checkpoint_target,
                    self.scan_mode,
                )
            if self.logger:
                self.logger.info(
                    "Incremental sync found no new messages in %s; checkpoint=%s latest=%s",
                    resolved_source.title,
                    plan.baseline,
                    plan.end_id,
                )
            write_status(
                self.config,
                "scan_complete",
                message="Tiada post baru untuk disync.",
                source=resolved_source.title,
                source_index=source_index,
                source_total=source_total,
                scan_mode=self.scan_mode,
                checkpoint=checkpoint_target or plan.baseline,
                latest_source_id=plan.end_id,
                added=0,
                skipped=0,
                existing=0,
            )
            return

        chunk_size = max(1, self.config.limits.get_messages_chunk_size)
        total_ids = plan.end_id - plan.start_id + 1
        if self.logger:
            self.logger.info(
                "Scanning %s (%s) message ids %s-%s mode=%s checkpoint=%s bootstrap=%s",
                resolved_source.title,
                resolved_source.chat_id,
                plan.start_id,
                plan.end_id,
                self.scan_mode,
                plan.baseline,
                plan.bootstrapped_from_queue,
            )

        messages: list[Message] = []
        for chunk_start in range(plan.start_id, plan.end_id + 1, chunk_size):
            if stop_event.is_set():
                break
            chunk_end = min(chunk_start + chunk_size - 1, plan.end_id)
            scanned = chunk_end - plan.start_id + 1
            write_status(
                self.config,
                "scanning",
                message=(
                    "Sedang sync post baru."
                    if self.scan_mode == "incremental"
                    else "Sedang full scan mesej source."
                ),
                source=resolved_source.title,
                current=scanned,
                total=total_ids,
                source_index=source_index,
                source_total=source_total,
                scan_mode=self.scan_mode,
                scan_start=plan.start_id,
                scan_end=plan.end_id,
                checkpoint=plan.baseline,
                checkpoint_bootstrap=plan.bootstrapped_from_queue,
            )
            ids = list(range(chunk_start, chunk_end + 1))
            result = await self.limiter.call("read", self.reader.get_messages, resolved_source.chat_id, ids)
            if not isinstance(result, list):
                result = [result]
            messages.extend([msg for msg in result if not message_is_empty(msg)])

        if stop_event.is_set():
            write_status(
                self.config,
                "stopping",
                message="Scan dihentikan dengan selamat. Checkpoint tidak digerakkan.",
                source=resolved_source.title,
                source_index=source_index,
                source_total=source_total,
                scan_mode=self.scan_mode,
                checkpoint=plan.baseline,
            )
            return

        grouped = self._group_messages(messages)
        added = skipped = existing = 0

        for group_index, group in enumerate(grouped, start=1):
            if stop_event.is_set():
                break
            if group_index == 1 or group_index % 100 == 0 or group_index == len(grouped):
                write_status(
                    self.config,
                    "scanning",
                    message="Menyusun queue migration.",
                    source=resolved_source.title,
                    current=group_index,
                    total=len(grouped),
                    source_index=source_index,
                    source_total=source_total,
                    scan_mode=self.scan_mode,
                    checkpoint=plan.baseline,
                )

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

        if stop_event.is_set():
            write_status(
                self.config,
                "stopping",
                message="Queue building dihentikan. Checkpoint tidak digerakkan.",
                source=resolved_source.title,
                source_index=source_index,
                source_total=source_total,
                scan_mode=self.scan_mode,
                checkpoint=plan.baseline,
            )
            return

        self.queue.set_scan_checkpoint(
            resolved_source.chat_id,
            resolved_source.topic_id,
            plan.end_id,
            self.scan_mode,
        )
        if self.logger:
            self.logger.info(
                "Scan complete: mode=%s added=%s skipped=%s already_queued=%s checkpoint=%s",
                self.scan_mode,
                added,
                skipped,
                existing,
                plan.end_id,
            )
        write_status(
            self.config,
            "scan_complete",
            message=(
                "Sync post baru selesai. Memulakan pemindahan queue."
                if self.scan_mode == "incremental"
                else "Full scan selesai. Memulakan pemindahan queue."
            ),
            source=resolved_source.title,
            source_index=source_index,
            source_total=source_total,
            added=added,
            skipped=skipped,
            existing=existing,
            source_messages=len(messages),
            scan_mode=self.scan_mode,
            scan_start=plan.start_id,
            scan_end=plan.end_id,
            checkpoint=plan.end_id,
            checkpoint_bootstrap=plan.bootstrapped_from_queue,
        )

    def _group_messages(self, messages: list[Message]) -> list[list[Message]]:
        groups: dict[str, list[Message]] = defaultdict(list)
        for message in messages:
            key = str(message.media_group_id) if message.media_group_id else f"single:{message.id}"
            groups[key].append(message)
        grouped = [sorted(group, key=lambda msg: msg.id) for group in groups.values()]
        return sorted(grouped, key=lambda group: group[0].id)

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

    def _group_unique_key(self, source_chat_id: int | str, messages: list[Message]) -> str:
        keys = [message_unique_key(message) for message in messages]
        keys = [key for key in keys if key]
        if keys:
            return "album:" + "|".join(keys) if len(keys) > 1 else keys[0]
        return "messages:" + str(source_chat_id) + ":" + ",".join(str(message.id) for message in messages)
