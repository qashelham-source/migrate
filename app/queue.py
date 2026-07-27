from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from sqlite3 import Row

from app.config import AppConfig
from app.db import Database, utc_now


@dataclass(frozen=True)
class MessageJob:
    id: int
    source_chat_id: str
    source_message_id: int
    dest_chat_id: str
    status: str
    attempts: int
    last_error: str | None
    next_retry_at: str | None
    file_unique_key: str
    source_topic_id: int | None
    dest_topic_id: int | None
    media_group_id: str | None
    source_message_ids: list[int]
    dest_message_ids: list[int]
    media_type: str
    file_size: int | None
    caption: str | None

    @classmethod
    def from_row(cls, row: Row) -> "MessageJob":
        return cls(
            id=int(row["id"]),
            source_chat_id=str(row["source_chat_id"]),
            source_message_id=int(row["source_message_id"]),
            dest_chat_id=str(row["dest_chat_id"]),
            status=str(row["status"]),
            attempts=int(row["attempts"]),
            last_error=row["last_error"],
            next_retry_at=row["next_retry_at"],
            file_unique_key=str(row["file_unique_key"]),
            source_topic_id=row["source_topic_id"],
            dest_topic_id=row["dest_topic_id"],
            media_group_id=row["media_group_id"],
            source_message_ids=json.loads(row["source_message_ids"]),
            dest_message_ids=json.loads(row["dest_message_ids"] or "[]"),
            media_type=str(row["media_type"] or "unsupported"),
            file_size=row["file_size"],
            caption=row["caption"],
        )


@dataclass(frozen=True)
class MediaCacheEntry:
    file_unique_key: str
    bot_file_ids: list[str]
    media_types: list[str]


class MessageQueue:
    def __init__(self, db: Database, config: AppConfig) -> None:
        self.db = db
        self.config = config

    def enqueue(
        self,
        *,
        source_chat_id: str,
        source_message_id: int,
        dest_chat_id: str,
        file_unique_key: str,
        source_message_ids: list[int],
        source_topic_id: int | None,
        dest_topic_id: int | None,
        media_group_id: str | None,
        media_type: str,
        file_size: int | None,
        caption: str | None,
        status: str = "pending",
        last_error: str | None = None,
    ) -> bool:
        return self.db.enqueue_message(
            source_chat_id=source_chat_id,
            source_message_id=source_message_id,
            dest_chat_id=dest_chat_id,
            file_unique_key=file_unique_key,
            source_message_ids=source_message_ids,
            source_topic_id=source_topic_id,
            dest_topic_id=dest_topic_id,
            media_group_id=media_group_id,
            media_type=media_type,
            file_size=file_size,
            caption=caption,
            status=status,
            last_error=last_error,
        )

    def fetch_due(self, limit: int) -> list[MessageJob]:
        return [MessageJob.from_row(row) for row in self.db.due_jobs(limit)]

    def fetch_for_verification(self, limit: int) -> list[MessageJob]:
        return [MessageJob.from_row(row) for row in self.db.copied_jobs_for_verification(limit)]

    def start_attempt(self, job: MessageJob) -> int:
        attempts = self.db.increment_attempt(job.id)
        self.db.set_status(job.id, "downloading")
        return attempts

    def set_phase(self, job_id: int, status: str) -> None:
        self.db.set_status(job_id, status)

    def mark_copied(self, job_id: int, dest_message_ids: list[int]) -> None:
        self.db.set_status(job_id, "copied", last_error="", dest_message_ids=dest_message_ids)

    def mark_skipped(self, job_id: int, reason: str) -> None:
        self.db.set_status(job_id, "skipped", last_error=reason)

    def mark_verified(self, job_id: int) -> None:
        self.db.set_status(job_id, "copied", verified_at=utc_now())

    def mark_failure(self, job: MessageJob, error: str, attempts: int) -> str:
        if attempts >= self.config.queue.max_attempts:
            self.db.set_status(job.id, "failed", last_error=error)
            return "failed"
        backoff = self._backoff_for_attempt(attempts)
        next_retry = (datetime.now(timezone.utc) + timedelta(seconds=backoff)).isoformat(timespec="seconds")
        self.db.set_status(job.id, "pending", last_error=error, next_retry_at=next_retry)
        return "pending"

    def recover_in_progress(self) -> int:
        return self.db.recover_in_progress()

    def counts_by_status(self) -> dict[str, int]:
        return self.db.counts_by_status()

    def get_media_cache(self, file_unique_key: str) -> MediaCacheEntry | None:
        row = self.db.get_media_cache(file_unique_key)
        if not row:
            return None
        try:
            bot_file_ids = [str(value) for value in json.loads(row["bot_file_ids"])]
            media_types = [str(value) for value in json.loads(row["media_types"])]
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
        if not bot_file_ids or len(bot_file_ids) != len(media_types):
            return None
        return MediaCacheEntry(
            file_unique_key=str(row["file_unique_key"]),
            bot_file_ids=bot_file_ids,
            media_types=media_types,
        )

    def save_media_cache(
        self,
        file_unique_key: str,
        bot_file_ids: list[str],
        media_types: list[str],
    ) -> None:
        self.db.save_media_cache(file_unique_key, bot_file_ids, media_types)

    def delete_media_cache(self, file_unique_key: str) -> None:
        self.db.delete_media_cache(file_unique_key)

    def media_cache_count(self) -> int:
        return self.db.media_cache_count()

    def _backoff_for_attempt(self, attempts: int) -> int:
        if not self.config.queue.retry_backoff_seconds:
            return 300
        index = min(max(attempts - 1, 0), len(self.config.queue.retry_backoff_seconds) - 1)
        return self.config.queue.retry_backoff_seconds[index]
