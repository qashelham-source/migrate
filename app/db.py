from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


STATUSES = {"pending", "downloading", "uploading", "copied", "failed", "skipped"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _topic_key(topic_id: int | None) -> int:
    return int(topic_id or 0)


class Database:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")

    def close(self) -> None:
        self.conn.close()

    def initialize(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_chat_id TEXT NOT NULL,
                source_message_id INTEGER NOT NULL,
                dest_chat_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending'
                    CHECK(status IN ('pending', 'downloading', 'uploading', 'copied', 'failed', 'skipped')),
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                next_retry_at TEXT,
                file_unique_key TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                source_topic_id INTEGER,
                dest_topic_id INTEGER,
                media_group_id TEXT,
                source_message_ids TEXT NOT NULL,
                dest_message_ids TEXT,
                media_type TEXT,
                file_size INTEGER,
                caption TEXT,
                verified_at TEXT
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_unique_job
                ON messages(source_chat_id, dest_chat_id, file_unique_key);
            CREATE INDEX IF NOT EXISTS idx_messages_due
                ON messages(status, next_retry_at, updated_at);
            CREATE INDEX IF NOT EXISTS idx_messages_source
                ON messages(source_chat_id, source_message_id);

            CREATE TABLE IF NOT EXISTS media_cache (
                file_unique_key TEXT PRIMARY KEY,
                bot_file_ids TEXT NOT NULL,
                media_types TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS scan_checkpoints (
                source_chat_id TEXT NOT NULL,
                source_topic_key INTEGER NOT NULL DEFAULT 0,
                last_scanned_message_id INTEGER NOT NULL,
                last_scan_mode TEXT NOT NULL DEFAULT 'incremental',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (source_chat_id, source_topic_key)
            );
            """
        )
        self.conn.commit()
        self.requeue_send_multi_media_errors()

    def execute(self, sql: str, params: Iterable[Any] = ()) -> sqlite3.Cursor:
        cursor = self.conn.execute(sql, tuple(params))
        self.conn.commit()
        return cursor

    def query(self, sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
        return list(self.conn.execute(sql, tuple(params)))

    def query_one(self, sql: str, params: Iterable[Any] = ()) -> sqlite3.Row | None:
        return self.conn.execute(sql, tuple(params)).fetchone()

    def enqueue_message(
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
        if status not in STATUSES:
            raise ValueError(f"Invalid message status: {status}")
        now = utc_now()
        cursor = self.conn.execute(
            """
            INSERT OR IGNORE INTO messages (
                source_chat_id, source_message_id, dest_chat_id, status, attempts,
                last_error, next_retry_at, file_unique_key, created_at, updated_at,
                source_topic_id, dest_topic_id, media_group_id, source_message_ids,
                media_type, file_size, caption
            ) VALUES (?, ?, ?, ?, 0, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source_chat_id,
                source_message_id,
                dest_chat_id,
                status,
                last_error,
                file_unique_key,
                now,
                now,
                source_topic_id,
                dest_topic_id,
                media_group_id,
                json.dumps(source_message_ids),
                media_type,
                file_size,
                caption,
            ),
        )
        self.conn.commit()
        return cursor.rowcount > 0

    def set_status(
        self,
        job_id: int,
        status: str,
        *,
        last_error: str | None = None,
        next_retry_at: str | None = None,
        dest_message_ids: list[int] | None = None,
        verified_at: str | None = None,
    ) -> None:
        if status not in STATUSES:
            raise ValueError(f"Invalid message status: {status}")
        fields = ["status = ?", "updated_at = ?"]
        values: list[Any] = [status, utc_now()]
        if last_error is not None:
            fields.append("last_error = ?")
            values.append(last_error[:4000])
        if next_retry_at is not None or status in {"copied", "failed", "skipped"}:
            fields.append("next_retry_at = ?")
            values.append(next_retry_at)
        if dest_message_ids is not None:
            fields.append("dest_message_ids = ?")
            values.append(json.dumps(dest_message_ids))
        if verified_at is not None:
            fields.append("verified_at = ?")
            values.append(verified_at)
        values.append(job_id)
        self.execute(f"UPDATE messages SET {', '.join(fields)} WHERE id = ?", values)

    def increment_attempt(self, job_id: int) -> int:
        now = utc_now()
        self.conn.execute(
            "UPDATE messages SET attempts = attempts + 1, updated_at = ? WHERE id = ?",
            (now, job_id),
        )
        row = self.conn.execute("SELECT attempts FROM messages WHERE id = ?", (job_id,)).fetchone()
        self.conn.commit()
        return int(row["attempts"])

    def recover_in_progress(self) -> int:
        cursor = self.conn.execute(
            """
            UPDATE messages
            SET status = 'pending',
                last_error = COALESCE(last_error, 'Recovered after interrupted run'),
                updated_at = ?
            WHERE status IN ('downloading', 'uploading')
            """,
            (utc_now(),),
        )
        self.conn.commit()
        return cursor.rowcount

    def requeue_peer_id_errors(self) -> int:
        cursor = self.conn.execute(
            """
            UPDATE messages
            SET status = 'pending',
                attempts = 0,
                last_error = NULL,
                next_retry_at = NULL,
                updated_at = ?
            WHERE status IN ('failed', 'skipped')
              AND (
                    LOWER(COALESCE(last_error, '')) LIKE '%peer id invalid%'
                 OR LOWER(COALESCE(last_error, '')) LIKE '%peer_id_invalid%'
              )
            """,
            (utc_now(),),
        )
        self.conn.commit()
        return cursor.rowcount

    def requeue_send_multi_media_errors(self) -> int:
        """Retry legacy album jobs that were skipped before individual upload fallback existed."""
        cursor = self.conn.execute(
            """
            UPDATE messages
            SET status = 'pending',
                attempts = 0,
                last_error = NULL,
                next_retry_at = NULL,
                updated_at = ?
            WHERE status IN ('failed', 'skipped')
              AND LOWER(COALESCE(last_error, '')) LIKE '%sendmultimedia%'
              AND (
                    LOWER(COALESCE(last_error, '')) LIKE '%media_empty%'
                 OR LOWER(COALESCE(last_error, '')) LIKE '%mediaempty%'
              )
            """,
            (utc_now(),),
        )
        self.conn.commit()
        return cursor.rowcount

    def due_jobs(self, limit: int) -> list[sqlite3.Row]:
        return self.query(
            """
            SELECT * FROM messages
            WHERE status = 'pending'
              AND (next_retry_at IS NULL OR next_retry_at <= ?)
            ORDER BY updated_at ASC, id ASC
            LIMIT ?
            """,
            (utc_now(), limit),
        )

    def copied_jobs_for_verification(self, limit: int) -> list[sqlite3.Row]:
        return self.query(
            """
            SELECT * FROM messages
            WHERE status = 'copied'
              AND dest_message_ids IS NOT NULL
              AND verified_at IS NULL
            ORDER BY updated_at ASC, id ASC
            LIMIT ?
            """,
            (limit,),
        )

    def counts_by_status(self) -> dict[str, int]:
        rows = self.query("SELECT status, COUNT(*) AS count FROM messages GROUP BY status")
        return {str(row["status"]): int(row["count"]) for row in rows}

    def get_scan_checkpoint(
        self,
        source_chat_id: int | str,
        source_topic_id: int | None = None,
    ) -> sqlite3.Row | None:
        return self.query_one(
            """
            SELECT * FROM scan_checkpoints
            WHERE source_chat_id = ? AND source_topic_key = ?
            """,
            (str(source_chat_id), _topic_key(source_topic_id)),
        )

    def set_scan_checkpoint(
        self,
        source_chat_id: int | str,
        source_topic_id: int | None,
        last_scanned_message_id: int,
        scan_mode: str,
    ) -> None:
        now = utc_now()
        self.execute(
            """
            INSERT INTO scan_checkpoints (
                source_chat_id, source_topic_key, last_scanned_message_id,
                last_scan_mode, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_chat_id, source_topic_key) DO UPDATE SET
                last_scanned_message_id = excluded.last_scanned_message_id,
                last_scan_mode = excluded.last_scan_mode,
                updated_at = excluded.updated_at
            """,
            (
                str(source_chat_id),
                _topic_key(source_topic_id),
                int(last_scanned_message_id),
                str(scan_mode),
                now,
                now,
            ),
        )

    def list_scan_checkpoints(self) -> list[sqlite3.Row]:
        return self.query(
            """
            SELECT source_chat_id, source_topic_key, last_scanned_message_id,
                   last_scan_mode, created_at, updated_at
            FROM scan_checkpoints
            ORDER BY updated_at DESC, source_chat_id ASC
            """
        )

    def reset_scan_checkpoints(
        self,
        source_chat_id: int | str | None = None,
        source_topic_id: int | None = None,
    ) -> int:
        if source_chat_id is None:
            cursor = self.execute("DELETE FROM scan_checkpoints")
            return int(cursor.rowcount)
        cursor = self.execute(
            "DELETE FROM scan_checkpoints WHERE source_chat_id = ? AND source_topic_key = ?",
            (str(source_chat_id), _topic_key(source_topic_id)),
        )
        return int(cursor.rowcount)

    def source_queue_highwater(self, source_chat_id: int | str) -> int | None:
        """Return the highest source message ID already represented by any queued group."""
        rows = self.query(
            """
            SELECT source_message_id, source_message_ids
            FROM messages
            WHERE source_chat_id = ?
            """,
            (str(source_chat_id),),
        )
        highest: int | None = None
        for row in rows:
            candidates = [int(row["source_message_id"])]
            try:
                decoded = json.loads(row["source_message_ids"] or "[]")
                if isinstance(decoded, list):
                    candidates.extend(int(value) for value in decoded)
            except (TypeError, ValueError, json.JSONDecodeError):
                pass
            row_highest = max(candidates)
            highest = row_highest if highest is None else max(highest, row_highest)
        return highest

    def get_media_cache(self, file_unique_key: str) -> sqlite3.Row | None:
        return self.query_one(
            "SELECT * FROM media_cache WHERE file_unique_key = ?",
            (file_unique_key,),
        )

    def save_media_cache(
        self,
        file_unique_key: str,
        bot_file_ids: list[str],
        media_types: list[str],
    ) -> None:
        if not file_unique_key or not bot_file_ids or len(bot_file_ids) != len(media_types):
            return
        now = utc_now()
        self.execute(
            """
            INSERT INTO media_cache (
                file_unique_key, bot_file_ids, media_types, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(file_unique_key) DO UPDATE SET
                bot_file_ids = excluded.bot_file_ids,
                media_types = excluded.media_types,
                updated_at = excluded.updated_at
            """,
            (
                file_unique_key,
                json.dumps(bot_file_ids),
                json.dumps(media_types),
                now,
                now,
            ),
        )

    def delete_media_cache(self, file_unique_key: str) -> None:
        self.execute("DELETE FROM media_cache WHERE file_unique_key = ?", (file_unique_key,))

    def media_cache_count(self) -> int:
        row = self.query_one("SELECT COUNT(*) AS count FROM media_cache")
        return int(row["count"]) if row else 0
