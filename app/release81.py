from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from sqlite3 import Connection
from typing import Iterable

WAITING_STATES = (
    "ready",
    "downloading",
    "waiting_floodwait",
    "ready_to_publish",
    "uploading",
    "verifying",
    "done",
    "failed",
)

PIPELINE_STAGES = ("scan", "download", "ready", "upload", "verify")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass(frozen=True)
class MigrationPlan:
    total_messages: int
    photos: int
    videos: int
    albums: int
    documents: int
    estimated_download_bytes: int
    temporary_storage_bytes: int
    estimated_seconds: int


class Release81Store:
    """Persistent Release 8.1 state for plans, albums and the waiting pipeline."""

    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    def initialize(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS migration_plans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_chat_id TEXT NOT NULL,
                destinations_json TEXT NOT NULL,
                total_messages INTEGER NOT NULL DEFAULT 0,
                photos INTEGER NOT NULL DEFAULT 0,
                videos INTEGER NOT NULL DEFAULT 0,
                albums INTEGER NOT NULL DEFAULT 0,
                documents INTEGER NOT NULL DEFAULT 0,
                estimated_download_bytes INTEGER NOT NULL DEFAULT 0,
                temporary_storage_bytes INTEGER NOT NULL DEFAULT 0,
                estimated_seconds INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'planned',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS pending_albums (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_chat_id TEXT NOT NULL,
                dest_chat_id TEXT NOT NULL,
                media_group_id TEXT NOT NULL,
                source_message_ids_json TEXT NOT NULL,
                downloaded_message_ids_json TEXT NOT NULL DEFAULT '[]',
                uploaded_message_ids_json TEXT NOT NULL DEFAULT '[]',
                state TEXT NOT NULL DEFAULT 'ready',
                last_error TEXT,
                next_retry_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(source_chat_id, dest_chat_id, media_group_id)
            );

            CREATE TABLE IF NOT EXISTS pipeline_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_key TEXT NOT NULL,
                stage TEXT NOT NULL,
                state TEXT NOT NULL,
                details_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL
            );
            """
        )
        self.connection.commit()

    def create_plan(
        self,
        *,
        source_chat_id: int | str,
        destinations: Iterable[int | str],
        plan: MigrationPlan,
    ) -> int:
        now = utc_now()
        cursor = self.connection.execute(
            """
            INSERT INTO migration_plans (
                source_chat_id, destinations_json, total_messages, photos, videos,
                albums, documents, estimated_download_bytes, temporary_storage_bytes,
                estimated_seconds, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(source_chat_id),
                json.dumps([str(value) for value in destinations]),
                plan.total_messages,
                plan.photos,
                plan.videos,
                plan.albums,
                plan.documents,
                plan.estimated_download_bytes,
                plan.temporary_storage_bytes,
                plan.estimated_seconds,
                now,
                now,
            ),
        )
        self.connection.commit()
        return int(cursor.lastrowid)

    def upsert_album(
        self,
        *,
        source_chat_id: int | str,
        dest_chat_id: int | str,
        media_group_id: str,
        source_message_ids: Iterable[int],
    ) -> int:
        ordered_ids = [int(value) for value in source_message_ids]
        now = utc_now()
        self.connection.execute(
            """
            INSERT INTO pending_albums (
                source_chat_id, dest_chat_id, media_group_id,
                source_message_ids_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_chat_id, dest_chat_id, media_group_id) DO UPDATE SET
                source_message_ids_json = excluded.source_message_ids_json,
                updated_at = excluded.updated_at
            """,
            (str(source_chat_id), str(dest_chat_id), str(media_group_id), json.dumps(ordered_ids), now, now),
        )
        self.connection.commit()
        row = self.connection.execute(
            """
            SELECT id FROM pending_albums
            WHERE source_chat_id = ? AND dest_chat_id = ? AND media_group_id = ?
            """,
            (str(source_chat_id), str(dest_chat_id), str(media_group_id)),
        ).fetchone()
        if row is None:
            raise RuntimeError("Unable to persist pending album")
        return int(row[0])

    def set_album_state(
        self,
        album_id: int,
        state: str,
        *,
        downloaded_message_ids: Iterable[int] | None = None,
        uploaded_message_ids: Iterable[int] | None = None,
        last_error: str | None = None,
        next_retry_at: str | None = None,
    ) -> None:
        if state not in WAITING_STATES:
            raise ValueError(f"Unsupported waiting state: {state}")
        fields = ["state = ?", "last_error = ?", "next_retry_at = ?", "updated_at = ?"]
        values: list[object] = [state, last_error, next_retry_at, utc_now()]
        if downloaded_message_ids is not None:
            fields.append("downloaded_message_ids_json = ?")
            values.append(json.dumps([int(value) for value in downloaded_message_ids]))
        if uploaded_message_ids is not None:
            fields.append("uploaded_message_ids_json = ?")
            values.append(json.dumps([int(value) for value in uploaded_message_ids]))
        values.append(int(album_id))
        self.connection.execute(
            f"UPDATE pending_albums SET {', '.join(fields)} WHERE id = ?",
            tuple(values),
        )
        self.connection.commit()

    def album_ready_to_publish(self, album_id: int) -> bool:
        row = self.connection.execute(
            """
            SELECT source_message_ids_json, downloaded_message_ids_json
            FROM pending_albums WHERE id = ?
            """,
            (int(album_id),),
        ).fetchone()
        if row is None:
            return False
        expected = [int(value) for value in json.loads(row[0])]
        downloaded = {int(value) for value in json.loads(row[1])}
        return bool(expected) and all(value in downloaded for value in expected)

    def record_pipeline_event(
        self,
        *,
        job_key: str,
        stage: str,
        state: str,
        details: dict[str, object] | None = None,
    ) -> int:
        if stage not in PIPELINE_STAGES:
            raise ValueError(f"Unsupported pipeline stage: {stage}")
        cursor = self.connection.execute(
            """
            INSERT INTO pipeline_events (job_key, stage, state, details_json, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (job_key, stage, state, json.dumps(details or {}, sort_keys=True), utc_now()),
        )
        self.connection.commit()
        return int(cursor.lastrowid)

    def waiting_counts(self) -> dict[str, int]:
        counts = {state: 0 for state in WAITING_STATES}
        for state, total in self.connection.execute(
            "SELECT state, COUNT(*) FROM pending_albums GROUP BY state"
        ).fetchall():
            counts[str(state)] = int(total)
        return counts
