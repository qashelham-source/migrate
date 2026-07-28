from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Iterable

from app.engine.state_machine import EngineState, normalize_state, require_transition


ENGINE_SCHEMA_VERSION = 1


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}


def _add_column(conn: sqlite3.Connection, table: str, definition: str) -> None:
    name = definition.split()[0]
    if name not in _columns(conn, table):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {definition}")


def initialize_engine_schema(conn: sqlite3.Connection) -> None:
    """Install the additive Release 11 phase-1 schema.

    The migration is deliberately idempotent and preserves all legacy columns and
    status values. Runtime adoption happens in later phases.
    """
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS engine_schema_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS migration_plans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            plan_key TEXT NOT NULL UNIQUE,
            source_chat_id TEXT NOT NULL,
            dest_chat_id TEXT NOT NULL,
            source_topic_id INTEGER,
            dest_topic_id INTEGER,
            config_snapshot TEXT NOT NULL,
            source_revision_fingerprint TEXT,
            created_at TEXT NOT NULL,
            sealed_at TEXT
        );

        CREATE TABLE IF NOT EXISTS migration_plan_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            plan_id INTEGER NOT NULL REFERENCES migration_plans(id) ON DELETE CASCADE,
            ordinal INTEGER NOT NULL,
            source_message_id INTEGER NOT NULL,
            media_group_id TEXT,
            media_type TEXT,
            expected_size INTEGER,
            caption_owner INTEGER NOT NULL DEFAULT 0 CHECK(caption_owner IN (0, 1)),
            item_fingerprint TEXT,
            created_at TEXT NOT NULL,
            UNIQUE(plan_id, ordinal),
            UNIQUE(plan_id, source_message_id)
        );

        CREATE TABLE IF NOT EXISTS job_attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
            attempt_no INTEGER NOT NULL,
            lease_token TEXT,
            phase TEXT NOT NULL,
            outcome TEXT,
            error_class TEXT,
            error_text TEXT,
            bytes_downloaded INTEGER NOT NULL DEFAULT 0,
            bytes_uploaded INTEGER NOT NULL DEFAULT 0,
            destination_message_ids TEXT,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            UNIQUE(job_id, attempt_no)
        );

        CREATE INDEX IF NOT EXISTS idx_plan_items_plan_order
            ON migration_plan_items(plan_id, ordinal);
        CREATE INDEX IF NOT EXISTS idx_job_attempts_job
            ON job_attempts(job_id, attempt_no);
        """
    )

    message_columns: Iterable[str] = (
        "engine_state TEXT",
        "plan_id INTEGER REFERENCES migration_plans(id)",
        "lease_owner TEXT",
        "lease_token TEXT",
        "lease_started_at TEXT",
        "lease_expires_at TEXT",
        "heartbeat_at TEXT",
        "active_attempt_id INTEGER REFERENCES job_attempts(id)",
        "state_version INTEGER NOT NULL DEFAULT 0",
    )
    for definition in message_columns:
        _add_column(conn, "messages", definition)

    conn.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_messages_engine_due
            ON messages(engine_state, next_retry_at, updated_at);
        CREATE INDEX IF NOT EXISTS idx_messages_lease_expiry
            ON messages(lease_expires_at, lease_owner);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_active_lease_token
            ON messages(lease_token)
            WHERE lease_token IS NOT NULL;
        """
    )

    conn.execute(
        """
        UPDATE messages
        SET engine_state = CASE status
            WHEN 'pending' THEN 'planned'
            WHEN 'downloading' THEN 'downloading'
            WHEN 'uploading' THEN 'uploading'
            WHEN 'copied' THEN 'verifying'
            WHEN 'failed' THEN 'failed_permanent'
            WHEN 'skipped' THEN 'cancelled'
            ELSE 'discovered'
        END
        WHERE engine_state IS NULL
        """
    )
    now = utc_now()
    conn.execute(
        """
        INSERT INTO engine_schema_meta(key, value, updated_at)
        VALUES('engine_schema_version', ?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
        """,
        (str(ENGINE_SCHEMA_VERSION), now),
    )
    conn.commit()


def compare_and_swap_state(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    expected_state: str | EngineState,
    target_state: str | EngineState,
    expected_version: int,
    lease_token: str | None = None,
) -> bool:
    source = normalize_state(expected_state)
    target = normalize_state(target_state)
    require_transition(source, target)

    clauses = ["id = ?", "engine_state = ?", "state_version = ?"]
    params: list[object] = [target.value, utc_now(), job_id, source.value, int(expected_version)]
    if lease_token is not None:
        clauses.append("lease_token = ?")
        params.append(lease_token)

    cursor = conn.execute(
        f"""
        UPDATE messages
        SET engine_state = ?, updated_at = ?, state_version = state_version + 1
        WHERE {' AND '.join(clauses)}
        """,
        params,
    )
    conn.commit()
    return cursor.rowcount == 1
