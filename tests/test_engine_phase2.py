from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

from app.engine.recovery import recover_expired_jobs
from app.engine.schema import initialize_engine_schema
from app.engine.scheduler import LeaseScheduler
from app.engine.state_machine import EngineState


def _stamp(delta_seconds: int = 0) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=delta_seconds)).isoformat(timespec="seconds")


def _database() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_chat_id TEXT NOT NULL,
            source_message_id INTEGER NOT NULL,
            dest_chat_id TEXT NOT NULL,
            status TEXT NOT NULL,
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
        )
        """
    )
    initialize_engine_schema(conn)
    return conn


def _insert_job(conn: sqlite3.Connection, *, state: EngineState = EngineState.PLANNED) -> int:
    now = _stamp()
    cursor = conn.execute(
        """
        INSERT INTO messages (
            source_chat_id, source_message_id, dest_chat_id, status,
            file_unique_key, created_at, updated_at, source_message_ids, engine_state
        ) VALUES ('source', 1, 'dest', 'pending', ?, ?, ?, '[1]', ?)
        """,
        (f"key-{now}-{state.value}", now, now, state.value),
    )
    conn.commit()
    return int(cursor.lastrowid)


def test_claim_is_atomic_and_second_worker_cannot_claim_same_job() -> None:
    conn = _database()
    job_id = _insert_job(conn)
    scheduler = LeaseScheduler(conn, lease_seconds=60)

    first = scheduler.claim_next("worker-a")
    second = scheduler.claim_next("worker-b")

    assert first is not None
    assert first.job_id == job_id
    assert second is None
    row = conn.execute("SELECT engine_state, lease_owner FROM messages WHERE id = ?", (job_id,)).fetchone()
    assert row["engine_state"] == EngineState.LEASED.value
    assert row["lease_owner"] == "worker-a"


def test_stale_worker_cannot_heartbeat_or_release_replaced_lease() -> None:
    conn = _database()
    job_id = _insert_job(conn)
    scheduler = LeaseScheduler(conn, lease_seconds=60)
    stale = scheduler.claim_next("worker-a")
    assert stale is not None

    conn.execute(
        """
        UPDATE messages
        SET lease_owner = 'worker-b', lease_token = 'replacement', state_version = state_version + 1
        WHERE id = ?
        """,
        (job_id,),
    )
    conn.commit()

    assert scheduler.heartbeat(stale) is None
    assert scheduler.release(stale) is False
    row = conn.execute("SELECT lease_owner, lease_token FROM messages WHERE id = ?", (job_id,)).fetchone()
    assert row["lease_owner"] == "worker-b"
    assert row["lease_token"] == "replacement"


def test_expired_download_is_retryable_but_upload_is_reconciled() -> None:
    conn = _database()
    download_id = _insert_job(conn, state=EngineState.DOWNLOADING)
    upload_id = _insert_job(conn, state=EngineState.UPLOADING)
    expired = _stamp(-120)
    for job_id in (download_id, upload_id):
        conn.execute(
            """
            UPDATE messages
            SET lease_owner = 'dead-worker', lease_token = ?, lease_expires_at = ?, heartbeat_at = ?
            WHERE id = ?
            """,
            (f"expired-{job_id}", expired, expired, job_id),
        )
    conn.commit()

    summary = recover_expired_jobs(conn)

    assert summary.retry_scheduled == 1
    assert summary.reconciliation == 1
    assert summary.released_leases == 2
    download = conn.execute("SELECT * FROM messages WHERE id = ?", (download_id,)).fetchone()
    upload = conn.execute("SELECT * FROM messages WHERE id = ?", (upload_id,)).fetchone()
    assert download["engine_state"] == EngineState.RETRY_SCHEDULED.value
    assert upload["engine_state"] == EngineState.NEEDS_RECONCILIATION.value
    assert download["lease_token"] is None
    assert upload["lease_token"] is None


def test_live_lease_is_not_recovered() -> None:
    conn = _database()
    job_id = _insert_job(conn, state=EngineState.UPLOADING)
    conn.execute(
        """
        UPDATE messages
        SET lease_owner = 'live-worker', lease_token = 'live-token',
            lease_expires_at = ?, heartbeat_at = ?
        WHERE id = ?
        """,
        (_stamp(120), _stamp(), job_id),
    )
    conn.commit()

    summary = recover_expired_jobs(conn)

    assert summary.released_leases == 0
    row = conn.execute("SELECT engine_state, lease_token FROM messages WHERE id = ?", (job_id,)).fetchone()
    assert row["engine_state"] == EngineState.UPLOADING.value
    assert row["lease_token"] == "live-token"
