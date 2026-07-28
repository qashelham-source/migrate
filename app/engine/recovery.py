from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone

from app.engine.state_machine import EngineState


@dataclass(frozen=True)
class RecoverySummary:
    retry_scheduled: int = 0
    reconciliation: int = 0
    released_leases: int = 0


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def recover_expired_jobs(conn: sqlite3.Connection, *, now: str | None = None) -> RecoverySummary:
    """Recover only jobs whose lease has expired.

    Downloads can be retried from their durable checkpoint. Upload-related states
    are never blindly requeued because Telegram may already have accepted media;
    they are routed to reconciliation instead.
    """
    current = now or utc_now()
    conn.execute("BEGIN IMMEDIATE")
    try:
        retry_cursor = conn.execute(
            """
            UPDATE messages
            SET engine_state = ?,
                lease_owner = NULL,
                lease_token = NULL,
                lease_started_at = NULL,
                lease_expires_at = NULL,
                heartbeat_at = NULL,
                state_version = state_version + 1,
                last_error = COALESCE(last_error, 'Recovered after interrupted pre-upload work'),
                updated_at = ?
            WHERE lease_expires_at IS NOT NULL
              AND lease_expires_at <= ?
              AND engine_state IN (?, ?, ?, ?)
            """,
            (
                EngineState.RETRY_SCHEDULED.value,
                current,
                current,
                EngineState.LEASED.value,
                EngineState.DOWNLOADING.value,
                EngineState.DOWNLOADED.value,
                EngineState.READY_TO_UPLOAD.value,
            ),
        )
        reconcile_cursor = conn.execute(
            """
            UPDATE messages
            SET engine_state = ?,
                lease_owner = NULL,
                lease_token = NULL,
                lease_started_at = NULL,
                lease_expires_at = NULL,
                heartbeat_at = NULL,
                state_version = state_version + 1,
                last_error = COALESCE(last_error, 'Interrupted upload requires reconciliation'),
                updated_at = ?
            WHERE lease_expires_at IS NOT NULL
              AND lease_expires_at <= ?
              AND engine_state IN (?, ?, ?)
            """,
            (
                EngineState.NEEDS_RECONCILIATION.value,
                current,
                current,
                EngineState.UPLOADING.value,
                EngineState.UPLOADED_UNCONFIRMED.value,
                EngineState.VERIFYING.value,
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    retry_count = int(retry_cursor.rowcount)
    reconcile_count = int(reconcile_cursor.rowcount)
    return RecoverySummary(
        retry_scheduled=retry_count,
        reconciliation=reconcile_count,
        released_leases=retry_count + reconcile_count,
    )
