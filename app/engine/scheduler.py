from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from app.engine.state_machine import EngineState


@dataclass(frozen=True)
class JobLease:
    job_id: int
    owner: str
    token: str
    state_version: int
    expires_at: str


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _stamp(value: datetime) -> str:
    return value.isoformat(timespec="seconds")


class LeaseScheduler:
    """Atomic SQLite job claiming for the durable engine.

    Claiming uses BEGIN IMMEDIATE so selecting and assigning a lease happen in one
    transaction. Every mutation checks both token and state_version, preventing a
    stale worker from writing after its lease has expired or been replaced.
    """

    CLAIMABLE = (
        EngineState.PLANNED.value,
        EngineState.RETRY_SCHEDULED.value,
        EngineState.WAITING_FLOODWAIT.value,
    )

    def __init__(self, conn: sqlite3.Connection, *, lease_seconds: int = 120) -> None:
        self.conn = conn
        self.lease_seconds = max(10, int(lease_seconds))

    def claim_next(self, owner: str) -> JobLease | None:
        if not owner.strip():
            raise ValueError("lease owner is required")
        now = _now()
        now_text = _stamp(now)
        expires_at = _stamp(now + timedelta(seconds=self.lease_seconds))
        token = uuid.uuid4().hex

        self.conn.execute("BEGIN IMMEDIATE")
        try:
            row = self.conn.execute(
                """
                SELECT id, engine_state, state_version
                FROM messages
                WHERE engine_state IN (?, ?, ?)
                  AND (next_retry_at IS NULL OR next_retry_at <= ?)
                  AND (lease_expires_at IS NULL OR lease_expires_at <= ?)
                ORDER BY updated_at ASC, id ASC
                LIMIT 1
                """,
                (*self.CLAIMABLE, now_text, now_text),
            ).fetchone()
            if row is None:
                self.conn.commit()
                return None

            cursor = self.conn.execute(
                """
                UPDATE messages
                SET engine_state = ?,
                    lease_owner = ?,
                    lease_token = ?,
                    lease_started_at = ?,
                    lease_expires_at = ?,
                    heartbeat_at = ?,
                    state_version = state_version + 1,
                    updated_at = ?
                WHERE id = ?
                  AND state_version = ?
                  AND engine_state = ?
                  AND (lease_expires_at IS NULL OR lease_expires_at <= ?)
                """,
                (
                    EngineState.LEASED.value,
                    owner,
                    token,
                    now_text,
                    expires_at,
                    now_text,
                    now_text,
                    int(row["id"]),
                    int(row["state_version"]),
                    str(row["engine_state"]),
                    now_text,
                ),
            )
            if cursor.rowcount != 1:
                self.conn.rollback()
                return None
            self.conn.commit()
            return JobLease(
                job_id=int(row["id"]),
                owner=owner,
                token=token,
                state_version=int(row["state_version"]) + 1,
                expires_at=expires_at,
            )
        except Exception:
            self.conn.rollback()
            raise

    def heartbeat(self, lease: JobLease) -> JobLease | None:
        now = _now()
        now_text = _stamp(now)
        expires_at = _stamp(now + timedelta(seconds=self.lease_seconds))
        cursor = self.conn.execute(
            """
            UPDATE messages
            SET heartbeat_at = ?, lease_expires_at = ?, updated_at = ?
            WHERE id = ?
              AND lease_owner = ?
              AND lease_token = ?
              AND state_version = ?
              AND lease_expires_at > ?
            """,
            (
                now_text,
                expires_at,
                now_text,
                lease.job_id,
                lease.owner,
                lease.token,
                lease.state_version,
                now_text,
            ),
        )
        self.conn.commit()
        if cursor.rowcount != 1:
            return None
        return JobLease(lease.job_id, lease.owner, lease.token, lease.state_version, expires_at)

    def transition(self, lease: JobLease, target: EngineState) -> JobLease | None:
        now_text = _stamp(_now())
        cursor = self.conn.execute(
            """
            UPDATE messages
            SET engine_state = ?, state_version = state_version + 1, updated_at = ?
            WHERE id = ?
              AND engine_state = ?
              AND lease_owner = ?
              AND lease_token = ?
              AND state_version = ?
              AND lease_expires_at > ?
            """,
            (
                target.value,
                now_text,
                lease.job_id,
                EngineState.LEASED.value,
                lease.owner,
                lease.token,
                lease.state_version,
                now_text,
            ),
        )
        self.conn.commit()
        if cursor.rowcount != 1:
            return None
        return JobLease(lease.job_id, lease.owner, lease.token, lease.state_version + 1, lease.expires_at)

    def release(self, lease: JobLease, *, target: EngineState = EngineState.RETRY_SCHEDULED) -> bool:
        now_text = _stamp(_now())
        cursor = self.conn.execute(
            """
            UPDATE messages
            SET engine_state = ?,
                lease_owner = NULL,
                lease_token = NULL,
                lease_started_at = NULL,
                lease_expires_at = NULL,
                heartbeat_at = NULL,
                state_version = state_version + 1,
                updated_at = ?
            WHERE id = ?
              AND lease_owner = ?
              AND lease_token = ?
              AND state_version = ?
            """,
            (target.value, now_text, lease.job_id, lease.owner, lease.token, lease.state_version),
        )
        self.conn.commit()
        return cursor.rowcount == 1
