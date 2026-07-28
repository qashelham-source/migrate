from __future__ import annotations

import os
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from app.engine.state_machine import EngineState


@dataclass(frozen=True)
class VerificationResult:
    acknowledged: bool
    structural_ok: bool
    content_confidence: float
    verified: bool
    reason: str | None = None


@dataclass(frozen=True)
class ReaperSummary:
    scanned: int = 0
    deleted: int = 0
    retained: int = 0


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def initialize_verification_schema(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS verification_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
            publish_intent_id INTEGER REFERENCES publish_intents(id) ON DELETE SET NULL,
            acknowledged INTEGER NOT NULL CHECK(acknowledged IN (0, 1)),
            structural_ok INTEGER NOT NULL CHECK(structural_ok IN (0, 1)),
            content_confidence REAL NOT NULL,
            verified INTEGER NOT NULL CHECK(verified IN (0, 1)),
            reason TEXT,
            created_at TEXT NOT NULL,
            UNIQUE(job_id, publish_intent_id)
        );

        CREATE TABLE IF NOT EXISTS temp_artifacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
            path TEXT NOT NULL UNIQUE,
            policy TEXT NOT NULL DEFAULT 'delete_after_commit',
            created_at TEXT NOT NULL,
            deleted_at TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_verification_job
            ON verification_records(job_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_temp_artifacts_job
            ON temp_artifacts(job_id, deleted_at);
        """
    )
    conn.commit()


def register_temp_artifact(conn: sqlite3.Connection, *, job_id: int, path: str, policy: str = "delete_after_commit") -> int:
    now = utc_now()
    conn.execute(
        """
        INSERT INTO temp_artifacts(job_id, path, policy, created_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(path) DO UPDATE SET job_id = excluded.job_id, policy = excluded.policy
        """,
        (job_id, path, policy, now),
    )
    row = conn.execute("SELECT id FROM temp_artifacts WHERE path = ?", (path,)).fetchone()
    conn.commit()
    return int(row[0])


def verify_job(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    publish_intent_id: int,
    expected_item_count: int,
    observed_item_count: int,
    content_confidence: float,
    confidence_threshold: float = 0.95,
) -> VerificationResult:
    intent = conn.execute(
        "SELECT state, destination_message_ids FROM publish_intents WHERE id = ?",
        (publish_intent_id,),
    ).fetchone()
    if intent is None:
        raise KeyError(f"Unknown publish intent: {publish_intent_id}")
    acknowledged = str(intent[0]) in {"acknowledged", "reconciled"} and bool(intent[1])
    structural_ok = expected_item_count > 0 and observed_item_count == expected_item_count
    confidence = max(0.0, min(1.0, float(content_confidence)))
    verified = acknowledged and structural_ok and confidence >= confidence_threshold
    reason = None
    if not acknowledged:
        reason = "destination acknowledgement is missing"
    elif not structural_ok:
        reason = "destination structure does not match the publish manifest"
    elif confidence < confidence_threshold:
        reason = "content confidence is below threshold"
    result = VerificationResult(acknowledged, structural_ok, confidence, verified, reason)
    conn.execute(
        """
        INSERT INTO verification_records(
            job_id, publish_intent_id, acknowledged, structural_ok,
            content_confidence, verified, reason, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(job_id, publish_intent_id) DO UPDATE SET
            acknowledged = excluded.acknowledged,
            structural_ok = excluded.structural_ok,
            content_confidence = excluded.content_confidence,
            verified = excluded.verified,
            reason = excluded.reason,
            created_at = excluded.created_at
        """,
        (job_id, publish_intent_id, int(acknowledged), int(structural_ok), confidence, int(verified), reason, utc_now()),
    )
    conn.commit()
    return result


def commit_verified_job(conn: sqlite3.Connection, *, job_id: int) -> bool:
    record = conn.execute(
        "SELECT verified FROM verification_records WHERE job_id = ? ORDER BY created_at DESC LIMIT 1",
        (job_id,),
    ).fetchone()
    if record is None or int(record[0]) != 1:
        return False
    cursor = conn.execute(
        """
        UPDATE messages
        SET engine_state = ?, verified_at = COALESCE(verified_at, ?),
            state_version = state_version + 1, updated_at = ?
        WHERE id = ? AND engine_state IN (?, ?)
        """,
        (EngineState.COMMITTED.value, utc_now(), utc_now(), job_id,
         EngineState.VERIFYING.value, EngineState.VERIFIED.value),
    )
    conn.commit()
    return cursor.rowcount == 1


def cleanup_committed_job(conn: sqlite3.Connection, *, job_id: int) -> int:
    row = conn.execute("SELECT engine_state FROM messages WHERE id = ?", (job_id,)).fetchone()
    if row is None:
        raise KeyError(f"Unknown job: {job_id}")
    if str(row[0]) not in {EngineState.COMMITTED.value, EngineState.CLEANING.value, EngineState.DONE.value}:
        return 0
    artifacts = conn.execute(
        "SELECT id, path FROM temp_artifacts WHERE job_id = ? AND deleted_at IS NULL",
        (job_id,),
    ).fetchall()
    deleted = 0
    for artifact_id, raw_path in artifacts:
        path = Path(str(raw_path))
        if path.is_dir():
            shutil.rmtree(path)
        elif path.exists():
            path.unlink()
        conn.execute("UPDATE temp_artifacts SET deleted_at = ? WHERE id = ?", (utc_now(), artifact_id))
        deleted += 1
    conn.execute(
        "UPDATE messages SET engine_state = ?, state_version = state_version + 1, updated_at = ? WHERE id = ?",
        (EngineState.DONE.value, utc_now(), job_id),
    )
    conn.commit()
    return deleted


def reap_stale_directories(conn: sqlite3.Connection, *, root: str, older_than_epoch: float) -> ReaperSummary:
    base = Path(root)
    if not base.exists():
        return ReaperSummary()
    scanned = deleted = retained = 0
    for candidate in base.iterdir():
        if not candidate.is_dir() or candidate.stat().st_mtime > older_than_epoch:
            continue
        scanned += 1
        artifact = conn.execute(
            "SELECT job_id, deleted_at FROM temp_artifacts WHERE path = ?",
            (str(candidate),),
        ).fetchone()
        if artifact is None:
            shutil.rmtree(candidate)
            deleted += 1
            continue
        job_id = int(artifact[0])
        owner = conn.execute(
            "SELECT engine_state, lease_token, lease_expires_at FROM messages WHERE id = ?",
            (job_id,),
        ).fetchone()
        unresolved = conn.execute(
            """
            SELECT 1 FROM publish_intents
            WHERE job_id = ? AND state IN ('intent_recorded','send_started','uploaded_unconfirmed','needs_reconciliation','quarantined')
            LIMIT 1
            """,
            (job_id,),
        ).fetchone()
        active_lease = bool(owner and owner[1] and owner[2] and str(owner[2]) > utc_now())
        safe_state = bool(owner and str(owner[0]) in {EngineState.COMMITTED.value, EngineState.CLEANING.value, EngineState.DONE.value})
        if active_lease or unresolved or not safe_state:
            retained += 1
            continue
        shutil.rmtree(candidate)
        conn.execute("UPDATE temp_artifacts SET deleted_at = ? WHERE path = ?", (utc_now(), str(candidate)))
        conn.commit()
        deleted += 1
    return ReaperSummary(scanned, deleted, retained)
