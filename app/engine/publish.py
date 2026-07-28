from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Iterable, Sequence


class PublishState(str, Enum):
    INTENT_RECORDED = "intent_recorded"
    SEND_STARTED = "send_started"
    ACKNOWLEDGED = "acknowledged"
    UPLOADED_UNCONFIRMED = "uploaded_unconfirmed"
    NEEDS_RECONCILIATION = "needs_reconciliation"
    RECONCILED = "reconciled"
    QUARANTINED = "quarantined"


@dataclass(frozen=True)
class DestinationEvidence:
    message_ids: tuple[int, ...]
    confidence: float
    fingerprint: str | None = None
    detail: str | None = None


@dataclass(frozen=True)
class ReconciliationResult:
    state: PublishState
    accepted: bool
    destination_message_ids: tuple[int, ...]
    confidence: float


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def initialize_publish_schema(conn: sqlite3.Connection) -> None:
    """Install additive Phase 4 publish-intent and reconciliation tables."""
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS publish_intents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            intent_key TEXT NOT NULL UNIQUE,
            job_id INTEGER REFERENCES messages(id) ON DELETE CASCADE,
            album_id INTEGER REFERENCES album_aggregates(id) ON DELETE CASCADE,
            destination_chat_id TEXT NOT NULL,
            destination_topic_id INTEGER,
            operation TEXT NOT NULL,
            payload_fingerprint TEXT NOT NULL,
            expected_item_count INTEGER NOT NULL DEFAULT 1,
            state TEXT NOT NULL,
            send_attempt_no INTEGER NOT NULL DEFAULT 0,
            destination_message_ids TEXT,
            telegram_ack_payload TEXT,
            created_at TEXT NOT NULL,
            send_started_at TEXT,
            acknowledged_at TEXT,
            reconciled_at TEXT,
            updated_at TEXT NOT NULL,
            quarantine_reason TEXT,
            CHECK(job_id IS NOT NULL OR album_id IS NOT NULL)
        );

        CREATE TABLE IF NOT EXISTS reconciliation_probes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            publish_intent_id INTEGER NOT NULL REFERENCES publish_intents(id) ON DELETE CASCADE,
            probe_no INTEGER NOT NULL,
            evidence_message_ids TEXT,
            evidence_fingerprint TEXT,
            confidence REAL NOT NULL,
            detail TEXT,
            decision TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(publish_intent_id, probe_no)
        );

        CREATE INDEX IF NOT EXISTS idx_publish_intents_state
            ON publish_intents(state, updated_at);
        CREATE INDEX IF NOT EXISTS idx_publish_intents_job
            ON publish_intents(job_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_publish_intents_album
            ON publish_intents(album_id, created_at);
        """
    )
    conn.commit()


def create_publish_intent(
    conn: sqlite3.Connection,
    *,
    destination_chat_id: str,
    operation: str,
    payload_fingerprint: str,
    job_id: int | None = None,
    album_id: int | None = None,
    destination_topic_id: int | None = None,
    expected_item_count: int = 1,
    intent_key: str | None = None,
) -> int:
    if job_id is None and album_id is None:
        raise ValueError("Publish intent requires job_id or album_id")
    if expected_item_count < 1:
        raise ValueError("expected_item_count must be positive")
    key = intent_key or uuid.uuid4().hex
    now = utc_now()
    conn.execute(
        """
        INSERT INTO publish_intents(
            intent_key, job_id, album_id, destination_chat_id, destination_topic_id,
            operation, payload_fingerprint, expected_item_count, state, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(intent_key) DO NOTHING
        """,
        (
            key, job_id, album_id, str(destination_chat_id), destination_topic_id,
            operation, payload_fingerprint, int(expected_item_count),
            PublishState.INTENT_RECORDED.value, now, now,
        ),
    )
    row = conn.execute("SELECT id FROM publish_intents WHERE intent_key = ?", (key,)).fetchone()
    conn.commit()
    if row is None:
        raise RuntimeError("Publish intent was not created")
    return int(row[0])


def mark_send_started(conn: sqlite3.Connection, *, intent_id: int) -> bool:
    now = utc_now()
    cursor = conn.execute(
        """
        UPDATE publish_intents
        SET state = ?, send_attempt_no = send_attempt_no + 1,
            send_started_at = ?, updated_at = ?
        WHERE id = ? AND state IN (?, ?)
        """,
        (
            PublishState.SEND_STARTED.value, now, now, intent_id,
            PublishState.INTENT_RECORDED.value, PublishState.NEEDS_RECONCILIATION.value,
        ),
    )
    conn.commit()
    return cursor.rowcount == 1


def record_telegram_ack(
    conn: sqlite3.Connection,
    *,
    intent_id: int,
    destination_message_ids: Iterable[int],
    ack_payload: object | None = None,
) -> bool:
    ids = tuple(int(value) for value in destination_message_ids)
    if not ids:
        raise ValueError("Telegram acknowledgement must contain destination message ids")
    now = utc_now()
    cursor = conn.execute(
        """
        UPDATE publish_intents
        SET state = ?, destination_message_ids = ?, telegram_ack_payload = ?,
            acknowledged_at = ?, updated_at = ?
        WHERE id = ? AND state IN (?, ?, ?)
        """,
        (
            PublishState.ACKNOWLEDGED.value,
            json.dumps(ids),
            json.dumps(ack_payload, sort_keys=True, default=str) if ack_payload is not None else None,
            now, now, intent_id,
            PublishState.SEND_STARTED.value,
            PublishState.UPLOADED_UNCONFIRMED.value,
            PublishState.NEEDS_RECONCILIATION.value,
        ),
    )
    conn.commit()
    return cursor.rowcount == 1


def mark_uploaded_unconfirmed(conn: sqlite3.Connection, *, intent_id: int) -> bool:
    now = utc_now()
    cursor = conn.execute(
        """
        UPDATE publish_intents
        SET state = ?, updated_at = ?
        WHERE id = ? AND state = ?
        """,
        (PublishState.UPLOADED_UNCONFIRMED.value, now, intent_id, PublishState.SEND_STARTED.value),
    )
    conn.commit()
    return cursor.rowcount == 1


def require_reconciliation(conn: sqlite3.Connection, *, intent_id: int) -> bool:
    now = utc_now()
    cursor = conn.execute(
        """
        UPDATE publish_intents
        SET state = ?, updated_at = ?
        WHERE id = ? AND state IN (?, ?)
        """,
        (
            PublishState.NEEDS_RECONCILIATION.value, now, intent_id,
            PublishState.SEND_STARTED.value, PublishState.UPLOADED_UNCONFIRMED.value,
        ),
    )
    conn.commit()
    return cursor.rowcount == 1


def may_retry_send(conn: sqlite3.Connection, *, intent_id: int) -> bool:
    row = conn.execute("SELECT state FROM publish_intents WHERE id = ?", (intent_id,)).fetchone()
    if row is None:
        raise KeyError(f"Unknown publish intent: {intent_id}")
    return str(row[0]) == PublishState.INTENT_RECORDED.value


def reconcile_publish_intent(
    conn: sqlite3.Connection,
    *,
    intent_id: int,
    evidence: DestinationEvidence,
    accept_threshold: float = 0.95,
) -> ReconciliationResult:
    row = conn.execute(
        "SELECT state, payload_fingerprint, expected_item_count FROM publish_intents WHERE id = ?",
        (intent_id,),
    ).fetchone()
    if row is None:
        raise KeyError(f"Unknown publish intent: {intent_id}")
    if str(row[0]) not in {
        PublishState.NEEDS_RECONCILIATION.value,
        PublishState.UPLOADED_UNCONFIRMED.value,
    }:
        raise ValueError("Publish intent is not awaiting reconciliation")

    ids = tuple(int(value) for value in evidence.message_ids)
    fingerprint_match = evidence.fingerprint is None or evidence.fingerprint == str(row[1])
    count_match = len(ids) == int(row[2])
    accepted = bool(ids) and count_match and fingerprint_match and evidence.confidence >= accept_threshold
    state = PublishState.RECONCILED if accepted else PublishState.QUARANTINED
    decision = "accepted" if accepted else "quarantined"
    reason = None if accepted else "destination evidence was not confident and complete"
    probe_no = int(
        conn.execute(
            "SELECT COALESCE(MAX(probe_no), 0) + 1 FROM reconciliation_probes WHERE publish_intent_id = ?",
            (intent_id,),
        ).fetchone()[0]
    )
    now = utc_now()
    conn.execute(
        """
        INSERT INTO reconciliation_probes(
            publish_intent_id, probe_no, evidence_message_ids, evidence_fingerprint,
            confidence, detail, decision, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            intent_id, probe_no, json.dumps(ids), evidence.fingerprint,
            float(evidence.confidence), evidence.detail, decision, now,
        ),
    )
    conn.execute(
        """
        UPDATE publish_intents
        SET state = ?, destination_message_ids = ?, reconciled_at = ?,
            updated_at = ?, quarantine_reason = ?
        WHERE id = ?
        """,
        (state.value, json.dumps(ids) if ids else None, now, now, reason, intent_id),
    )
    conn.commit()
    return ReconciliationResult(state, accepted, ids, float(evidence.confidence))


def destination_message_ids(conn: sqlite3.Connection, *, intent_id: int) -> tuple[int, ...]:
    row = conn.execute(
        "SELECT destination_message_ids FROM publish_intents WHERE id = ?", (intent_id,)
    ).fetchone()
    if row is None:
        raise KeyError(f"Unknown publish intent: {intent_id}")
    if not row[0]:
        return ()
    return tuple(int(value) for value in json.loads(str(row[0])))
