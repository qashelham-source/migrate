from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from enum import StrEnum

from app.engine.state_machine import EngineState


class FaultPoint(StrEnum):
    DURING_DOWNLOAD = "during_download"
    AFTER_DOWNLOAD = "after_download"
    DURING_UPLOAD = "during_upload"
    AFTER_TELEGRAM_ACK = "after_telegram_ack"
    DURING_VERIFICATION = "during_verification"
    DATABASE_LOCKED = "database_locked"
    DISK_NEARLY_FULL = "disk_nearly_full"
    DOWNLOAD_FLOODWAIT = "download_floodwait"
    UPLOAD_FLOODWAIT = "upload_floodwait"
    DESTINATION_PERMISSION_LOSS = "destination_permission_loss"
    MISSING_ALBUM_MEMBER = "missing_album_member"
    DUPLICATE_WORKER_CLAIM = "duplicate_worker_claim"
    CONFIGURATION_CHANGED = "configuration_changed"


@dataclass(frozen=True)
class FaultDecision:
    state: EngineState
    retry_allowed: bool
    reconciliation_required: bool = False
    retain_temp_data: bool = True
    reason: str = ""


_DECISIONS: dict[FaultPoint, FaultDecision] = {
    FaultPoint.DURING_DOWNLOAD: FaultDecision(
        EngineState.RETRY_SCHEDULED, True, reason="Interrupted download resumes from durable state"
    ),
    FaultPoint.AFTER_DOWNLOAD: FaultDecision(
        EngineState.READY_TO_UPLOAD, True, reason="Completed download remains ready for upload"
    ),
    FaultPoint.DURING_UPLOAD: FaultDecision(
        EngineState.NEEDS_RECONCILIATION,
        False,
        reconciliation_required=True,
        reason="Upload outcome is ambiguous",
    ),
    FaultPoint.AFTER_TELEGRAM_ACK: FaultDecision(
        EngineState.VERIFYING,
        False,
        reason="Persisted acknowledgement prevents duplicate publication",
    ),
    FaultPoint.DURING_VERIFICATION: FaultDecision(
        EngineState.RETRY_SCHEDULED, True, reason="Verification can be repeated safely"
    ),
    FaultPoint.DATABASE_LOCKED: FaultDecision(
        EngineState.RETRY_SCHEDULED, True, reason="Transient database contention"
    ),
    FaultPoint.DISK_NEARLY_FULL: FaultDecision(
        EngineState.WAITING_STORAGE, True, reason="Download waits for reserved storage"
    ),
    FaultPoint.DOWNLOAD_FLOODWAIT: FaultDecision(
        EngineState.WAITING_FLOODWAIT, True, reason="Source-side FloodWait"
    ),
    FaultPoint.UPLOAD_FLOODWAIT: FaultDecision(
        EngineState.WAITING_FLOODWAIT, True, reason="Destination-side FloodWait"
    ),
    FaultPoint.DESTINATION_PERMISSION_LOSS: FaultDecision(
        EngineState.PAUSED_DESTINATION, True, reason="Destination lane is isolated"
    ),
    FaultPoint.MISSING_ALBUM_MEMBER: FaultDecision(
        EngineState.WAITING_DEPENDENCY, True, reason="Incomplete album cannot publish"
    ),
    FaultPoint.DUPLICATE_WORKER_CLAIM: FaultDecision(
        EngineState.LEASED, False, retain_temp_data=False, reason="Existing lease wins"
    ),
    FaultPoint.CONFIGURATION_CHANGED: FaultDecision(
        EngineState.PLANNED, True, reason="Active job keeps immutable plan snapshot"
    ),
}


def classify_fault(point: FaultPoint | str) -> FaultDecision:
    return _DECISIONS[FaultPoint(point)]


def assert_release_invariants(conn: sqlite3.Connection) -> None:
    """Raise AssertionError when a final Release 11 safety invariant is broken."""
    duplicate_publish = conn.execute(
        """
        SELECT payload_fingerprint, destination_chat_id, COUNT(*)
        FROM publish_intents
        WHERE state IN ('acknowledged', 'reconciled')
        GROUP BY payload_fingerprint, destination_chat_id
        HAVING COUNT(*) > 1
        LIMIT 1
        """
    ).fetchone()
    assert duplicate_publish is None, "duplicate publication detected"

    partial_album = conn.execute(
        """
        SELECT a.id
        FROM album_aggregates a
        JOIN publish_intents p ON p.album_id = a.id
        WHERE p.state IN ('acknowledged', 'reconciled')
          AND (
            a.sealed_at IS NULL OR
            a.expected_count IS NULL OR
            (SELECT COUNT(*) FROM album_members m WHERE m.album_id = a.id) != a.expected_count
          )
        LIMIT 1
        """
    ).fetchone()
    assert partial_album is None, "partial album publication detected"

    lost_job = conn.execute(
        """
        SELECT id FROM messages
        WHERE engine_state IS NULL OR engine_state = ''
        LIMIT 1
        """
    ).fetchone()
    assert lost_job is None, "job without durable engine state detected"

    unsafe_cleanup = conn.execute(
        """
        SELECT t.id
        FROM temporary_artifacts t
        JOIN messages m ON m.id = t.job_id
        WHERE t.deleted_at IS NOT NULL
          AND m.engine_state NOT IN ('committed', 'cleaning', 'done')
        LIMIT 1
        """
    ).fetchone()
    assert unsafe_cleanup is None, "temporary data deleted before verified commit"
