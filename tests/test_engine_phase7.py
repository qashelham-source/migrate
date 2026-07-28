from __future__ import annotations

import sqlite3

import pytest

from app.engine.fault_injection import FaultPoint, assert_release_invariants, classify_fault
from app.engine.state_machine import EngineState


def make_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            engine_state TEXT
        );
        CREATE TABLE album_aggregates (
            id INTEGER PRIMARY KEY,
            expected_count INTEGER,
            sealed_at TEXT
        );
        CREATE TABLE album_members (
            id INTEGER PRIMARY KEY,
            album_id INTEGER NOT NULL
        );
        CREATE TABLE publish_intents (
            id INTEGER PRIMARY KEY,
            album_id INTEGER,
            destination_chat_id TEXT NOT NULL,
            payload_fingerprint TEXT NOT NULL,
            state TEXT NOT NULL
        );
        CREATE TABLE temporary_artifacts (
            id INTEGER PRIMARY KEY,
            job_id INTEGER NOT NULL,
            deleted_at TEXT
        );
        """
    )
    return conn


@pytest.mark.parametrize(
    ("point", "state", "retry", "reconcile"),
    [
        (FaultPoint.DURING_DOWNLOAD, EngineState.RETRY_SCHEDULED, True, False),
        (FaultPoint.AFTER_DOWNLOAD, EngineState.READY_TO_UPLOAD, True, False),
        (FaultPoint.DURING_UPLOAD, EngineState.NEEDS_RECONCILIATION, False, True),
        (FaultPoint.AFTER_TELEGRAM_ACK, EngineState.VERIFYING, False, False),
        (FaultPoint.DURING_VERIFICATION, EngineState.RETRY_SCHEDULED, True, False),
        (FaultPoint.DATABASE_LOCKED, EngineState.RETRY_SCHEDULED, True, False),
        (FaultPoint.DISK_NEARLY_FULL, EngineState.WAITING_STORAGE, True, False),
        (FaultPoint.DOWNLOAD_FLOODWAIT, EngineState.WAITING_FLOODWAIT, True, False),
        (FaultPoint.UPLOAD_FLOODWAIT, EngineState.WAITING_FLOODWAIT, True, False),
        (FaultPoint.DESTINATION_PERMISSION_LOSS, EngineState.PAUSED_DESTINATION, True, False),
        (FaultPoint.MISSING_ALBUM_MEMBER, EngineState.WAITING_DEPENDENCY, True, False),
        (FaultPoint.DUPLICATE_WORKER_CLAIM, EngineState.LEASED, False, False),
        (FaultPoint.CONFIGURATION_CHANGED, EngineState.PLANNED, True, False),
    ],
)
def test_fault_matrix(point, state, retry, reconcile) -> None:
    decision = classify_fault(point)
    assert decision.state is state
    assert decision.retry_allowed is retry
    assert decision.reconciliation_required is reconcile


def test_ambiguous_upload_never_allows_blind_retry() -> None:
    decision = classify_fault(FaultPoint.DURING_UPLOAD)
    assert decision.retry_allowed is False
    assert decision.reconciliation_required is True


def test_acknowledged_send_routes_to_verification_without_duplicate_retry() -> None:
    decision = classify_fault(FaultPoint.AFTER_TELEGRAM_ACK)
    assert decision.state is EngineState.VERIFYING
    assert decision.retry_allowed is False


def test_release_invariants_accept_healthy_database() -> None:
    conn = make_db()
    conn.execute("INSERT INTO messages VALUES (1, 'done')")
    conn.execute("INSERT INTO temporary_artifacts VALUES (1, 1, 'now')")
    assert_release_invariants(conn)


def test_duplicate_publication_is_rejected() -> None:
    conn = make_db()
    conn.execute("INSERT INTO messages VALUES (1, 'done')")
    conn.executemany(
        "INSERT INTO publish_intents VALUES (?, NULL, '-1002', 'same', 'acknowledged')",
        [(1,), (2,)],
    )
    with pytest.raises(AssertionError, match="duplicate publication"):
        assert_release_invariants(conn)


def test_partial_album_publication_is_rejected() -> None:
    conn = make_db()
    conn.execute("INSERT INTO messages VALUES (1, 'done')")
    conn.execute("INSERT INTO album_aggregates VALUES (10, 2, 'now')")
    conn.execute("INSERT INTO album_members VALUES (1, 10)")
    conn.execute(
        "INSERT INTO publish_intents VALUES (1, 10, '-1002', 'album', 'acknowledged')"
    )
    with pytest.raises(AssertionError, match="partial album"):
        assert_release_invariants(conn)


def test_job_without_durable_state_is_rejected() -> None:
    conn = make_db()
    conn.execute("INSERT INTO messages VALUES (1, NULL)")
    with pytest.raises(AssertionError, match="durable engine state"):
        assert_release_invariants(conn)


def test_cleanup_before_commit_is_rejected() -> None:
    conn = make_db()
    conn.execute("INSERT INTO messages VALUES (1, 'verifying')")
    conn.execute("INSERT INTO temporary_artifacts VALUES (1, 1, 'now')")
    with pytest.raises(AssertionError, match="before verified commit"):
        assert_release_invariants(conn)
