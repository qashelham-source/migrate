from __future__ import annotations

import sqlite3

import pytest

from app.engine.publish import (
    DestinationEvidence,
    PublishState,
    create_publish_intent,
    destination_message_ids,
    initialize_publish_schema,
    mark_send_started,
    mark_uploaded_unconfirmed,
    may_retry_send,
    reconcile_publish_intent,
    record_telegram_ack,
    require_reconciliation,
)


def make_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(
        """
        CREATE TABLE messages (id INTEGER PRIMARY KEY AUTOINCREMENT);
        CREATE TABLE album_aggregates (id INTEGER PRIMARY KEY AUTOINCREMENT);
        INSERT INTO messages DEFAULT VALUES;
        INSERT INTO album_aggregates DEFAULT VALUES;
        """
    )
    initialize_publish_schema(conn)
    return conn


def state(conn: sqlite3.Connection, intent_id: int) -> str:
    return str(conn.execute("SELECT state FROM publish_intents WHERE id = ?", (intent_id,)).fetchone()[0])


def test_publish_intent_exists_before_send_and_is_idempotent() -> None:
    conn = make_db()
    first = create_publish_intent(
        conn,
        job_id=1,
        destination_chat_id="-1002",
        operation="send_document",
        payload_fingerprint="abc",
        intent_key="stable-key",
    )
    second = create_publish_intent(
        conn,
        job_id=1,
        destination_chat_id="-1002",
        operation="send_document",
        payload_fingerprint="abc",
        intent_key="stable-key",
    )
    assert first == second
    assert state(conn, first) == PublishState.INTENT_RECORDED.value
    assert may_retry_send(conn, intent_id=first) is True


def test_ack_is_persisted_immediately() -> None:
    conn = make_db()
    intent_id = create_publish_intent(
        conn,
        job_id=1,
        destination_chat_id="-1002",
        operation="send_video",
        payload_fingerprint="video-hash",
    )
    assert mark_send_started(conn, intent_id=intent_id) is True
    assert record_telegram_ack(
        conn,
        intent_id=intent_id,
        destination_message_ids=[901],
        ack_payload={"id": 901},
    ) is True
    assert state(conn, intent_id) == PublishState.ACKNOWLEDGED.value
    assert destination_message_ids(conn, intent_id=intent_id) == (901,)
    assert may_retry_send(conn, intent_id=intent_id) is False


def test_interrupted_send_cannot_be_blindly_retried() -> None:
    conn = make_db()
    intent_id = create_publish_intent(
        conn,
        job_id=1,
        destination_chat_id="-1002",
        operation="send_photo",
        payload_fingerprint="photo-hash",
    )
    mark_send_started(conn, intent_id=intent_id)
    mark_uploaded_unconfirmed(conn, intent_id=intent_id)
    require_reconciliation(conn, intent_id=intent_id)
    assert state(conn, intent_id) == PublishState.NEEDS_RECONCILIATION.value
    assert may_retry_send(conn, intent_id=intent_id) is False
    assert mark_send_started(conn, intent_id=intent_id) is True


def test_confident_destination_evidence_is_accepted() -> None:
    conn = make_db()
    intent_id = create_publish_intent(
        conn,
        album_id=1,
        destination_chat_id="-1002",
        operation="send_media_group",
        payload_fingerprint="album-hash",
        expected_item_count=2,
    )
    mark_send_started(conn, intent_id=intent_id)
    require_reconciliation(conn, intent_id=intent_id)
    result = reconcile_publish_intent(
        conn,
        intent_id=intent_id,
        evidence=DestinationEvidence((1001, 1002), 0.99, "album-hash"),
    )
    assert result.accepted is True
    assert result.state is PublishState.RECONCILED
    assert destination_message_ids(conn, intent_id=intent_id) == (1001, 1002)


def test_uncertain_or_incomplete_evidence_enters_quarantine() -> None:
    conn = make_db()
    intent_id = create_publish_intent(
        conn,
        album_id=1,
        destination_chat_id="-1002",
        operation="send_media_group",
        payload_fingerprint="album-hash",
        expected_item_count=2,
    )
    mark_send_started(conn, intent_id=intent_id)
    require_reconciliation(conn, intent_id=intent_id)
    result = reconcile_publish_intent(
        conn,
        intent_id=intent_id,
        evidence=DestinationEvidence((1001,), 0.80, "wrong-hash", "weak match"),
    )
    assert result.accepted is False
    assert result.state is PublishState.QUARANTINED
    assert state(conn, intent_id) == PublishState.QUARANTINED.value
    assert may_retry_send(conn, intent_id=intent_id) is False


def test_reconciliation_rejects_non_ambiguous_intent() -> None:
    conn = make_db()
    intent_id = create_publish_intent(
        conn,
        job_id=1,
        destination_chat_id="-1002",
        operation="send_message",
        payload_fingerprint="text-hash",
    )
    with pytest.raises(ValueError, match="not awaiting reconciliation"):
        reconcile_publish_intent(
            conn,
            intent_id=intent_id,
            evidence=DestinationEvidence((1,), 1.0, "text-hash"),
        )
