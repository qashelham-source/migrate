from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

from app.engine.capacity import (
    StorageMode,
    active_reserved_bytes,
    claim_destination_job,
    classify_storage_mode,
    complete_destination_job,
    enqueue_destination_job,
    finish_reservation,
    initialize_capacity_schema,
    pause_destination,
    reserve_storage,
)


def stamp(seconds: int = 0) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat(timespec="seconds")


def make_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("CREATE TABLE messages (id INTEGER PRIMARY KEY AUTOINCREMENT)")
    for _ in range(6):
        conn.execute("INSERT INTO messages DEFAULT VALUES")
    initialize_capacity_schema(conn)
    return conn


def test_storage_modes_follow_available_capacity() -> None:
    assert classify_storage_mode(total_bytes=1000, free_bytes=500).mode is StorageMode.NORMAL
    assert classify_storage_mode(total_bytes=1000, free_bytes=100).mode is StorageMode.PRESSURE
    assert classify_storage_mode(total_bytes=1000, free_bytes=50).mode is StorageMode.CRITICAL
    assert classify_storage_mode(total_bytes=1000, free_bytes=20).mode is StorageMode.EMERGENCY


def test_reservations_prevent_concurrent_overcommit() -> None:
    conn = make_db()
    assert reserve_storage(
        conn,
        reservation_key="first",
        job_id=1,
        estimated_bytes=600,
        total_bytes=1000,
        free_bytes=1000,
    ) is True
    assert active_reserved_bytes(conn) == 600
    assert reserve_storage(
        conn,
        reservation_key="second",
        job_id=2,
        estimated_bytes=360,
        total_bytes=1000,
        free_bytes=1000,
    ) is False
    assert active_reserved_bytes(conn) == 600
    assert finish_reservation(conn, reservation_key="first", consumed=False) is True
    assert active_reserved_bytes(conn) == 0


def test_reservation_is_idempotent() -> None:
    conn = make_db()
    kwargs = dict(
        reservation_key="stable",
        job_id=1,
        estimated_bytes=100,
        total_bytes=1000,
        free_bytes=1000,
    )
    assert reserve_storage(conn, **kwargs) is True
    assert reserve_storage(conn, **kwargs) is True
    count = conn.execute("SELECT COUNT(*) FROM storage_reservations").fetchone()[0]
    assert count == 1


def test_destination_lane_preserves_order() -> None:
    conn = make_db()
    assert enqueue_destination_job(conn, destination_chat_id="-1001", job_id=2) == 1
    assert enqueue_destination_job(conn, destination_chat_id="-1001", job_id=1) == 2
    first = claim_destination_job(
        conn,
        destination_chat_id="-1001",
        owner="writer-a",
        token="token-a",
        lease_expires_at=stamp(60),
    )
    assert first == 2
    assert complete_destination_job(
        conn,
        destination_chat_id="-1001",
        job_id=2,
        token="token-a",
    ) is True
    second = claim_destination_job(
        conn,
        destination_chat_id="-1001",
        owner="writer-a",
        token="token-b",
        lease_expires_at=stamp(60),
    )
    assert second == 1


def test_one_paused_destination_does_not_block_another() -> None:
    conn = make_db()
    enqueue_destination_job(conn, destination_chat_id="paused", job_id=1)
    enqueue_destination_job(conn, destination_chat_id="healthy", job_id=2)
    pause_destination(
        conn,
        destination_chat_id="paused",
        until=stamp(120),
        reason="FloodWait",
    )
    assert claim_destination_job(
        conn,
        destination_chat_id="paused",
        owner="writer",
        token="paused-token",
        lease_expires_at=stamp(60),
    ) is None
    assert claim_destination_job(
        conn,
        destination_chat_id="healthy",
        owner="writer",
        token="healthy-token",
        lease_expires_at=stamp(60),
    ) == 2


def test_lane_allows_only_one_active_writer() -> None:
    conn = make_db()
    enqueue_destination_job(conn, destination_chat_id="dest", job_id=1)
    enqueue_destination_job(conn, destination_chat_id="dest", job_id=2)
    assert claim_destination_job(
        conn,
        destination_chat_id="dest",
        owner="writer-a",
        token="a",
        lease_expires_at=stamp(60),
    ) == 1
    assert claim_destination_job(
        conn,
        destination_chat_id="dest",
        owner="writer-b",
        token="b",
        lease_expires_at=stamp(60),
    ) is None
    assert complete_destination_job(
        conn,
        destination_chat_id="dest",
        job_id=1,
        token="wrong",
    ) is False
