from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum


class StorageMode(StrEnum):
    NORMAL = "normal"
    PRESSURE = "pressure"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


@dataclass(frozen=True)
class StorageSnapshot:
    total_bytes: int
    free_bytes: int
    reserved_bytes: int
    available_bytes: int
    mode: StorageMode


@dataclass(frozen=True)
class DestinationLane:
    destination_chat_id: str
    owner: str | None
    token: str | None
    paused_until: str | None
    pause_reason: str | None
    next_sequence: int


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def initialize_capacity_schema(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS storage_reservations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reservation_key TEXT NOT NULL UNIQUE,
            job_id INTEGER REFERENCES messages(id) ON DELETE CASCADE,
            bytes_reserved INTEGER NOT NULL CHECK(bytes_reserved > 0),
            state TEXT NOT NULL CHECK(state IN ('active', 'released', 'consumed')),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            released_at TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_storage_reservations_active
            ON storage_reservations(state, job_id);

        CREATE TABLE IF NOT EXISTS destination_lanes (
            destination_chat_id TEXT PRIMARY KEY,
            lease_owner TEXT,
            lease_token TEXT,
            lease_expires_at TEXT,
            paused_until TEXT,
            pause_reason TEXT,
            next_sequence INTEGER NOT NULL DEFAULT 1,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS destination_lane_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            destination_chat_id TEXT NOT NULL REFERENCES destination_lanes(destination_chat_id) ON DELETE CASCADE,
            job_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
            sequence_no INTEGER NOT NULL,
            state TEXT NOT NULL DEFAULT 'queued' CHECK(state IN ('queued', 'active', 'done', 'cancelled')),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(destination_chat_id, sequence_no),
            UNIQUE(destination_chat_id, job_id)
        );

        CREATE INDEX IF NOT EXISTS idx_destination_lane_items_order
            ON destination_lane_items(destination_chat_id, state, sequence_no);
        """
    )
    conn.commit()


def classify_storage_mode(*, total_bytes: int, free_bytes: int, reserved_bytes: int = 0) -> StorageSnapshot:
    total = max(1, int(total_bytes))
    free = max(0, int(free_bytes))
    reserved = max(0, int(reserved_bytes))
    available = max(0, free - reserved)
    ratio = available / total
    if ratio <= 0.02:
        mode = StorageMode.EMERGENCY
    elif ratio <= 0.05:
        mode = StorageMode.CRITICAL
    elif ratio <= 0.10:
        mode = StorageMode.PRESSURE
    else:
        mode = StorageMode.NORMAL
    return StorageSnapshot(total, free, reserved, available, mode)


def active_reserved_bytes(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT COALESCE(SUM(bytes_reserved), 0) FROM storage_reservations WHERE state = 'active'"
    ).fetchone()
    return int(row[0])


def reserve_storage(
    conn: sqlite3.Connection,
    *,
    reservation_key: str,
    job_id: int,
    estimated_bytes: int,
    total_bytes: int,
    free_bytes: int,
) -> bool:
    requested = int(estimated_bytes)
    if requested <= 0:
        raise ValueError("estimated_bytes must be positive")
    now = utc_now()
    conn.execute("BEGIN IMMEDIATE")
    try:
        existing = conn.execute(
            "SELECT state FROM storage_reservations WHERE reservation_key = ?", (reservation_key,)
        ).fetchone()
        if existing is not None:
            conn.commit()
            return str(existing[0]) == "active"
        reserved = active_reserved_bytes(conn)
        snapshot = classify_storage_mode(
            total_bytes=total_bytes,
            free_bytes=free_bytes,
            reserved_bytes=reserved + requested,
        )
        if requested > max(0, int(free_bytes) - reserved) or snapshot.mode in {
            StorageMode.CRITICAL,
            StorageMode.EMERGENCY,
        }:
            conn.rollback()
            return False
        conn.execute(
            """
            INSERT INTO storage_reservations(
                reservation_key, job_id, bytes_reserved, state, created_at, updated_at
            ) VALUES (?, ?, ?, 'active', ?, ?)
            """,
            (reservation_key, int(job_id), requested, now, now),
        )
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise


def finish_reservation(conn: sqlite3.Connection, *, reservation_key: str, consumed: bool) -> bool:
    now = utc_now()
    target = "consumed" if consumed else "released"
    cursor = conn.execute(
        """
        UPDATE storage_reservations
        SET state = ?, released_at = ?, updated_at = ?
        WHERE reservation_key = ? AND state = 'active'
        """,
        (target, now, now, reservation_key),
    )
    conn.commit()
    return cursor.rowcount == 1


def ensure_destination_lane(conn: sqlite3.Connection, *, destination_chat_id: str) -> None:
    now = utc_now()
    conn.execute(
        """
        INSERT INTO destination_lanes(destination_chat_id, updated_at)
        VALUES (?, ?)
        ON CONFLICT(destination_chat_id) DO NOTHING
        """,
        (str(destination_chat_id), now),
    )
    conn.commit()


def enqueue_destination_job(conn: sqlite3.Connection, *, destination_chat_id: str, job_id: int) -> int:
    destination = str(destination_chat_id)
    now = utc_now()
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(
            """
            INSERT INTO destination_lanes(destination_chat_id, updated_at)
            VALUES (?, ?)
            ON CONFLICT(destination_chat_id) DO NOTHING
            """,
            (destination, now),
        )
        existing = conn.execute(
            "SELECT sequence_no FROM destination_lane_items WHERE destination_chat_id = ? AND job_id = ?",
            (destination, int(job_id)),
        ).fetchone()
        if existing is not None:
            conn.commit()
            return int(existing[0])
        row = conn.execute(
            "SELECT next_sequence FROM destination_lanes WHERE destination_chat_id = ?",
            (destination,),
        ).fetchone()
        sequence = int(row[0])
        conn.execute(
            """
            INSERT INTO destination_lane_items(
                destination_chat_id, job_id, sequence_no, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (destination, int(job_id), sequence, now, now),
        )
        conn.execute(
            "UPDATE destination_lanes SET next_sequence = next_sequence + 1, updated_at = ? WHERE destination_chat_id = ?",
            (now, destination),
        )
        conn.commit()
        return sequence
    except Exception:
        conn.rollback()
        raise


def pause_destination(conn: sqlite3.Connection, *, destination_chat_id: str, until: str, reason: str) -> None:
    ensure_destination_lane(conn, destination_chat_id=destination_chat_id)
    conn.execute(
        """
        UPDATE destination_lanes
        SET paused_until = ?, pause_reason = ?, updated_at = ?
        WHERE destination_chat_id = ?
        """,
        (until, reason, utc_now(), str(destination_chat_id)),
    )
    conn.commit()


def claim_destination_job(
    conn: sqlite3.Connection,
    *,
    destination_chat_id: str,
    owner: str,
    token: str,
    lease_expires_at: str,
    now: str | None = None,
) -> int | None:
    destination = str(destination_chat_id)
    current = now or utc_now()
    conn.execute("BEGIN IMMEDIATE")
    try:
        lane = conn.execute(
            "SELECT lease_expires_at, paused_until FROM destination_lanes WHERE destination_chat_id = ?",
            (destination,),
        ).fetchone()
        if lane is None:
            conn.commit()
            return None
        if lane[1] is not None and str(lane[1]) > current:
            conn.commit()
            return None
        if lane[0] is not None and str(lane[0]) > current:
            conn.commit()
            return None
        row = conn.execute(
            """
            SELECT id, job_id FROM destination_lane_items
            WHERE destination_chat_id = ? AND state = 'queued'
            ORDER BY sequence_no ASC LIMIT 1
            """,
            (destination,),
        ).fetchone()
        if row is None:
            conn.commit()
            return None
        conn.execute(
            """
            UPDATE destination_lanes
            SET lease_owner = ?, lease_token = ?, lease_expires_at = ?, updated_at = ?
            WHERE destination_chat_id = ?
            """,
            (owner, token, lease_expires_at, current, destination),
        )
        conn.execute(
            "UPDATE destination_lane_items SET state = 'active', updated_at = ? WHERE id = ?",
            (current, int(row[0])),
        )
        conn.commit()
        return int(row[1])
    except Exception:
        conn.rollback()
        raise


def complete_destination_job(
    conn: sqlite3.Connection,
    *,
    destination_chat_id: str,
    job_id: int,
    token: str,
) -> bool:
    destination = str(destination_chat_id)
    now = utc_now()
    conn.execute("BEGIN IMMEDIATE")
    try:
        lane = conn.execute(
            "SELECT lease_token FROM destination_lanes WHERE destination_chat_id = ?",
            (destination,),
        ).fetchone()
        if lane is None or lane[0] != token:
            conn.rollback()
            return False
        cursor = conn.execute(
            """
            UPDATE destination_lane_items
            SET state = 'done', updated_at = ?
            WHERE destination_chat_id = ? AND job_id = ? AND state = 'active'
            """,
            (now, destination, int(job_id)),
        )
        if cursor.rowcount != 1:
            conn.rollback()
            return False
        conn.execute(
            """
            UPDATE destination_lanes
            SET lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL, updated_at = ?
            WHERE destination_chat_id = ?
            """,
            (now, destination),
        )
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise
