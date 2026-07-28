from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Sequence


@dataclass(frozen=True)
class AlbumMember:
    source_message_id: int
    ordinal: int
    media_type: str | None = None
    expected_size: int | None = None
    caption_owner: bool = False
    fingerprint: str | None = None


@dataclass(frozen=True)
class AlbumValidation:
    complete: bool
    expected_count: int
    present_count: int
    downloaded_count: int
    missing_ordinals: tuple[int, ...]
    manifest_identity: str | None


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def initialize_album_schema(conn: sqlite3.Connection) -> None:
    """Install additive Phase 3 album aggregate tables."""
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS album_aggregates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            plan_id INTEGER REFERENCES migration_plans(id) ON DELETE CASCADE,
            source_chat_id TEXT NOT NULL,
            media_group_id TEXT NOT NULL,
            expected_count INTEGER,
            caption_source_message_id INTEGER,
            quiet_until TEXT,
            sealed_at TEXT,
            manifest_identity TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(source_chat_id, media_group_id)
        );

        CREATE TABLE IF NOT EXISTS album_members (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            album_id INTEGER NOT NULL REFERENCES album_aggregates(id) ON DELETE CASCADE,
            source_message_id INTEGER NOT NULL,
            ordinal INTEGER NOT NULL,
            media_type TEXT,
            expected_size INTEGER,
            caption_owner INTEGER NOT NULL DEFAULT 0 CHECK(caption_owner IN (0, 1)),
            item_fingerprint TEXT,
            local_path TEXT,
            actual_size INTEGER,
            content_sha256 TEXT,
            downloaded_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(album_id, source_message_id),
            UNIQUE(album_id, ordinal)
        );

        CREATE INDEX IF NOT EXISTS idx_album_members_order
            ON album_members(album_id, ordinal);
        CREATE INDEX IF NOT EXISTS idx_album_quiet_window
            ON album_aggregates(sealed_at, quiet_until);
        """
    )
    conn.commit()


def get_or_create_album(
    conn: sqlite3.Connection,
    *,
    source_chat_id: str,
    media_group_id: str,
    plan_id: int | None = None,
    quiet_seconds: int = 3,
    now: datetime | None = None,
) -> int:
    moment = now or datetime.now(timezone.utc)
    now_text = moment.isoformat(timespec="seconds")
    quiet_until = (moment + timedelta(seconds=max(0, quiet_seconds))).isoformat(timespec="seconds")
    conn.execute(
        """
        INSERT INTO album_aggregates(
            plan_id, source_chat_id, media_group_id, quiet_until, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_chat_id, media_group_id) DO UPDATE SET
            plan_id = COALESCE(album_aggregates.plan_id, excluded.plan_id),
            quiet_until = CASE
                WHEN album_aggregates.sealed_at IS NULL THEN excluded.quiet_until
                ELSE album_aggregates.quiet_until
            END,
            updated_at = excluded.updated_at
        """,
        (plan_id, str(source_chat_id), str(media_group_id), quiet_until, now_text, now_text),
    )
    row = conn.execute(
        "SELECT id FROM album_aggregates WHERE source_chat_id = ? AND media_group_id = ?",
        (str(source_chat_id), str(media_group_id)),
    ).fetchone()
    conn.commit()
    if row is None:
        raise RuntimeError("Album aggregate was not created")
    return int(row[0])


def add_album_member(conn: sqlite3.Connection, *, album_id: int, member: AlbumMember) -> None:
    album = conn.execute(
        "SELECT sealed_at FROM album_aggregates WHERE id = ?", (album_id,)
    ).fetchone()
    if album is None:
        raise KeyError(f"Unknown album: {album_id}")
    if album[0] is not None:
        raise ValueError("Cannot mutate a sealed album")

    now = utc_now()
    conn.execute(
        """
        INSERT INTO album_members(
            album_id, source_message_id, ordinal, media_type, expected_size,
            caption_owner, item_fingerprint, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(album_id, source_message_id) DO UPDATE SET
            ordinal = excluded.ordinal,
            media_type = excluded.media_type,
            expected_size = excluded.expected_size,
            caption_owner = excluded.caption_owner,
            item_fingerprint = excluded.item_fingerprint,
            updated_at = excluded.updated_at
        """,
        (
            album_id,
            int(member.source_message_id),
            int(member.ordinal),
            member.media_type,
            member.expected_size,
            int(member.caption_owner),
            member.fingerprint,
            now,
            now,
        ),
    )
    if member.caption_owner:
        conn.execute(
            """
            UPDATE album_aggregates
            SET caption_source_message_id = ?, updated_at = ?
            WHERE id = ?
            """,
            (int(member.source_message_id), now, album_id),
        )
    conn.commit()


def _manifest_payload(rows: Iterable[sqlite3.Row | Sequence[object]]) -> list[dict[str, object]]:
    payload: list[dict[str, object]] = []
    for row in rows:
        payload.append(
            {
                "source_message_id": int(row[0]),
                "ordinal": int(row[1]),
                "media_type": row[2],
                "expected_size": row[3],
                "caption_owner": bool(row[4]),
                "item_fingerprint": row[5],
            }
        )
    return payload


def build_manifest_identity(conn: sqlite3.Connection, *, album_id: int) -> str:
    rows = conn.execute(
        """
        SELECT source_message_id, ordinal, media_type, expected_size,
               caption_owner, item_fingerprint
        FROM album_members
        WHERE album_id = ?
        ORDER BY ordinal, source_message_id
        """,
        (album_id,),
    ).fetchall()
    encoded = json.dumps(
        _manifest_payload(rows), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def seal_album(
    conn: sqlite3.Connection,
    *,
    album_id: int,
    expected_count: int | None = None,
    now: datetime | None = None,
    force: bool = False,
) -> str:
    moment = now or datetime.now(timezone.utc)
    album = conn.execute(
        "SELECT quiet_until, sealed_at FROM album_aggregates WHERE id = ?", (album_id,)
    ).fetchone()
    if album is None:
        raise KeyError(f"Unknown album: {album_id}")
    if album[1] is not None:
        row = conn.execute(
            "SELECT manifest_identity FROM album_aggregates WHERE id = ?", (album_id,)
        ).fetchone()
        return str(row[0])
    if not force and album[0] and datetime.fromisoformat(str(album[0])) > moment:
        raise ValueError("Album quiet window is still open")

    present_count = int(
        conn.execute("SELECT COUNT(*) FROM album_members WHERE album_id = ?", (album_id,)).fetchone()[0]
    )
    count = present_count if expected_count is None else int(expected_count)
    if count < present_count:
        raise ValueError("Expected member count cannot be smaller than present count")
    if present_count == 0:
        raise ValueError("Cannot seal an empty album")

    caption_count = int(
        conn.execute(
            "SELECT COUNT(*) FROM album_members WHERE album_id = ? AND caption_owner = 1",
            (album_id,),
        ).fetchone()[0]
    )
    if caption_count > 1:
        raise ValueError("Album has more than one caption owner")

    identity = build_manifest_identity(conn, album_id=album_id)
    timestamp = moment.isoformat(timespec="seconds")
    conn.execute(
        """
        UPDATE album_aggregates
        SET expected_count = ?, sealed_at = ?, manifest_identity = ?, updated_at = ?
        WHERE id = ? AND sealed_at IS NULL
        """,
        (count, timestamp, identity, timestamp, album_id),
    )
    conn.commit()
    return identity


def record_downloaded_member(
    conn: sqlite3.Connection,
    *,
    album_id: int,
    source_message_id: int,
    local_path: str,
    actual_size: int,
    content_sha256: str,
) -> bool:
    cursor = conn.execute(
        """
        UPDATE album_members
        SET local_path = ?, actual_size = ?, content_sha256 = ?,
            downloaded_at = ?, updated_at = ?
        WHERE album_id = ? AND source_message_id = ?
        """,
        (
            local_path,
            int(actual_size),
            content_sha256,
            utc_now(),
            utc_now(),
            album_id,
            int(source_message_id),
        ),
    )
    conn.commit()
    return cursor.rowcount == 1


def validate_album(conn: sqlite3.Connection, *, album_id: int) -> AlbumValidation:
    album = conn.execute(
        "SELECT expected_count, sealed_at, manifest_identity FROM album_aggregates WHERE id = ?",
        (album_id,),
    ).fetchone()
    if album is None:
        raise KeyError(f"Unknown album: {album_id}")

    rows = conn.execute(
        """
        SELECT ordinal, downloaded_at, expected_size, actual_size
        FROM album_members WHERE album_id = ? ORDER BY ordinal
        """,
        (album_id,),
    ).fetchall()
    expected = int(album[0] if album[0] is not None else len(rows))
    ordinals = {int(row[0]) for row in rows}
    missing = tuple(index for index in range(expected) if index not in ordinals)
    downloaded = sum(
        1
        for _, downloaded_at, expected_size, actual_size in rows
        if downloaded_at is not None
        and (expected_size is None or int(expected_size) == int(actual_size or -1))
    )
    complete = (
        album[1] is not None
        and len(rows) == expected
        and not missing
        and downloaded == expected
        and build_manifest_identity(conn, album_id=album_id) == album[2]
    )
    return AlbumValidation(
        complete=complete,
        expected_count=expected,
        present_count=len(rows),
        downloaded_count=downloaded,
        missing_ordinals=missing,
        manifest_identity=album[2],
    )


def ordered_album_members(conn: sqlite3.Connection, *, album_id: int) -> list[sqlite3.Row | tuple]:
    return conn.execute(
        "SELECT * FROM album_members WHERE album_id = ? ORDER BY ordinal, source_message_id",
        (album_id,),
    ).fetchall()
