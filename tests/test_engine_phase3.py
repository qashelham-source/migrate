from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from app.engine.albums import (
    AlbumMember,
    add_album_member,
    get_or_create_album,
    initialize_album_schema,
    ordered_album_members,
    record_downloaded_member,
    seal_album,
    validate_album,
)


def make_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(
        """
        CREATE TABLE migration_plans (
            id INTEGER PRIMARY KEY AUTOINCREMENT
        );
        """
    )
    initialize_album_schema(conn)
    return conn


def test_album_order_survives_reopen(tmp_path) -> None:
    db_path = tmp_path / "album.sqlite3"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE migration_plans (id INTEGER PRIMARY KEY AUTOINCREMENT)")
    initialize_album_schema(conn)
    album_id = get_or_create_album(
        conn, source_chat_id="-1001", media_group_id="group-a", quiet_seconds=0
    )
    add_album_member(conn, album_id=album_id, member=AlbumMember(30, 2, "video"))
    add_album_member(conn, album_id=album_id, member=AlbumMember(10, 0, "photo", caption_owner=True))
    add_album_member(conn, album_id=album_id, member=AlbumMember(20, 1, "photo"))
    seal_album(conn, album_id=album_id, force=True)
    conn.close()

    reopened = sqlite3.connect(db_path)
    rows = ordered_album_members(reopened, album_id=album_id)
    assert [row[2] for row in rows] == [10, 20, 30]


def test_missing_member_never_validates_complete() -> None:
    conn = make_db()
    album_id = get_or_create_album(
        conn, source_chat_id="-1001", media_group_id="group-b", quiet_seconds=0
    )
    add_album_member(
        conn,
        album_id=album_id,
        member=AlbumMember(100, 0, "photo", expected_size=5, caption_owner=True),
    )
    add_album_member(
        conn,
        album_id=album_id,
        member=AlbumMember(102, 2, "video", expected_size=7),
    )
    seal_album(conn, album_id=album_id, expected_count=3, force=True)
    record_downloaded_member(
        conn,
        album_id=album_id,
        source_message_id=100,
        local_path="/tmp/100.jpg",
        actual_size=5,
        content_sha256="a" * 64,
    )
    record_downloaded_member(
        conn,
        album_id=album_id,
        source_message_id=102,
        local_path="/tmp/102.mp4",
        actual_size=7,
        content_sha256="b" * 64,
    )

    result = validate_album(conn, album_id=album_id)
    assert result.complete is False
    assert result.missing_ordinals == (1,)
    assert result.expected_count == 3


def test_complete_album_validates_only_after_every_download() -> None:
    conn = make_db()
    album_id = get_or_create_album(
        conn, source_chat_id="-1001", media_group_id="group-c", quiet_seconds=0
    )
    for ordinal, message_id, size in [(0, 201, 11), (1, 202, 13)]:
        add_album_member(
            conn,
            album_id=album_id,
            member=AlbumMember(
                message_id,
                ordinal,
                "photo",
                expected_size=size,
                caption_owner=ordinal == 0,
            ),
        )
    seal_album(conn, album_id=album_id, expected_count=2, force=True)

    record_downloaded_member(
        conn,
        album_id=album_id,
        source_message_id=201,
        local_path="/tmp/201.jpg",
        actual_size=11,
        content_sha256="c" * 64,
    )
    assert validate_album(conn, album_id=album_id).complete is False

    record_downloaded_member(
        conn,
        album_id=album_id,
        source_message_id=202,
        local_path="/tmp/202.jpg",
        actual_size=13,
        content_sha256="d" * 64,
    )
    assert validate_album(conn, album_id=album_id).complete is True


def test_quiet_window_prevents_early_seal() -> None:
    conn = make_db()
    now = datetime.now(timezone.utc)
    album_id = get_or_create_album(
        conn,
        source_chat_id="-1001",
        media_group_id="group-d",
        quiet_seconds=30,
        now=now,
    )
    add_album_member(conn, album_id=album_id, member=AlbumMember(301, 0, "photo"))

    with pytest.raises(ValueError, match="quiet window"):
        seal_album(conn, album_id=album_id, now=now + timedelta(seconds=5))

    identity = seal_album(conn, album_id=album_id, now=now + timedelta(seconds=31))
    assert len(identity) == 64


def test_sealed_album_is_immutable() -> None:
    conn = make_db()
    album_id = get_or_create_album(
        conn, source_chat_id="-1001", media_group_id="group-e", quiet_seconds=0
    )
    add_album_member(conn, album_id=album_id, member=AlbumMember(401, 0, "photo"))
    seal_album(conn, album_id=album_id, force=True)

    with pytest.raises(ValueError, match="sealed"):
        add_album_member(conn, album_id=album_id, member=AlbumMember(402, 1, "video"))
