from pathlib import Path

from app.dashboard_v2 import active_source_progress, dashboard_snapshot, source_migration_progress
from app.db import Database


def _enqueue(
    db: Database,
    *,
    source_message_id: int,
    dest_chat_id: str,
    file_unique_key: str,
    status: str = "pending",
    last_error: str | None = None,
) -> None:
    assert db.enqueue_message(
        source_chat_id="-100001",
        source_message_id=source_message_id,
        dest_chat_id=dest_chat_id,
        file_unique_key=file_unique_key,
        source_message_ids=[source_message_id],
        source_topic_id=None,
        dest_topic_id=None,
        media_group_id=None,
        media_type="video",
        file_size=1024,
        caption=None,
        status=status,
        last_error=last_error,
    )


def test_source_migration_percentage_counts_each_post_or_album_once(tmp_path: Path) -> None:
    db_path = tmp_path / "migration.sqlite3"
    db = Database(db_path)
    db.initialize()
    try:
        _enqueue(
            db,
            source_message_id=1,
            dest_chat_id="-100101",
            file_unique_key="item:1",
            status="copied",
        )
        _enqueue(
            db,
            source_message_id=2,
            dest_chat_id="-100101",
            file_unique_key="item:2",
        )
        _enqueue(
            db,
            source_message_id=3,
            dest_chat_id="-100101",
            file_unique_key="album:3",
            status="copied",
        )
        _enqueue(
            db,
            source_message_id=3,
            dest_chat_id="-100102",
            file_unique_key="album:3",
        )
        _enqueue(
            db,
            source_message_id=4,
            dest_chat_id="-100101",
            file_unique_key="item:4",
            status="failed",
            last_error="Network retry limit reached",
        )
        _enqueue(
            db,
            source_message_id=5,
            dest_chat_id="-100101",
            file_unique_key="item:5",
            status="skipped",
            last_error="Filtered out by config",
        )
        _enqueue(
            db,
            source_message_id=6,
            dest_chat_id="-100101",
            file_unique_key="repair:6",
            status="copied",
        )

        progress = source_migration_progress(db)

        assert progress == [
            {
                "source_chat_id": "-100001",
                "title": "-100001",
                "total_items": 5,
                "filtered_items": 1,
                "eligible_items": 4,
                "copied_items": 1,
                "active_items": 2,
                "blocked_items": 1,
                "remaining_items": 3,
                "percent": 25,
            }
        ]
        assert dashboard_snapshot(db, db_path)["source_progress"] == progress
    finally:
        db.close()


def test_source_migration_percentage_uses_registered_channel_title(tmp_path: Path) -> None:
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    try:
        db.execute(
            "CREATE TABLE source_registry (source_chat_id TEXT PRIMARY KEY, title TEXT)"
        )
        db.execute(
            "INSERT INTO source_registry (source_chat_id, title) VALUES (?, ?)",
            ("-100001", "Channel Utama"),
        )
        _enqueue(
            db,
            source_message_id=1,
            dest_chat_id="-100101",
            file_unique_key="item:1",
            status="copied",
        )

        assert source_migration_progress(db)[0]["title"] == "Channel Utama"
    finally:
        db.close()


def test_active_source_progress_uses_live_source_and_hides_completed_history() -> None:
    progress = [
        {
            "source_chat_id": "-100001",
            "title": "VVIP 02",
            "eligible_items": 1674,
            "copied_items": 1582,
            "active_items": 92,
            "blocked_items": 0,
            "remaining_items": 92,
            "percent": 95,
        },
        {
            "source_chat_id": "-100002",
            "title": "Tudung Media 69",
            "eligible_items": 179,
            "copied_items": 179,
            "active_items": 0,
            "blocked_items": 0,
            "remaining_items": 0,
            "percent": 100,
        },
    ]

    active = active_source_progress(
        {"phase": "downloading", "source_chat": -100001, "source": "VVIP 02"},
        progress,
    )

    assert active is not None
    assert active["title"] == "VVIP 02"
    assert active_source_progress({"phase": "watching"}, progress) is None
