from pathlib import Path

from app.dashboard_v2 import dashboard_snapshot, delivery_matrix, format_eta, issue_center, source_library
from app.db import Database
from app.release3_store import Release3Store


def make_db(tmp_path: Path) -> Database:
    db = Database(tmp_path / "queue.db")
    db.initialize()
    Release3Store(db).initialize()
    return db


def enqueue(db: Database, *, status: str, dest: str = "-200", error: str | None = None) -> None:
    message_id = len(db.query("SELECT id FROM messages")) + 1
    db.enqueue_message(
        source_chat_id="-100",
        source_message_id=message_id,
        dest_chat_id=dest,
        file_unique_key=f"file-{message_id}-{dest}",
        source_message_ids=[message_id],
        source_topic_id=None,
        dest_topic_id=None,
        media_group_id=None,
        media_type="video",
        file_size=1024,
        caption=None,
        status=status,
        last_error=error,
    )


def test_dashboard_snapshot_combines_release3_data(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    try:
        enqueue(db, status="pending")
        enqueue(db, status="copied")
        enqueue(db, status="failed", error="permission denied")
        store = Release3Store(db)
        store.upsert_source(
            source_chat_id="-100",
            title="Source A",
            username="source_a",
            chat_type="channel",
            latest_seen_message_id=3,
        )
        store.set_live_watch("-100", True)
        store.pause_destination("-200", "permission", "permission denied")
        snapshot = dashboard_snapshot(db, tmp_path)
        assert snapshot["queue"]["pending"] == 1
        assert snapshot["queue"]["copied"] == 1
        assert snapshot["queue"]["failed"] == 1
        assert snapshot["sources"]["total"] == 1
        assert snapshot["sources"]["live"] == 1
        assert snapshot["destinations"]["paused"] == 1
        assert snapshot["storage"].total_bytes > 0
    finally:
        db.close()


def test_issue_center_and_delivery_matrix(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    try:
        enqueue(db, status="copied", dest="-200")
        enqueue(db, status="failed", dest="-200", error="network timeout")
        Release3Store(db).pause_destination("-200", "permission", "admin rights required")
        issues = issue_center(db)
        assert any(item["kind"] == "job" and item["status"] == "failed" for item in issues)
        assert any(item["kind"] == "destination" and item["status"] == "paused" for item in issues)
        matrix = delivery_matrix(db)
        assert matrix[0]["dest_chat_id"] == "-200"
        assert matrix[0]["copied"] == 1
        assert matrix[0]["issues"] == 1
        assert matrix[0]["paused"] is True
    finally:
        db.close()


def test_source_library_and_eta_format(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    try:
        Release3Store(db).upsert_source(
            source_chat_id="-100",
            title="My Library",
            username=None,
            chat_type="channel",
            latest_seen_message_id=10,
        )
        rows = source_library(db)
        assert rows[0]["title"] == "My Library"
        assert format_eta(None) == "belum cukup data"
        assert format_eta(65) == "1m 5s"
        assert format_eta(3661) == "1j 1m"
    finally:
        db.close()
