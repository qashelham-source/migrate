from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.dashboard_v2 import dashboard_snapshot, issue_center
from app.db import Database
from app.job_health import find_stalled_jobs


def _enqueue(db: Database, *, source_message_id: int, status: str, file_size: int | None = None) -> int:
    assert db.enqueue_message(
        source_chat_id="-1001",
        source_message_id=source_message_id,
        dest_chat_id="-1002",
        file_unique_key=f"fingerprint-{source_message_id}",
        source_message_ids=[source_message_id],
        source_topic_id=None,
        dest_topic_id=None,
        media_group_id=None,
        media_type="document",
        file_size=file_size,
        caption=None,
        status=status,
    )
    row = db.query_one("SELECT id FROM messages WHERE source_message_id = ?", (source_message_id,))
    assert row is not None
    return int(row["id"])


def test_stalled_detector_uses_heartbeats_and_large_file_thresholds(tmp_path: Path) -> None:
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    try:
        now = datetime(2026, 7, 30, tzinfo=timezone.utc)
        stalled_download = _enqueue(db, source_message_id=1, status="downloading")
        large_recent = _enqueue(
            db,
            source_message_id=2,
            status="uploading",
            file_size=1024 * 1024 * 1024,
        )
        verifying = _enqueue(db, source_message_id=3, status="copied")
        assert db.start_verification(verifying)

        db.execute(
            "UPDATE messages SET updated_at = ? WHERE id = ?",
            ((now - timedelta(minutes=16)).isoformat(timespec="seconds"), stalled_download),
        )
        db.execute(
            "UPDATE messages SET updated_at = ? WHERE id = ?",
            ((now - timedelta(minutes=16)).isoformat(timespec="seconds"), large_recent),
        )
        db.execute(
            "UPDATE messages SET updated_at = ? WHERE id = ?",
            ((now - timedelta(minutes=17)).isoformat(timespec="seconds"), verifying),
        )

        stalled = find_stalled_jobs(db, now=now)

        assert {(job.id, job.phase) for job in stalled} == {
            (stalled_download, "downloading"),
            (verifying, "verifying"),
        }
    finally:
        db.close()


def test_dashboard_surfaces_stalled_jobs_without_raw_errors(tmp_path: Path) -> None:
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    try:
        job_id = _enqueue(db, source_message_id=5, status="uploading")
        old = (datetime.now(timezone.utc) - timedelta(minutes=16)).isoformat(timespec="seconds")
        db.execute("UPDATE messages SET updated_at = ? WHERE id = ?", (old, job_id))

        snapshot = dashboard_snapshot(db, tmp_path)
        issues = issue_center(db)

        assert snapshot["health"]["status"] == "job_stalled"
        assert snapshot["health"]["stalled_jobs"][0]["id"] == job_id
        assert issues[0]["kind"] == "stalled"
        assert issues[0]["status"] == "Job Stalled"
        assert "ValueError" not in issues[0]["error"]
    finally:
        db.close()


def test_touch_active_job_never_revives_completed_work(tmp_path: Path) -> None:
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    try:
        job_id = _enqueue(db, source_message_id=7, status="downloading")
        assert db.touch_active_job(job_id)
        db.set_status(job_id, "copied", last_error="")
        assert not db.touch_active_job(job_id)
    finally:
        db.close()
