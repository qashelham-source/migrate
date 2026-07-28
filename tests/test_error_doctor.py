from pathlib import Path

from app.db import Database
from app.error_doctor import diagnose_error, diagnose_job, diagnose_open_issues, explain_performance
from app.release3_store import Release3Store


def make_db(tmp_path: Path) -> Database:
    db = Database(tmp_path / "queue.db")
    db.initialize()
    Release3Store(db).initialize()
    return db


def enqueue(db: Database, *, error: str, status: str = "failed", media_type: str = "video") -> int:
    message_id = len(db.query("SELECT id FROM messages")) + 1
    assert db.enqueue_message(
        source_chat_id="-1001",
        source_message_id=message_id,
        dest_chat_id="-2001",
        file_unique_key=f"key-{message_id}",
        source_message_ids=[message_id],
        source_topic_id=None,
        dest_topic_id=None,
        media_group_id=None,
        media_type=media_type,
        file_size=1024,
        caption=None,
        status=status,
        last_error=error,
    )
    row = db.query_one("SELECT id FROM messages WHERE file_unique_key = ?", (f"key-{message_id}",))
    assert row is not None
    return int(row["id"])


def test_rule_diagnoses_permission_and_retry_safety() -> None:
    diagnosis = diagnose_error("ChatWriteForbidden: not enough rights")
    assert diagnosis.category == "permission"
    assert diagnosis.severity == "critical"
    assert diagnosis.retry_safe is False
    assert diagnosis.pause_destination is True
    assert diagnosis.confidence >= 0.95


def test_unknown_error_is_honest_about_low_confidence() -> None:
    diagnosis = diagnose_error("Something unusual happened")
    assert diagnosis.category == "unknown"
    assert diagnosis.confidence < 0.50
    assert diagnosis.retry_safe is True


def test_diagnose_job_persists_additive_record(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    try:
        job_id = enqueue(db, error="MediaEmpty caused by messages.SendMultiMedia", status="skipped")
        result = diagnose_job(db, job_id)
        assert result is not None
        assert result["category"] == "media"
        row = db.query_one("SELECT category, retry_safe FROM error_diagnoses WHERE job_id = ?", (job_id,))
        assert row is not None
        assert row["category"] == "media"
        assert row["retry_safe"] == 1
        assert db.query_one("SELECT status FROM messages WHERE id = ?", (job_id,))["status"] == "skipped"
    finally:
        db.close()


def test_open_issue_diagnosis_covers_multiple_categories(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    try:
        enqueue(db, error="PeerIdInvalid: peer id invalid")
        enqueue(db, error="ConnectionError: network timed out")
        results = diagnose_open_issues(db)
        assert {item["category"] for item in results} == {"peer_id", "network"}
    finally:
        db.close()


def test_performance_explanation_detects_blocked_queue(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    try:
        enqueue(db, error="temporary", status="pending")
        db.execute(
            """
            INSERT INTO job_telemetry (
                job_id, stage, bytes_total, bytes_processed, speed_bps, eta_seconds,
                started_at, stage_started_at, updated_at
            ) VALUES (999, 'copied', 1000, 1000, 100.0, 0, 'x', 'x', 'x')
            """
        )
        report = explain_performance(db)
        assert report["state"] == "blocked"
        assert report["sample_count"] == 1
        assert report["average_speed_bps"] == 100.0
        assert report["suggestions"]
    finally:
        db.close()
