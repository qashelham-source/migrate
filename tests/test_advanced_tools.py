from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from app.advanced import (
    checkpoint_rows,
    classify_repair_error,
    load_health_report,
    repair_summary,
    request_run_mode,
    requeue_repair_category,
    requeue_retryable_repairs,
    reset_all_checkpoints,
    save_health_report,
)
from app.db import Database


def config_for(tmp_path: Path) -> object:
    return SimpleNamespace(
        queue=SimpleNamespace(db_path=tmp_path / "data" / "migration.sqlite3"),
    )


def enqueue_error(
    db: Database,
    *,
    source_message_id: int,
    key: str,
    media_type: str,
    error: str,
    status: str = "skipped",
) -> None:
    assert db.enqueue_message(
        source_chat_id="-1001111111111",
        source_message_id=source_message_id,
        dest_chat_id="-1002222222222",
        file_unique_key=key,
        source_message_ids=[source_message_id],
        source_topic_id=None,
        dest_topic_id=None,
        media_group_id=None,
        media_type=media_type,
        file_size=123,
        caption=None,
        status=status,
        last_error=error,
    )


def test_request_run_mode_writes_atomic_mode_and_wakeup_marker(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    request_run_mode(config, "sync")  # type: ignore[arg-type]

    runtime_dir = tmp_path / "data"
    assert (runtime_dir / "run_mode").read_text(encoding="utf-8") == "sync"
    assert (runtime_dir / "run_now").exists()


def test_health_report_round_trip(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    report = {"overall": "pass", "checks": [{"name": "storage", "status": "pass"}]}

    save_health_report(config, report)  # type: ignore[arg-type]

    assert load_health_report(config) == report  # type: ignore[arg-type]


def test_checkpoint_helpers_list_and_reset(tmp_path: Path) -> None:
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    try:
        db.set_scan_checkpoint(-1001111111111, None, 846, "incremental")
        db.set_scan_checkpoint(-1002222222222, 7, 91, "full")

        rows = checkpoint_rows(db)
        by_source = {row["source_chat_id"]: row for row in rows}

        assert by_source["-1001111111111"]["last_scanned_message_id"] == 846
        assert by_source["-1001111111111"]["source_topic_id"] is None
        assert by_source["-1002222222222"]["source_topic_id"] == 7
        assert reset_all_checkpoints(db) == 2
        assert checkpoint_rows(db) == []
    finally:
        db.close()


def test_repair_summary_classifies_errors_and_preserves_unsupported(tmp_path: Path) -> None:
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    try:
        enqueue_error(
            db,
            source_message_id=9,
            key="media-empty",
            media_type="video",
            error='MediaEmpty: [400 MEDIA_EMPTY] (caused by "messages.SendMultiMedia")',
        )
        enqueue_error(
            db,
            source_message_id=10,
            key="permission",
            media_type="video",
            error="ChatWriteForbidden: not enough rights",
        )
        enqueue_error(
            db,
            source_message_id=11,
            key="network",
            media_type="video",
            error="ConnectionError: network timed out",
            status="failed",
        )
        enqueue_error(
            db,
            source_message_id=12,
            key="unsupported",
            media_type="unsupported",
            error="Filtered out by config",
        )

        summary = repair_summary(db)

        assert summary["media_empty"] == 1
        assert summary["permission"] == 1
        assert summary["temporary"] == 1
        assert summary["unsupported"] == 1

        revived = requeue_retryable_repairs(db)
        rows = db.query("SELECT file_unique_key, status FROM messages ORDER BY id")
        states = {str(row["file_unique_key"]): str(row["status"]) for row in rows}

        assert revived == 2
        assert states["media-empty"] == "pending"
        assert states["network"] == "pending"
        assert states["permission"] == "skipped"
        assert states["unsupported"] == "skipped"
    finally:
        db.close()


def test_permission_retry_is_explicit(tmp_path: Path) -> None:
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    try:
        enqueue_error(
            db,
            source_message_id=20,
            key="permission",
            media_type="video",
            error="ChannelPrivate: channel is private",
        )

        revived = requeue_repair_category(db, "permission")
        row = db.query_one("SELECT status, attempts, last_error FROM messages WHERE file_unique_key = ?", ("permission",))

        assert revived == 1
        assert row is not None
        assert row["status"] == "pending"
        assert row["attempts"] == 0
        assert row["last_error"] is None
    finally:
        db.close()


def test_classify_peer_id_error() -> None:
    category = classify_repair_error(
        {
            "media_type": "video",
            "last_error": "PeerIdInvalid: Peer id invalid: -100123",
        }
    )
    assert category == "peer_id"
