from __future__ import annotations

from pathlib import Path

from app.db import Database
from app.scanner import build_scan_plan


def test_incremental_plan_starts_after_checkpoint() -> None:
    plan = build_scan_plan(
        configured_start=1,
        configured_end=None,
        latest_message_id=900,
        checkpoint=846,
        queue_highwater=700,
        scan_mode="incremental",
    )

    assert plan is not None
    assert plan.start_id == 847
    assert plan.end_id == 900
    assert plan.baseline == 846
    assert plan.bootstrapped_from_queue is False
    assert plan.has_work is True


def test_first_incremental_sync_bootstraps_from_existing_queue() -> None:
    plan = build_scan_plan(
        configured_start=1,
        configured_end=None,
        latest_message_id=900,
        checkpoint=None,
        queue_highwater=846,
        scan_mode="incremental",
    )

    assert plan is not None
    assert plan.start_id == 847
    assert plan.end_id == 900
    assert plan.baseline == 846
    assert plan.bootstrapped_from_queue is True


def test_full_scan_ignores_checkpoint() -> None:
    plan = build_scan_plan(
        configured_start=10,
        configured_end=500,
        latest_message_id=900,
        checkpoint=450,
        queue_highwater=450,
        scan_mode="full",
    )

    assert plan is not None
    assert plan.start_id == 10
    assert plan.end_id == 500
    assert plan.baseline is None
    assert plan.bootstrapped_from_queue is False


def test_incremental_plan_reports_no_work_when_checkpoint_is_current() -> None:
    plan = build_scan_plan(
        configured_start=1,
        configured_end=None,
        latest_message_id=846,
        checkpoint=846,
        queue_highwater=None,
        scan_mode="incremental",
    )

    assert plan is not None
    assert plan.start_id == 847
    assert plan.end_id == 846
    assert plan.has_work is False


def test_checkpoint_round_trip_and_album_highwater(tmp_path: Path) -> None:
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    try:
        assert db.enqueue_message(
            source_chat_id="-1001111111111",
            source_message_id=335,
            dest_chat_id="-1002222222222",
            file_unique_key="album:one|two|three",
            source_message_ids=[335, 336, 337],
            source_topic_id=None,
            dest_topic_id=None,
            media_group_id="album-1",
            media_type="video",
            file_size=123,
            caption=None,
        )
        assert db.source_queue_highwater(-1001111111111) == 337

        db.set_scan_checkpoint(-1001111111111, None, 337, "incremental")
        row = db.get_scan_checkpoint(-1001111111111, None)

        assert row is not None
        assert row["last_scanned_message_id"] == 337
        assert row["last_scan_mode"] == "incremental"

        db.set_scan_checkpoint(-1001111111111, None, 900, "full")
        row = db.get_scan_checkpoint(-1001111111111, None)
        assert row is not None
        assert row["last_scanned_message_id"] == 900
        assert row["last_scan_mode"] == "full"
    finally:
        db.close()
