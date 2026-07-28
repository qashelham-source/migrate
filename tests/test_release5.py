from __future__ import annotations

from pathlib import Path

from app.db import Database
from app.release5 import (
    Release5Store,
    build_shadow_plan,
    choose_safe_parallelism,
    estimate_storage,
    smart_eta,
)
from app.telemetry import StoragePolicy


def enqueue(db: Database, message_id: int, destination: str, size: int | None) -> None:
    assert db.enqueue_message(
        source_chat_id="-1001",
        source_message_id=message_id,
        dest_chat_id=destination,
        file_unique_key=f"file-{message_id}-{destination}",
        source_message_ids=[message_id],
        source_topic_id=None,
        dest_topic_id=None,
        media_group_id=None,
        media_type="video",
        file_size=size,
        caption=None,
    )


def test_shadow_plan_is_read_only_and_persistent(tmp_path: Path) -> None:
    db = Database(tmp_path / "queue.db")
    db.initialize()
    try:
        enqueue(db, 1, "-200", 10 * 1024 * 1024)
        enqueue(db, 2, "-300", 20 * 1024 * 1024)
        before = db.query("SELECT id, status, attempts FROM messages ORDER BY id")
        plan = build_shadow_plan(db, tmp_path, configured_max_workers=3)
        after = db.query("SELECT id, status, attempts FROM messages ORDER BY id")

        assert plan["mode"] == "shadow"
        assert plan["read_only"] is True
        assert plan["pending_jobs"] == 2
        assert plan["destinations"] == 2
        assert [(r["id"], r["status"], r["attempts"]) for r in before] == [
            (r["id"], r["status"], r["attempts"]) for r in after
        ]
        saved = db.query_one("SELECT pending_jobs, recommended_workers FROM shadow_runs")
        assert saved is not None and saved["pending_jobs"] == 2
    finally:
        db.close()


def test_storage_estimation_counts_unknown_jobs(tmp_path: Path) -> None:
    db = Database(tmp_path / "queue.db")
    db.initialize()
    try:
        enqueue(db, 1, "-200", 100)
        enqueue(db, 2, "-200", None)
        estimate = estimate_storage(
            db,
            tmp_path,
            unknown_job_bytes=1000,
            simultaneous_downloads=1,
            policy=StoragePolicy(reserve_bytes=0, warning_free_bytes=0, critical_free_bytes=0),
        )
        assert estimate.queued_bytes == 1100
        assert estimate.unknown_jobs == 1
        assert estimate.temporary_peak_bytes == 100
        assert estimate.required_bytes >= 64 * 1024 * 1024
    finally:
        db.close()


def test_parallelism_is_destination_isolated_and_storage_safe(tmp_path: Path) -> None:
    db = Database(tmp_path / "queue.db")
    db.initialize()
    try:
        enqueue(db, 1, "-200", 100)
        enqueue(db, 2, "-300", 100)
        estimate = estimate_storage(
            db,
            tmp_path,
            simultaneous_downloads=2,
            policy=StoragePolicy(reserve_bytes=0, warning_free_bytes=0, critical_free_bytes=0),
        )
        decision = choose_safe_parallelism(db, estimate, configured_max=4)
        assert decision.workers == 2
        assert decision.pending_destinations == 2
    finally:
        db.close()


def test_smart_eta_uses_median_and_confidence(tmp_path: Path) -> None:
    db = Database(tmp_path / "queue.db")
    db.initialize()
    try:
        enqueue(db, 1, "-200", 3_000)
        eta = smart_eta(db, [1000, 1000, 10_000])
        assert eta.speed_bps == 1000
        assert eta.seconds == 3
        assert eta.remaining_bytes == 3000
        assert eta.sample_count == 3
        assert 0 < eta.confidence < 1
    finally:
        db.close()


def test_performance_samples_round_trip(tmp_path: Path) -> None:
    db = Database(tmp_path / "queue.db")
    db.initialize()
    try:
        store = Release5Store(db)
        store.initialize()
        store.record_sample(bytes_total=2000, duration_seconds=2, route="upload", media_type="video")
        store.record_sample(bytes_total=1000, duration_seconds=2, successful=False)
        assert store.recent_speeds() == [1000.0]
    finally:
        db.close()
