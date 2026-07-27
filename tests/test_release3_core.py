from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from app.db import Database
from app.live import consume_run_request
from app.queue import MessageQueue
from app.telemetry import StoragePolicy, estimate_eta_seconds, storage_snapshot


def config_for(tmp_path: Path) -> object:
    return SimpleNamespace(
        queue=SimpleNamespace(
            db_path=tmp_path / "data" / "migration.sqlite3",
            max_attempts=3,
            retry_backoff_seconds=[1, 2, 3],
        ),
    )


def enqueue(
    queue: MessageQueue,
    *,
    source_id: int = -100111,
    message_id: int = 10,
    destination_id: int = -100222,
    key: str = "media-key",
) -> None:
    assert queue.enqueue(
        source_chat_id=source_id,
        source_message_id=message_id,
        dest_chat_id=destination_id,
        file_unique_key=key,
        source_message_ids=[message_id],
        source_topic_id=None,
        dest_topic_id=None,
        media_group_id=None,
        media_type="video",
        file_size=1024,
        caption="hello",
    )


def test_release3_tables_are_additive_and_source_registry_round_trips(tmp_path: Path) -> None:
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    try:
        queue = MessageQueue(db, config_for(tmp_path))  # type: ignore[arg-type]
        enqueue(queue)
        queue.register_source(
            source_chat_id=-100111,
            title="VIP Archive",
            username="vip_archive",
            chat_type="channel",
            latest_seen_message_id=99,
        )
        rows = queue.list_registered_sources()

        assert len(rows) == 1
        assert rows[0]["source_chat_id"] == "-100111"
        assert rows[0]["title"] == "VIP Archive"
        assert rows[0]["latest_seen_message_id"] == 99
        assert db.query_one("SELECT COUNT(*) AS count FROM messages")["count"] == 1
    finally:
        db.close()


def test_delivery_matrix_tracks_every_destination_and_verification(tmp_path: Path) -> None:
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    try:
        queue = MessageQueue(db, config_for(tmp_path))  # type: ignore[arg-type]
        enqueue(queue, destination_id=-100222, key="same-file")
        enqueue(queue, destination_id=-100333, key="same-file")
        jobs = queue.fetch_due(10)
        assert len(jobs) == 2

        for index, job in enumerate(jobs, start=1):
            queue.mark_copied(job.id, [100 + index])
            queue.record_verification(
                job_id=job.id,
                status="verified",
                expected_count=1,
                present_count=1,
                media_match=True,
                caption_match=True,
                size_match=True,
            )
            queue.mark_verified(job.id)

        matrix = queue.delivery_matrix(source_chat_id=-100111, source_message_id=10)
        assert len(matrix) == 1
        assert matrix[0]["total_destinations"] == 2
        assert matrix[0]["verified_destinations"] == 2
        assert matrix[0]["overall_status"] == "verified"
    finally:
        db.close()


def test_paused_destination_does_not_block_other_destination(tmp_path: Path) -> None:
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    try:
        queue = MessageQueue(db, config_for(tmp_path))  # type: ignore[arg-type]
        enqueue(queue, destination_id=-100222, key="one")
        enqueue(queue, destination_id=-100333, key="two")
        queue.release3.pause_destination(-100222, "permission missing")

        due = queue.fetch_due(10)
        assert [job.dest_chat_id for job in due] == [-100333]

        queue.resume_destination(-100222)
        due = queue.fetch_due(10)
        assert {job.dest_chat_id for job in due} == {-100222, -100333}
    finally:
        db.close()


def test_missing_item_repair_completes_parent_verification(tmp_path: Path) -> None:
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    try:
        queue = MessageQueue(db, config_for(tmp_path))  # type: ignore[arg-type]
        assert queue.enqueue(
            source_chat_id=-100111,
            source_message_id=20,
            dest_chat_id=-100222,
            file_unique_key="album:a|b",
            source_message_ids=[20, 21],
            source_topic_id=None,
            dest_topic_id=None,
            media_group_id="album-1",
            media_type="video",
            file_size=2048,
            caption="album",
        )
        parent = queue.fetch_due(1)[0]
        queue.mark_copied(parent.id, [500, 501])
        queue.record_verification(
            job_id=parent.id,
            status="repairing",
            expected_count=2,
            present_count=1,
            media_match=True,
            caption_match=True,
            size_match=True,
            missing_source_message_ids=[21],
        )
        repair_id = queue.enqueue_repair_item(parent, 21)
        assert repair_id is not None

        repair = next(job for job in queue.fetch_due(10) if job.id == repair_id)
        queue.mark_copied(repair.id, [700])
        queue.record_verification(
            job_id=repair.id,
            status="verified",
            expected_count=1,
            present_count=1,
            media_match=True,
            caption_match=True,
            size_match=True,
        )
        queue.mark_verified(repair.id)

        assert queue.release3.verification_status(parent.id) == "verified_repaired"
        parent_row = db.query_one("SELECT verified_at FROM messages WHERE id = ?", (parent.id,))
        assert parent_row is not None and parent_row["verified_at"] is not None
    finally:
        db.close()


def test_eta_and_storage_snapshot_are_safe(tmp_path: Path) -> None:
    eta = estimate_eta_seconds(total=1_000, current=250, elapsed_seconds=5)
    assert eta == 15

    snapshot = storage_snapshot(
        tmp_path,
        StoragePolicy(reserve_bytes=0, warning_free_bytes=0, critical_free_bytes=0),
    )
    assert snapshot.total_bytes > 0
    assert snapshot.free_bytes >= 0
    assert snapshot.usable_bytes == snapshot.free_bytes
    assert snapshot.has_capacity(1) is True


def test_live_service_consumes_admin_run_request(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    runtime = config.queue.db_path.parent
    runtime.mkdir(parents=True)
    (runtime / "run_mode").write_text("run", encoding="utf-8")
    (runtime / "run_now").touch()

    assert consume_run_request(config) == "run"  # type: ignore[arg-type]
    assert not (runtime / "run_mode").exists()
    assert not (runtime / "run_now").exists()
