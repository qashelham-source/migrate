from __future__ import annotations

import asyncio
import errno
import os
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import yaml

import main as migration_main
from app.advanced import classify_repair_error, requeue_retryable_repairs, resolve_uncertain_upload
from app.db import Database
from app.destination_manager import set_destinations
from app.queue import MessageQueue
from app.worker import Worker


def make_config(tmp_path: Path) -> object:
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
    source_message_id: int,
    file_unique_key: str = "same-media",
    status: str = "pending",
    last_error: str | None = None,
) -> bool:
    return queue.enqueue(
        source_chat_id=-100111,
        source_message_id=source_message_id,
        dest_chat_id=-100222,
        file_unique_key=file_unique_key,
        source_message_ids=[source_message_id],
        source_topic_id=None,
        dest_topic_id=None,
        media_group_id=None,
        media_type="video",
        file_size=1024,
        caption="caption",
        status=status,
        last_error=last_error,
    )


def worker_config(tmp_path: Path) -> object:
    return SimpleNamespace(
        queue=SimpleNamespace(
            db_path=tmp_path / "data" / "migration.sqlite3",
            max_attempts=3,
            retry_backoff_seconds=[1, 2, 3],
        ),
        downloads=SimpleNamespace(root=tmp_path / "downloads"),
    )


def test_same_media_in_two_source_posts_creates_two_delivery_jobs(tmp_path: Path) -> None:
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    try:
        queue = MessageQueue(db, make_config(tmp_path))  # type: ignore[arg-type]

        assert enqueue(queue, source_message_id=10)
        assert enqueue(queue, source_message_id=11)
        assert not enqueue(queue, source_message_id=10, file_unique_key="changed-media")

        due = queue.fetch_due(10)
        assert [job.source_message_id for job in due] == [10, 11]
    finally:
        db.close()


def test_claim_is_exclusive_and_interrupted_upload_is_held_for_review(tmp_path: Path) -> None:
    db_path = tmp_path / "migration.sqlite3"
    db_one = Database(db_path)
    db_one.initialize()
    db_two: Database | None = None
    try:
        queue_one = MessageQueue(db_one, make_config(tmp_path))  # type: ignore[arg-type]
        assert enqueue(queue_one, source_message_id=10, file_unique_key="first")
        assert enqueue(queue_one, source_message_id=11, file_unique_key="second")

        db_two = Database(db_path)
        db_two.initialize()
        queue_two = MessageQueue(db_two, make_config(tmp_path))  # type: ignore[arg-type]

        claimed = queue_one.claim_due(10)
        assert len(claimed) == 2
        assert {job.status for job in claimed} == {"downloading"}
        assert {job.attempts for job in claimed} == {1}
        assert queue_two.claim_due(10) == []

        queue_one.set_phase(claimed[0].id, "uploading")
        recovery = queue_two.recover_in_progress()
        assert recovery.requeued_downloads == 1
        assert recovery.held_uploads == 1

        upload_row = db_two.query_one("SELECT status, last_error FROM messages WHERE id = ?", (claimed[0].id,))
        download_row = db_two.query_one("SELECT status FROM messages WHERE id = ?", (claimed[1].id,))
        assert upload_row is not None
        assert upload_row["status"] == "failed"
        assert "Verify destination" in upload_row["last_error"]
        assert download_row is not None
        assert download_row["status"] == "pending"
    finally:
        if db_two is not None:
            db_two.close()
        db_one.close()


def test_config_save_does_not_leave_plaintext_backup(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "telegram": {"api_hash": "secret-hash", "bot_token": "secret-token"},
                "migration": {"sources": [], "destinations": []},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    set_destinations(["@destination"], config_path)

    assert not config_path.with_suffix(".yaml.bak").exists()
    assert not config_path.with_suffix(".yaml.tmp").exists()
    assert os.stat(config_path).st_mode & 0o077 == 0


def test_live_service_waits_for_trigger_before_first_migration_cycle(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = make_config(tmp_path)
    config.ensure_directories = lambda: None
    db = Database(config.queue.db_path)
    db.initialize()
    queue = MessageQueue(db, config)  # type: ignore[arg-type]

    class Reader:
        def add_handler(self, handler) -> None:
            self.handler = handler

        def remove_handler(self, handler) -> None:
            assert handler is self.handler

    async def no_registry(*args, **kwargs) -> None:
        return None

    async def source_ids(*args, **kwargs) -> set[int]:
        return set()

    async def wait_without_trigger(self, current_config, stop_event, **kwargs):
        assert kwargs["allow_reconciliation"] is False
        return None

    async def should_not_run_cycle(*args, **kwargs) -> None:
        raise AssertionError("Live service started a migration cycle before a trigger")

    monkeypatch.setattr(migration_main, "load_config", lambda _: config)
    monkeypatch.setattr(migration_main, "refresh_source_registry", no_registry)
    monkeypatch.setattr(migration_main, "_resolved_source_ids", source_ids)
    monkeypatch.setattr(migration_main.LiveTrigger, "wait", wait_without_trigger)
    monkeypatch.setattr(migration_main, "_execute_cycle", should_not_run_cycle)

    try:
        asyncio.run(
            migration_main._run_live_service(
                config,
                "unused.yaml",
                reader=Reader(),
                bot=None,
                limiter=object(),
                queue=queue,
                stop_event=asyncio.Event(),
                reader_me=object(),
                writer_me=object(),
                logger=None,
            )
        )
    finally:
        db.close()


def test_live_service_resumes_safe_pending_work_after_restart(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = make_config(tmp_path)
    config.ensure_directories = lambda: None
    db = Database(config.queue.db_path)
    db.initialize()
    queue = MessageQueue(db, config)  # type: ignore[arg-type]
    calls: list[tuple[str, str | None]] = []

    class Reader:
        def add_handler(self, handler) -> None:
            self.handler = handler

        def remove_handler(self, handler) -> None:
            assert handler is self.handler

    class Trigger:
        def __init__(self, source_ids) -> None:
            self.source_ids = source_ids
            self.handler = object()
            self.settings = SimpleNamespace(reconcile_interval_seconds=300)

        async def wait(self, *args, **kwargs):
            raise AssertionError("Safe queued work should resume without waiting for Start Queue")

    async def no_registry(*args, **kwargs) -> None:
        return None

    async def source_ids(*args, **kwargs) -> set[int]:
        return set()

    async def safe_pending(*args, **kwargs):
        return migration_main.CycleOutcome("retry", retry_after_seconds=2)

    async def run_once(*args, **kwargs):
        calls.append((args[1], kwargs.get("trigger_reason")))
        kwargs["stop_event"].set()
        return migration_main.CycleOutcome("complete")

    monkeypatch.setattr(migration_main, "load_config", lambda _: config)
    monkeypatch.setattr(migration_main, "refresh_source_registry", no_registry)
    monkeypatch.setattr(migration_main, "_resolved_source_ids", source_ids)
    monkeypatch.setattr(migration_main, "_write_initial_wait_status", safe_pending)
    monkeypatch.setattr(migration_main, "_execute_cycle", run_once)
    monkeypatch.setattr(migration_main, "LiveTrigger", Trigger)

    try:
        asyncio.run(
            migration_main._run_live_service(
                config,
                "unused.yaml",
                reader=Reader(),
                bot=None,
                limiter=object(),
                queue=queue,
                stop_event=asyncio.Event(),
                reader_me=object(),
                writer_me=object(),
                logger=None,
            )
        )
        assert calls == [("process", "automatic_resume")]
    finally:
        db.close()


def test_download_broken_pipe_retries_automatically_after_the_normal_attempt_limit(tmp_path: Path) -> None:
    config = worker_config(tmp_path)
    db = Database(config.queue.db_path)
    db.initialize()

    class DownloadInterrupted:
        async def process(self, job, stop_event, on_phase):
            await on_phase("downloading")
            raise OSError(errno.EPIPE, "Broken pipe")

    try:
        queue = MessageQueue(db, config)  # type: ignore[arg-type]
        assert enqueue(queue, source_message_id=10)
        job = queue.claim_due(1)[0]
        db.execute("UPDATE messages SET attempts = ? WHERE id = ?", (9, job.id))
        job = replace(job, attempts=9)

        worker = Worker(config, queue, DownloadInterrupted())  # type: ignore[arg-type]
        asyncio.run(worker._process_one(job, asyncio.Event(), batch_index=1, batch_total=1))

        row = db.query_one("SELECT status, last_error, next_retry_at FROM messages WHERE id = ?", (job.id,))
        assert row is not None
        assert row["status"] == "pending"
        assert "retrying automatically" in row["last_error"]
        assert row["next_retry_at"]
    finally:
        db.close()


def test_upload_broken_pipe_is_held_to_prevent_a_duplicate(tmp_path: Path) -> None:
    config = worker_config(tmp_path)
    db = Database(config.queue.db_path)
    db.initialize()

    class UploadInterrupted:
        async def process(self, job, stop_event, on_phase):
            await on_phase("uploading")
            raise OSError(errno.EPIPE, "Broken pipe")

    try:
        queue = MessageQueue(db, config)  # type: ignore[arg-type]
        assert enqueue(queue, source_message_id=10)
        job = queue.claim_due(1)[0]

        worker = Worker(config, queue, UploadInterrupted())  # type: ignore[arg-type]
        asyncio.run(worker._process_one(job, asyncio.Event(), batch_index=1, batch_total=1))

        row = db.query_one("SELECT status, last_error FROM messages WHERE id = ?", (job.id,))
        assert row is not None
        assert row["status"] == "failed"
        assert "destination result is unknown" in row["last_error"]
        assert classify_repair_error(row) == "needs_review"
        assert requeue_retryable_repairs(db) == 0
    finally:
        db.close()


def test_legacy_broken_pipe_can_be_confirmed_or_retried_after_one_check(tmp_path: Path) -> None:
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    try:
        queue = MessageQueue(db, make_config(tmp_path))  # type: ignore[arg-type]
        assert enqueue(
            queue,
            source_message_id=10,
            status="failed",
            last_error="OSError: [Errno 32] Broken pipe",
        )
        first = db.query_one("SELECT id, status, media_type, last_error FROM messages WHERE source_message_id = 10")
        assert first is not None
        assert classify_repair_error(first) == "needs_review"
        assert resolve_uncertain_upload(db, int(first["id"]), delivered=False)

        retried = db.query_one("SELECT status, attempts, last_error FROM messages WHERE id = ?", (first["id"],))
        assert retried is not None
        assert retried["status"] == "pending"
        assert retried["attempts"] == 0
        assert retried["last_error"] is None

        assert enqueue(
            queue,
            source_message_id=11,
            file_unique_key="second",
            status="failed",
            last_error="OSError: [Errno 32] Broken pipe",
        )
        second = db.query_one("SELECT id FROM messages WHERE source_message_id = 11")
        assert second is not None
        assert resolve_uncertain_upload(db, int(second["id"]), delivered=True)

        confirmed = db.query_one("SELECT status, verified_at FROM messages WHERE id = ?", (second["id"],))
        assert confirmed is not None
        assert confirmed["status"] == "copied"
        assert confirmed["verified_at"]
    finally:
        db.close()


def test_repair_work_retries_automatically_instead_of_blocking_the_next_source() -> None:
    state = {
        "failed_jobs": 0,
        "skipped_issue_jobs": 0,
        "verification_failed_jobs": 0,
        "paused_jobs": 0,
        "active_jobs": 0,
        "delayed_jobs": 0,
        "verification_pending_jobs": 0,
        "verification_repairing_jobs": 1,
        "runnable_jobs": 1,
    }

    outcome = migration_main._source_outcome(
        state,
        source_chat_id=-100111,
        source_index=1,
        source_total=4,
    )

    assert outcome.state == "retry"
    assert outcome.retry_after_seconds == 2
    assert "automatically" in str(outcome.message)


def test_waiting_repair_verification_never_blocks_source_progress_on_its_own() -> None:
    state = {
        "failed_jobs": 0,
        "skipped_issue_jobs": 0,
        "verification_failed_jobs": 0,
        "paused_jobs": 0,
        "active_jobs": 0,
        "delayed_jobs": 0,
        "verification_pending_jobs": 0,
        "verification_repairing_jobs": 1,
        "runnable_jobs": 0,
    }

    outcome = migration_main._source_outcome(
        state,
        source_chat_id=-100111,
        source_index=1,
        source_total=4,
    )

    assert outcome.state == "retry"
    assert outcome.retry_after_seconds == 5
