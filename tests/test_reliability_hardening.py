from __future__ import annotations

import asyncio
import os
from pathlib import Path
from types import SimpleNamespace

import yaml

import main as migration_main
from app.db import Database
from app.destination_manager import set_destinations
from app.queue import MessageQueue


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

    async def wait_without_trigger(self, current_config, stop_event):
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
