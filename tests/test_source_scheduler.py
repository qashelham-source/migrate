from __future__ import annotations

import errno
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from app.db import Database
from app.destination_manager import blacklist_source, get_source_blacklist, get_sources, set_sources
from app.queue import MessageQueue


def _config(tmp_path: Path) -> object:
    return SimpleNamespace(
        queue=SimpleNamespace(
            db_path=tmp_path / "migration.sqlite3",
            max_attempts=3,
            retry_backoff_seconds=[1, 2, 3],
        ),
    )


def _enqueue(
    queue: MessageQueue,
    *,
    source_chat_id: int,
    source_message_id: int,
    status: str = "pending",
    last_error: str | None = None,
) -> None:
    assert queue.enqueue(
        source_chat_id=source_chat_id,
        source_message_id=source_message_id,
        dest_chat_id=-100900,
        file_unique_key=f"source:{source_chat_id}:message:{source_message_id}",
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


def test_claiming_one_source_does_not_start_the_waiting_source(tmp_path: Path) -> None:
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    try:
        queue = MessageQueue(db, _config(tmp_path))  # type: ignore[arg-type]
        _enqueue(queue, source_chat_id=-100001, source_message_id=1)
        _enqueue(queue, source_chat_id=-100002, source_message_id=1)

        claimed = queue.claim_due(10, source_chat_id=-100001)

        assert [job.source_chat_id for job in claimed] == [-100001]
        waiting = db.query_one(
            "SELECT status FROM messages WHERE source_chat_id = ?",
            ("-100002",),
        )
        assert waiting is not None
        assert waiting["status"] == "pending"
    finally:
        db.close()


def test_source_work_state_marks_delayed_retry_without_marking_it_blocked(tmp_path: Path) -> None:
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    try:
        queue = MessageQueue(db, _config(tmp_path))  # type: ignore[arg-type]
        _enqueue(queue, source_chat_id=-100001, source_message_id=1)
        row = db.query_one("SELECT id FROM messages WHERE source_chat_id = ?", ("-100001",))
        assert row is not None
        retry_at = (datetime.now(timezone.utc) + timedelta(minutes=2)).isoformat(timespec="seconds")
        db.set_status(int(row["id"]), "pending", last_error="Temporary network error", next_retry_at=retry_at)

        state = queue.source_work_state(-100001)

        assert state["delayed_jobs"] == 1
        assert state["runnable_jobs"] == 0
        assert state["failed_jobs"] == 0
        assert state["next_retry_at"] == retry_at
    finally:
        db.close()


def test_source_work_state_ignores_filtered_items_but_holds_terminal_failure(tmp_path: Path) -> None:
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    try:
        queue = MessageQueue(db, _config(tmp_path))  # type: ignore[arg-type]
        _enqueue(
            queue,
            source_chat_id=-100001,
            source_message_id=1,
            status="skipped",
            last_error="Filtered out by config",
        )
        _enqueue(
            queue,
            source_chat_id=-100001,
            source_message_id=2,
            status="failed",
            last_error="Interrupted during upload; destination result is unknown.",
        )

        state = queue.source_work_state(-100001)

        assert state["skipped_issue_jobs"] == 0
        assert state["failed_jobs"] == 1
        assert state["last_error"] == "Interrupted during upload; destination result is unknown."
    finally:
        db.close()


def test_source_queue_order_is_preserved_when_saved(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "migration:\n  sources: []\n  destinations:\n    - chat: '@destination'\n",
        encoding="utf-8",
    )

    set_sources(["@source-first", "@source-second", "@source-third"], config_path)

    assert [item["chat"] for item in get_sources(config_path)] == [
        "@source-first",
        "@source-second",
        "@source-third",
    ]


def test_blacklisting_source_removes_it_from_queue_and_keeps_it_hidden(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "migration:\n  sources: []\n  destinations:\n    - chat: '@destination'\n",
        encoding="utf-8",
    )
    set_sources(["@source-first", "@source-second"], config_path)

    blacklisted = blacklist_source("@source-second", config_path)

    assert blacklisted == "@source-second"
    assert get_source_blacklist(config_path) == ["@source-second"]
    assert [item["chat"] for item in get_sources(config_path)] == ["@source-first"]
    assert [item["chat"] for item in set_sources(["@source-first", "@source-second"], config_path)] == [
        "@source-first"
    ]


def test_purging_a_blacklisted_source_deletes_its_jobs_and_checkpoints(tmp_path: Path) -> None:
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    try:
        queue = MessageQueue(db, _config(tmp_path))  # type: ignore[arg-type]
        queue.register_source(
            source_chat_id=-100001,
            title="Old source",
            username="old_source",
            chat_type="channel",
            latest_seen_message_id=2,
        )
        queue.register_source(
            source_chat_id=-100002,
            title="Keep source",
            username="keep_source",
            chat_type="channel",
            latest_seen_message_id=1,
        )
        _enqueue(queue, source_chat_id=-100001, source_message_id=1)
        _enqueue(queue, source_chat_id=-100001, source_message_id=2)
        _enqueue(queue, source_chat_id=-100002, source_message_id=1)
        old_jobs = db.query("SELECT id FROM messages WHERE source_chat_id = ? ORDER BY id", ("-100001",))
        assert len(old_jobs) == 2
        parent_id, repair_id = (int(row["id"]) for row in old_jobs)
        queue.record_verification(
            job_id=parent_id,
            status="repairing",
            expected_count=1,
            present_count=0,
            media_match=False,
            caption_match=None,
            size_match=None,
        )
        queue.update_telemetry(parent_id, stage="processing")
        queue.release3.link_repair(
            parent_job_id=parent_id,
            repair_job_id=repair_id,
            source_message_id=2,
        )
        queue.release3.log_repair(
            action="test_cleanup",
            job_id=parent_id,
            source_chat_id=-100001,
            dest_chat_id=-100900,
        )
        queue.set_scan_checkpoint(-100001, None, 2, "full")
        queue.set_scan_checkpoint(-100001, 99, 2, "full")
        queue.set_scan_checkpoint(-100002, None, 1, "full")
        queue.save_media_cache("shared-cache", ["file-id"], ["video"])

        deleted = queue.purge_source_jobs("@old_source")

        assert deleted["jobs"] == 2
        assert db.query_one("SELECT id FROM messages WHERE source_chat_id = ?", ("-100001",)) is None
        assert db.query_one("SELECT id FROM messages WHERE source_chat_id = ?", ("-100002",)) is not None
        assert db.query_one("SELECT 1 FROM scan_checkpoints WHERE source_chat_id = ?", ("-100001",)) is None
        assert db.query_one("SELECT 1 FROM scan_checkpoints WHERE source_chat_id = ?", ("-100002",)) is not None
        assert db.query_one("SELECT 1 FROM source_registry WHERE source_chat_id = ?", ("-100001",)) is None
        assert db.query_one("SELECT 1 FROM source_registry WHERE source_chat_id = ?", ("-100002",)) is not None
        assert db.query_one("SELECT 1 FROM verification_results WHERE job_id = ?", (parent_id,)) is None
        assert db.query_one("SELECT 1 FROM job_telemetry WHERE job_id = ?", (parent_id,)) is None
        assert db.query_one("SELECT 1 FROM repair_links WHERE parent_job_id = ?", (parent_id,)) is None
        assert db.query_one("SELECT 1 FROM repair_actions WHERE job_id = ?", (parent_id,)) is None
        assert queue.media_cache_count() == 1
    finally:
        db.close()


def test_clearing_old_source_history_keeps_future_posts_enabled(tmp_path: Path) -> None:
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    try:
        queue = MessageQueue(db, _config(tmp_path))  # type: ignore[arg-type]
        queue.register_source(
            source_chat_id=-100001,
            title="Keep watching",
            username="keep_watching",
            chat_type="channel",
            latest_seen_message_id=500,
        )
        _enqueue(queue, source_chat_id=-100001, source_message_id=1)
        _enqueue(queue, source_chat_id=-100002, source_message_id=1)
        queue.set_scan_checkpoint(-100001, 77, 10, "full")

        cleared = queue.clear_source_history(-100001, 500)

        assert cleared["jobs"] == 1
        assert cleared["checkpoint"] == 500
        assert db.query_one("SELECT 1 FROM messages WHERE source_chat_id = ?", ("-100001",)) is None
        assert db.query_one("SELECT 1 FROM messages WHERE source_chat_id = ?", ("-100002",)) is not None
        checkpoint = db.get_scan_checkpoint(-100001)
        assert checkpoint is not None
        assert int(checkpoint["last_scanned_message_id"]) == 500
        assert checkpoint["last_scan_mode"] == "skip_history"
        assert queue.history_clear_is_pending(-100001)
        assert db.query_one("SELECT 1 FROM source_registry WHERE source_chat_id = ?", ("-100001",)) is not None

        queue.set_scan_checkpoint(-100001, None, 501, "incremental")

        assert not queue.history_clear_is_pending(-100001)
    finally:
        db.close()


def test_config_save_falls_back_when_bind_mounted_file_is_busy(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "migration:\n  sources: []\n  destinations:\n    - chat: '@destination'\n",
        encoding="utf-8",
    )
    original_replace = Path.replace

    def busy_replace(self: Path, target: str | Path) -> Path:
        if self == config_path.with_suffix(".yaml.tmp"):
            raise OSError(errno.EBUSY, "Device or resource busy", str(self), str(target))
        return original_replace(self, target)

    with patch.object(Path, "replace", busy_replace):
        set_sources(["@source-first"], config_path)

    assert [item["chat"] for item in get_sources(config_path)] == ["@source-first"]
    assert not config_path.with_suffix(".yaml.tmp").exists()
