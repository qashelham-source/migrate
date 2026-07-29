from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from app.db import Database
from app.destination_manager import get_sources, set_sources
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
