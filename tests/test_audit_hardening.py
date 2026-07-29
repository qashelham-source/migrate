from __future__ import annotations

import asyncio
import os
import sqlite3
import stat
import time
from pathlib import Path

import pytest
import yaml

from app.config import load_config
from app.db import Database
from app.telegram_client import TelegramLimiter, load_accounts, save_accounts
from optimize_database import optimize


def _write_config(path: Path, **overrides: object) -> Path:
    data: dict[str, object] = {
        "telegram": {
            "api_id": 12345,
            "api_hash": "test-hash",
            "admin_ids": [111],
            "bot": {"enabled": False, "use_for_uploads": False},
        },
        "migration": {"sources": [], "destinations": []},
        "limits": {
            "global_min_delay_seconds": 0,
            "resolve_delay_seconds": 0,
            "read_delay_seconds": 0,
            "download_delay_seconds": 0,
            "copy_delay_seconds": 0,
            "upload_delay_seconds": 0.2,
            "verify_delay_seconds": 0,
            "floodwait_extra_min_seconds": 0,
            "floodwait_extra_max_seconds": 0,
        },
    }
    for key, value in overrides.items():
        data[key] = value
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
    return path


def _enqueue(
    db: Database,
    *,
    source_message_id: int,
    dest_topic_id: int | None,
    status: str = "pending",
    last_error: str | None = None,
) -> bool:
    return db.enqueue_message(
        source_chat_id="-1001",
        source_message_id=source_message_id,
        dest_chat_id="-1002",
        file_unique_key=f"item:{source_message_id}",
        source_message_ids=[source_message_id],
        source_topic_id=None,
        dest_topic_id=dest_topic_id,
        media_group_id=None,
        media_type="video",
        file_size=100,
        caption=None,
        status=status,
        last_error=last_error,
    )


def test_admin_ids_are_loaded_from_yaml_and_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ADMIN_USER_ID", "222,333")
    config = load_config(_write_config(tmp_path / "config.yaml"))
    assert config.telegram.admin_ids == (111, 222, 333)


def test_invalid_boolean_and_floodwait_ranges_are_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ADMIN_USER_ID", raising=False)
    path = _write_config(
        tmp_path / "invalid.yaml",
        telegram={
            "api_id": 12345,
            "api_hash": "test-hash",
            "bot": {"enabled": "sometimes"},
        },
    )
    with pytest.raises(ValueError, match="boolean"):
        load_config(path)

    path = _write_config(
        tmp_path / "invalid-range.yaml",
        limits={"floodwait_extra_min_seconds": 10, "floodwait_extra_max_seconds": 2},
    )
    with pytest.raises(ValueError, match="min cannot exceed max"):
        load_config(path)


def test_same_message_can_target_different_topics_without_duplication(tmp_path: Path) -> None:
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    try:
        assert _enqueue(db, source_message_id=7, dest_topic_id=10)
        assert _enqueue(db, source_message_id=7, dest_topic_id=20)
        assert not _enqueue(db, source_message_id=7, dest_topic_id=10)
        count = db.query_one("SELECT COUNT(*) AS count FROM messages")
        assert count is not None
        assert int(count["count"]) == 2
    finally:
        db.close()


def test_database_initialize_does_not_mutate_queue_state(tmp_path: Path) -> None:
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    try:
        assert _enqueue(
            db,
            source_message_id=8,
            dest_topic_id=None,
            status="failed",
            last_error="SendMultiMedia MEDIA_EMPTY",
        )
        db.initialize()
        row = db.query_one("SELECT status, last_error FROM messages WHERE source_message_id = 8")
        assert row is not None
        assert row["status"] == "failed"
        assert row["last_error"] == "SendMultiMedia MEDIA_EMPTY"

        assert db.requeue_send_multi_media_errors() == 1
        row = db.query_one("SELECT status FROM messages WHERE source_message_id = 8")
        assert row is not None
        assert row["status"] == "pending"
    finally:
        db.close()


def test_optimizer_bootstraps_a_fresh_database(tmp_path: Path) -> None:
    path = tmp_path / "fresh.sqlite3"
    optimize(path)
    connection = sqlite3.connect(path)
    try:
        tables = {
            str(row[0])
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        assert {"messages", "verification_results", "repair_actions", "destination_health"}.issubset(tables)
        indexes = {
            str(row[0])
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'index'")
        }
        assert "idx_messages_pending_order" in indexes
        assert "idx_verification_results_status" in indexes
    finally:
        connection.close()


def test_account_cache_is_atomic_private_and_recoverable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ADMIN_USER_ID", raising=False)
    config = load_config(_write_config(tmp_path / "config.yaml"))
    config.ensure_directories()

    save_accounts(config, {"user": {"id": 111}})
    path = config.telegram.sessions_dir / "accounts.json"
    assert load_accounts(config) == {"user": {"id": 111}}
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert not path.with_suffix(".json.tmp").exists()

    path.write_text("{broken", encoding="utf-8")
    assert load_accounts(config) == {}
    assert not path.exists()
    assert list(config.telegram.sessions_dir.glob("accounts.json.corrupt-*"))


def test_operation_delay_does_not_hold_the_global_limiter_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ADMIN_USER_ID", raising=False)
    config = load_config(_write_config(tmp_path / "config.yaml"))
    limiter = TelegramLimiter(config)

    async def scenario() -> None:
        limiter._last_by_operation["upload"] = time.monotonic()
        delayed_upload = asyncio.create_task(limiter.wait("upload"))
        await asyncio.sleep(0.01)
        await asyncio.wait_for(limiter.wait("read"), timeout=0.05)
        assert not delayed_upload.done()
        delayed_upload.cancel()
        with pytest.raises(asyncio.CancelledError):
            await delayed_upload

    asyncio.run(scenario())
