from __future__ import annotations

import asyncio
import errno
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

import app.destination_manager as destination_manager
from app.admin_bot import _authorized_ids
from app.config import ChatSpec, load_config
from app.db import Database
from app.queue import MessageJob, MessageQueue
from app.scanner import Scanner
from app.telegram_client import save_accounts
from app.upload import Uploader
from main import choose_writer_for_destinations


def _write_config(
    path: Path,
    *,
    admin_ids: list[int] | None = None,
    user_session: str = "operator",
    migration: dict[str, object] | None = None,
) -> Path:
    data: dict[str, object] = {
        "telegram": {
            "api_id": 12345,
            "api_hash": "test-hash",
            "user_session": user_session,
            "admin_ids": [111] if admin_ids is None else admin_ids,
            "bot": {"enabled": False, "use_for_uploads": False},
        },
        "migration": migration or {"sources": [], "destinations": []},
        "limits": {
            "global_min_delay_seconds": 0,
            "resolve_delay_seconds": 0,
            "read_delay_seconds": 0,
            "download_delay_seconds": 0,
            "copy_delay_seconds": 0,
            "upload_delay_seconds": 0,
            "verify_delay_seconds": 0,
            "floodwait_extra_min_seconds": 0,
            "floodwait_extra_max_seconds": 0,
        },
    }
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
    return path


def test_config_save_stages_in_tmpfs_when_config_parent_is_read_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("before: true\n", encoding="utf-8")
    real_mkstemp = destination_manager.tempfile.mkstemp
    real_replace = Path.replace

    def staged_mkstemp(*args: object, **kwargs: object) -> tuple[int, str]:
        if Path(str(kwargs["dir"])) == config_path.parent:
            raise OSError(errno.EROFS, "read-only filesystem")
        return real_mkstemp(*args, **kwargs)

    def busy_replace(self: Path, target: str | Path) -> Path:
        raise OSError(errno.EBUSY, "bind-mounted config")

    monkeypatch.setattr(destination_manager.tempfile, "mkstemp", staged_mkstemp)
    monkeypatch.setattr(Path, "replace", busy_replace)
    destination_manager._save_yaml(config_path, {"after": True})

    assert yaml.safe_load(config_path.read_text(encoding="utf-8")) == {"after": True}
    monkeypatch.setattr(Path, "replace", real_replace)


def test_admin_fallback_never_authorizes_an_unrelated_cached_account(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Keep an empty value present so python-dotenv cannot restore a value from
    # another test's temporary .env file.
    monkeypatch.setenv("ADMIN_USER_ID", "")
    config = load_config(_write_config(tmp_path / "config.yaml", admin_ids=[]))
    assert config.telegram.admin_ids == ()
    config.ensure_directories()
    save_accounts(config, {"operator": {"id": 111}, "stale-session": {"id": 999}})

    assert _authorized_ids(config) == {111}


def test_source_topic_configuration_is_rejected_before_any_scan(tmp_path: Path) -> None:
    path = _write_config(
        tmp_path / "topic-source.yaml",
        migration={
            "sources": [{"chat": "-100123", "topic_id": 77}],
            "destinations": ["-100456"],
        },
    )

    with pytest.raises(ValueError, match="sources topic_id is not supported"):
        load_config(path)


class _DirectLimiter:
    async def call(self, _operation: str, function: object, *args: object, **kwargs: object) -> object:
        return await function(*args, **kwargs)


class _ResolvableClient:
    def __init__(self, destinations: set[str]) -> None:
        self.destinations = destinations

    async def get_chat(self, chat: str) -> SimpleNamespace:
        if str(chat) not in self.destinations:
            raise RuntimeError(f"unavailable: {chat}")
        return SimpleNamespace(id=len(str(chat)), title=str(chat), username=None)


def test_writer_fallback_requires_the_same_client_to_resolve_every_destination(tmp_path: Path) -> None:
    config = load_config(_write_config(tmp_path / "config.yaml"))
    config = replace(config, destinations=[ChatSpec("one"), ChatSpec("two")])
    writer = _ResolvableClient({"one"})
    reader = _ResolvableClient({"two"})

    selected, ready = asyncio.run(
        choose_writer_for_destinations(config, reader, writer, _DirectLimiter())
    )

    assert selected is writer
    assert ready is False

    unavailable_user = _ResolvableClient(set())
    selected, ready = asyncio.run(
        choose_writer_for_destinations(config, unavailable_user, unavailable_user, _DirectLimiter())
    )
    assert selected is unavailable_user
    assert ready is False


def test_filtered_album_uses_only_enabled_members_and_never_native_copies_whole_album(
    tmp_path: Path,
) -> None:
    config = load_config(_write_config(tmp_path / "config.yaml"))
    scanner = Scanner.__new__(Scanner)
    scanner.config = config

    photo = SimpleNamespace(
        id=1,
        video=None,
        photo=SimpleNamespace(file_size=1, file_unique_id="photo-1"),
        document=None,
        animation=None,
        audio=None,
        voice=None,
        video_note=None,
        text=None,
        caption=None,
    )
    document = SimpleNamespace(
        id=2,
        video=None,
        photo=None,
        document=SimpleNamespace(file_size=1, file_unique_id="document-2"),
        animation=None,
        audio=None,
        voice=None,
        video_note=None,
        text=None,
        caption=None,
    )
    assert [message.id for message in scanner._processable_messages([photo, document])] == [1]

    class CopyClient:
        def __init__(self) -> None:
            self.group_calls = 0
            self.message_calls: list[int] = []

        async def copy_media_group(self, **_kwargs: object) -> list[SimpleNamespace]:
            self.group_calls += 1
            return []

        async def copy_message(self, *, message_id: int, **_kwargs: object) -> SimpleNamespace:
            self.message_calls.append(message_id)
            return SimpleNamespace(id=1000 + message_id)

    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    try:
        queue = MessageQueue(db, config)
        client = CopyClient()
        uploader = Uploader(config, client, client, _DirectLimiter(), queue)
        job = MessageJob(
            id=1,
            source_chat_id=-1001,
            source_message_id=1,
            dest_chat_id=-1002,
            status="pending",
            attempts=0,
            last_error=None,
            next_retry_at=None,
            file_unique_key="filtered-album",
            source_topic_id=None,
            dest_topic_id=None,
            media_group_id=None,
            source_message_ids=[1, 3],
            dest_message_ids=[],
            media_type="photo",
            file_size=None,
            caption=None,
        )
        source_messages = [
            SimpleNamespace(id=1, media_group_id="album-1"),
            SimpleNamespace(id=3, media_group_id="album-1"),
        ]

        result = asyncio.run(uploader._copy_or_forward(job, source_messages))

        assert result.dest_message_ids == [1001, 1003]
        assert client.group_calls == 0
        assert client.message_calls == [1, 3]
    finally:
        db.close()


def test_terminal_repair_failure_is_visible_and_can_be_requeued(tmp_path: Path) -> None:
    config = load_config(_write_config(tmp_path / "config.yaml"))
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    try:
        queue = MessageQueue(db, config)
        assert queue.enqueue(
            source_chat_id="-1001",
            source_message_id=7,
            dest_chat_id="-1002",
            file_unique_key="parent:7",
            source_message_ids=[7],
            source_topic_id=None,
            dest_topic_id=None,
            media_group_id=None,
            media_type="photo",
            file_size=1,
            caption=None,
            status="copied",
        )
        parent_row = db.query_one("SELECT * FROM messages WHERE file_unique_key = 'parent:7'")
        assert parent_row is not None
        parent = MessageJob.from_row(parent_row)
        queue.record_verification(
            job_id=parent.id,
            status="repairing",
            expected_count=1,
            present_count=0,
            media_match=False,
            caption_match=True,
            size_match=True,
            missing_source_message_ids=[7],
        )
        repair_job_id = queue.enqueue_repair_item(parent, 7)
        assert repair_job_id is not None
        repair_row = db.query_one("SELECT * FROM messages WHERE id = ?", (repair_job_id,))
        assert repair_row is not None
        repair = MessageJob.from_row(repair_row)

        assert queue.mark_failure(repair, "destination denied", config.queue.max_attempts) == "failed"
        assert queue.release3.verification_status(parent.id) == "failed"
        state = queue.source_work_state(parent.source_chat_id)
        assert state["verification_repairing_jobs"] == 0
        assert state["verification_failed_jobs"] == 1
        link = db.query_one("SELECT status FROM repair_links WHERE repair_job_id = ?", (repair_job_id,))
        assert link is not None
        assert link["status"] == "failed"

        assert queue.enqueue_repair_item(parent, 7) == repair_job_id
        requeued = db.query_one("SELECT status, attempts FROM messages WHERE id = ?", (repair_job_id,))
        assert requeued is not None
        assert requeued["status"] == "pending"
        assert requeued["attempts"] == 0
        link = db.query_one("SELECT status FROM repair_links WHERE repair_job_id = ?", (repair_job_id,))
        assert link is not None
        assert link["status"] == "pending"
    finally:
        db.close()
