from __future__ import annotations

import asyncio
import errno
import importlib
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

import app.destination_manager as destination_manager
import app.admin_bot as admin_bot
from app.config import ChatSpec, load_config
from app.dashboard_v2 import issue_center, source_migration_progress
from app.db import Database
from app.queue import MessageJob, MessageQueue
from app.scanner import Scanner
from app.telegram_client import save_accounts
from app.upload import UploadResult, Uploader
from main import _resume_healthy_destinations, choose_writer_for_destinations


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

    reloaded_admin_bot = importlib.reload(admin_bot)
    assert reloaded_admin_bot._authorized_ids(config) == {111}


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


class _PostableClient:
    def __init__(self, *, can_post: bool) -> None:
        self.can_post = can_post

    async def get_me(self) -> SimpleNamespace:
        return SimpleNamespace(id=777)

    async def get_chat(self, chat: str) -> SimpleNamespace:
        return SimpleNamespace(id=len(str(chat)), title=str(chat), username=None)

    async def get_chat_member(self, _chat_id: int, _member_id: int) -> SimpleNamespace:
        return SimpleNamespace(
            status="administrator",
            privileges=SimpleNamespace(can_post_messages=self.can_post),
        )


def test_permission_paused_destination_only_resumes_after_post_permission_check(tmp_path: Path) -> None:
    config = load_config(
        _write_config(
            tmp_path / "config.yaml",
            migration={"sources": [], "destinations": ["destination"]},
        )
    )
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    try:
        queue = MessageQueue(db, config)
        assert queue.enqueue(
            source_chat_id="-1001",
            source_message_id=1,
            dest_chat_id=str(len("destination")),
            file_unique_key="permission-paused",
            source_message_ids=[1],
            source_topic_id=None,
            dest_topic_id=None,
            media_group_id=None,
            media_type="document",
            file_size=1,
            caption=None,
        )
        job = queue.claim_due(1)[0]
        queue.pause_destination(job, "Destination access/permission failed", "ChatWriteForbidden")
        assert queue.release3.is_destination_paused(job.dest_chat_id)

        asyncio.run(
            _resume_healthy_destinations(config, queue, _PostableClient(can_post=False), _DirectLimiter())
        )
        assert queue.release3.is_destination_paused(job.dest_chat_id)

        asyncio.run(
            _resume_healthy_destinations(config, queue, _PostableClient(can_post=True), _DirectLimiter())
        )
        assert not queue.release3.is_destination_paused(job.dest_chat_id)
    finally:
        db.close()


def test_schema_initialization_removes_legacy_destination_pause_trigger(tmp_path: Path) -> None:
    config = load_config(_write_config(tmp_path / "config.yaml"))
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    try:
        MessageQueue(db, config)
        db.execute(
            """
            CREATE TRIGGER trg_keep_permission_destination_paused
            BEFORE UPDATE OF paused ON destination_health
            BEGIN
                SELECT RAISE(IGNORE);
            END;
            """
        )

        MessageQueue(db, config)

        assert db.query_one(
            "SELECT 1 FROM sqlite_master WHERE type = 'trigger' AND name = ?",
            ("trg_keep_permission_destination_paused",),
        ) is None
    finally:
        db.close()


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

def test_fast_duplicate_detector_skips_only_same_destination(tmp_path: Path) -> None:
    config = load_config(_write_config(tmp_path / "config.yaml"))
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    try:
        queue = MessageQueue(db, config)
        first = {
            "source_chat_id": "-1001",
            "source_message_id": 7,
            "dest_chat_id": "-1002",
            "file_unique_key": "telegram-media-unique-id",
            "source_message_ids": [7],
            "source_topic_id": None,
            "dest_topic_id": None,
            "media_group_id": None,
            "media_type": "video",
            "file_size": 10,
            "caption": None,
        }
        assert queue.enqueue(**first, status="copied")

        inserted, duplicate = queue.enqueue_with_duplicate_detection(
            **{**first, "source_chat_id": "-1009", "source_message_id": 8, "source_message_ids": [8]}
        )

        assert inserted
        assert duplicate is not None
        assert duplicate.source_message_id == 7
        duplicate_row = db.query_one(
            "SELECT status, last_error FROM messages WHERE source_chat_id = ? AND source_message_id = ?",
            ("-1009", 8),
        )
        assert duplicate_row is not None
        assert duplicate_row["status"] == "skipped"
        assert "job #" in str(duplicate_row["last_error"])

        inserted, duplicate = queue.enqueue_with_duplicate_detection(
            **{
                **first,
                "source_chat_id": "-1010",
                "source_message_id": 9,
                "source_message_ids": [9],
                "dest_chat_id": "-1003",
            }
        )
        assert inserted
        assert duplicate is None

        inserted, duplicate = queue.enqueue_with_duplicate_detection(
            **{
                **first,
                "source_chat_id": "-1011",
                "source_message_id": 10,
                "source_message_ids": [10],
                "dest_topic_id": 42,
            }
        )
        assert inserted
        assert duplicate is None
    finally:
        db.close()


def test_fast_duplicate_detector_ignores_text_fallback_and_failed_media(tmp_path: Path) -> None:
    config = load_config(_write_config(tmp_path / "config.yaml"))
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    try:
        queue = MessageQueue(db, config)
        text_job = {
            "source_chat_id": "-1001",
            "source_message_id": 11,
            "dest_chat_id": "-1002",
            "file_unique_key": "messages:-1001:11",
            "source_message_ids": [11],
            "source_topic_id": None,
            "dest_topic_id": None,
            "media_group_id": None,
            "media_type": "text",
            "file_size": None,
            "caption": "same text",
        }
        assert queue.enqueue(**text_job, status="copied")
        inserted, duplicate = queue.enqueue_with_duplicate_detection(
            **{**text_job, "source_chat_id": "-1009", "source_message_id": 12, "source_message_ids": [12]}
        )
        assert inserted
        assert duplicate is None

        failed_media = {
            **text_job,
            "source_chat_id": "-1010",
            "source_message_id": 13,
            "source_message_ids": [13],
            "file_unique_key": "failed-media-unique-id",
            "media_type": "photo",
        }
        assert queue.enqueue(**failed_media, status="failed", last_error="upload failed")
        inserted, duplicate = queue.enqueue_with_duplicate_detection(
            **{
                **failed_media,
                "source_chat_id": "-1011",
                "source_message_id": 14,
                "source_message_ids": [14],
            }
        )
        assert inserted
        assert duplicate is None
    finally:
        db.close()



def test_expected_skips_stay_out_of_dashboard_and_issue_center(tmp_path: Path) -> None:
    config = load_config(_write_config(tmp_path / "config.yaml"))
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    try:
        queue = MessageQueue(db, config)
        base = {
            "source_chat_id": "-1001",
            "dest_chat_id": "-1002",
            "source_topic_id": None,
            "dest_topic_id": None,
            "media_group_id": None,
            "media_type": "video",
            "file_size": 10,
            "caption": None,
        }

        assert queue.enqueue(
            **{
                **base,
                "source_message_id": 1,
                "source_message_ids": [1],
                "file_unique_key": "copied:1",
            },
            status="copied",
        )
        for message_id, reason in (
            (2, "PermanentJobError: File is 2579663297 bytes, above configured bot upload limit 2097152000"),
            (3, "Filtered out by config"),
            (4, "Skipped duplicate media fingerprint already queued or delivered for this destination"),
        ):
            assert queue.enqueue(
                **{
                    **base,
                    "source_message_id": message_id,
                    "source_message_ids": [message_id],
                    "file_unique_key": f"skipped:{message_id}",
                },
                status="skipped",
                last_error=reason,
            )
        assert queue.enqueue(
            **{
                **base,
                "source_message_id": 5,
                "source_message_ids": [5],
                "file_unique_key": "failed:5",
            },
            status="failed",
            last_error="destination denied",
        )

        progress = source_migration_progress(db)
        assert len(progress) == 1
        assert progress[0]["total_items"] == 5
        assert progress[0]["filtered_items"] == 3
        assert progress[0]["eligible_items"] == 2
        assert progress[0]["copied_items"] == 1
        assert progress[0]["blocked_items"] == 1
        assert progress[0]["remaining_items"] == 1
        assert progress[0]["percent"] == 50

        issues = issue_center(db)
        assert [(item["kind"], item["source_message_id"]) for item in issues] == [("job", 5)]
        state = queue.source_work_state("-1001")
        assert state["skipped_issue_jobs"] == 0
        assert state["primary_skipped_issue_jobs"] == 0
    finally:
        db.close()


def test_expected_skip_allows_source_state_to_finish(tmp_path: Path) -> None:
    config = load_config(_write_config(tmp_path / "config.yaml"))
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    try:
        queue = MessageQueue(db, config)
        queue.register_source(
            source_chat_id="-1001",
            title="Source",
            username=None,
            chat_type="channel",
            latest_seen_message_id=1,
        )
        assert queue.enqueue(
            source_chat_id="-1001",
            source_message_id=1,
            dest_chat_id="-1002",
            file_unique_key="too-large:1",
            source_message_ids=[1],
            source_topic_id=None,
            dest_topic_id=None,
            media_group_id=None,
            media_type="video",
            file_size=2579663297,
            caption=None,
        )
        row = db.query_one("SELECT id FROM messages WHERE file_unique_key = ?", ("too-large:1",))
        assert row is not None
        queue.mark_skipped(
            int(row["id"]),
            "PermanentJobError: File is 2579663297 bytes, above configured bot upload limit 2097152000",
        )

        state = queue.source_work_state("-1001")
        assert state["primary_skipped_issue_jobs"] == 0
        source = db.query_one("SELECT migration_state FROM source_registry WHERE source_chat_id = ?", ("-1001",))
        assert source is not None
        assert source["migration_state"] == "verified"
    finally:
        db.close()



def test_source_queue_uses_saved_channel_titles_after_bot_restart(tmp_path: Path) -> None:
    path = _write_config(
        tmp_path / "config.yaml",
        migration={
            "sources": ["-1002843617976", "-1001678732307"],
            "destinations": ["-100999"],
        },
    )
    config = load_config(path)
    user_id = 991

    admin_bot._CHANNEL_CACHE.pop(user_id, None)
    admin_bot._SELECTIONS.pop(user_id, None)
    admin_bot._SOURCE_TITLE_CACHE.pop(path.resolve(), None)
    admin_bot._persist_scanned_source_titles(
        config,
        path,
        [
            {
                "chat": "-1002843617976",
                "title": "Awek Bigo Mango",
                "username": None,
                "kind": "Channel",
            },
            {
                "chat": "-1001678732307",
                "title": "Channel Kedua",
                "username": "channel_kedua",
                "kind": "Channel",
            },
        ],
    )

    try:
        text = admin_bot._source_text(user_id, path)
        assert "Awek Bigo Mango" in text
        assert "Channel Kedua" in text
        assert "-1002843617976" not in text
        assert "Showing your saved queue." in text

        root_labels = [
            button.text
            for row in admin_bot._source_menu(user_id, path).inline_keyboard
            for button in row
        ]
        assert "🛠 Manage Queue" in root_labels
        assert not any("New Posts" in label or "Remove Source" in label for label in root_labels)

        manage_labels = [
            button.text
            for row in admin_bot._source_queue_menu(user_id, path).inline_keyboard
            for button in row
        ]
        assert "🧹 New Posts Only" in manage_labels
        assert "🗑 Remove Source" in manage_labels
    finally:
        admin_bot._CHANNEL_CACHE.pop(user_id, None)
        admin_bot._SELECTIONS.pop(user_id, None)
        admin_bot._SOURCE_TITLE_CACHE.pop(path.resolve(), None)


def _document_message(message_id: int = 7) -> SimpleNamespace:
    return SimpleNamespace(
        id=message_id,
        video=None,
        photo=None,
        document=SimpleNamespace(file_id="source-document"),
        animation=None,
        audio=None,
        voice=None,
        video_note=None,
        text=None,
        caption=None,
        media_group_id=None,
    )


def _document_job(file_unique_key: str = "cache-type-mismatch") -> MessageJob:
    return MessageJob(
        id=71,
        source_chat_id=-1001,
        source_message_id=7,
        dest_chat_id=-1002,
        status="pending",
        attempts=1,
        last_error=None,
        next_retry_at=None,
        file_unique_key=file_unique_key,
        source_topic_id=None,
        dest_topic_id=None,
        media_group_id=None,
        source_message_ids=[7],
        dest_message_ids=[],
        media_type="document",
        file_size=1,
        caption=None,
    )


def test_incompatible_cached_media_type_falls_back_before_sending(tmp_path: Path) -> None:
    config = load_config(_write_config(tmp_path / "config.yaml"))
    config = replace(config, transfer=replace(config.transfer, include_documents=True))

    class CacheQueue:
        def __init__(self) -> None:
            self.deleted: list[str] = []

        def get_media_cache(self, file_unique_key: str) -> object:
            return SimpleNamespace(
                file_unique_key=file_unique_key,
                bot_file_ids=["animation-file-id"],
                media_types=["animation"],
            )

        def delete_media_cache(self, file_unique_key: str) -> None:
            self.deleted.append(file_unique_key)

    queue = CacheQueue()
    uploader = Uploader.__new__(Uploader)
    uploader.config = config
    uploader.queue = queue
    uploader.logger = None
    calls: list[str] = []

    async def send_cached(*_args: object) -> UploadResult:
        raise AssertionError("incompatible cached file_id must not be sent")

    async def load_source_messages(*_args: object) -> list[SimpleNamespace]:
        return [_document_message()]

    async def copy_or_forward(*_args: object) -> UploadResult:
        calls.append("native-copy")
        return UploadResult(status="copied", dest_message_ids=[9001])

    async def phase(_name: str) -> None:
        return None

    uploader._send_cached = send_cached
    uploader._load_source_messages = load_source_messages
    uploader._copy_or_forward = copy_or_forward

    result = asyncio.run(uploader.process(_document_job(), asyncio.Event(), phase))

    assert result.dest_message_ids == [9001]
    assert queue.deleted == ["cache-type-mismatch"]
    assert calls == ["native-copy"]


def test_cached_file_id_type_error_is_discarded_then_retried_natively(tmp_path: Path) -> None:
    config = load_config(_write_config(tmp_path / "config.yaml"))
    config = replace(config, transfer=replace(config.transfer, include_documents=True))

    class CacheQueue:
        def __init__(self) -> None:
            self.deleted: list[str] = []

        def get_media_cache(self, file_unique_key: str) -> object:
            return SimpleNamespace(
                file_unique_key=file_unique_key,
                bot_file_ids=["stale-file-id"],
                media_types=["document"],
            )

        def delete_media_cache(self, file_unique_key: str) -> None:
            self.deleted.append(file_unique_key)

    queue = CacheQueue()
    uploader = Uploader.__new__(Uploader)
    uploader.config = config
    uploader.queue = queue
    uploader.logger = None
    calls: list[str] = []

    async def send_cached(*_args: object) -> UploadResult:
        raise ValueError("Expected DOCUMENT, got ANIMATION file id instead")

    async def load_source_messages(*_args: object) -> list[SimpleNamespace]:
        return [_document_message()]

    async def copy_or_forward(*_args: object) -> UploadResult:
        calls.append("native-copy")
        return UploadResult(status="copied", dest_message_ids=[9002])

    async def phase(_name: str) -> None:
        return None

    uploader._send_cached = send_cached
    uploader._load_source_messages = load_source_messages
    uploader._copy_or_forward = copy_or_forward

    result = asyncio.run(uploader.process(_document_job(), asyncio.Event(), phase))

    assert result.dest_message_ids == [9002]
    assert queue.deleted == ["cache-type-mismatch"]
    assert calls == ["native-copy"]


def test_recover_cached_file_id_mismatches_requeues_without_cache(tmp_path: Path) -> None:
    config = load_config(_write_config(tmp_path / "config.yaml"))
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    try:
        queue = MessageQueue(db, config)
        assert queue.enqueue(
            source_chat_id="-1001",
            source_message_id=88,
            dest_chat_id="-1002",
            file_unique_key="cache-type-mismatch",
            source_message_ids=[88],
            source_topic_id=None,
            dest_topic_id=None,
            media_group_id=None,
            media_type="document",
            file_size=1,
            caption=None,
            status="failed",
            last_error="ValueError: Expected DOCUMENT, got ANIMATION file id instead",
        )
        queue.save_media_cache("cache-type-mismatch", ["animation-file-id"], ["animation"])

        assert queue.recover_cached_file_id_mismatches() == 1
        row = db.query_one("SELECT status, attempts, last_error FROM messages WHERE source_message_id = 88")
        assert row is not None
        assert dict(row) == {"status": "pending", "attempts": 0, "last_error": None}
        assert queue.get_media_cache("cache-type-mismatch") is None
    finally:
        db.close()


def test_terminal_nonrepair_failure_is_cancelled_and_hidden_from_issue_center(tmp_path: Path) -> None:
    config = load_config(_write_config(tmp_path / "config.yaml"))
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    try:
        queue = MessageQueue(db, config)
        assert queue.enqueue(
            source_chat_id="-1001",
            source_message_id=99,
            dest_chat_id="-1002",
            file_unique_key="terminal:99",
            source_message_ids=[99],
            source_topic_id=None,
            dest_topic_id=None,
            media_group_id=None,
            media_type="document",
            file_size=1,
            caption=None,
        )
        row = db.query_one("SELECT * FROM messages WHERE source_message_id = 99")
        assert row is not None
        job = MessageJob.from_row(row)

        assert queue.mark_failure(job, "Telegram rejected the upload", config.queue.max_attempts) == "cancelled"
        saved = db.query_one("SELECT status, last_error FROM messages WHERE id = ?", (job.id,))
        assert saved is not None
        assert saved["status"] == "skipped"
        assert "Cancelled by policy" in str(saved["last_error"])
        assert issue_center(db) == []
    finally:
        db.close()


def test_startup_cleanup_cancels_old_terminal_jobs_but_keeps_uncertain_uploads(tmp_path: Path) -> None:
    config = load_config(_write_config(tmp_path / "config.yaml"))
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    try:
        queue = MessageQueue(db, config)
        base = {
            "source_chat_id": "-1001",
            "dest_chat_id": "-1002",
            "source_topic_id": None,
            "dest_topic_id": None,
            "media_group_id": None,
            "media_type": "document",
            "file_size": 1,
            "caption": None,
        }
        assert queue.enqueue(
            **base,
            source_message_id=100,
            file_unique_key="old-terminal:100",
            source_message_ids=[100],
            status="failed",
            last_error="Telegram rejected the upload",
        )
        assert queue.enqueue(
            **base,
            source_message_id=101,
            file_unique_key="uncertain-upload:101",
            source_message_ids=[101],
            status="failed",
            last_error="Interrupted during upload; destination result is unknown. Verify first.",
        )

        assert queue.cancel_terminal_issues() == 1
        cancelled = db.query_one("SELECT status, last_error FROM messages WHERE source_message_id = 100")
        uncertain = db.query_one("SELECT status FROM messages WHERE source_message_id = 101")
        assert cancelled is not None
        assert cancelled["status"] == "skipped"
        assert "Cancelled by policy" in str(cancelled["last_error"])
        assert uncertain is not None
        assert uncertain["status"] == "failed"
    finally:
        db.close()
