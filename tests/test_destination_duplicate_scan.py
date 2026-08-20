import asyncio
from pathlib import Path
from types import SimpleNamespace

import main as migration_main

from app.destination_duplicate_scan import (
    complete_destination_duplicate_cleanup,
    delete_destination_duplicate_history,
    load_destination_duplicate_plan,
    request_destination_duplicate_cleanup,
    request_destination_duplicate_scan,
    scan_destination_content_duplicates,
    scan_destination_duplicate_history,
)


class FakeLimiter:
    def __init__(self) -> None:
        self.operations: list[str] = []

    async def call(self, operation: str, fn, *args, **kwargs):  # type: ignore[no-untyped-def]
        self.operations.append(operation)
        return await fn(*args, **kwargs)


class FakeClient:
    def __init__(self, messages: dict[int, list[object]]) -> None:
        self.messages = messages
        self.get_chat_calls: list[int] = []
        self.delete_calls: list[tuple[int, tuple[int, ...], bool]] = []

    async def get_chat(self, chat_id: int) -> object:
        self.get_chat_calls.append(int(chat_id))
        return SimpleNamespace(id=chat_id, title=f"Destination {chat_id}")

    async def get_chat_history(self, chat_id: int):  # type: ignore[no-untyped-def]
        for message in self.messages[int(chat_id)]:
            yield message

    async def get_messages(self, chat_id: int, message_ids: list[int]) -> list[object]:
        wanted = {int(message_id) for message_id in message_ids}
        return [
            message
            for message in self.messages[int(chat_id)]
            if int(getattr(message, "id", 0) or 0) in wanted
        ]

    async def delete_messages(
        self,
        chat_id: int,
        message_ids: list[int],
        revoke: bool,
    ) -> None:
        self.delete_calls.append((int(chat_id), tuple(message_ids), revoke))


def media(
    message_id: int,
    file_unique_id: str,
    *,
    media_type: str = "video",
    content: bytes | None = None,
) -> object:
    content = content if content is not None else f"content-{file_unique_id}".encode()

    async def download(*, file_name: str) -> str:
        Path(file_name).write_bytes(content)
        return file_name

    values = {
        "id": message_id,
        "video": None,
        "photo": None,
        "document": None,
        "animation": None,
        "audio": None,
        "voice": None,
        "video_note": None,
        "text": None,
        "caption": None,
        "reply_to_message_id": None,
        "reply_to_top_message_id": None,
    }
    field = "video" if media_type == "video" else "photo"
    values[field] = SimpleNamespace(
        file_unique_id=file_unique_id,
        file_id=f"fallback-{file_unique_id}",
        file_size=len(content),
    )
    values["download"] = download
    return SimpleNamespace(**values)


def text(message_id: int) -> object:
    return SimpleNamespace(
        id=message_id,
        video=None,
        photo=None,
        document=None,
        animation=None,
        audio=None,
        voice=None,
        video_note=None,
        text="hello",
        caption=None,
        reply_to_message_id=None,
        reply_to_top_message_id=None,
    )


def config_for(tmp_path: Path) -> object:
    return SimpleNamespace(
        queue=SimpleNamespace(db_path=tmp_path / "data" / "migration.sqlite3"),
        destinations=[SimpleNamespace(chat="-2001", topic_id=None)],
    )


def test_destination_scan_reads_history_and_keeps_the_oldest_exact_copy(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    requested = request_destination_duplicate_scan(config)  # type: ignore[arg-type]
    client = FakeClient(
        {
            -2001: [
                media(30, "same-video"),
                media(20, "same-video"),
                media(10, "same-video"),
                media(9, "different-video"),
                text(8),
            ]
        }
    )

    plan = asyncio.run(
        scan_destination_duplicate_history(
            config,  # type: ignore[arg-type]
            client,  # type: ignore[arg-type]
            FakeLimiter(),  # type: ignore[arg-type]
            asyncio.Event(),
        )
    )

    assert plan.state == "ready"
    assert plan.scan_id == requested.scan_id
    assert plan.scanned_message_count == 5
    assert plan.media_message_count == 4
    assert plan.group_count == 1
    assert plan.message_count == 2
    group = plan.groups[0]
    assert group.kept_message_id == 10
    assert group.duplicate_message_ids == (20, 30)


def test_completed_cleanup_invalidates_the_preview(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    request_destination_duplicate_scan(config)  # type: ignore[arg-type]
    client = FakeClient({-2001: [media(2, "same"), media(1, "same")]})
    plan = asyncio.run(
        scan_destination_duplicate_history(
            config,  # type: ignore[arg-type]
            client,  # type: ignore[arg-type]
            FakeLimiter(),  # type: ignore[arg-type]
            asyncio.Event(),
        )
    )

    completed = complete_destination_duplicate_cleanup(
        config,  # type: ignore[arg-type]
        plan,
        deleted_message_count=1,
    )
    reloaded = load_destination_duplicate_plan(config)  # type: ignore[arg-type]

    assert completed.state == "completed"
    assert completed.groups == ()
    assert reloaded is not None
    assert reloaded.state == "completed"
    assert reloaded.message_count == 0


def test_approved_cleanup_uses_the_scanning_manager_session_and_batches_deletes(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    request_destination_duplicate_scan(config)  # type: ignore[arg-type]
    client = FakeClient(
        {
            -2001: [
                media(message_id, "same")
                for message_id in range(102, 0, -1)
            ]
        }
    )
    limiter = FakeLimiter()
    scanned = asyncio.run(
        scan_destination_duplicate_history(
            config,  # type: ignore[arg-type]
            client,  # type: ignore[arg-type]
            limiter,  # type: ignore[arg-type]
            asyncio.Event(),
        )
    )

    queued = request_destination_duplicate_cleanup(config)  # type: ignore[arg-type]
    completed = asyncio.run(
        delete_destination_duplicate_history(
            config,  # type: ignore[arg-type]
            client,  # type: ignore[arg-type]
            limiter,  # type: ignore[arg-type]
            asyncio.Event(),
        )
    )

    assert scanned.state == "ready"
    assert scanned.message_count == 101
    assert queued is not None
    assert queued.state == "delete_pending"
    assert completed.state == "completed"
    assert completed.deleted_message_count == 101
    assert client.delete_calls == [
        (-2001, tuple(range(2, 102)), True),
        (-2001, (102,), True),
    ]
    assert limiter.operations.count("delete") == 2
    # The same client that read the destination history resolved and deleted it.
    assert client.get_chat_calls == [-2001, -2001]


def test_cleanup_stop_invalidates_the_approved_preview(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    request_destination_duplicate_scan(config)  # type: ignore[arg-type]
    client = FakeClient({-2001: [media(2, "same"), media(1, "same")]})
    scanned = asyncio.run(
        scan_destination_duplicate_history(
            config,  # type: ignore[arg-type]
            client,  # type: ignore[arg-type]
            FakeLimiter(),  # type: ignore[arg-type]
            asyncio.Event(),
        )
    )
    assert scanned.state == "ready"
    assert request_destination_duplicate_cleanup(config) is not None  # type: ignore[arg-type]

    stop_event = asyncio.Event()
    stop_event.set()
    cancelled = asyncio.run(
        delete_destination_duplicate_history(
            config,  # type: ignore[arg-type]
            client,  # type: ignore[arg-type]
            FakeLimiter(),  # type: ignore[arg-type]
            stop_event,
        )
    )

    assert cancelled.state == "delete_cancelled"
    assert cancelled.message_count == 0
    assert client.delete_calls == []


def test_content_scan_finds_byte_identical_files_with_different_telegram_fingerprints(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    request_destination_duplicate_scan(config, scan_mode="content")  # type: ignore[arg-type]
    client = FakeClient(
        {
            -2001: [
                media(30, "telegram-fingerprint-new", content=b"same bytes"),
                media(10, "telegram-fingerprint-old", content=b"same bytes"),
                media(9, "different", content=b"other bytes"),
            ]
        }
    )

    plan = asyncio.run(
        scan_destination_content_duplicates(
            config,  # type: ignore[arg-type]
            client,  # type: ignore[arg-type]
            FakeLimiter(),  # type: ignore[arg-type]
            asyncio.Event(),
        )
    )

    assert plan.state == "ready"
    assert plan.scan_mode == "content"
    assert plan.content_candidate_count == 2
    assert plan.content_hashed_count == 2
    assert plan.group_count == 1
    assert plan.groups[0].kept_message_id == 10
    assert plan.groups[0].duplicate_message_ids == (30,)
    assert plan.groups[0].match_kind == "content_sha256"


def test_content_scan_never_marks_different_bytes_as_duplicates(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    request_destination_duplicate_scan(config, scan_mode="content")  # type: ignore[arg-type]
    client = FakeClient(
        {
            -2001: [
                media(2, "first", content=b"one"),
                media(1, "second", content=b"two"),
            ]
        }
    )

    plan = asyncio.run(
        scan_destination_content_duplicates(
            config,  # type: ignore[arg-type]
            client,  # type: ignore[arg-type]
            FakeLimiter(),  # type: ignore[arg-type]
            asyncio.Event(),
        )
    )

    assert plan.state == "ready"
    assert plan.group_count == 0
    assert plan.message_count == 0


def test_manager_cycle_runs_approved_cleanup_with_the_reader_session(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    request_destination_duplicate_scan(config)  # type: ignore[arg-type]
    client = FakeClient({-2001: [media(2, "same"), media(1, "same")]})
    limiter = FakeLimiter()
    asyncio.run(
        scan_destination_duplicate_history(
            config,  # type: ignore[arg-type]
            client,  # type: ignore[arg-type]
            limiter,  # type: ignore[arg-type]
            asyncio.Event(),
        )
    )
    assert request_destination_duplicate_cleanup(config) is not None  # type: ignore[arg-type]

    outcome = asyncio.run(
        migration_main._execute_cycle(
            config,  # type: ignore[arg-type]
            "duplicate_cleanup_delete",
            reader=client,  # type: ignore[arg-type]
            bot=None,
            limiter=limiter,  # type: ignore[arg-type]
            queue=SimpleNamespace(config=None),
            stop_event=asyncio.Event(),
            reader_me=SimpleNamespace(id=1),
            writer_me=SimpleNamespace(id=1),
            logger=None,
        )
    )

    assert outcome.state == "complete"
    assert client.delete_calls == [(-2001, (2,), True)]


def test_manager_cycle_runs_deep_content_scan_with_the_reader_session(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    request_destination_duplicate_scan(config, scan_mode="content")  # type: ignore[arg-type]
    client = FakeClient(
        {
            -2001: [
                media(2, "new-file-id", content=b"same"),
                media(1, "old-file-id", content=b"same"),
            ]
        }
    )

    outcome = asyncio.run(
        migration_main._execute_cycle(
            config,  # type: ignore[arg-type]
            "duplicate_cleanup_content_scan",
            reader=client,  # type: ignore[arg-type]
            bot=None,
            limiter=FakeLimiter(),  # type: ignore[arg-type]
            queue=SimpleNamespace(config=None),
            stop_event=asyncio.Event(),
            reader_me=SimpleNamespace(id=1),
            writer_me=SimpleNamespace(id=1),
            logger=None,
        )
    )

    plan = load_destination_duplicate_plan(config)  # type: ignore[arg-type]
    assert outcome.state == "complete"
    assert plan is not None
    assert plan.scan_mode == "content"
    assert plan.groups[0].duplicate_message_ids == (2,)
