import asyncio
from pathlib import Path
from types import SimpleNamespace

from app.destination_duplicate_scan import (
    complete_destination_duplicate_cleanup,
    load_destination_duplicate_plan,
    request_destination_duplicate_scan,
    scan_destination_duplicate_history,
)


class FakeLimiter:
    async def call(self, _operation: str, fn, *args):  # type: ignore[no-untyped-def]
        return await fn(*args)


class FakeClient:
    def __init__(self, messages: dict[int, list[object]]) -> None:
        self.messages = messages

    async def get_chat(self, chat_id: int) -> object:
        return SimpleNamespace(id=chat_id, title=f"Destination {chat_id}")

    async def get_chat_history(self, chat_id: int):  # type: ignore[no-untyped-def]
        for message in self.messages[int(chat_id)]:
            yield message


def media(message_id: int, file_unique_id: str, *, media_type: str = "video") -> object:
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
    values[field] = SimpleNamespace(file_unique_id=file_unique_id, file_id=f"fallback-{file_unique_id}")
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
