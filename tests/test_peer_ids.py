from __future__ import annotations

import asyncio
from types import SimpleNamespace

from app.config import ChatSpec
from app.queue import MessageJob
from app.telegram_client import resolve_chat, telegram_peer


def test_telegram_peer_converts_numeric_strings_to_int() -> None:
    assert telegram_peer("-1001234567890") == -1001234567890
    assert telegram_peer("12345") == 12345
    assert telegram_peer(-1001234567890) == -1001234567890


def test_telegram_peer_keeps_usernames_and_links_as_strings() -> None:
    assert telegram_peer("@destination") == "@destination"
    assert telegram_peer("https://t.me/destination") == "https://t.me/destination"


def test_message_job_restores_numeric_peer_ids_from_sqlite_text() -> None:
    row = {
        "id": 1,
        "source_chat_id": "-1001111111111",
        "source_message_id": 10,
        "dest_chat_id": "-1002222222222",
        "status": "pending",
        "attempts": 0,
        "last_error": None,
        "next_retry_at": None,
        "file_unique_key": "file-key",
        "source_topic_id": None,
        "dest_topic_id": None,
        "media_group_id": None,
        "source_message_ids": "[10]",
        "dest_message_ids": "[]",
        "media_type": "video",
        "file_size": 123,
        "caption": None,
    }

    job = MessageJob.from_row(row)  # type: ignore[arg-type]

    assert job.source_chat_id == -1001111111111
    assert isinstance(job.source_chat_id, int)
    assert job.dest_chat_id == -1002222222222
    assert isinstance(job.dest_chat_id, int)


def test_resolve_chat_passes_numeric_id_as_int() -> None:
    seen: list[object] = []

    class DummyClient:
        async def get_chat(self, chat_id: int | str) -> object:
            seen.append(chat_id)
            return SimpleNamespace(id=-1002222222222, title="Destination", username=None)

    class DummyLimiter:
        async def call(self, operation: str, fn: object, *args: object, **kwargs: object) -> object:
            assert operation == "resolve"
            return await fn(*args, **kwargs)  # type: ignore[operator]

    resolved = asyncio.run(
        resolve_chat(
            DummyClient(),  # type: ignore[arg-type]
            DummyLimiter(),  # type: ignore[arg-type]
            ChatSpec(chat="-1002222222222"),
        )
    )

    assert seen == [-1002222222222]
    assert resolved.chat_id == -1002222222222
    assert isinstance(resolved.chat_id, int)
