from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

from app.config import ChatSpec
from app.db import Database
from main import choose_writer_for_destinations, warm_dialog_cache


class DummyLimiter:
    async def call(self, operation: str, fn: object, *args: object, **kwargs: object) -> object:
        return await fn(*args, **kwargs)  # type: ignore[operator]


class DummyClient:
    def __init__(self, *, resolves: bool, name: str) -> None:
        self.resolves = resolves
        self.name = name

    async def get_chat(self, chat_id: int | str) -> object:
        if not self.resolves:
            raise RuntimeError(f"{self.name} cannot resolve {chat_id}")
        return SimpleNamespace(id=-1003941419294, title="Destination", username=None)


class DummyDialogClient:
    def __init__(self, chat_ids: list[int]) -> None:
        self.chat_ids = chat_ids
        self.calls = 0

    async def get_dialogs(self):  # type: ignore[no-untyped-def]
        self.calls += 1
        for chat_id in self.chat_ids:
            yield SimpleNamespace(chat=SimpleNamespace(id=chat_id))


def test_writer_falls_back_to_reader_for_private_destination() -> None:
    reader = DummyClient(resolves=True, name="reader")
    writer = DummyClient(resolves=False, name="writer")
    config = SimpleNamespace(destinations=[ChatSpec(chat="-1003941419294")])

    selected, ready = asyncio.run(
        choose_writer_for_destinations(
            config,  # type: ignore[arg-type]
            reader,  # type: ignore[arg-type]
            writer,  # type: ignore[arg-type]
            DummyLimiter(),  # type: ignore[arg-type]
        )
    )

    assert ready is True
    assert selected is reader


def test_dialog_cache_warmup_loads_until_configured_peers_are_found(tmp_path: Path) -> None:
    reader = DummyDialogClient(
        [-1001111111111, -1002222222222, -1003941419294, -1009999999999]
    )
    config = SimpleNamespace(
        telegram=SimpleNamespace(load_dialogs_on_start=True),
        sources=[ChatSpec(chat="-1001111111111")],
        destinations=[ChatSpec(chat="-1003941419294")],
        queue=SimpleNamespace(db_path=tmp_path / "migration.sqlite3"),
    )

    loaded, found = asyncio.run(
        warm_dialog_cache(
            config,  # type: ignore[arg-type]
            reader,  # type: ignore[arg-type]
            asyncio.Event(),
        )
    )

    assert reader.calls == 1
    assert loaded == 3
    assert found == {-1001111111111, -1003941419294}


def test_dialog_cache_warmup_can_be_disabled(tmp_path: Path) -> None:
    reader = DummyDialogClient([-1003941419294])
    config = SimpleNamespace(
        telegram=SimpleNamespace(load_dialogs_on_start=False),
        sources=[],
        destinations=[ChatSpec(chat="-1003941419294")],
        queue=SimpleNamespace(db_path=tmp_path / "migration.sqlite3"),
    )

    loaded, found = asyncio.run(
        warm_dialog_cache(
            config,  # type: ignore[arg-type]
            reader,  # type: ignore[arg-type]
            asyncio.Event(),
        )
    )

    assert reader.calls == 0
    assert loaded == 0
    assert found == set()


def test_peer_id_failures_are_returned_to_pending(tmp_path: Path) -> None:
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    try:
        inserted = db.enqueue_message(
            source_chat_id="-1001111111111",
            source_message_id=10,
            dest_chat_id="-1003941419294",
            file_unique_key="file-key",
            source_message_ids=[10],
            source_topic_id=None,
            dest_topic_id=None,
            media_group_id=None,
            media_type="video",
            file_size=123,
            caption=None,
            status="skipped",
            last_error="PeerIdInvalid: Peer id invalid: -1003941419294",
        )
        assert inserted is True

        revived = db.requeue_peer_id_errors()
        row = db.query_one("SELECT status, attempts, last_error, next_retry_at FROM messages")

        assert revived == 1
        assert row is not None
        assert row["status"] == "pending"
        assert row["attempts"] == 0
        assert row["last_error"] is None
        assert row["next_retry_at"] is None
    finally:
        db.close()
