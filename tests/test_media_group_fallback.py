from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from app.db import Database
from app.upload import Uploader


class FakeMediaEmpty(Exception):
    pass


class DummyLimiter:
    async def call(self, operation: str, fn: object, *args: object, **kwargs: object) -> object:
        return await fn(*args, **kwargs)  # type: ignore[operator]


class DummyWriter:
    def __init__(self) -> None:
        self.group_calls = 0
        self.video_calls: list[dict[str, object]] = []

    async def send_media_group(self, **kwargs: object) -> object:
        self.group_calls += 1
        raise FakeMediaEmpty("MEDIA_EMPTY from messages.SendMultiMedia")

    async def send_video(self, **kwargs: object) -> object:
        self.video_calls.append(kwargs)
        return SimpleNamespace(id=500 + len(self.video_calls))


def fake_video(message_id: int, caption: str | None = None) -> object:
    return SimpleNamespace(
        id=message_id,
        video=SimpleNamespace(file_id=f"video-{message_id}"),
        photo=None,
        document=None,
        animation=None,
        audio=None,
        voice=None,
        video_note=None,
        text=None,
        caption=caption,
    )


def test_media_empty_album_falls_back_to_individual_uploads(tmp_path: Path) -> None:
    writer = DummyWriter()
    uploader = Uploader(
        SimpleNamespace(transfer=SimpleNamespace(drop_caption=False)),  # type: ignore[arg-type]
        SimpleNamespace(),  # type: ignore[arg-type]
        writer,  # type: ignore[arg-type]
        DummyLimiter(),  # type: ignore[arg-type]
        SimpleNamespace(),  # type: ignore[arg-type]
    )
    job = SimpleNamespace(id=7, dest_chat_id=-1003941419294, dest_topic_id=None)
    downloaded = [
        (fake_video(10, "Album caption"), tmp_path / "10.mp4"),
        (fake_video(11), tmp_path / "11.mp4"),
    ]

    with patch("app.upload.MediaEmpty", FakeMediaEmpty):
        result, sent = asyncio.run(
            uploader._upload_downloaded(  # noqa: SLF001
                job,  # type: ignore[arg-type]
                downloaded,  # type: ignore[arg-type]
            )
        )

    assert writer.group_calls == 1
    assert len(writer.video_calls) == 2
    assert writer.video_calls[0]["caption"] == "Album caption"
    assert writer.video_calls[1]["caption"] is None
    assert len(sent) == 2
    assert result.status == "copied"
    assert result.dest_message_ids == [501, 502]


def test_legacy_send_multi_media_requeue_is_explicit(tmp_path: Path) -> None:
    db_path = tmp_path / "migration.sqlite3"
    db = Database(db_path)
    db.initialize()
    try:
        assert db.enqueue_message(
            source_chat_id="-1001111111111",
            source_message_id=9,
            dest_chat_id="-1003941419294",
            file_unique_key="album-error",
            source_message_ids=[9, 10],
            source_topic_id=None,
            dest_topic_id=None,
            media_group_id="album-1",
            media_type="video",
            file_size=123,
            caption=None,
            status="skipped",
            last_error=(
                'MediaEmpty: Telegram says: [400 MEDIA_EMPTY] - invalid '
                '(caused by "messages.SendMultiMedia")'
            ),
        )
        assert db.enqueue_message(
            source_chat_id="-1001111111111",
            source_message_id=114,
            dest_chat_id="-1003941419294",
            file_unique_key="unsupported",
            source_message_ids=[114],
            source_topic_id=None,
            dest_topic_id=None,
            media_group_id=None,
            media_type="unsupported",
            file_size=None,
            caption=None,
            status="skipped",
            last_error="Filtered out by config",
        )
    finally:
        db.close()

    reopened = Database(db_path)
    reopened.initialize()
    try:
        album = reopened.query_one(
            "SELECT status, attempts, last_error FROM messages WHERE file_unique_key = ?",
            ("album-error",),
        )
        assert album is not None
        assert album["status"] == "skipped"
        assert "SendMultiMedia" in album["last_error"]

        assert reopened.requeue_send_multi_media_errors() == 1
        album = reopened.query_one(
            "SELECT status, attempts, last_error FROM messages WHERE file_unique_key = ?",
            ("album-error",),
        )
        unsupported = reopened.query_one(
            "SELECT status, last_error FROM messages WHERE file_unique_key = ?",
            ("unsupported",),
        )

        assert album is not None
        assert album["status"] == "pending"
        assert album["attempts"] == 0
        assert album["last_error"] is None

        assert unsupported is not None
        assert unsupported["status"] == "skipped"
        assert unsupported["last_error"] == "Filtered out by config"
    finally:
        reopened.close()
