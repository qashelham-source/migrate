from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import yaml

from app.db import Database
from app.destination_manager import add_destination, list_destinations, remove_destination
from app.queue import MessageQueue


def make_config(tmp_path: Path):
    return SimpleNamespace(
        queue=SimpleNamespace(
            max_attempts=4,
            retry_backoff_seconds=[1, 2, 3],
        )
    )


def test_media_cache_round_trip(tmp_path: Path) -> None:
    db = Database(tmp_path / "migration.sqlite3")
    db.initialize()
    queue = MessageQueue(db, make_config(tmp_path))

    assert queue.get_media_cache("video-key") is None
    queue.save_media_cache(
        "video-key",
        ["BOT_FILE_ID_1", "BOT_FILE_ID_2"],
        ["photo", "video"],
    )

    cached = queue.get_media_cache("video-key")
    assert cached is not None
    assert cached.bot_file_ids == ["BOT_FILE_ID_1", "BOT_FILE_ID_2"]
    assert cached.media_types == ["photo", "video"]
    assert queue.media_cache_count() == 1

    queue.delete_media_cache("video-key")
    assert queue.get_media_cache("video-key") is None
    db.close()


def test_destination_commands(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump({"migration": {"sources": [], "destinations": []}}, sort_keys=False),
        encoding="utf-8",
    )

    assert add_destination("channel_one", config_path=config_path) == {"chat": "@channel_one"}
    assert add_destination("-100123", 77, config_path) == {
        "chat": "-100123",
        "topic_id": 77,
    }
    assert list_destinations(config_path) == [
        {"chat": "@channel_one"},
        {"chat": "-100123", "topic_id": 77},
    ]

    removed = remove_destination(1, config_path)
    assert removed == {"chat": "@channel_one"}
    assert list_destinations(config_path) == [{"chat": "-100123", "topic_id": 77}]
