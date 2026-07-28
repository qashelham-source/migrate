from __future__ import annotations

import sqlite3

import pytest

from app.release81 import MigrationPlan, Release81Store


def make_store() -> Release81Store:
    connection = sqlite3.connect(":memory:")
    store = Release81Store(connection)
    store.initialize()
    return store


def test_plan_and_album_are_persistent() -> None:
    store = make_store()
    plan_id = store.create_plan(
        source_chat_id=-1001,
        destinations=[-1002, -1003],
        plan=MigrationPlan(
            total_messages=100,
            photos=20,
            videos=10,
            albums=3,
            documents=5,
            estimated_download_bytes=5000,
            temporary_storage_bytes=6000,
            estimated_seconds=120,
        ),
    )
    assert plan_id > 0

    album_id = store.upsert_album(
        source_chat_id=-1001,
        dest_chat_id=-1002,
        media_group_id="album-1",
        source_message_ids=[3, 1, 2],
    )
    assert not store.album_ready_to_publish(album_id)

    store.set_album_state(album_id, "downloading", downloaded_message_ids=[1, 2])
    assert not store.album_ready_to_publish(album_id)

    store.set_album_state(album_id, "ready_to_publish", downloaded_message_ids=[1, 2, 3])
    assert store.album_ready_to_publish(album_id)
    assert store.waiting_counts()["ready_to_publish"] == 1


def test_pipeline_rejects_unknown_stage() -> None:
    store = make_store()
    with pytest.raises(ValueError):
        store.record_pipeline_event(job_key="1", stage="unknown", state="started")


def test_album_rejects_unknown_state() -> None:
    store = make_store()
    album_id = store.upsert_album(
        source_chat_id=-1001,
        dest_chat_id=-1002,
        media_group_id="album-1",
        source_message_ids=[1],
    )
    with pytest.raises(ValueError):
        store.set_album_state(album_id, "invalid")
