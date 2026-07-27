from __future__ import annotations

import asyncio
from types import SimpleNamespace

from app.control import (
    clear_stop,
    is_active_phase,
    is_stop_requested,
    read_status,
    request_stop,
    watch_stop_request,
    write_status,
)


def make_config(tmp_path):
    return SimpleNamespace(
        queue=SimpleNamespace(db_path=tmp_path / "data" / "migration.sqlite3")
    )


def test_status_round_trip(tmp_path):
    config = make_config(tmp_path)

    write_status(config, "uploading", job_id=7, current=2, total=10)
    status = read_status(config)

    assert status["phase"] == "uploading"
    assert status["job_id"] == 7
    assert status["current"] == 2
    assert status["total"] == 10
    assert status["updated_at"]


def test_stop_marker_and_phase_helpers(tmp_path):
    config = make_config(tmp_path)

    clear_stop(config)
    assert not is_stop_requested(config)

    request_stop(config)
    assert is_stop_requested(config)
    assert is_active_phase("scanning")
    assert is_active_phase("uploading")
    assert not is_active_phase("idle")

    clear_stop(config)
    assert not is_stop_requested(config)


def test_stop_watcher_sets_event(tmp_path):
    config = make_config(tmp_path)

    async def scenario() -> None:
        stop_event = asyncio.Event()
        watcher = asyncio.create_task(
            watch_stop_request(config, stop_event, poll_seconds=0.01)
        )
        request_stop(config)
        await asyncio.wait_for(watcher, timeout=1)
        assert stop_event.is_set()
        assert read_status(config)["phase"] == "stopping"

    asyncio.run(scenario())
