from __future__ import annotations

import asyncio
from types import SimpleNamespace

from app.control import (
    clear_pause,
    clear_stop,
    is_active_phase,
    is_pause_requested,
    is_stop_requested,
    is_stoppable_phase,
    read_status,
    request_pause,
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


def test_stop_and_pause_markers_and_phase_helpers(tmp_path):
    config = make_config(tmp_path)

    clear_stop(config)
    clear_pause(config)
    assert not is_stop_requested(config)
    assert not is_pause_requested(config)

    runtime = tmp_path / "data"
    runtime.mkdir(parents=True, exist_ok=True)
    (runtime / "run_now").touch()
    (runtime / "run_mode").write_text("run", encoding="utf-8")

    request_stop(config)
    assert is_stop_requested(config)
    assert not (runtime / "run_now").exists()
    assert not (runtime / "run_mode").exists()

    request_pause(config)
    assert is_pause_requested(config)
    assert is_active_phase("scanning")
    assert is_active_phase("uploading")
    assert is_stoppable_phase("watching")
    assert not is_active_phase("idle")

    clear_stop(config)
    assert not is_stop_requested(config)
    assert is_pause_requested(config)

    clear_pause(config)
    assert not is_pause_requested(config)


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
