import asyncio
import json
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.destination_manager import clear_content_filter, load_content_filter, save_content_filter
from app.shared_state import get_floodwait, record_floodwait
from app.telegram_client import TelegramLimiter


def test_empty_content_filter_is_persisted_as_explicit_allow_all(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("{}\n", encoding="utf-8")

    save_content_filter(config_path, set())

    assert load_content_filter(config_path) == frozenset()
    assert (tmp_path / "data" / "content_filter.json").exists()

    clear_content_filter(config_path)
    assert load_content_filter(config_path) is None


def test_floodwait_snapshot_is_shared_and_countdown_refreshes(tmp_path: Path) -> None:
    state_path = tmp_path / "data" / "floodwait.json"
    deadline = time.time() + 30
    record_floodwait(
        {
            "floodwait_events": 1,
            "floodwait_total_seconds": 30,
            "floodwait_operations": {
                "upload": {
                    "events": 1,
                    "total_wait_seconds": 30,
                    "cooldown_until_epoch": deadline,
                }
            },
        },
        state_path,
    )

    snapshot = get_floodwait(state_path)

    assert state_path.exists()
    assert snapshot["floodwait_operations"]["upload"]["cooldown_remaining_seconds"] > 0


def test_floodwait_snapshot_reloads_updates_from_another_container(tmp_path: Path) -> None:
    """The dashboard must see a newer cooldown written by the manager."""
    state_path = tmp_path / "data" / "floodwait.json"
    record_floodwait(
        {
            "floodwait_events": 1,
            "floodwait_total_seconds": 30,
            "floodwait_operations": {
                "copy": {
                    "events": 1,
                    "total_wait_seconds": 30,
                    "cooldown_until_epoch": time.time() + 30,
                }
            },
        },
        state_path,
    )
    assert get_floodwait(state_path)["floodwait_operations"]["copy"]["events"] == 1

    # Simulate the manager atomically replacing the shared state file after a
    # later FloodWait. This bypasses record_floodwait's in-process cache.
    replacement = state_path.with_name("floodwait.updated.json")
    replacement.write_text(
        json.dumps(
            {
                "floodwait_events": 2,
                "floodwait_total_seconds": 3385,
                "floodwait_operations": {
                    "copy": {
                        "events": 2,
                        "total_wait_seconds": 3385,
                        "cooldown_until_epoch": time.time() + 300,
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    replacement.replace(state_path)

    snapshot = get_floodwait(state_path)

    assert snapshot["floodwait_operations"]["copy"]["events"] == 2
    assert snapshot["floodwait_operations"]["copy"]["cooldown_remaining_seconds"] > 0


def test_copy_recovery_pacing_uses_long_floodwait_history_then_relaxes(tmp_path: Path) -> None:
    state_path = tmp_path / "data" / "floodwait.json"
    record_floodwait(
        {
            "floodwait_events": 3,
            "floodwait_total_seconds": 5864,
            "floodwait_operations": {
                "copy": {
                    "events": 3,
                    "total_wait_seconds": 5864,
                    "cooldown_until_epoch": time.time() - 1,
                }
            },
        },
        state_path,
    )
    config = SimpleNamespace(
        base_dir=tmp_path,
        limits=SimpleNamespace(
            global_min_delay_seconds=0,
            delay_for=lambda operation: 3 if operation == "copy" else 0,
        ),
    )
    limiter = TelegramLimiter(config)

    assert limiter._operation_delay("copy") == 5

    async def recover_cleanly() -> None:
        for _ in range(limiter._COPY_RECOVERY_DECAY_SUCCESSFUL_CALLS):
            await limiter._record_success("copy")

    asyncio.run(recover_cleanly())

    assert limiter._operation_delay("copy") == 4
    restarted = TelegramLimiter(config)
    assert restarted._operation_delay("copy") == 4


def test_new_long_copy_floodwait_activates_recovery_pacing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class TestFloodWait(Exception):
        def __init__(self, value: int) -> None:
            self.value = value

    config = SimpleNamespace(
        base_dir=tmp_path,
        limits=SimpleNamespace(
            global_min_delay_seconds=0,
            delay_for=lambda operation: 3 if operation == "copy" else 0,
            floodwait_extra_min_seconds=0,
            floodwait_extra_max_seconds=0,
        ),
    )
    limiter = TelegramLimiter(config)
    attempts = 0

    async def copy_once() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise TestFloodWait(2479)
        return "copied"

    async def fast_forward_wait(_delay: float) -> None:
        limiter._floodwait_until_by_operation["copy"] = 0
        limiter._last_by_operation["copy"] = 0

    monkeypatch.setattr("app.telegram_client.FloodWait", TestFloodWait)
    monkeypatch.setattr("app.telegram_client.asyncio.sleep", fast_forward_wait)

    assert asyncio.run(limiter.call("copy", copy_once)) == "copied"
    assert attempts == 2
    assert limiter._operation_delay("copy") == 5
    snapshot = get_floodwait(tmp_path / "data" / "floodwait.json")
    assert snapshot["floodwait_operations"]["copy"]["recovery_delay_seconds"] == 5


def test_limiter_restores_active_floodwait_after_manager_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = tmp_path / "data" / "floodwait.json"
    record_floodwait(
        {
            "floodwait_events": 1,
            "floodwait_total_seconds": 30,
            "floodwait_operations": {
                "copy": {
                    "events": 1,
                    "total_wait_seconds": 30,
                    "cooldown_until_epoch": time.time() + 30,
                }
            },
        },
        state_path,
    )
    config = SimpleNamespace(
        base_dir=tmp_path,
        limits=SimpleNamespace(
            global_min_delay_seconds=0,
            delay_for=lambda _operation: 0,
        ),
    )
    limiter = TelegramLimiter(config)
    sleep_delays: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleep_delays.append(delay)
        limiter._floodwait_until_by_operation["copy"] = 0

    monkeypatch.setattr("app.telegram_client.asyncio.sleep", fake_sleep)
    asyncio.run(limiter.wait("copy"))

    assert sleep_delays and sleep_delays[0] > 0
    assert limiter.floodwait_snapshot()["floodwait_operations"]["copy"]["events"] == 1
