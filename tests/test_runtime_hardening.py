import time
from pathlib import Path

from app.destination_manager import clear_content_filter, load_content_filter, save_content_filter
from app.shared_state import get_floodwait, record_floodwait


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
