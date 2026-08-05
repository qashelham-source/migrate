"""Cross-process shared state for lightweight runtime telemetry.

The manager and admin bot run in separate containers, so in-memory state is
not enough for dashboard values such as Telegram FloodWait countdowns.  The
latest snapshot is persisted under the shared data volume and refreshed on
read.
"""
from __future__ import annotations

import copy
import json
import os
import time
from pathlib import Path
from typing import Any

_DEFAULT_FLOODWAIT_PATH = Path("data") / "floodwait.json"
_floodwait: dict[str, Any] = {}
_floodwait_source: Path | None = None


def _state_path(path: str | Path | None) -> Path:
    return Path(path) if path is not None else _DEFAULT_FLOODWAIT_PATH


def _refresh_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    refreshed = copy.deepcopy(snapshot)
    operations = refreshed.get("floodwait_operations")
    if isinstance(operations, dict):
        now = time.time()
        for details in operations.values():
            if not isinstance(details, dict):
                continue
            deadline = details.get("cooldown_until_epoch")
            if isinstance(deadline, (int, float)):
                details["cooldown_remaining_seconds"] = max(0, int(deadline - now))
    return refreshed


def record_floodwait(snapshot: dict[str, Any], path: str | Path | None = None) -> None:
    """Record a snapshot in memory and atomically persist it for other containers."""
    global _floodwait, _floodwait_source
    target = _state_path(path)
    _floodwait = copy.deepcopy(snapshot)
    _floodwait_source = target
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
        temporary.write_text(json.dumps(snapshot), encoding="utf-8")
        temporary.replace(target)
    except (OSError, TypeError, ValueError):
        pass


def get_floodwait(path: str | Path | None = None) -> dict[str, Any]:
    """Read the latest snapshot and refresh cooldowns against wall-clock time."""
    global _floodwait, _floodwait_source
    target = _state_path(path)
    if target != _floodwait_source:
        try:
            loaded = json.loads(target.read_text(encoding="utf-8"))
        except (OSError, TypeError, ValueError):
            return {}
        if not isinstance(loaded, dict):
            return {}
        _floodwait = loaded
        _floodwait_source = target
    return _refresh_snapshot(_floodwait)
