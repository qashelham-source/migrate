from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.config import AppConfig


ACTIVE_PHASES = {
    "starting",
    "scanning",
    "processing",
    "downloading",
    "uploading",
    "batch_pause",
    "waiting_retry",
    "stopping",
}


def _runtime_dir(config: AppConfig) -> Path:
    path = config.queue.db_path.parent
    path.mkdir(parents=True, exist_ok=True)
    return path


def _status_path(config: AppConfig) -> Path:
    return _runtime_dir(config) / "runtime_status.json"


def _stop_path(config: AppConfig) -> Path:
    return _runtime_dir(config) / "stop_requested"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def write_status(config: AppConfig, phase: str, **details: Any) -> None:
    """Best-effort atomic status update shared by migration and admin bot processes."""
    payload: dict[str, Any] = {
        "phase": str(phase),
        "updated_at": utc_now(),
    }
    payload.update({key: value for key, value in details.items() if value is not None})

    path = _status_path(config)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"))
        temporary.replace(path)
    except OSError:
        temporary.unlink(missing_ok=True)


def read_status(config: AppConfig) -> dict[str, Any]:
    path = _status_path(config)
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        if isinstance(data, dict):
            return data
    except (OSError, ValueError, json.JSONDecodeError):
        pass
    return {"phase": "idle", "updated_at": None, "message": "Belum ada cycle aktif"}


def request_stop(config: AppConfig) -> None:
    path = _stop_path(config)
    path.touch()


def clear_stop(config: AppConfig) -> None:
    _stop_path(config).unlink(missing_ok=True)


def is_stop_requested(config: AppConfig) -> bool:
    return _stop_path(config).exists()


def is_active_phase(phase: str | None) -> bool:
    return str(phase or "").lower() in ACTIVE_PHASES


async def watch_stop_request(
    config: AppConfig,
    stop_event: asyncio.Event,
    poll_seconds: float = 0.5,
) -> None:
    while not stop_event.is_set():
        if is_stop_requested(config):
            write_status(
                config,
                "stopping",
                message="Arahan stop diterima. Menunggu operasi Telegram semasa selesai.",
            )
            stop_event.set()
            return
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=poll_seconds)
        except asyncio.TimeoutError:
            continue
