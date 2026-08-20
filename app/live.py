from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pyrogram.handlers import MessageHandler

from app.config import AppConfig


VALID_RUN_MODES = {
    "health",
    "process",
    "sync",
    "run",
    "duplicate_cleanup_scan",
    "duplicate_cleanup_delete",
}


def _runtime_dir(config: AppConfig) -> Path:
    path = config.queue.db_path.parent
    path.mkdir(parents=True, exist_ok=True)
    return path


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return max(0.1, float(raw))
    except ValueError:
        return default


@dataclass(frozen=True)
class LiveSettings:
    reconcile_interval_seconds: float
    event_debounce_seconds: float
    poll_seconds: float

    @classmethod
    def from_environment(cls) -> "LiveSettings":
        return cls(
            reconcile_interval_seconds=_env_float(
                "LIVE_RECONCILE_SECONDS",
                _env_float("RUN_INTERVAL_SECONDS", 300.0),
            ),
            event_debounce_seconds=_env_float("LIVE_EVENT_DEBOUNCE_SECONDS", 2.0),
            poll_seconds=_env_float("LIVE_CONTROL_POLL_SECONDS", 0.5),
        )


class LiveTrigger:
    """Wake a single persistent migration client when a configured source receives a post."""

    def __init__(self, source_ids: set[int], settings: LiveSettings | None = None) -> None:
        self.source_ids = {int(value) for value in source_ids}
        self.settings = settings or LiveSettings.from_environment()
        self.event = asyncio.Event()
        self.last_source_id: int | None = None
        self.last_message_id: int | None = None
        self.handler = MessageHandler(self._on_message)

    async def _on_message(self, _: Any, message: Any) -> None:
        chat = getattr(message, "chat", None)
        chat_id = getattr(chat, "id", None)
        if chat_id is None or int(chat_id) not in self.source_ids:
            return
        self.last_source_id = int(chat_id)
        self.last_message_id = int(getattr(message, "id", 0) or 0) or None
        self.event.set()

    async def wait(
        self,
        config: AppConfig,
        stop_event: asyncio.Event,
        *,
        allow_reconciliation: bool = True,
    ) -> tuple[str, str] | None:
        started = time.monotonic()
        while not stop_event.is_set():
            requested = consume_run_request(config)
            if requested:
                return requested, "admin"
            if self.event.is_set():
                await self._debounce(stop_event)
                self.event.clear()
                return "sync", "live_event"
            if allow_reconciliation and time.monotonic() - started >= self.settings.reconcile_interval_seconds:
                return "sync", "reconciliation"
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self.settings.poll_seconds)
            except asyncio.TimeoutError:
                continue
        return None

    async def wait_for_resume(
        self,
        config: AppConfig,
        stop_event: asyncio.Event,
    ) -> tuple[str, str] | None:
        """Wait only for an explicit admin Start while the service is paused."""
        while not stop_event.is_set():
            requested = consume_run_request(config)
            if requested:
                # Ignore source posts that arrived while paused; Start is the
                # explicit signal that determines the next cycle.
                self.event.clear()
                return requested, "admin"
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=self.settings.poll_seconds,
                )
            except asyncio.TimeoutError:
                continue
        return None

    async def wait_for_retry(
        self,
        config: AppConfig,
        stop_event: asyncio.Event,
        seconds: float,
    ) -> tuple[str, str] | None:
        """Wait for a safe retry deadline while still responding to admin and source events."""
        deadline = time.monotonic() + max(0.1, float(seconds))
        while not stop_event.is_set():
            requested = consume_run_request(config)
            if requested:
                return requested, "admin"
            if self.event.is_set():
                await self._debounce(stop_event)
                self.event.clear()
                return "sync", "live_event"
            if time.monotonic() >= deadline:
                return "process", "scheduled_retry"
            remaining = max(0.05, deadline - time.monotonic())
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=min(self.settings.poll_seconds, remaining),
                )
            except asyncio.TimeoutError:
                continue
        return None

    async def _debounce(self, stop_event: asyncio.Event) -> None:
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=self.settings.event_debounce_seconds)
        except asyncio.TimeoutError:
            return


def consume_run_request(config: AppConfig) -> str | None:
    runtime = _runtime_dir(config)
    wake_path = runtime / "run_now"
    if not wake_path.exists():
        return None
    mode_path = runtime / "run_mode"
    try:
        mode = mode_path.read_text(encoding="utf-8").strip().lower() if mode_path.exists() else "sync"
    except OSError:
        mode = "sync"
    wake_path.unlink(missing_ok=True)
    mode_path.unlink(missing_ok=True)
    return mode if mode in VALID_RUN_MODES else "sync"
