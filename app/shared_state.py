"""Lightweight cross-module shared state.

Written by background subsystems (e.g. TelegramLimiter), read by the
admin-bot dashboard.  Intentionally imports nothing from the app so
there is zero risk of circular imports.
"""
from __future__ import annotations
from typing import Any

# Floodwait snapshot — overwritten by TelegramLimiter after every FloodWait
# event.  Stays as the last known snapshot between events so the dashboard
# can show a countdown even between rate-limit notifications.
_floodwait: dict[str, Any] = {}


def record_floodwait(snapshot: dict[str, Any]) -> None:
    """Overwrite the in-memory floodwait snapshot (called from TelegramLimiter)."""
    global _floodwait
    _floodwait = snapshot


def get_floodwait() -> dict[str, Any]:
    """Return the latest floodwait snapshot; empty dict if none recorded yet."""
    return _floodwait
