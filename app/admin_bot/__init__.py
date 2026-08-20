from __future__ import annotations

import asyncio
import json
import sqlite3
import tempfile
import time
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from pyrogram import Client, filters, idle
from pyrogram.errors import MessageNotModified
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    WebAppInfo,
)

from app.advanced import (
    REPAIR_CATEGORIES,
    checkpoint_rows,
    load_health_report,
    repair_samples,
    repair_summary,
    request_run_mode,
    requeue_repair_category,
    requeue_retryable_repairs,
    reset_all_checkpoints,
    resolve_uncertain_upload,
)
from app.admin_auth import authorized_ids as _authorized_ids, is_authorized as _is_authorized
from app.config import AppConfig, load_config
from app.control import (
    clear_pause,
    clear_stop,
    has_pending_run_request,
    is_active_phase,
    is_stoppable_phase,
    read_status,
    request_pause,
    request_stop,
    write_status,
)
from app.dashboard_v2 import active_source_progress, dashboard_snapshot, format_bytes, format_eta, issue_center
from app.db import Database
from app.destination_manager import (
    blacklist_source,
    clear_content_filter,
    get_source_blacklist,
    get_sources,
    list_destinations,
    load_content_filter,
    save_content_filter,
    set_destinations,
    set_sources,
    unblacklist_source,
)
from app.destination_duplicate_scan import (
    DestinationDuplicatePlan,
    load_destination_duplicate_plan,
    request_destination_duplicate_cleanup,
    request_destination_duplicate_scan,
)
from app.media_finder import duplicate_groups, find_by_reference, index_existing_queue, media_finder_stats
from app.queue import MessageQueue
from app.shared_state import get_floodwait
from app.telegram_client import start_client_with_floodwait
from app.logging import setup_logging

_LIVE_TASKS: dict[int, asyncio.Task[None]] = {}
_CHANNEL_CACHE: dict[int, list[dict[str, Any]]] = {}
_SOURCE_TITLE_CACHE: dict[Path, dict[str, str]] = {}
_SELECTIONS: dict[int, dict[str, list[str]]] = {}
_CONTENT_TYPES: dict[int, set[str]] = {}
_FINDER_INPUTS: set[int] = set()
_PAGE_SIZE = 8

_ALL_CONTENT_TYPES: tuple[str, ...] = ("video", "photo", "text")
_CONTENT_TYPE_LABELS: dict[str, str] = {
    "video": "📹 Video",
    "photo": "📷 Photo",
    "text": "📝 Text",
}

CATEGORY_LABELS = {
    "media_empty": "MEDIA_EMPTY",
    "peer_id": "Peer ID",
    "permission": "Permission",
    "temporary": "Temporary/Network",
    "source_missing": "Source Missing",
    "unsupported": "Unsupported",
    "needs_review": "Verify Before Retry",
    "other": "Other",
}


def _buttons(rows: list[list[tuple[str, str]]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(label, callback_data=data) for label, data in row] for row in rows]
    )


def _menu(config: AppConfig | None = None) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton("▶️ Start", callback_data="run:now"),
         InlineKeyboardButton("⏹ Stop", callback_data="stop:current")],
        [InlineKeyboardButton("🔄 Refresh", callback_data="dashboard:view"),
         InlineKeyboardButton("⋯ More", callback_data="menu:more")],
    ]
    if config and config.mini_app.enabled:
        rows.insert(
            0,
            [InlineKeyboardButton("✨ Open Mini Dashboard", web_app=WebAppInfo(url=config.mini_app.public_url))],
        )
    return InlineKeyboardMarkup(rows)


def _back(target: str = "menu", label: str = "⬅️ Dashboard") -> InlineKeyboardMarkup:
    return _buttons([[(label, target)]])


def _source_state_label(
    item: dict,
    current_chat: str,
    phase: str,
    status: dict,
) -> str:
    """Return a short, honest status line for one source based on DB + runtime state."""
    chat_id = str(item["source_chat_id"])
    total = int(item.get("total_items") or 0)
    eligible = int(item["eligible_items"])
    copied = int(item["copied_items"])
    active = int(item["active_items"])
    blocked = int(item["blocked_items"])
    remaining = int(item["remaining_items"])
    percent = int(item["percent"])
    is_current = bool(current_chat and current_chat == chat_id)

    if is_current:
        if phase == "scanning":
            scan_cur = status.get("current")
            scan_tot = status.get("total")
            if scan_cur is not None and scan_tot is not None:
                try:
                    return f"🔎 Scanning {int(scan_cur):,}/{int(scan_tot):,}"
                except (TypeError, ValueError):
                    pass
            return "🔎 Scanning…"
        if phase in {"downloading", "uploading", "processing"}:
            if eligible:
                if active:
                    return f"⚡ {copied:,}/{eligible:,} · {active} active"
                return f"⚡ {copied:,}/{eligible:,} sending"
            return "⚡ Processing…"
        if phase == "blocked":
            if eligible:
                return f"⛔ Blocked — {copied:,}/{eligible:,} done"
            return "⛔ Blocked"
        if phase == "waiting_retry":
            return f"🔄 Retry pending — {copied:,}/{eligible:,}"

    # DB-derived state (not currently active, or phase not recognised above)
    if eligible == 0 and total > 0:
        return "✅ All filtered (0 match content type)"
    if eligible == 0:
        return "⏳ Not yet scanned"
    if percent == 100 and remaining == 0 and blocked == 0:
        return f"✅ {copied:,}/{eligible:,} done"
    if blocked > 0 and active == 0 and copied == 0:
        return f"⛔ {blocked:,} items blocked — open Issue Center"
    if active > 0:
        bar = _progress_bar(percent, width=6)
        return f"⚡ {bar} {copied:,}/{eligible:,}"
    if blocked > 0:
        return f"⚠️ {copied:,}/{eligible:,} · {blocked} need review"
    if copied > 0 or remaining > 0:
        return f"⏳ {copied:,}/{eligible:,} queued"
    return "⏳ Queued"


def _more_menu() -> InlineKeyboardMarkup:
    return _buttons([
        [("📚 Source Queue", "sources:view"), ("🎯 Destinations", "destinations:view")],
        [("🧠 Smart Center", "smart:menu"), ("⚙️ Settings", "settings:view")],
        [("⬅️ Dashboard", "menu")],
    ])


# ---------------------------------------------------------------------------
# Content-type filter helpers
# ---------------------------------------------------------------------------

def _content_types_for(user_id: int) -> set[str]:
    """Return the set of EXCLUDED types for this user (default: exclude nothing)."""
    if user_id not in _CONTENT_TYPES:
        _CONTENT_TYPES[user_id] = set()
    return _CONTENT_TYPES[user_id]


def _toggle_content_type(user_id: int, type_name: str) -> None:
    """Toggle type_name in the excluded-types set: add to exclude, remove to un-exclude."""
    excluded = _content_types_for(user_id)
    if type_name in excluded:
        excluded.discard(type_name)   # un-exclude: allow this type through
    else:
        excluded.add(type_name)       # exclude: skip this type


def _content_type_text(user_id: int) -> str:
    excluded = _content_types_for(user_id)
    lines = [
        "📦 Content-type filter",
        "",
        "✅ = will migrate   ·   ☐ = will be skipped",
        "Tap a type to toggle it.",
        "",
    ]
    for t in _ALL_CONTENT_TYPES:
        icon = "☐" if t in excluded else "✅"
        lines.append(f"{icon} {_CONTENT_TYPE_LABELS[t]}")
    lines.append("")
    if not excluded:
        lines.append("Everything will migrate (including files, GIFs, voice, audio).")
    else:
        skipped = " · ".join(_CONTENT_TYPE_LABELS[t] for t in _ALL_CONTENT_TYPES if t in excluded)
        lines.append(f"Will skip: {skipped}")
        lines.append("All other types (files, GIFs, voice, audio, etc.) will still migrate.")
    return "\n".join(lines)


def _content_type_menu(user_id: int) -> InlineKeyboardMarkup:
    excluded = _content_types_for(user_id)
    rows: list[list[tuple[str, str]]] = []
    for t in _ALL_CONTENT_TYPES:
        icon = "☐" if t in excluded else "✅"
        label = f"{icon} {_CONTENT_TYPE_LABELS[t]}"
        rows.append([(label, f"sources:ctype_toggle:{t}")])
    rows += [
        [("▶️ Start Migration", "sources:ctype_confirm")],
        [("⬅️ Dashboard", "menu")],
    ]
    return _buttons(rows)


def _active_filter_line(path: Path) -> str:
    """Return a one-line summary of excluded types, or empty string when nothing excluded."""
    try:
        excluded = load_content_filter(path)
        if not excluded:   # None or empty frozenset → no exclusions
            return ""
        labels = " · ".join(
            _CONTENT_TYPE_LABELS.get(t, t) for t in _ALL_CONTENT_TYPES if t in excluded
        )
        return f"🚫 Excluding: {labels}"
    except Exception:
        return ""


def _smart_menu() -> InlineKeyboardMarkup:
    return _buttons(
        [
            [("🤖 AI Error Doctor", "advanced:repair")],
            [("🔍 Original Media Finder", "finder:view")],
            [("👥 Duplicate Detector", "duplicates:view")],
            [("🚨 Issue Center", "issues:view"), ("📈 Resource Monitor", "capacity:view")],
            [("🩺 Pre-flight Check", "advanced:health")],
            [("🧰 Recovery Tools", "advanced:menu")],
            [("⬅️ Dashboard", "menu")],
        ]
    )


def _advanced_menu() -> InlineKeyboardMarkup:
    return _buttons(
        [
            [("🩺 Health Check", "advanced:health"), ("🛠 Repair Queue", "advanced:repair")],
            [("▶️ Resume Pending", "advanced:resume"), ("🆕 Sync New Posts", "advanced:sync")],
            [("🔍 Full Scan", "advanced:full"), ("📍 Checkpoints", "checkpoint:view")],
            [("🔓 Force Recover Stuck Jobs", "advanced:force_recover")],
            [("⬅️ Smart Center", "smart:menu")],
        ]
    )


def _database(config: AppConfig) -> Database:
    db = Database(config.queue.db_path)
    db.initialize()
    return db


def _queue_counts(config: AppConfig) -> dict[str, int]:
    db = _database(config)
    try:
        return MessageQueue(db, config).counts_by_status()
    finally:
        db.close()


def _request_mode(config: AppConfig, mode: str) -> None:
    clear_pause(config)
    clear_stop(config)
    request_run_mode(config, mode)


def _progress_bar(percent: int, width: int = 10) -> str:
    value = max(0, min(100, int(percent)))
    filled = min(width, (value * width) // 100)
    return "█" * filled + "░" * (width - filled)


def _phase_summary(phase: str, current_source: dict[str, Any] | None) -> tuple[str, str]:
    blocked = int(current_source.get("blocked_items") or 0) if current_source else 0
    if phase == "blocked":
        return "⛔ Queue blocked", "Action is required before the queue can continue."
    if phase == "waiting_retry":
        return "🟡 Automatic retry", "The bot will resume automatically when the retry time arrives."
    if phase == "watching":
        return "🛰 Live Watcher", "Waiting for new posts or a Start Queue command."
    if phase == "queued":
        return "⏸ Queue waiting to start", "Tap Start Queue to resume queued sources."
    if phase == "scanning":
        return "🔎 Scanning source", "Organising posts/albums for migration."
    if phase in {"downloading", "uploading", "processing", "batch_pause"}:
        if blocked:
            return "🟠 Active source needs attention", "The queue continues with safe jobs."
        return "🟢 Running now", {
            "downloading": "⬇️ Downloading content.",
            "uploading": "⬆️ Uploading to the destination.",
            "processing": "⚙️ Processing migration.",
            "batch_pause": "⏳ Paused between batches; the queue will resume automatically.",
        }[phase]
    if phase == "stopping":
        return "⏹ Stopping safely", "Waiting for the current operation to finish safely."
    if phase == "stopped":
        return "⏹ Stopped", "The queue is saved and can be resumed later."
    if phase == "waiting":
        return "⚙️ Setup required", "Set a source and destination first."
    if phase == "error":
        return "🔴 Migration error", "Open Issue Center for details."
    return "🟢 Ready", "Tap Start Queue when you are ready to begin."


def _media_label(value: Any) -> str:
    labels = {
        "album": "Album",
        "video": "Video",
        "photo": "Photo",
        "document": "Document",
        "text": "Text",
    }
    return labels.get(str(value or "").lower(), str(value or "Media").title())


def _dashboard_text(config: AppConfig, config_path: Path | None = None) -> str:
    db = _database(config)
    try:
        data = dashboard_snapshot(db, config.queue.db_path)
    finally:
        db.close()
    status = read_status(config)

    phase = str(status.get("phase") or "idle").lower()
    current_chat = str(status.get("source_chat") or "").strip()
    error = str(status.get("last_error") or status.get("error") or "").strip()
    stalled_jobs = list(data["health"].get("stalled_jobs") or [])
    telemetry = data["telemetry"]
    review_total = int(data["review"]["total"])
    storage = data["storage"]

    # Destination count from config.yaml — same source the sender uses.
    dest_count = len(list_destinations(config_path)) if config_path else int(data["destinations"]["total"])

    # ── Headline ──────────────────────────────────────────────────────────
    if stalled_jobs:
        headline = "🚨 Job stalled — open Smart Center › Recovery Tools"
    elif phase == "source_complete":
        si = status.get("source_index")
        st = status.get("source_total")
        try:
            si_i, st_i = int(si), int(st)
            if si_i < st_i:
                headline = f"✅ Source {si_i}/{st_i} done — moving to source {si_i + 1}"
            else:
                headline = f"✅ All {st_i} source(s) done"
        except (TypeError, ValueError):
            # si absent = called after ALL sources finished (source_total written, no source_index)
            try:
                headline = f"✅ All {int(st)} source(s) done"
            except (TypeError, ValueError):
                headline = "✅ All sources done"
    elif phase == "scanning":
        si = status.get("source_index")
        st = status.get("source_total")
        scan_cur = status.get("current")
        scan_tot = status.get("total")
        try:
            si_txt = f"source {int(si)}/{int(st)}"
        except (TypeError, ValueError):
            si_txt = None
        try:
            prog = f"{int(scan_cur):,}/{int(scan_tot):,} scanned"
        except (TypeError, ValueError):
            prog = None
        parts: list[str] = ["🔎 Scanning"]
        if si_txt:
            parts.append(si_txt)
        if prog:
            parts.append(prog)
        headline = " — ".join(parts)
    elif phase in {"downloading", "uploading", "processing",
                   "scan_complete", "verifying", "starting"}:
        si = status.get("source_index")
        st = status.get("source_total")
        try:
            si_txt = f"source {int(si)}/{int(st)}"
        except (TypeError, ValueError):
            si_txt = None
        _phase_icon = {
            "downloading": "⬇️ Downloading",
            "uploading":   "⬆️ Copying",
            "processing":  "⬆️ Copying",
            "scan_complete": "⚡ Running",
            "verifying":   "🔍 Verifying",
            "starting":    "⚡ Starting",
        }.get(phase, "⚡ Running")
        headline = f"{_phase_icon} — {si_txt}" if si_txt else _phase_icon
    elif phase == "batch_pause":
        si = status.get("source_index")
        st = status.get("source_total")
        try:
            headline = f"⏸ Paused between batches — source {int(si)}/{int(st)} — resumes on its own"
        except (TypeError, ValueError):
            headline = "⏸ Paused between batches — resumes on its own"
    elif phase == "queued":
        si = status.get("source_index")
        st = status.get("source_total")
        try:
            headline = f"⏸ Scan done, source {int(si)}/{int(st)} — tap ▶️ Start to upload"
        except (TypeError, ValueError):
            headline = "⏸ Scan done — tap ▶️ Start to upload"
    elif phase == "blocked":
        headline = "⛔ Blocked — action needed (see below)"
    elif phase == "waiting_retry":
        headline = "🔄 Retry scheduled — will resume automatically"
    elif phase in {"stopping", "stopped"}:
        headline = "⏹ Stopped"
    elif phase == "watching":
        headline = "🛰 Watching for new posts"
    elif phase in {"waiting", "idle"} or dest_count == 0:
        headline = "⚙️ Not started — configure source and destination first"
    else:
        headline = "🟢 Ready — tap ▶️ Start"

    # ── DB safety net ─────────────────────────────────────────────────────
    # The headline is derived from status.json (phase), which can lag or miss
    # phases.  If the DB shows jobs actively in flight for any source, the
    # headline must never say "Ready" — that contradicts what the list shows.
    source_progress = data["source_progress"]
    if headline in {"🟢 Ready — tap ▶️ Start",
                    "⚙️ Not started — configure source and destination first"}:
        any_active = any(
            int(item.get("active_items") or 0) > 0
            for item in source_progress
        )
        if any_active:
            si = status.get("source_index")
            st = status.get("source_total")
            try:
                headline = f"⚡ Running — source {int(si)}/{int(st)}"
            except (TypeError, ValueError):
                headline = "⚡ Running"

    # ── FloodWait headline override ───────────────────────────────────────
    # Rate-limit is the most useful thing to show — always wins the headline
    # when Telegram is making us wait, regardless of the underlying phase.
    _fw = get_floodwait(config.base_dir / "data" / "floodwait.json")
    _fw_remaining = 0
    if _fw:
        _fw_remaining = max(
            (v.get("cooldown_remaining_seconds", 0)
             for v in _fw.get("floodwait_operations", {}).values()),
            default=0,
        )
    if _fw_remaining > 0:
        _fw_si = status.get("source_index")
        _fw_st = status.get("source_total")
        try:
            _fw_src = f" — source {int(_fw_si)}/{int(_fw_st)}"
        except (TypeError, ValueError):
            _fw_src = ""
        headline = f"⏳ Rate limited — resumes in ~{_fw_remaining}s{_fw_src}"

    lines = ["🤖 Migration Bot", "", headline]

    # ── Per-source status list ────────────────────────────────────────────
    source_total_in_status = int(status.get("source_total") or 0)
    if source_progress:
        lines.append("")

        def _source_is_complete(item: dict) -> bool:
            """True when a source has nothing left to do (100 % copied or all filtered)."""
            eligible = int(item.get("eligible_items") or 0)
            total    = int(item.get("total_items")    or 0)
            # All-filtered: scanner ran, nothing matched the content-type filter
            if eligible == 0 and total > 0:
                return True
            # Normal completion: every eligible item copied, nothing blocked/active
            return (
                eligible > 0
                and int(item.get("percent")         or 0) == 100
                and int(item.get("remaining_items") or 0) == 0
                and int(item.get("blocked_items")   or 0) == 0
            )

        active_sources = [s for s in source_progress if not _source_is_complete(s)]

        # Active / in-progress sources — full detail, one entry each
        for i, item in enumerate(active_sources, 1):
            title = str(item["title"])[:38]
            label = _source_state_label(item, current_chat, phase, status)
            lines.append(f"{i}. {title}")
            lines.append(f"   {label}")

        # Sources not yet scanned (configured but not started)
        waiting = source_total_in_status - len(source_progress)
        if waiting > 0:
            lines.append(f"   📋 +{waiting} more source(s) waiting in queue")

        # Completed sources are intentionally omitted from the live dashboard.
        # Their jobs and checkpoints remain in the database, and configured
        # sources continue to be watched for new posts. Showing a permanent
        # completion summary here makes finished work look like an active queue.

    # ── Loud errors ───────────────────────────────────────────────────────
    if dest_count == 0 and phase not in {"stopped", "stopping", "watching"}:
        lines += [
            "",
            "⚠️ No destination configured.",
            "→ Tap ⋯ More › Destinations to add one.",
        ]
    elif error and phase in {"blocked", "waiting_retry", "error"}:
        lines += ["", f"⚠️ {str(error).replace(chr(10), ' ')[:250]}"]
        if phase == "blocked":
            lines.append("→ Fix the issue above, then tap ▶️ Start.")

    if stalled_jobs:
        lines += [
            "",
            f"🚨 {len(stalled_jobs)} job(s) not responding.",
            "→ Tap ⋯ More › Smart Center › Recovery Tools.",
        ]

    if review_total:
        lines += ["", f"⚠️ {review_total} item(s) in Issue Center (tap ⋯ More › Smart Center)"]

    # ── Speed / filter / storage ──────────────────────────────────────────
    speed = float(telemetry["speed_bps"] or 0)
    if speed > 0:
        speed_line = f"🚀 {format_bytes(speed)}/s"
        if telemetry["eta_seconds"] is not None:
            speed_line += f" · ETA {format_eta(telemetry['eta_seconds'])}"
        lines.append(speed_line)

    if config_path is not None:
        filter_line = _active_filter_line(config_path)
        if filter_line:
            lines.append(filter_line)

    if storage.percent_used >= 80:
        level = "🚨 Critical storage" if storage.percent_used >= 90 else "⚠️ Low storage"
        lines.append(f"{level} — {format_bytes(storage.free_bytes)} free")

    # ── FloodWait detail (headline already overridden above when active) ──
    # Show per-operation breakdown only when multiple ops are throttled, so
    # the user can see which operation is waiting (read vs upload, etc.).
    if _fw_remaining > 0:
        ops = _fw.get("floodwait_operations", {}) or {}
        throttled = {op: v for op, v in ops.items()
                     if int(v.get("cooldown_remaining_seconds", 0)) > 0}
        if len(throttled) > 1:
            for op, v in sorted(throttled.items()):
                rem = int(v["cooldown_remaining_seconds"])
                lines.append(f"   ↳ {op}: ~{rem}s")

    # ── Footer ────────────────────────────────────────────────────────────
    dest_label = "destination" if dest_count == 1 else "destinations"
    tz_hours = getattr(config, "display_timezone_hours", 8)
    local_tz = timezone(timedelta(hours=tz_hours))
    ts = datetime.now(tz=local_tz).strftime("%H:%M")
    tz_sign = "+" if tz_hours >= 0 else ""
    tz_label = f"UTC{tz_sign}{tz_hours}"
    lines += ["", f"🕐 Last edit: {ts} ({tz_label}) · {dest_count} {dest_label} · ↻ auto"]

    return "\n".join(lines)[:3900]

def _settings_text(path: Path) -> str:
    sources = get_sources(path)
    destinations = list_destinations(path)
    return "\n".join(
        [
            "⚙️ System Settings",
            "",
            f"Selected sources: {len(sources)}",
            f"Selected destinations: {len(destinations)}",
            "",
            "Sources are managed in Source Queue. Destinations are managed in the Destinations menu.",
            "Worker, retry, cache, and log settings remain in the system configuration.",
        ]
    )


def _issues_text(config: AppConfig) -> str:
    db = _database(config)
    try:
        rows = issue_center(db, 20)
    finally:
        db.close()
    lines = ["🚨 Issue Center", ""]
    if not rows:
        return "\n".join(lines + ["✅ No active issues."])
    for item in rows:
        if item["kind"] == "stalled":
            lines += [f"🚨 Job #{item['id']} stalled", str(item["error"])[:180], ""]
        elif item["kind"] == "destination":
            lines += [f"⏸ Destination {item['id']} paused", str(item["error"])[:180], ""]
        elif item["kind"] == "verification":
            lines += [
                f"⚠️ Verification · Job #{item['id']}",
                f"Source {item['source_chat_id']} · post {item['source_message_id']}",
                str(item["error"]).replace("\n", " ")[:180],
                "",
            ]
        else:
            lines += [
                f"❌ Job #{item['id']} · {item['status']}",
                f"Source {item['source_chat_id']} · post {item['source_message_id']}",
                str(item["error"]).replace("\n", " ")[:180],
                "",
            ]
    return "\n".join(lines)[:3900]


def _capacity_text(config: AppConfig) -> str:
    db = _database(config)
    try:
        data = dashboard_snapshot(db, config.queue.db_path)
    finally:
        db.close()
    telemetry, storage = data["telemetry"], data["storage"]
    warning = "🚨 Critical" if storage.percent_used >= 90 else "⚠️ Low" if storage.percent_used >= 80 else "✅ Healthy"
    return "\n".join(
        [
            "📈 Resource Monitor", "", f"Status: {warning}",
            f"Speed: {format_bytes(telemetry['speed_bps'])}/s", f"ETA: {format_eta(telemetry['eta_seconds'])}",
            f"Working storage: {format_bytes(storage.used_bytes)} / {format_bytes(storage.total_bytes)}",
            f"Free: {format_bytes(storage.free_bytes)}",
            "", "Temporary files are removed after verified upload.",
        ]
    )


def _finder_text_from_stats(stats: dict[str, Any], indexed_now: int | None = None) -> str:
    lines = [
        "🔍 Original Media Finder",
        "",
        f"Indexed media: {stats['indexed']}",
        f"Unique fingerprints: {stats['unique_fingerprints']}",
        f"Duplicate records: {stats['duplicate_records']} ({stats['duplicate_rate']}%)",
        f"Search history: {stats['match_history']}",
    ]
    if indexed_now is not None:
        lines += ["", f"✅ {indexed_now} new media queue item(s) indexed."]
    lines += [
        "",
        "Index Queue reads existing metadata only—no media is downloaded or changed.",
        "Find Link accepts a t.me link or an indexed message number.",
    ]
    return "\n".join(lines)[:3900]


def _finder_text(config: AppConfig, *, indexed_now: int | None = None) -> str:
    db = _database(config)
    try:
        stats = media_finder_stats(db)
    finally:
        db.close()
    return _finder_text_from_stats(stats, indexed_now)


def _finder_menu() -> InlineKeyboardMarkup:
    return _buttons(
        [
            [("📥 Index Queue", "finder:index"), ("🔎 Find Link", "finder:search")],
            [("🔄 Refresh", "finder:view")],
            [("👥 Duplicate Detector", "duplicates:view")],
            [("⬅️ Smart Center", "smart:menu")],
        ]
    )


def _duplicates_text_from_data(
    stats: dict[str, Any],
    groups: list[dict[str, Any]],
    indexed_now: int | None = None,
) -> str:
    lines = [
        "👥 Duplicate Detector",
        "",
        f"Indexed media: {stats['indexed']}",
        f"Duplicate records: {stats['duplicate_records']} ({stats['duplicate_rate']}%)",
    ]
    if indexed_now is not None:
        lines += ["", f"✅ {indexed_now} new media queue item(s) indexed."]
    if not groups:
        lines += ["", "✅ No duplicates found in indexed media."]
        return "\n".join(lines)
    lines += ["", f"Top duplicate groups: {len(groups)}"]
    for index, group in enumerate(groups, start=1):
        lines += [
            "",
            f"{index}. {int(group.get('copies') or 0)} copies",
            f"Original: {group.get('original_chat_id')}/{group.get('original_message_id')}",
        ]
        locations = str(group.get("locations") or "")
        if locations:
            lines.append(f"Locations: {locations[:240]}")
    return "\n".join(lines)[:3900]


def _duplicates_text(config: AppConfig, *, indexed_now: int | None = None) -> str:
    db = _database(config)
    try:
        stats = media_finder_stats(db)
        groups = duplicate_groups(db, limit=10)
    finally:
        db.close()
    return _duplicates_text_from_data(stats, groups, indexed_now)


def _duplicates_menu() -> InlineKeyboardMarkup:
    return _buttons(
        [
            [("📥 Index Queue", "duplicates:index"), ("🔄 Refresh", "duplicates:view")],
            [("🔎 Scan Destination", "duplicates:destination:scan")],
            [("🧬 Deep Scan Same Files", "duplicates:destination:content-scan")],
            [("🧹 Clean Scan Results", "duplicates:destination:preview")],
            [("⬅️ Smart Center", "smart:menu")],
        ]
    )


def _destination_duplicate_cleanup_text(plan: DestinationDuplicatePlan | None) -> str:
    """Render a live destination-history scan without making any deletion."""

    content_scan = bool(plan and plan.scan_mode == "content")
    lines = ["🧹 Destination Duplicate Cleanup", ""]
    if content_scan:
        lines += [
            "This checks byte-identical media files when Telegram fingerprints differ.",
            "Visually similar re-encoded files are not auto-deleted. Source posts are never touched.",
        ]
    else:
        lines += [
            "This reads the actual configured destination history.",
            "Only exact Telegram media fingerprints match. Source posts are never touched.",
        ]
    if plan is None:
        lines += ["", "Tap Scan Destination first. No deletion happens during the scan."]
        return "\n".join(lines)
    if plan.state == "pending":
        scan_name = "Deep content scan" if content_scan else "Scan"
        lines += ["", f"⏳ {scan_name} queued. The manager session will start it shortly.", "Tap Refresh to view progress."]
        return "\n".join(lines)
    if plan.state == "running":
        if content_scan:
            lines += [
                "",
                "⏳ Deep-scanning destination media…",
                f"Messages checked so far: {plan.scanned_message_count}",
                f"Same-size candidates: {plan.content_candidate_count}",
                f"Files hashed so far: {plan.content_hashed_count}",
                "Tap Refresh after a moment.",
            ]
        else:
            lines += [
                "",
                "⏳ Scanning destination history…",
                f"Messages checked so far: {plan.scanned_message_count}",
                f"Media fingerprints read so far: {plan.media_message_count}",
                "Tap Refresh after a moment.",
            ]
        return "\n".join(lines)
    if plan.state == "delete_pending":
        lines += [
            "",
            "⏳ Cleanup queued.",
            "The manager session that scanned this destination will delete the approved IDs.",
            "Tap Refresh shortly to view progress.",
        ]
        return "\n".join(lines)
    if plan.state == "deleting":
        lines += [
            "",
            "⏳ Deleting approved exact duplicates through the manager session…",
            f"Deleted so far: {plan.deleted_message_count}",
            "Tap Refresh shortly to view progress.",
        ]
        return "\n".join(lines)
    if plan.state == "completed":
        lines += [
            "",
            f"✅ Previous cleanup deleted: {plan.deleted_message_count} Telegram message(s).",
            "Run a fresh scan to verify the destination.",
        ]
        if plan.error:
            lines += ["", f"Some manager delete requests failed: {plan.error[:300]}"]
        return "\n".join(lines)
    if plan.state in {"failed", "cancelled", "delete_failed", "delete_cancelled"}:
        action = "Cleanup" if plan.state.startswith("delete_") else "Scan"
        lines += [
            "",
            f"❌ {action} {plan.state.removeprefix('delete_')}: {plan.error or 'Unknown error'}",
            "Tap Scan Destination to create a fresh review.",
        ]
        return "\n".join(lines)

    lines += [
        "",
        f"Destination messages scanned: {plan.scanned_message_count}",
        f"Media fingerprints checked: {plan.media_message_count}",
        f"{'Byte-identical' if content_scan else 'Exact'} duplicate groups: {plan.group_count}",
        f"Telegram messages to delete: {plan.message_count}",
    ]
    if not plan.groups:
        no_match = "byte-identical" if content_scan else "exact"
        lines += ["", f"✅ No {no_match} duplicate media was found in the destination history."]
        if plan.error:
            lines += ["", f"Some candidates could not be checked: {plan.error[:300]}"]
        return "\n".join(lines)

    lines += ["", "Preview (first 8):"]
    for index, group in enumerate(plan.groups[:8], start=1):
        topic = f" · topic {group.dest_topic_id}" if group.dest_topic_id is not None else ""
        lines += [
            f"{index}. {group.dest_title}{topic} · {group.media_type} · {'checksum' if group.match_kind == 'content_sha256' else 'Telegram fingerprint'}",
            f"   Keep: {group.kept_message_id}",
            f"   Delete: {', '.join(str(value) for value in group.duplicate_message_ids)}",
        ]
    if plan.group_count > 8:
        adjective = "byte-identical" if content_scan else "exact"
        lines.append(f"… and {plan.group_count - 8} more {adjective} duplicate group(s).")
    if plan.error:
        lines += ["", f"Some candidates could not be checked: {plan.error[:300]}"]
    lines += ["", "This cannot be undone from Telegram."]
    return "\n".join(lines)[:3900]


def _destination_duplicate_cleanup_menu(plan: DestinationDuplicatePlan | None) -> InlineKeyboardMarkup:
    rows: list[list[tuple[str, str]]] = []
    if plan is None or plan.state not in {"pending", "running", "delete_pending", "deleting"}:
        rows.append([("🔎 Scan Destination", "duplicates:destination:scan")])
        rows.append([("🧬 Deep Scan Same Files", "duplicates:destination:content-scan")])
    if plan and plan.state == "ready" and plan.message_count:
        rows.append(
            [
                (
                    f"🗑 Delete {plan.message_count} Exact Duplicate Message(s)",
                    "duplicates:destination:confirm",
                )
            ]
        )
    rows += [[("🔄 Refresh", "duplicates:destination:preview")], [("⬅️ Duplicate Detector", "duplicates:view")]]
    return _buttons(rows)


def _finder_result_text(reference: str, match: dict[str, Any] | None) -> str:
    if match is None:
        return "\n".join(
            [
                "🔍 Original Media Finder",
                "",
                "❌ Original media was not found in the index.",
                "Tap Index Queue, then try the t.me link or message ID again.",
            ]
        )
    size = int(match.get("file_size") or 0)
    lines = [
        "🔍 Original Media Finder",
        "",
        "✅ Original media found.",
        f"Source: {match.get('source_chat_id')}",
        f"Message ID: {match.get('source_message_id')}",
        f"Type: {match.get('media_type') or '-'}",
        f"Size: {format_bytes(size) if size else '-'}",
    ]
    if match.get("file_name"):
        lines.append(f"File: {str(match['file_name'])[:180]}")
    return "\n".join(lines)[:3900]


def _health_text(config: AppConfig) -> str:
    report = load_health_report(config)
    if not report:
        return "🩺 Pre-flight Health Check\n\nNo report yet. Tap Run Check."
    lines = ["🩺 Pre-flight Health Check", "", f"Overall: {str(report.get('overall') or 'unknown').upper()}", ""]
    for item in report.get("checks") or []:
        icon = {"pass": "✅", "warn": "⚠️", "fail": "❌"}.get(str(item.get("status")), "•")
        lines.append(f"{icon} {item.get('name')}: {item.get('detail')}")
    return "\n".join(lines)[:3900]


def _repair_text(config: AppConfig, details: bool = False) -> str:
    db = _database(config)
    try:
        if details:
            rows: list[tuple[str, dict[str, Any]]] = []
            for category in REPAIR_CATEGORIES:
                rows.extend((category, item) for item in repair_samples(db, category, limit=5))
            rows.sort(key=lambda pair: int(pair[1]["id"]), reverse=True)
            lines = ["🤖 AI Error Doctor", ""]
            for category, item in rows[:15]:
                lines += [f"#{item['id']} · {CATEGORY_LABELS[category]}", str(item['last_error']).replace("\n", " ")[:180], ""]
            return "\n".join(lines or ["No issues."])[:3900]
        summary = repair_summary(db)
    finally:
        db.close()
    lines = ["🤖 AI Error Doctor", "", f"Jobs requiring attention: {sum(summary.values())}", ""]
    lines += [f"• {CATEGORY_LABELS[key]}: {summary.get(key, 0)}" for key in REPAIR_CATEGORIES]
    return "\n".join(lines)


def _review_upload_page(config: AppConfig) -> tuple[str, InlineKeyboardMarkup]:
    db = _database(config)
    try:
        items = repair_samples(db, "needs_review", limit=1)
    finally:
        db.close()
    if not items:
        return (
            "🧭 Verify Before Retry\n\n✅ No uncertain uploads need review.",
            _back("advanced:repair", "⬅️ AI Error Doctor"),
        )

    item = items[0]
    job_id = int(item["id"])
    text = "\n".join(
        [
            "🧭 Verify Before Retry",
            "",
            f"Job #{job_id} · {str(item['media_type']).title()}",
            f"Destination: {item['dest_chat_id']}",
            "",
            "Check the destination once before choosing an action.",
            "• Found once: mark it delivered and continue.",
            "• Not found: retry it safely.",
            "",
            f"⚠️ {str(item['last_error']).replace(chr(10), ' ')[:260]}",
        ]
    )
    return (
        text[:3900],
        _buttons(
            [
                [("✅ Found — continue", f"repair:confirm:{job_id}")],
                [("↻ Not found — retry", f"repair:missing:{job_id}")],
                [("⬅️ AI Error Doctor", "advanced:repair")],
            ]
        ),
    )


def _advanced_text(config: AppConfig) -> str:
    counts, status = _queue_counts(config), read_status(config)
    db = _database(config)
    try:
        checkpoints = len(checkpoint_rows(db))
    finally:
        db.close()
    return "\n".join(["🧰 Recovery Tools", "", f"Migration: {'active' if is_active_phase(status.get('phase')) else 'idle'}", f"Pending: {counts.get('pending', 0)}", f"Failed: {counts.get('failed', 0)}", f"Checkpoints: {checkpoints}"])


def _checkpoint_text(config: AppConfig) -> str:
    db = _database(config)
    try:
        rows = checkpoint_rows(db)
    finally:
        db.close()
    lines = ["📍 Incremental Sync Checkpoints", ""]
    for index, row in enumerate(rows, 1):
        lines += [f"{index}. Source {row['source_chat_id']}", f"Last message: {row['last_scanned_message_id']}", ""]
    return "\n".join(lines + ([] if rows else ["No checkpoints yet."]))[:3900]


def _ordered_chats(values: list[str] | set[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        chat = str(value)
        if chat not in seen:
            seen.add(chat)
            result.append(chat)
    return result


def _selection_for(user_id: int, path: Path) -> dict[str, list[str]]:
    if user_id not in _SELECTIONS:
        _SELECTIONS[user_id] = {
            "sources": _ordered_chats([str(item["chat"]) for item in get_sources(path)]),
            "destinations": _ordered_chats([str(item["chat"]) for item in list_destinations(path)]),
        }
    selection = _SELECTIONS[user_id]
    blacklisted = {chat.lower() for chat in get_source_blacklist(path)}
    for key in ("sources", "destinations"):
        selection[key] = _ordered_chats(selection.get(key, []))
    selection["sources"] = [
        chat for chat in selection["sources"] if str(chat).lower() not in blacklisted
    ]
    return selection


def _toggle_selection(selected: list[str], chat: str) -> bool:
    if chat in selected:
        selected.remove(chat)
        return False
    selected.append(chat)
    return True


def _snapshot_session_database(source: Path, destination: Path) -> None:
    """Create an isolated SQLite snapshot without locking the live Telegram session.

    Kept for compatibility but no longer called by _scan_channels or
    _latest_source_position — those now use the shared channel cache and
    the local database respectively, avoiding concurrent auth-key usage.
    """
    if not source.is_file():
        raise RuntimeError("Telegram session was not found. Run login first.")

    destination.parent.mkdir(parents=True, exist_ok=True)
    source_uri = f"{source.resolve().as_uri()}?mode=ro"
    last_error: sqlite3.OperationalError | None = None
    for attempt in range(5):
        try:
            if destination.exists():
                destination.unlink()
            with sqlite3.connect(source_uri, uri=True, timeout=3.0) as reader:
                with sqlite3.connect(destination, timeout=3.0) as writer:
                    reader.backup(writer, pages=100, sleep=0.05)
            return
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower():
                raise
            last_error = exc
            time.sleep(0.15 * (attempt + 1))

    raise RuntimeError("Telegram session is still busy. Try Scan / Refresh again.") from last_error


async def _scan_channels(config: AppConfig, user_id: int) -> list[dict[str, Any]]:
    """Return the channel list from the shared cache written by migration-manager.

    The manager exports this file at startup (via _dump_channel_cache in main.py)
    so the admin bot never needs to open a second Pyrogram client with the same
    auth key, which was the structural cause of SESSION_REVOKED loops.

    Raises RuntimeError if the cache file is missing (manager has not run yet)
    or unreadable.
    """
    cache_path = config.queue.db_path.parent / "channel_cache.json"
    if not cache_path.is_file():
        raise RuntimeError(
            "Channel list not available yet.\n\n"
            "The migration manager writes this cache at startup. "
            "Start the manager once, wait a few seconds, then tap Scan again."
        )
    try:
        data = json.loads(cache_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise RuntimeError(
            f"Could not read channel cache ({exc}). "
            "Restart the migration manager to regenerate it."
        ) from exc
    if not isinstance(data, list):
        raise RuntimeError(
            "Channel cache format is invalid. Restart the migration manager."
        )
    return data


async def _latest_source_position(
    config: AppConfig,
    user_id: int,
    chat: str | int,
) -> tuple[int, int]:
    """Return (source_chat_id, latest_message_id) using only the local database.

    Replaces the old approach of copying the user session and opening a second
    Pyrogram client, which caused concurrent auth-key usage and SESSION_REVOKED.

    The latest message ID is taken from the scan checkpoint (the last message
    the scanner reached), which is the correct anchor for 'New Posts Only'.
    Falls back to the highest source_message_id already in the messages table,
    then to 0 (start from beginning) if the source has never been scanned.
    """
    chat_str = str(chat).strip()
    db = _database(config)
    try:
        # Resolve numeric chat ID from source_registry
        source_id: int | None = None
        rows = db.query(
            "SELECT source_chat_id FROM source_registry WHERE source_chat_id = ? LIMIT 1",
            (chat_str,),
        )
        if rows:
            source_id = int(rows[0]["source_chat_id"])
        else:
            # Fallback: treat the value as a raw integer chat ID
            try:
                source_id = int(chat_str)
            except ValueError:
                source_id = None

        if source_id is None:
            raise RuntimeError(
                f"Source '{chat}' not found in the local registry. "
                "Run a full scan first so the source is registered."
            )

        # Prefer the scan checkpoint — it is the last message the scanner reached
        checkpoint_row = db.get_scan_checkpoint(source_id)
        if checkpoint_row is not None:
            latest = int(checkpoint_row["last_scanned_message_id"] or 0)
            return source_id, latest

        # Fallback: highest source_message_id already queued
        row = db.query_one(
            "SELECT MAX(source_message_id) AS latest FROM messages WHERE source_chat_id = ?",
            (str(source_id),),
        )
        latest = int(row["latest"] or 0) if row and row["latest"] is not None else 0
        return source_id, latest
    finally:
        db.close()


def _stored_source_titles(path: Path) -> dict[str, str]:
    """Return durable titles for saved sources when Telegram has not been scanned this session."""
    cache_key = path.resolve()
    cached = _SOURCE_TITLE_CACHE.get(cache_key)
    if cached is not None:
        return cached

    titles: dict[str, str] = {}
    try:
        config = load_config(cache_key)
        db = _database(config)
        try:
            MessageQueue(db, config)
            rows = db.query("SELECT source_chat_id, title FROM source_registry")
        finally:
            db.close()
    except (OSError, sqlite3.Error, ValueError):
        rows = []

    for row in rows:
        chat_id = str(row["source_chat_id"])
        title = str(row["title"] or "").strip()
        if title and title != chat_id:
            titles[chat_id] = title
    _SOURCE_TITLE_CACHE[cache_key] = titles
    return titles


def _persist_scanned_source_titles(
    config: AppConfig,
    path: Path,
    channels: list[dict[str, Any]],
) -> None:
    """Persist selected source titles so a later bot restart does not expose raw IDs."""
    selected = {str(item["chat"]) for item in get_sources(path)}
    if not selected:
        return

    db: Database | None = None
    changed = False
    try:
        db = _database(config)
        queue = MessageQueue(db, config)
        for channel in channels:
            chat = str(channel["chat"])
            title = str(channel.get("title") or "").strip()
            if chat not in selected or not title or title == chat:
                continue
            queue.register_source(
                source_chat_id=chat,
                title=title,
                username=channel.get("username"),
                chat_type=str(channel.get("kind") or "channel").lower(),
                latest_seen_message_id=None,
            )
            changed = True
    except (OSError, sqlite3.Error):
        return
    finally:
        if db is not None:
            db.close()

    if changed:
        _SOURCE_TITLE_CACHE.pop(path.resolve(), None)


def _channel_title(user_id: int, chat: str, path: Path | None = None) -> str:
    for item in _CHANNEL_CACHE.get(user_id, []):
        if str(item["chat"]) == str(chat):
            return str(item["title"])
    if path is not None:
        saved_title = _stored_source_titles(path).get(str(chat))
        if saved_title:
            return saved_title
    return "Unnamed channel"


def _source_channels(user_id: int, path: Path) -> list[dict[str, Any]]:
    blacklisted = {chat.lower() for chat in get_source_blacklist(path)}
    return [
        item
        for item in _CHANNEL_CACHE.get(user_id, [])
        if str(item["chat"]).lower() not in blacklisted
    ]


def _source_page(channels: list[dict[str, Any]], page: int) -> int:
    if not channels:
        return 0
    return max(0, min(page, (len(channels) - 1) // _PAGE_SIZE))


def _active_queue_source(path: Path) -> str | None:
    try:
        status = read_status(load_config(path))
    except Exception:
        return None
    phase = str(status.get("phase") or "").lower()
    if phase in {"idle", "watching", "stopped", "source_complete", ""}:
        return None
    source_chat = status.get("source_chat")
    return str(source_chat) if source_chat is not None else None


def _source_queue_lines(
    user_id: int,
    path: Path,
    selected: list[str],
    active_source_chat: str | None = None,
) -> list[str]:
    if not selected:
        return ["No sources in the queue."]
    lines = [f"Queue order · {len(selected)} source(s):"]
    for index, chat in enumerate(selected[:8], start=1):
        if active_source_chat is not None and str(chat) == active_source_chat:
            state = "🟢 Running now"
        elif index == 1 and active_source_chat is None:
            state = "▶️ Next to run"
        else:
            state = "⏳ Waiting in queue"
        lines.append(f"{index}. {state} · {_channel_title(user_id, chat, path)[:54]}")
    if len(selected) > 8:
        lines.append(f"… and {len(selected) - 8} more source(s).")
    return lines


def _source_text(user_id: int, path: Path, page: int = 0) -> str:
    scanned_channels = _CHANNEL_CACHE.get(user_id, [])
    channels = _source_channels(user_id, path)
    selected = _selection_for(user_id, path)
    active_source_chat = _active_queue_source(path)
    if not scanned_channels:
        return "\n".join([
            "📚 Source Queue",
            "",
            * _source_queue_lines(user_id, path, selected["sources"], active_source_chat),
            "",
            "Showing your saved queue. Tap Scan / Refresh to discover accessible channels and refresh their names.",
        ])
    if not channels:
        return "\n".join([
            "📚 Source Queue",
            "",
            * _source_queue_lines(user_id, path, selected["sources"], active_source_chat),
            "",
            "All scanned sources are blacklisted.",
        ])
    page = _source_page(channels, page)
    start = page * _PAGE_SIZE
    end = min(len(channels), start + _PAGE_SIZE)
    return "\n".join([
        "📚 Source Queue",
        "",
        f"Selected sources: {len(selected['sources'])}",
        * _source_queue_lines(user_id, path, selected["sources"], active_source_chat),
        "",
        f"Available channels: {start + 1}-{end} of {len(channels)}",
        "Select sources in the order they should run. The next source stays in the waiting list until the first one is complete.",
    ])


def _source_menu(user_id: int, path: Path, page: int = 0) -> InlineKeyboardMarkup:
    channels = _source_channels(user_id, path)
    selected = _selection_for(user_id, path)
    page = _source_page(channels, page)
    rows: list[list[tuple[str, str]]] = []
    start = page * _PAGE_SIZE
    for index, item in enumerate(channels[start:start + _PAGE_SIZE], start=start):
        chat = str(item["chat"])
        source_icon = "☑️" if chat in selected["sources"] else "☐"
        title = str(item["title"])[:32]
        rows.append([
            (f"{source_icon} {title}"[:38], f"sources:toggle:{index}"),
            ("🚫 Blacklist", f"sources:blacklist:{chat}"),
        ])
    nav: list[tuple[str, str]] = []
    if page > 0:
        nav.append(("⬅️", f"sources:page:{page - 1}"))
    if start + _PAGE_SIZE < len(channels):
        nav.append(("➡️", f"sources:page:{page + 1}"))
    if nav:
        rows.append(nav)
    blacklist = get_source_blacklist(path)
    rows += [
        [("🛠 Manage Queue", "sources:queue"), ("🔄 Scan / Refresh", "sources:scan")],
        [("🚫 Blacklist (%d)" % len(blacklist), "sources:blacklist_view")],
        [("✅ Save & Start Queue", "sources:save")],
        [("🎯 Manage Destinations", "destinations:view"), ("⬅️ Dashboard", "menu")],
    ]
    return _buttons(rows)


def _source_queue_text(user_id: int, path: Path) -> str:
    selected = _selection_for(user_id, path)
    active_source_chat = _active_queue_source(path)
    return "\n".join([
        "📋 Arrange Source Queue",
        "",
        * _source_queue_lines(user_id, path, selected["sources"], active_source_chat),
        "",
        "Use ▲ or ▼ to change the order. The dashboard shows the source running now.",
        "New Posts Only removes old work but keeps the source active for future posts.",
        "Remove Source permanently blacklists it and deletes its saved work and checkpoints.",
    ])


def _source_queue_menu(user_id: int, path: Path) -> InlineKeyboardMarkup:
    selected = _selection_for(user_id, path)
    rows: list[list[tuple[str, str]]] = []
    for index, chat in enumerate(selected["sources"]):
        label = _channel_title(user_id, chat, path)[:28]
        rows.append([(f"{index + 1}. {label}", f"sources:noopq:{index}")])
        rows.append([
            ("▲ Up", f"sources:move:{index}:up"),
            ("▼ Down", f"sources:move:{index}:down"),
        ])
        rows.append([
            ("🧹 New Posts Only", f"sources:clear:{index}"),
            ("🗑 Remove Source", f"sources:delete:{index}"),
        ])
    rows += [
        [("⬅️ Source Queue", "sources:view")],
        [("✅ Save & Start Queue", "sources:save")],
    ]
    return _buttons(rows)


def _blacklist_text(user_id: int, path: Path) -> str:
    blacklist = get_source_blacklist(path)
    if not blacklist:
        return "\n".join([
            "🚫 Source Blacklist",
            "",
            "No sources are blacklisted.",
            "",
            "Blacklisted sources are permanently skipped by the migration engine.",
        ])
    lines = ["🚫 Source Blacklist", "", f"{len(blacklist)} source(s) blocked:"]
    for i, chat in enumerate(blacklist, 1):
        title = _channel_title(user_id, chat, path)[:48]
        lines.append(f"{i}. {title}")
    lines += ["", "Tap a source below to unblacklist it and allow it to run again."]
    return "\n".join(lines)


def _blacklist_menu(user_id: int, path: Path) -> InlineKeyboardMarkup:
    blacklist = get_source_blacklist(path)
    rows: list[list[tuple[str, str]]] = []
    for chat in blacklist:
        label = _channel_title(user_id, chat, path)[:38]
        rows.append([(f"✅ Unblacklist: {label}"[:52], f"sources:unblacklist:{chat}")])
    rows.append([("⬅️ Source Queue", "sources:view")])
    return _buttons(rows)


def _destination_text(user_id: int, path: Path, page: int = 0) -> str:
    channels = _CHANNEL_CACHE.get(user_id, [])
    selected = _selection_for(user_id, path)
    available = [item for item in channels if item["can_destination"]]
    if not channels:
        return "\n".join([
            "🎯 Destinations",
            "",
            f"Selected destinations: {len(selected['destinations'])}",
            "Telegram has not been scanned. Tap Scan / Refresh to select destinations.",
        ])
    start = page * _PAGE_SIZE
    end = min(len(available), start + _PAGE_SIZE)
    lines = [
        "🎯 Destinations",
        "",
        f"Selected destinations: {len(selected['destinations'])}",
        "Only channels/groups you can post to are shown.",
        "",
    ]
    if selected["destinations"]:
        lines.append("Active: " + ", ".join(_channel_title(user_id, chat, path)[:24] for chat in selected["destinations"][:4]))
    if available:
        lines += [f"Available channels: {start + 1}-{end} of {len(available)}", "Choose destinations, then save."]
    else:
        lines += ["No channel with posting access was found. Make sure your account is an admin in the destination."]
    return "\n".join(lines)


def _destination_menu(user_id: int, path: Path, page: int = 0) -> InlineKeyboardMarkup:
    selected = _selection_for(user_id, path)
    channels = [item for item in _CHANNEL_CACHE.get(user_id, []) if item["can_destination"]]
    rows: list[list[tuple[str, str]]] = []
    start = page * _PAGE_SIZE
    for index, item in enumerate(channels[start:start + _PAGE_SIZE], start=start):
        chat = str(item["chat"])
        icon = "☑️" if chat in selected["destinations"] else "☐"
        title = str(item["title"])[:34]
        rows.append([(f"{icon} 🟢 {title}"[:48], f"destinations:toggle:{index}")])
    nav: list[tuple[str, str]] = []
    if page > 0:
        nav.append(("⬅️", f"destinations:page:{page - 1}"))
    if start + _PAGE_SIZE < len(channels):
        nav.append(("➡️", f"destinations:page:{page + 1}"))
    if nav:
        rows.append(nav)
    rows += [
        [("🔄 Scan / Refresh", "destinations:scan"), ("✅ Save Destinations", "destinations:save")],
        [("📚 Source Queue", "sources:view"), ("⬅️ Dashboard", "menu")],
    ]
    return _buttons(rows)


def _cancel_live(user_id: int) -> None:
    task = _LIVE_TASKS.pop(user_id, None)
    if task and not task.done():
        task.cancel()


async def run_admin_bot(config: AppConfig, config_path: str | Path = "config.yaml") -> None:
    if not config.telegram.bot_enabled or not config.telegram.bot_token:
        raise ValueError("Uploader bot must be enabled before starting the control panel")
    path = Path(config_path).resolve()
    logger = setup_logging(config.logging)
    app = Client(name="manager_admin", api_id=config.telegram.api_id, api_hash=config.telegram.api_hash, bot_token=config.telegram.bot_token, in_memory=True)
    async def edit(query: CallbackQuery, text: str, markup: InlineKeyboardMarkup) -> None:
        try:
            await query.message.edit_text(text, reply_markup=markup)
        except MessageNotModified:
            pass

    async def reject(message: Message | None = None, query: CallbackQuery | None = None) -> None:
        text = "Access denied. This bot is only for the user-session owner."
        if query:
            await query.answer(text, show_alert=True)
        elif message:
            await message.reply_text(text)

    async def live_loop(user_id: int, message: Message) -> None:
        last: str | None = None
        try:
            while _LIVE_TASKS.get(user_id) is asyncio.current_task():
                text = _dashboard_text(config, path)
                if text != last:
                    with suppress(MessageNotModified):
                        await message.edit_text(text, reply_markup=_menu(config))
                    last = text
                await asyncio.sleep(4)
        except (asyncio.CancelledError, Exception):
            pass
        finally:
            if _LIVE_TASKS.get(user_id) is asyncio.current_task():
                _LIVE_TASKS.pop(user_id, None)

    def start_live(user_id: int, message: Message) -> None:
        _cancel_live(user_id)
        _LIVE_TASKS[user_id] = asyncio.create_task(live_loop(user_id, message))

    @app.on_message(filters.private & filters.command("start"))
    async def start_handler(_: Client, message: Message) -> None:
        user_id = message.from_user.id if message.from_user else None
        if not _is_authorized(config, user_id):
            await reject(message=message)
            return
        assert user_id is not None
        sent = await message.reply_text(_dashboard_text(config, path), reply_markup=_menu(config))
        start_live(user_id, sent)

    @app.on_callback_query()
    async def callback_handler(_: Client, query: CallbackQuery) -> None:
        user_id = query.from_user.id if query.from_user else None
        if not _is_authorized(config, user_id):
            await reject(query=query)
            return
        assert user_id is not None
        data = query.data or ""
        if data not in {"menu", "dashboard:view", "stop:current"}:
            _cancel_live(user_id)
        if data != "finder:search":
            _FINDER_INPUTS.discard(user_id)

        if data in {"menu", "dashboard:view"}:
            await edit(query, _dashboard_text(config, path), _menu(config))
            start_live(user_id, query.message)
            await query.answer("Updated" if data == "dashboard:view" else None)
            return
        if data in {"sources:view", "channels:view"}:
            await edit(query, _source_text(user_id, path), _source_menu(user_id, path))
            await query.answer()
            return
        if data == "destinations:view":
            await edit(query, _destination_text(user_id, path), _destination_menu(user_id, path))
            await query.answer()
            return
        if data in {"sources:scan", "destinations:scan", "channels:scan"}:
            target = "destinations" if data == "destinations:scan" else "sources"
            title = "🎯 Destinations" if target == "destinations" else "📚 Source Queue"
            await query.answer("Scanning Telegram…", show_alert=False)
            await edit(query, f"{title}\n\nScanning channels and groups…", _back(f"{target}:view", "⬅️ Cancel"))
            try:
                _CHANNEL_CACHE[user_id] = await _scan_channels(config, user_id)
                _persist_scanned_source_titles(config, path, _CHANNEL_CACHE[user_id])
                _SELECTIONS.pop(user_id, None)
                if target == "destinations":
                    await edit(query, _destination_text(user_id, path), _destination_menu(user_id, path))
                else:
                    await edit(query, _source_text(user_id, path), _source_menu(user_id, path))
            except Exception as exc:
                await edit(
                    query,
                    f"{title}\n\n❌ Scan failed: {str(exc)[:500]}\n\nMake sure the user session is not locked by another process.",
                    _back(f"{target}:view", "⬅️ Back"),
                )
            return
        if data.startswith("sources:page:"):
            page = max(0, int(data.rsplit(":", 1)[1]))
            await edit(query, _source_text(user_id, path, page), _source_menu(user_id, path, page))
            await query.answer()
            return
        if data.startswith("destinations:page:"):
            page = max(0, int(data.rsplit(":", 1)[1]))
            await edit(query, _destination_text(user_id, path, page), _destination_menu(user_id, path, page))
            await query.answer()
            return
        if data.startswith("sources:toggle:"):
            index = int(data.rsplit(":", 1)[1])
            channels = _source_channels(user_id, path)
            if index >= len(channels):
                await query.answer("Channel cache expired. Scan again.", show_alert=True)
                return
            selected = _selection_for(user_id, path)
            chat = str(channels[index]["chat"])
            if chat not in selected["sources"] and chat in selected["destinations"]:
                await query.answer("The same channel cannot be both a source and a destination.", show_alert=True)
                return
            _toggle_selection(selected["sources"], chat)
            page = index // _PAGE_SIZE
            await edit(query, _source_text(user_id, path, page), _source_menu(user_id, path, page))
            await query.answer()
            return
        if data.startswith("sources:blacklist:"):
            chat = data.rsplit(":", 1)[1]
            channels = _source_channels(user_id, path)
            item = next((candidate for candidate in channels if str(candidate["chat"]) == chat), None)
            if item is None:
                await query.answer("Source is no longer available. Scan again.", show_alert=True)
                return
            source_index = channels.index(item)
            title = str(item["title"])[:40]
            try:
                blacklisted = blacklist_source(chat, path)
            except Exception as exc:
                await query.answer(
                    f"Could not blacklist source: {exc.__class__.__name__}: {exc}"[:190],
                    show_alert=True,
                )
                return
            selected = _selection_for(user_id, path)
            selected["sources"] = [
                value for value in selected["sources"] if str(value).lower() != blacklisted.lower()
            ]
            await query.answer(f"Blacklisted and removed: {title}", show_alert=True)
            page = _source_page(_source_channels(user_id, path), source_index // _PAGE_SIZE)
            with suppress(Exception):
                await edit(query, _source_text(user_id, path, page), _source_menu(user_id, path, page))
            return
        if data == "sources:blacklist_view":
            await edit(query, _blacklist_text(user_id, path), _blacklist_menu(user_id, path))
            await query.answer()
            return
        if data.startswith("sources:unblacklist:"):
            chat = data[len("sources:unblacklist:"):]
            try:
                unblacklist_source(chat, path)
            except Exception as exc:
                await query.answer(
                    f"Could not unblacklist: {exc.__class__.__name__}: {exc}"[:190],
                    show_alert=True,
                )
                return
            title = _channel_title(user_id, chat, path)[:48]
            await query.answer(f"✅ Unblacklisted: {title}", show_alert=True)
            await edit(query, _blacklist_text(user_id, path), _blacklist_menu(user_id, path))
            return
        if data.startswith("destinations:toggle:"):
            index = int(data.rsplit(":", 1)[1])
            channels = [item for item in _CHANNEL_CACHE.get(user_id, []) if item["can_destination"]]
            if index >= len(channels):
                await query.answer("Channel cache expired. Scan again.", show_alert=True)
                return
            selected = _selection_for(user_id, path)
            chat = str(channels[index]["chat"])
            if chat not in selected["destinations"] and chat in selected["sources"]:
                await query.answer("The same channel cannot be both a source and a destination.", show_alert=True)
                return
            _toggle_selection(selected["destinations"], chat)
            page = index // _PAGE_SIZE
            await edit(query, _destination_text(user_id, path, page), _destination_menu(user_id, path, page))
            await query.answer()
            return
        if data == "sources:queue":
            await edit(query, _source_queue_text(user_id, path), _source_queue_menu(user_id, path))
            await query.answer()
            return
        if data.startswith("sources:move:"):
            _, _, raw_index, direction = data.split(":", 3)
            index = int(raw_index)
            selected = _selection_for(user_id, path)["sources"]
            target = index - 1 if direction == "up" else index + 1
            if 0 <= index < len(selected) and 0 <= target < len(selected):
                selected[index], selected[target] = selected[target], selected[index]
            await edit(query, _source_queue_text(user_id, path), _source_queue_menu(user_id, path))
            await query.answer()
            return
        if data.startswith("sources:clear:"):
            index = int(data.rsplit(":", 1)[1])
            selected = _selection_for(user_id, path)["sources"]
            if not 0 <= index < len(selected):
                await query.answer("Source queue changed. Open Arrange Queue again.", show_alert=True)
                return
            chat = selected[index]
            title = _channel_title(user_id, chat, path)[:60]
            await edit(
                query,
                "\n".join([
                    "🧹 Start From New Posts?",
                    "",
                    title,
                    "",
                    "This removes existing jobs, retries, and repair records for this source.",
                    "",
                    "The source stays active. The bot will migrate only posts added after now.",
                ]),
                _buttons([
                    [("🧹 Keep New Posts Only", f"sources:clearok:{index}")],
                    [("⬅️ Cancel", "sources:queue")],
                ]),
            )
            await query.answer()
            return
        if data.startswith("sources:clearok:"):
            index = int(data.rsplit(":", 1)[1])
            selected = _selection_for(user_id, path)["sources"]
            if not 0 <= index < len(selected):
                await query.answer("Source queue changed. Nothing was cleared.", show_alert=True)
                return
            chat = selected[index]
            title = _channel_title(user_id, chat, path)[:60]
            await edit(
                query,
                "🧹 New Posts Only\n\nReading the latest source post and clearing existing work…",
                _back("sources:view", "⬅️ Source Queue"),
            )
            try:
                source_id, latest_message_id = await _latest_source_position(config, user_id, chat)
                db = _database(config)
                try:
                    cleared = MessageQueue(db, config).clear_source_history(
                        source_id,
                        latest_message_id,
                    )
                finally:
                    db.close()
            except Exception as exc:
                await query.answer("Could not clear old jobs. The source was not changed.", show_alert=True)
                await edit(
                    query,
                    "\n".join([
                        "🧹 New Posts Only",
                        "",
                        f"❌ Could not clear {title}: {exc.__class__.__name__}: {exc}"[:700],
                        "",
                        "The source remains unchanged. Try again once Telegram is available.",
                    ]),
                    _back("sources:view", "⬅️ Source Queue"),
                )
                return
            _request_mode(config, "sync")
            await query.answer(
                f"Removed {cleared['jobs']} old job(s). {title} will now migrate new posts only.",
                show_alert=True,
            )
            await edit(query, _dashboard_text(config, path), _menu(config))
            start_live(user_id, query.message)
            return
        if data.startswith("sources:delete:"):
            index = int(data.rsplit(":", 1)[1])
            selected = _selection_for(user_id, path)["sources"]
            if not 0 <= index < len(selected):
                await query.answer("Source queue changed. Open Arrange Queue again.", show_alert=True)
                return
            chat = selected[index]
            title = _channel_title(user_id, chat, path)[:60]
            await edit(
                query,
                "\n".join([
                    "🗑 Remove Source Permanently?",
                    "",
                    title,
                    "",
                    "This removes the source from the queue, adds it to the blacklist, and deletes all saved migration jobs and checkpoints.",
                    "",
                    "It will not be scanned again. The next source will start automatically.",
                ]),
                _buttons([
                    [("🗑 Remove Permanently", f"sources:purge:{index}")],
                    [("⬅️ Cancel", "sources:queue")],
                ]),
            )
            await query.answer()
            return
        if data.startswith("sources:purge:"):
            index = int(data.rsplit(":", 1)[1])
            selected = _selection_for(user_id, path)["sources"]
            if not 0 <= index < len(selected):
                await query.answer("Source queue changed. Nothing was deleted.", show_alert=True)
                return
            chat = selected[index]
            title = _channel_title(user_id, chat, path)[:60]
            try:
                db = _database(config)
                try:
                    deleted = MessageQueue(db, config).purge_source_jobs(chat)
                finally:
                    db.close()
                blacklisted = blacklist_source(chat, path)
            except Exception as exc:
                await query.answer(
                    f"Could not delete source jobs: {exc.__class__.__name__}: {exc}"[:190],
                    show_alert=True,
                )
                return
            selected["sources"] = [
                value for value in selected["sources"] if str(value).lower() != blacklisted.lower()
            ]
            _request_mode(config, "run")
            await query.answer(
                f"Removed {deleted['jobs']} saved job(s) for {title}. Starting the next source.",
                show_alert=True,
            )
            await edit(query, _dashboard_text(config, path), _menu(config))
            start_live(user_id, query.message)
            return
        if data.startswith("sources:remove:"):
            index = int(data.rsplit(":", 1)[1])
            selected = _selection_for(user_id, path)["sources"]
            if 0 <= index < len(selected):
                selected.pop(index)
            await edit(query, _source_queue_text(user_id, path), _source_queue_menu(user_id, path))
            await query.answer()
            return
        if data.startswith(("sources:noop:", "sources:noopq:", "destinations:noop:", "channels:noop:")):
            await query.answer()
            return
        if data in {"sources:save", "channels:save"}:
            selected = _selection_for(user_id, path)
            destinations = [str(item["chat"]) for item in list_destinations(path)]
            if not selected["sources"]:
                await query.answer("Select at least one source first.", show_alert=True)
                return
            if not destinations:
                await query.answer("Add a destination in the Destinations menu first.", show_alert=True)
                return
            if set(selected["sources"]) & set(destinations):
                await query.answer("The source and destination cannot be the same channel.", show_alert=True)
                return
            # Show content-type selection before starting; reset to all-on each time
            _CONTENT_TYPES.pop(user_id, None)
            await edit(query, _content_type_text(user_id), _content_type_menu(user_id))
            await query.answer()
            return
        if data.startswith("sources:ctype_toggle:"):
            type_name = data[len("sources:ctype_toggle:"):]
            if type_name in _ALL_CONTENT_TYPES:
                _toggle_content_type(user_id, type_name)
            await edit(query, _content_type_text(user_id), _content_type_menu(user_id))
            await query.answer()
            return
        if data == "sources:ctype_confirm":
            selected = _selection_for(user_id, path)
            excluded = _content_types_for(user_id)  # set of excluded types
            # save_content_filter clears the file when excluded is empty
            save_content_filter(path, excluded)
            # Only update sources list when the user came through the Source Queue
            # selection flow (session has sources). When initiated from ▶️ Start on
            # the dashboard the sources are already saved in config — don't overwrite.
            if selected["sources"]:
                try:
                    set_sources(selected["sources"], path)
                    _persist_scanned_source_titles(config, path, _CHANNEL_CACHE.get(user_id, []))
                except Exception:
                    pass
            _request_mode(config, "run")
            if excluded:
                skipped = ", ".join(_CONTENT_TYPE_LABELS.get(t, t) for t in _ALL_CONTENT_TYPES if t in excluded)
                filter_note = f" (skipping {skipped})"
            else:
                filter_note = ""
            await query.answer(f"Migration started{filter_note}.", show_alert=True)
            await edit(query, _dashboard_text(config, path), _menu(config))
            start_live(user_id, query.message)
            return
        if data == "destinations:save":
            selected = _selection_for(user_id, path)
            sources = [str(item["chat"]) for item in get_sources(path)]
            if not selected["destinations"]:
                await query.answer("Select at least one destination first.", show_alert=True)
                return
            if set(sources) & set(selected["destinations"]):
                await query.answer("The source and destination cannot be the same channel.", show_alert=True)
                return
            set_destinations(selected["destinations"], path)
            await query.answer(
                "Destinations saved. The existing queue will not be rerun automatically.",
                show_alert=True,
            )
            await edit(query, _destination_text(user_id, path), _destination_menu(user_id, path))
            return

        pages: dict[str, tuple[Callable[[AppConfig], str], InlineKeyboardMarkup]] = {
            "smart:menu": (lambda _: "🧠 Smart Center\n\nDiagnostics, recovery, and media intelligence.", _smart_menu()),
            "settings:view": (lambda _: _settings_text(path), _back()),
            "issues:view": (_issues_text, _buttons([[("🔄 Refresh", "issues:view"), ("🛠 Repair", "advanced:repair")], [("⬅️ Smart Center", "smart:menu")]])),
            "capacity:view": (_capacity_text, _back("smart:menu", "⬅️ Smart Center")),
            "advanced:menu": (_advanced_text, _advanced_menu()),
            "advanced:health": (_health_text, _buttons([[("▶️ Run Check", "health:run"), ("🔄 Refresh", "advanced:health")], [("⬅️ Smart Center", "smart:menu")]])),
            "advanced:repair": (_repair_text, _buttons([[("🔄 Retry safe jobs", "repair:retry:all")], [("🧭 Review uploads", "repair:review")], [("📋 Details", "repair:details")], [("⬅️ Smart Center", "smart:menu")]])),
            "checkpoint:view": (_checkpoint_text, _buttons([[("♻️ Reset + Full Scan", "checkpoint:reset:confirm")], [("⬅️ Recovery Tools", "advanced:menu")]])),
        }
        if data == "finder:view":
            await edit(query, _finder_text(config), _finder_menu())
            await query.answer()
            return
        if data == "finder:search":
            _FINDER_INPUTS.add(user_id)
            await edit(
                query,
                "🔍 Original Media Finder\n\nSend a t.me link or a message number to find the original media in the index.",
                _back("finder:view", "⬅️ Finder"),
            )
            await query.answer()
            return
        if data in {"finder:index", "duplicates:index"}:
            await query.answer("Indexing up to 500 media queue items…")
            db = _database(config)
            try:
                indexed = index_existing_queue(db, limit=500)
            finally:
                db.close()
            if data == "finder:index":
                await edit(query, _finder_text(config, indexed_now=indexed), _finder_menu())
            else:
                await edit(query, _duplicates_text(config, indexed_now=indexed), _duplicates_menu())
            return
        if data == "duplicates:view":
            await edit(query, _duplicates_text(config), _duplicates_menu())
            await query.answer()
            return
        if data in {
            "duplicates:cleanup:preview",
            "duplicates:destination:scan",
            "duplicates:destination:content-scan",
        }:
            if is_active_phase(read_status(config).get("phase")):
                await query.answer("Stop the migration before scanning destination history.", show_alert=True)
                return
            content_scan = data == "duplicates:destination:content-scan"
            plan = request_destination_duplicate_scan(
                config,
                scan_mode="content" if content_scan else "fingerprint",
            )
            request_run_mode(
                config,
                "duplicate_cleanup_content_scan" if content_scan else "duplicate_cleanup_scan",
            )
            await edit(
                query,
                _destination_duplicate_cleanup_text(plan),
                _destination_duplicate_cleanup_menu(plan),
            )
            await query.answer(
                "Deep content scan queued. It checks matching file bytes." if content_scan else "Destination scan queued. Tap Refresh shortly.",
                show_alert=True,
            )
            return
        if data == "duplicates:destination:preview":
            plan = load_destination_duplicate_plan(config)
            await edit(
                query,
                _destination_duplicate_cleanup_text(plan),
                _destination_duplicate_cleanup_menu(plan),
            )
            await query.answer()
            return
        if data in {"duplicates:cleanup:confirm", "duplicates:destination:confirm"}:
            if is_active_phase(read_status(config).get("phase")):
                await query.answer("Stop the migration before deleting duplicate copies.", show_alert=True)
                return
            plan = request_destination_duplicate_cleanup(config)
            if plan is None:
                current_plan = load_destination_duplicate_plan(config)
                await edit(
                    query,
                    _destination_duplicate_cleanup_text(current_plan),
                    _destination_duplicate_cleanup_menu(current_plan),
                )
                await query.answer("Run and refresh a destination scan first.", show_alert=True)
                return
            request_run_mode(config, "duplicate_cleanup_delete")
            await edit(
                query,
                _destination_duplicate_cleanup_text(plan),
                _destination_duplicate_cleanup_menu(plan),
            )
            await query.answer(
                "Cleanup queued. The manager session will delete the reviewed IDs.",
                show_alert=True,
            )
            return
        if data in pages:
            fn, markup = pages[data]
            await edit(query, fn(config), markup)
            await query.answer()
            return
        if data == "repair:review":
            await edit(query, *_review_upload_page(config))
            await query.answer()
            return
        if data == "repair:details":
            await edit(query, _repair_text(config, True), _back("advanced:repair", "⬅️ AI Error Doctor"))
            await query.answer()
            return
        if data == "health:run":
            _request_mode(config, "health")
            await query.answer("Pre-flight check scheduled.", show_alert=True)
            return
        if data.startswith("repair:retry:"):
            if is_active_phase(read_status(config).get("phase")):
                await query.answer("Migration is still active.", show_alert=True)
                return
            category = data.rsplit(":", 1)[1]
            db = _database(config)
            try:
                revived = requeue_retryable_repairs(db) if category == "all" else requeue_repair_category(db, category)
            finally:
                db.close()
            if revived:
                _request_mode(config, "process")
            await query.answer(
                f"{revived} safe job(s) returned to pending. Unknown upload results remain held.",
                show_alert=True,
            )
            return
        if data.startswith(("repair:confirm:", "repair:missing:")):
            if is_active_phase(read_status(config).get("phase")):
                await query.answer("Migration is still active.", show_alert=True)
                return
            try:
                job_id = int(data.rsplit(":", 1)[1])
            except (TypeError, ValueError):
                await query.answer("This review action has expired.", show_alert=True)
                return
            delivered = data.startswith("repair:confirm:")
            db = _database(config)
            try:
                resolved = resolve_uncertain_upload(db, job_id, delivered=delivered)
            finally:
                db.close()
            if not resolved:
                await query.answer("This job is no longer waiting for review.", show_alert=True)
                await edit(query, *_review_upload_page(config))
                return
            _request_mode(config, "process")
            await query.answer(
                "Marked as delivered. The queue will continue." if delivered else "Returned to pending. The queue will retry it.",
                show_alert=True,
            )
            await edit(query, *_review_upload_page(config))
            return
        if data == "checkpoint:reset:confirm":
            await edit(query, "♻️ Reset all checkpoints?\n\nThe existing queue will not be deleted.", _buttons([[("✅ Reset", "checkpoint:reset:all")], [("❌ Cancel", "checkpoint:view")]]))
            await query.answer()
            return
        if data == "checkpoint:reset:all":
            db = _database(config)
            try:
                removed = reset_all_checkpoints(db)
            finally:
                db.close()
            _request_mode(config, "run")
            await query.answer(f"{removed} checkpoint(s) removed.", show_alert=True)
            return
        if data == "advanced:force_recover":
            db = _database(config)
            try:
                recovery = MessageQueue(db, config).recover_in_progress()
            finally:
                db.close()
            if recovery.total:
                msg = (
                    f"Recovered {recovery.requeued_downloads} stuck download(s) → pending.\n"
                    f"Held {recovery.held_uploads} stuck upload(s) as failed."
                )
            else:
                msg = "No stuck jobs found. All active jobs have valid worker leases."
            await query.answer(msg[:200], show_alert=True)
            if recovery.total:
                _request_mode(config, "process")
            await edit(query, _dashboard_text(config, path), _menu(config))
            start_live(user_id, query.message)
            return
        if data == "advanced:delete_active_migration":
            status = read_status(config)
            active_source_chat = str(status.get("source_chat") or "")
            if not active_source_chat:
                await query.answer("No active source found in status.", show_alert=True)
                return
            source_title = _channel_title(user_id, active_source_chat, path)[:60]
            await edit(
                query,
                "\n".join([
                    "🗑 Delete Active Migration?",
                    "",
                    source_title,
                    "",
                    "Permanently removes all jobs and checkpoints for this source "
                    "and blacklists it. The next source starts automatically.",
                ]),
                _buttons([
                    [("🗑 Delete Permanently", "advanced:delete_active_confirm")],
                    [("⬅️ Cancel", "menu")],
                ]),
            )
            await query.answer()
            return
        if data == "advanced:delete_active_confirm":
            status = read_status(config)
            active_source_chat = str(status.get("source_chat") or "")
            if not active_source_chat:
                await query.answer("No active source found. Nothing deleted.", show_alert=True)
                return
            source_title = _channel_title(user_id, active_source_chat, path)[:60]
            try:
                db = _database(config)
                try:
                    deleted = MessageQueue(db, config).purge_source_jobs(active_source_chat)
                finally:
                    db.close()
                blacklisted = blacklist_source(active_source_chat, path)
            except Exception as exc:
                await query.answer(f"Delete failed: {exc.__class__.__name__}: {exc}"[:190], show_alert=True)
                return
            if user_id in _SELECTIONS:
                _SELECTIONS[user_id]["sources"] = [
                    v for v in _SELECTIONS[user_id].get("sources", [])
                    if str(v).lower() != blacklisted.lower()
                ]
            _request_mode(config, "run")
            await query.answer(
                f"Deleted {deleted['jobs']} job(s) for {source_title}. Starting next source.",
                show_alert=True,
            )
            await edit(query, _dashboard_text(config, path), _menu(config))
            start_live(user_id, query.message)
            return
        if data == "run:now":
            sources, destinations = get_sources(path), list_destinations(path)
            if not sources:
                await query.answer("Add sources in Source Queue (⋯ More) first.", show_alert=True)
                return
            if not destinations:
                await query.answer("Add a destination in Destinations (⋯ More) first.", show_alert=True)
                return
            if {item["chat"] for item in sources} & {item["chat"] for item in destinations}:
                await query.answer("A source and destination are the same channel — fix in ⋯ More.", show_alert=True)
                return
            # Always show content-type selection; preload with any existing filter
            existing = load_content_filter(path)
            if existing is not None:
                _CONTENT_TYPES[user_id] = set(existing)
            else:
                _CONTENT_TYPES.pop(user_id, None)
            await edit(query, _content_type_text(user_id), _content_type_menu(user_id))
            await query.answer()
            return
        if data == "menu:more":
            await edit(query, "⋯ More options", _more_menu())
            await query.answer()
            return
        if data in {"advanced:resume", "advanced:full", "advanced:sync"}:
            mode = {"advanced:resume": "process", "advanced:full": "run", "advanced:sync": "sync"}[data]
            _request_mode(config, mode)
            await query.answer("Command scheduled.", show_alert=True)
            return
        if data == "stop:current":
            status = read_status(config)
            if is_stoppable_phase(status.get("phase")) or has_pending_run_request(config):
                request_pause(config)
                request_stop(config)
                write_status(
                    config,
                    "stopping",
                    message="Stop request sent. Migration will stay paused until Start is pressed.",
                    paused=True,
                )
                await query.answer("Stopped and paused. Press Start to resume.", show_alert=True)
            else:
                await query.answer("No active migration.", show_alert=True)
            await edit(query, _dashboard_text(config, path), _menu(config))
            start_live(user_id, query.message)
            return
        await query.answer()

    @app.on_message(filters.private, group=1)
    async def input_handler(_: Client, message: Message) -> None:
        user_id = message.from_user.id if message.from_user else None
        if not _is_authorized(config, user_id):
            await reject(message=message)
            return
        text = (message.text or "").strip()
        if user_id is not None and user_id in _FINDER_INPUTS:
            _FINDER_INPUTS.discard(user_id)
            db = _database(config)
            try:
                match = find_by_reference(db, text)
            finally:
                db.close()
            await message.reply_text(_finder_result_text(text, match), reply_markup=_finder_menu())
            return
        if not text.startswith("/start"):
            sent = await message.reply_text(_dashboard_text(config, path), reply_markup=_menu(config))
            if user_id is not None:
                start_live(user_id, sent)

    await start_client_with_floodwait(
        app,
        label="admin bot",
        logger=logger,
    )
    try:
        await idle()
    finally:
        for user_id in list(_LIVE_TASKS):
            _cancel_live(user_id)
        for task in list(_LIVE_TASKS.values()):
            with suppress(asyncio.CancelledError):
                await task
        await app.stop()
