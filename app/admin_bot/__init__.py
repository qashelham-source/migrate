from __future__ import annotations

import asyncio
import os
import sqlite3
import tempfile
import time
from contextlib import suppress
from pathlib import Path
from typing import Any, Callable

from pyrogram import Client, filters, idle
from pyrogram.errors import MessageNotModified
from pyrogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

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
)
from app.config import AppConfig
from app.control import clear_stop, is_active_phase, read_status, request_stop, write_status
from app.dashboard_v2 import dashboard_snapshot, format_bytes, format_eta, issue_center
from app.db import Database
from app.destination_manager import get_sources, list_destinations, set_destinations, set_sources
from app.media_finder import duplicate_groups, find_by_reference, index_existing_queue, media_finder_stats
from app.queue import MessageQueue
from app.telegram_client import load_accounts

_LIVE_TASKS: dict[int, asyncio.Task[None]] = {}
_CHANNEL_CACHE: dict[int, list[dict[str, Any]]] = {}
_SELECTIONS: dict[int, dict[str, list[str]]] = {}
_FINDER_INPUTS: set[int] = set()
_PAGE_SIZE = 8

CATEGORY_LABELS = {
    "media_empty": "MEDIA_EMPTY",
    "peer_id": "Peer ID",
    "permission": "Permission",
    "temporary": "Temporary/Network",
    "source_missing": "Source Missing",
    "unsupported": "Unsupported",
    "other": "Other",
}


def _buttons(rows: list[list[tuple[str, str]]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(label, callback_data=data) for label, data in row] for row in rows]
    )


def _menu() -> InlineKeyboardMarkup:
    return _buttons(
        [
            [("📚 Source Queue", "sources:view"), ("🎯 Destinations", "destinations:view")],
            [("🧠 Smart Center", "smart:menu"), ("⚙️ Settings", "settings:view")],
            [("▶️ Start Queue", "run:now"), ("⏹ Stop", "stop:current")],
            [("🔄 Refresh", "dashboard:view")],
        ]
    )


def _back(target: str = "menu", label: str = "⬅️ Dashboard") -> InlineKeyboardMarkup:
    return _buttons([[(label, target)]])


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
            [("⬅️ Smart Center", "smart:menu")],
        ]
    )


def _authorized_ids(config: AppConfig) -> set[int]:
    result: set[int] = set()
    for account in load_accounts(config).values():
        try:
            result.add(int(account.get("id")))
        except (TypeError, ValueError, AttributeError):
            pass
    for value in os.getenv("ADMIN_USER_ID", "").split(","):
        if value.strip().isdigit():
            result.add(int(value.strip()))
    return result


def _is_authorized(config: AppConfig, user_id: int | None) -> bool:
    return user_id is not None and user_id in _authorized_ids(config)


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
    clear_stop(config)
    request_run_mode(config, mode)


def _dashboard_text(config: AppConfig) -> str:
    db = _database(config)
    try:
        data = dashboard_snapshot(db, config.queue.db_path)
    finally:
        db.close()
    status = read_status(config)
    q, s, d, v, t, storage = (
        data["queue"], data["sources"], data["destinations"], data["verification"], data["telemetry"], data["storage"]
    )
    source_progress = data["source_progress"]
    phase = str(status.get("phase") or "idle").lower()
    active = q["downloading"] + q["uploading"]
    issue_total = q["failed"] + q["skipped"] + d["paused"] + v["failed"]
    state = "🟢 Healthy" if issue_total == 0 else "🟠 Perlu perhatian"
    phase_labels = {
        "idle": "Idle", "watching": "Live watcher", "scanning": "Scanning", "processing": "Processing",
        "downloading": "Downloading", "uploading": "Uploading", "stopping": "Stopping", "stopped": "Stopped",
        "waiting": "Setup diperlukan", "queued": "Queue menunggu mula",
        "waiting_retry": "Retry automatik", "blocked": "Queue tersekat",
        "source_complete": "Source lengkap", "scan_complete": "Scan selesai",
        "batch_pause": "Rehat antara batch",
        "error": "Error", "health_check": "Pre-flight check", "health_complete": "Health check complete",
    }
    current = phase_labels.get(phase, phase.replace("_", " ").title())
    lines = [
        "🏠 Migration Dashboard",
        "",
        f"{state}",
        f"Status: {current}",
        "",
        f"Pending   {q['pending']}",
        f"Running   {active}",
        f"Completed {q['copied']}",
        f"Failed    {q['failed'] + q['skipped']}",
        "",
        f"Speed: {format_bytes(t['speed_bps'])}/s",
        f"ETA: {format_eta(t['eta_seconds'])}",
        f"Sources: {s['total']} · Destinations: {d['total']}",
    ]
    if status.get("source_index") is not None and status.get("source_total") is not None:
        try:
            source_index = int(status["source_index"])
            source_total = int(status["source_total"])
            source_name = str(status.get("source") or status.get("source_chat") or "-")[:52]
            lines += ["", f"Source Queue: {source_index}/{source_total} · {source_name}"]
            if phase == "waiting_retry":
                lines.append("⏳ Source ini retry sendiri; source lain kekal menunggu.")
            elif phase == "blocked":
                lines.append("⛔ Source ini perlukan tindakan; source lain kekal menunggu.")
            elif phase == "queued":
                lines.append("⏸ Tekan Start Queue untuk meneruskan kerja tertangguh.")
        except (TypeError, ValueError):
            pass
    if source_progress:
        lines += ["", "Source Migration:"]
        for item in source_progress[:3]:
            title = str(item["title"])[:44]
            eligible = int(item["eligible_items"])
            if eligible <= 0:
                lines.append(f"• {title}: tiada item ikut filter")
                continue
            copied = int(item["copied_items"])
            remaining = int(item["remaining_items"])
            percent = int(item["percent"])
            lines.append(f"• {title}: {percent}% siap — {copied}/{eligible} post/album")
            lines.append(
                f"  Baki: {remaining} ({100 - percent}%) · Jalan: {item['active_items']} · Isu: {item['blocked_items']}"
            )
            if item["filtered_items"]:
                lines.append(f"  Tidak ikut filter: {item['filtered_items']}")
    if status.get("job_id") is not None:
        lines += ["", f"Current job: #{status['job_id']}", f"Media: {status.get('media_type') or '-'}"]
    if status.get("current") is not None and status.get("total") is not None:
        try:
            current_count = max(0, int(status["current"]))
            total_count = max(0, int(status["total"]))
            label = "Scan source" if phase == "scanning" else "Progress"
            if total_count:
                lines.append(f"{label}: {current_count}/{total_count} ({round(current_count / total_count * 100)}%)")
            else:
                lines.append(f"{label}: {current_count}/{total_count}")
        except (TypeError, ValueError):
            lines.append(f"Progress: {status['current']}/{status['total']}")
    error = status.get("last_error") or status.get("error")
    if error:
        lines += ["", f"Last error: {str(error).replace(chr(10), ' ')[:300]}"]
    if storage.percent_used >= 80:
        level = "🚨 Critical" if storage.percent_used >= 90 else "⚠️ Low working storage"
        lines += ["", level, f"Free: {format_bytes(storage.free_bytes)}"]
    lines += ["", "Dashboard dikemas kini secara automatik."]
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
            "Source diurus dalam Source Queue. Destination diurus dalam menu Destinations.",
            "Tetapan worker, retry, cache dan log kekal dalam konfigurasi sistem.",
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
        return "\n".join(lines + ["✅ Tiada isu aktif."])
    for item in rows:
        if item["kind"] == "destination":
            lines += [f"⏸ Destination {item['id']} paused", str(item["error"])[:180], ""]
        else:
            lines += [f"❌ Job #{item['id']} · {item['status']}", str(item["error"]).replace("\n", " ")[:180], ""]
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
        lines += ["", f"✅ {indexed_now} media queue item baru diindex."]
    lines += [
        "",
        "Index Queue membaca metadata sedia ada sahaja—tiada media dimuat turun atau diubah.",
        "Find Link menerima t.me link atau nombor message yang sudah diindex.",
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
        lines += ["", f"✅ {indexed_now} media queue item baru diindex."]
    if not groups:
        lines += ["", "✅ Tiada duplicate dijumpai dalam media yang telah diindex."]
        return "\n".join(lines)
    lines += ["", f"Top duplicate groups: {len(groups)}"]
    for index, group in enumerate(groups, start=1):
        lines += [
            "",
            f"{index}. {int(group.get('copies') or 0)} salinan",
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
            [("⬅️ Smart Center", "smart:menu")],
        ]
    )


def _finder_result_text(reference: str, match: dict[str, Any] | None) -> str:
    if match is None:
        return "\n".join(
            [
                "🔍 Original Media Finder",
                "",
                "❌ Media asal tidak dijumpai dalam index.",
                "Tekan Index Queue dahulu, kemudian cuba t.me link atau message ID semula.",
            ]
        )
    size = int(match.get("file_size") or 0)
    lines = [
        "🔍 Original Media Finder",
        "",
        "✅ Media asal dijumpai.",
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
        return "🩺 Pre-flight Health Check\n\nBelum ada laporan. Tekan Run Check."
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
            return "\n".join(lines or ["Tiada isu."])[:3900]
        summary = repair_summary(db)
    finally:
        db.close()
    lines = ["🤖 AI Error Doctor", "", f"Jobs requiring attention: {sum(summary.values())}", ""]
    lines += [f"• {CATEGORY_LABELS[key]}: {summary.get(key, 0)}" for key in REPAIR_CATEGORIES]
    return "\n".join(lines)


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
    return "\n".join(lines + ([] if rows else ["Belum ada checkpoint."]))[:3900]


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
    for key in ("sources", "destinations"):
        selection[key] = _ordered_chats(selection.get(key, []))
    return selection


def _toggle_selection(selected: list[str], chat: str) -> bool:
    if chat in selected:
        selected.remove(chat)
        return False
    selected.append(chat)
    return True


def _snapshot_session_database(source: Path, destination: Path) -> None:
    """Create an isolated SQLite snapshot without locking the live Telegram session."""
    if not source.is_file():
        raise RuntimeError("Session Telegram tidak ditemui. Jalankan login dahulu.")

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

    raise RuntimeError("Session Telegram masih sibuk. Cuba Scan / Refresh lagi.") from last_error


async def _scan_channels(config: AppConfig, user_id: int) -> list[dict[str, Any]]:
    sessions_dir = config.telegram.sessions_dir
    sessions_dir.mkdir(parents=True, exist_ok=True)
    source_session = sessions_dir / f"{config.telegram.user_session}.session"

    with tempfile.TemporaryDirectory(prefix="channel-scan-", dir=str(sessions_dir)) as temp_dir:
        workdir = Path(temp_dir)
        session_name = f"channel-scan-{user_id}"
        await asyncio.to_thread(
            _snapshot_session_database,
            source_session,
            workdir / f"{session_name}.session",
        )
        client = Client(
            name=session_name,
            api_id=config.telegram.api_id,
            api_hash=config.telegram.api_hash,
            workdir=str(workdir),
            no_updates=True,
            max_concurrent_transmissions=1,
        )
        channels: list[dict[str, Any]] = []
        started = False
        try:
            await client.start()
            started = True
            async for dialog in client.get_dialogs():
                chat = dialog.chat
                kind = str(getattr(chat, "type", "")).lower()
                if not any(value in kind for value in ("channel", "group", "supergroup")):
                    continue
                chat_id = str(chat.id)
                title = str(chat.title or chat.username or chat.id)
                can_destination = False
                access = "🟢 Ready"
                try:
                    member = await client.get_chat_member(chat.id, "me")
                    status = str(getattr(member, "status", "")).lower()
                    can_destination = "owner" in status or "administrator" in status
                except Exception:
                    access = "🟡 Limited"
                channels.append({
                    "chat": chat_id,
                    "title": title,
                    "kind": "Channel" if "channel" in kind else "Group",
                    "can_source": True,
                    "can_destination": can_destination,
                    "access": access,
                })
        finally:
            if started:
                await client.stop()

    channels.sort(key=lambda item: (item["title"].lower(), item["chat"]))
    return channels


def _channel_title(user_id: int, chat: str) -> str:
    for item in _CHANNEL_CACHE.get(user_id, []):
        if str(item["chat"]) == str(chat):
            return str(item["title"])
    return str(chat)


def _source_queue_lines(user_id: int, selected: list[str]) -> list[str]:
    if not selected:
        return ["Belum ada source dalam queue."]
    lines = ["Turutan queue:"]
    for index, chat in enumerate(selected[:8], start=1):
        state = "🟢 Semasa" if index == 1 else "⏳ Waiting"
        lines.append(f"{index}. {state} · {_channel_title(user_id, chat)[:54]}")
    if len(selected) > 8:
        lines.append(f"… dan {len(selected) - 8} source lagi.")
    return lines


def _source_text(user_id: int, path: Path, page: int = 0) -> str:
    channels = _CHANNEL_CACHE.get(user_id, [])
    selected = _selection_for(user_id, path)
    if not channels:
        return "\n".join([
            "📚 Source Queue",
            "",
            * _source_queue_lines(user_id, selected["sources"]),
            "",
            "Belum scan Telegram. Tekan Scan / Refresh untuk tambah source.",
        ])
    start = page * _PAGE_SIZE
    end = min(len(channels), start + _PAGE_SIZE)
    return "\n".join([
        "📚 Source Queue",
        "",
        f"Source dipilih: {len(selected['sources'])}",
        * _source_queue_lines(user_id, selected["sources"]),
        "",
        f"Channel tersedia: {start + 1}-{end} daripada {len(channels)}",
        "Pilih source mengikut turutan kerja. Source seterusnya kekal waiting sehingga yang pertama siap.",
    ])


def _source_menu(user_id: int, path: Path, page: int = 0) -> InlineKeyboardMarkup:
    channels = _CHANNEL_CACHE.get(user_id, [])
    selected = _selection_for(user_id, path)
    rows: list[list[tuple[str, str]]] = []
    start = page * _PAGE_SIZE
    for index, item in enumerate(channels[start:start + _PAGE_SIZE], start=start):
        chat = str(item["chat"])
        source_icon = "☑️" if chat in selected["sources"] else "☐"
        title = str(item["title"])[:34]
        rows.append([(f"{source_icon} {item['access']} {title}"[:48], f"sources:toggle:{index}")])
    nav: list[tuple[str, str]] = []
    if page > 0:
        nav.append(("⬅️", f"sources:page:{page - 1}"))
    if start + _PAGE_SIZE < len(channels):
        nav.append(("➡️", f"sources:page:{page + 1}"))
    if nav:
        rows.append(nav)
    rows += [
        [("📋 Susun Queue", "sources:queue"), ("🔄 Scan / Refresh", "sources:scan")],
        [("✅ Simpan + Mula Queue", "sources:save")],
        [("🎯 Urus Destinations", "destinations:view"), ("⬅️ Dashboard", "menu")],
    ]
    return _buttons(rows)


def _source_queue_text(user_id: int, path: Path) -> str:
    selected = _selection_for(user_id, path)
    return "\n".join([
        "📋 Susun Source Queue",
        "",
        * _source_queue_lines(user_id, selected["sources"]),
        "",
        "Gunakan ▲ atau ▼ untuk ubah turutan. Hanya source pertama akan berjalan.",
    ])


def _source_queue_menu(user_id: int, path: Path) -> InlineKeyboardMarkup:
    selected = _selection_for(user_id, path)
    rows: list[list[tuple[str, str]]] = []
    for index, chat in enumerate(selected["sources"]):
        label = _channel_title(user_id, chat)[:28]
        rows.append([
            (f"{index + 1}. {label}", f"sources:noopq:{index}"),
            ("▲", f"sources:move:{index}:up"),
            ("▼", f"sources:move:{index}:down"),
            ("✕", f"sources:remove:{index}"),
        ])
    rows += [
        [("⬅️ Source Queue", "sources:view")],
        [("✅ Simpan + Mula Queue", "sources:save")],
    ]
    return _buttons(rows)


def _destination_text(user_id: int, path: Path, page: int = 0) -> str:
    channels = _CHANNEL_CACHE.get(user_id, [])
    selected = _selection_for(user_id, path)
    available = [item for item in channels if item["can_destination"]]
    if not channels:
        return "\n".join([
            "🎯 Destinations",
            "",
            f"Destination dipilih: {len(selected['destinations'])}",
            "Belum scan Telegram. Tekan Scan / Refresh untuk pilih destination.",
        ])
    start = page * _PAGE_SIZE
    end = min(len(available), start + _PAGE_SIZE)
    lines = [
        "🎯 Destinations",
        "",
        f"Destination dipilih: {len(selected['destinations'])}",
        "Hanya channel/group yang anda boleh post dipaparkan.",
        "",
    ]
    if selected["destinations"]:
        lines.append("Aktif: " + ", ".join(_channel_title(user_id, chat)[:24] for chat in selected["destinations"][:4]))
    if available:
        lines += [f"Channel tersedia: {start + 1}-{end} daripada {len(available)}", "Pilih destination, kemudian simpan."]
    else:
        lines += ["Tiada channel dengan akses post ditemui. Pastikan akaun anda admin di destination."]
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
        [("🔄 Scan / Refresh", "destinations:scan"), ("✅ Simpan Destination", "destinations:save")],
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
    app = Client(name="manager_admin", api_id=config.telegram.api_id, api_hash=config.telegram.api_hash, bot_token=config.telegram.bot_token, in_memory=True)

    async def edit(query: CallbackQuery, text: str, markup: InlineKeyboardMarkup) -> None:
        try:
            await query.message.edit_text(text, reply_markup=markup)
        except MessageNotModified:
            pass

    async def reject(message: Message | None = None, query: CallbackQuery | None = None) -> None:
        text = "Akses ditolak. Bot ini hanya untuk pemilik user session."
        if query:
            await query.answer(text, show_alert=True)
        elif message:
            await message.reply_text(text)

    async def live_loop(user_id: int, message: Message) -> None:
        last: str | None = None
        try:
            while _LIVE_TASKS.get(user_id) is asyncio.current_task():
                text = _dashboard_text(config)
                if text != last:
                    with suppress(MessageNotModified):
                        await message.edit_text(text, reply_markup=_menu())
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
        sent = await message.reply_text(_dashboard_text(config), reply_markup=_menu())
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
            await edit(query, _dashboard_text(config), _menu())
            start_live(user_id, query.message)
            await query.answer("Dikemas kini" if data == "dashboard:view" else None)
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
            await edit(query, f"{title}\n\nScanning channel dan group…", _back(f"{target}:view", "⬅️ Cancel"))
            try:
                _CHANNEL_CACHE[user_id] = await _scan_channels(config, user_id)
                _SELECTIONS.pop(user_id, None)
                if target == "destinations":
                    await edit(query, _destination_text(user_id, path), _destination_menu(user_id, path))
                else:
                    await edit(query, _source_text(user_id, path), _source_menu(user_id, path))
            except Exception as exc:
                await edit(
                    query,
                    f"{title}\n\n❌ Scan gagal: {str(exc)[:500]}\n\nPastikan user session tidak sedang dikunci proses lain.",
                    _back(f"{target}:view", "⬅️ Kembali"),
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
            channels = _CHANNEL_CACHE.get(user_id, [])
            if index >= len(channels):
                await query.answer("Channel cache expired. Scan semula.", show_alert=True)
                return
            selected = _selection_for(user_id, path)
            chat = str(channels[index]["chat"])
            if chat not in selected["sources"] and chat in selected["destinations"]:
                await query.answer("Channel yang sama tak boleh jadi source dan destination.", show_alert=True)
                return
            _toggle_selection(selected["sources"], chat)
            page = index // _PAGE_SIZE
            await edit(query, _source_text(user_id, path, page), _source_menu(user_id, path, page))
            await query.answer()
            return
        if data.startswith("destinations:toggle:"):
            index = int(data.rsplit(":", 1)[1])
            channels = [item for item in _CHANNEL_CACHE.get(user_id, []) if item["can_destination"]]
            if index >= len(channels):
                await query.answer("Channel cache expired. Scan semula.", show_alert=True)
                return
            selected = _selection_for(user_id, path)
            chat = str(channels[index]["chat"])
            if chat not in selected["destinations"] and chat in selected["sources"]:
                await query.answer("Channel yang sama tak boleh jadi source dan destination.", show_alert=True)
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
                await query.answer("Pilih sekurang-kurangnya satu source dahulu.", show_alert=True)
                return
            if not destinations:
                await query.answer("Tambah destination dalam menu Destinations dahulu.", show_alert=True)
                return
            if set(selected["sources"]) & set(destinations):
                await query.answer("Source dan destination tak boleh channel yang sama.", show_alert=True)
                return
            set_sources(selected["sources"], path)
            _request_mode(config, "run")
            await query.answer("Source queue disimpan dan dimulakan mengikut turutan.", show_alert=True)
            await edit(query, _dashboard_text(config), _menu())
            start_live(user_id, query.message)
            return
        if data == "destinations:save":
            selected = _selection_for(user_id, path)
            sources = [str(item["chat"]) for item in get_sources(path)]
            if not selected["destinations"]:
                await query.answer("Pilih sekurang-kurangnya satu destination dahulu.", show_alert=True)
                return
            if set(sources) & set(selected["destinations"]):
                await query.answer("Source dan destination tak boleh channel yang sama.", show_alert=True)
                return
            set_destinations(selected["destinations"], path)
            await query.answer(
                "Destination disimpan. Queue sedia ada tidak diulang secara automatik.",
                show_alert=True,
            )
            await edit(query, _destination_text(user_id, path), _destination_menu(user_id, path))
            return

        pages: dict[str, tuple[Callable[[AppConfig], str], InlineKeyboardMarkup]] = {
            "smart:menu": (lambda _: "🧠 Smart Center\n\nDiagnostics, recovery dan media intelligence.", _smart_menu()),
            "settings:view": (lambda _: _settings_text(path), _back()),
            "issues:view": (_issues_text, _buttons([[("🔄 Refresh", "issues:view"), ("🛠 Repair", "advanced:repair")], [("⬅️ Smart Center", "smart:menu")]])),
            "capacity:view": (_capacity_text, _back("smart:menu", "⬅️ Smart Center")),
            "advanced:menu": (_advanced_text, _advanced_menu()),
            "advanced:health": (_health_text, _buttons([[("▶️ Run Check", "health:run"), ("🔄 Refresh", "advanced:health")], [("⬅️ Smart Center", "smart:menu")]])),
            "advanced:repair": (_repair_text, _buttons([[("🔄 Retry temporary", "repair:retry:all")], [("📋 Details", "repair:details")], [("⬅️ Smart Center", "smart:menu")]])),
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
                "🔍 Original Media Finder\n\nHantar t.me link atau nombor message untuk cari media asal dalam index.",
                _back("finder:view", "⬅️ Finder"),
            )
            await query.answer()
            return
        if data in {"finder:index", "duplicates:index"}:
            await query.answer("Mengindex hingga 500 media queue item…")
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
        if data in pages:
            fn, markup = pages[data]
            await edit(query, fn(config), markup)
            await query.answer()
            return
        if data == "repair:details":
            await edit(query, _repair_text(config, True), _back("advanced:repair", "⬅️ AI Error Doctor"))
            await query.answer()
            return
        if data == "health:run":
            _request_mode(config, "health")
            await query.answer("Pre-flight check dijadualkan.", show_alert=True)
            return
        if data.startswith("repair:retry:"):
            if is_active_phase(read_status(config).get("phase")):
                await query.answer("Migration masih aktif.", show_alert=True)
                return
            category = data.rsplit(":", 1)[1]
            db = _database(config)
            try:
                revived = requeue_retryable_repairs(db) if category == "all" else requeue_repair_category(db, category)
            finally:
                db.close()
            if revived:
                _request_mode(config, "process")
            await query.answer(f"{revived} job dikembalikan ke pending.", show_alert=True)
            return
        if data == "checkpoint:reset:confirm":
            await edit(query, "♻️ Reset semua checkpoint?\n\nQueue lama tidak dipadam.", _buttons([[("✅ Reset", "checkpoint:reset:all")], [("❌ Cancel", "checkpoint:view")]]))
            await query.answer()
            return
        if data == "checkpoint:reset:all":
            db = _database(config)
            try:
                removed = reset_all_checkpoints(db)
            finally:
                db.close()
            _request_mode(config, "run")
            await query.answer(f"{removed} checkpoint dibuang.", show_alert=True)
            return
        if data in {"advanced:resume", "advanced:full", "advanced:sync", "run:now"}:
            if data == "run:now":
                sources, destinations = get_sources(path), list_destinations(path)
                if not sources or not destinations:
                    await query.answer("Sediakan Source Queue dan Destination dahulu.", show_alert=True)
                    return
                if {item["chat"] for item in sources} & {item["chat"] for item in destinations}:
                    await query.answer("Source dan destination bertindih. Betulkan dalam menu masing-masing.", show_alert=True)
                    return
            mode = {"advanced:resume": "process", "advanced:full": "run", "advanced:sync": "sync", "run:now": "sync"}[data]
            _request_mode(config, mode)
            await query.answer("Arahan dijadualkan.", show_alert=True)
            return
        if data == "stop:current":
            if is_active_phase(read_status(config).get("phase")):
                request_stop(config)
                write_status(config, "stopping", message="Arahan stop dihantar. Menunggu operasi semasa selesai.")
                await query.answer("Arahan stop dihantar.", show_alert=True)
            else:
                await query.answer("Tiada migration aktif.", show_alert=True)
            await edit(query, _dashboard_text(config), _menu())
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
            sent = await message.reply_text(_dashboard_text(config), reply_markup=_menu())
            if user_id is not None:
                start_live(user_id, sent)

    await app.start()
    try:
        await idle()
    finally:
        for user_id in list(_LIVE_TASKS):
            _cancel_live(user_id)
        for task in list(_LIVE_TASKS.values()):
            with suppress(asyncio.CancelledError):
                await task
        await app.stop()
