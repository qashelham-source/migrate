from __future__ import annotations

import asyncio
import os
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
from app.queue import MessageQueue
from app.telegram_client import load_accounts, make_user_client

_LIVE_TASKS: dict[int, asyncio.Task[None]] = {}
_CHANNEL_CACHE: dict[int, list[dict[str, Any]]] = {}
_SELECTIONS: dict[int, dict[str, set[str]]] = {}
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
            [("📚 Channel Manager", "channels:view")],
            [("🧠 Smart Center", "smart:menu"), ("⚙️ Settings", "settings:view")],
            [("▶️ Sync", "run:now"), ("⏹ Stop", "stop:current")],
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
    phase = str(status.get("phase") or "idle").lower()
    active = q["downloading"] + q["uploading"]
    issue_total = q["failed"] + q["skipped"] + d["paused"] + v["failed"]
    state = "🟢 Healthy" if issue_total == 0 else "🟠 Perlu perhatian"
    phase_labels = {
        "idle": "Idle", "watching": "Live watcher", "scanning": "Scanning", "processing": "Processing",
        "downloading": "Downloading", "uploading": "Uploading", "stopping": "Stopping", "stopped": "Stopped",
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
    if status.get("job_id") is not None:
        lines += ["", f"Current job: #{status['job_id']}", f"Media: {status.get('media_type') or '-'}"]
    if status.get("current") is not None and status.get("total") is not None:
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
            "Source dan destination diurus melalui Channel Manager.",
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


def _selection_for(user_id: int, path: Path) -> dict[str, set[str]]:
    if user_id not in _SELECTIONS:
        _SELECTIONS[user_id] = {
            "sources": {str(item["chat"]) for item in get_sources(path)},
            "destinations": {str(item["chat"]) for item in list_destinations(path)},
        }
    return _SELECTIONS[user_id]


async def _scan_channels(config: AppConfig) -> list[dict[str, Any]]:
    client = make_user_client(config)
    channels: list[dict[str, Any]] = []
    await client.start()
    try:
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
        await client.stop()
    channels.sort(key=lambda item: (item["title"].lower(), item["chat"]))
    return channels


def _channel_text(user_id: int, path: Path, page: int = 0) -> str:
    channels = _CHANNEL_CACHE.get(user_id, [])
    selected = _selection_for(user_id, path)
    if not channels:
        return "📚 Channel Manager\n\nBelum scan Telegram. Tekan Scan / Refresh."
    start = page * _PAGE_SIZE
    end = min(len(channels), start + _PAGE_SIZE)
    return "\n".join([
        "📚 Channel Manager", "", f"Found: {len(channels)} channels/groups",
        f"Selected: {len(selected['sources'])} source · {len(selected['destinations'])} destination",
        "", f"Showing {start + 1}-{end}", "Tap S or D to tick/untick, then Save.",
    ])


def _channel_menu(user_id: int, path: Path, page: int = 0) -> InlineKeyboardMarkup:
    channels = _CHANNEL_CACHE.get(user_id, [])
    selected = _selection_for(user_id, path)
    rows: list[list[tuple[str, str]]] = []
    start = page * _PAGE_SIZE
    for index, item in enumerate(channels[start:start + _PAGE_SIZE], start=start):
        chat = item["chat"]
        source_icon = "☑" if chat in selected["sources"] else "☐"
        dest_icon = "☑" if chat in selected["destinations"] else "☐"
        title = item["title"][:28]
        rows.append([(f"{item['access']} {title}"[:48], f"channels:noop:{index}")])
        role_row = [(f"{source_icon} Source", f"channels:toggle:s:{index}")]
        if item["can_destination"]:
            role_row.append((f"{dest_icon} Destination", f"channels:toggle:d:{index}"))
        else:
            role_row.append(("🔒 No post access", f"channels:noop:{index}"))
        rows.append(role_row)
    nav: list[tuple[str, str]] = []
    if page > 0:
        nav.append(("⬅️", f"channels:page:{page - 1}"))
    if start + _PAGE_SIZE < len(channels):
        nav.append(("➡️", f"channels:page:{page + 1}"))
    if nav:
        rows.append(nav)
    rows += [
        [("🔄 Scan / Refresh", "channels:scan"), ("✅ Save", "channels:save")],
        [("⬅️ Dashboard", "menu")],
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

        if data in {"menu", "dashboard:view"}:
            await edit(query, _dashboard_text(config), _menu())
            start_live(user_id, query.message)
            await query.answer("Dikemas kini" if data == "dashboard:view" else None)
            return
        if data == "channels:view":
            await edit(query, _channel_text(user_id, path), _channel_menu(user_id, path))
            await query.answer()
            return
        if data == "channels:scan":
            await query.answer("Scanning Telegram…", show_alert=False)
            await edit(query, "📚 Channel Manager\n\nScanning channel dan group…", _back("channels:view", "⬅️ Cancel"))
            try:
                _CHANNEL_CACHE[user_id] = await _scan_channels(config)
                _SELECTIONS.pop(user_id, None)
                await edit(query, _channel_text(user_id, path), _channel_menu(user_id, path))
            except Exception as exc:
                await edit(query, f"📚 Channel Manager\n\n❌ Scan gagal: {str(exc)[:500]}\n\nPastikan user session tidak sedang dikunci proses lain.", _back())
            return
        if data.startswith("channels:page:"):
            page = max(0, int(data.rsplit(":", 1)[1]))
            await edit(query, _channel_text(user_id, path, page), _channel_menu(user_id, path, page))
            await query.answer()
            return
        if data.startswith("channels:toggle:"):
            _, _, role, raw_index = data.split(":", 3)
            index = int(raw_index)
            channels = _CHANNEL_CACHE.get(user_id, [])
            if index >= len(channels):
                await query.answer("Channel cache expired. Scan semula.", show_alert=True)
                return
            item = channels[index]
            selected = _selection_for(user_id, path)
            key = "sources" if role == "s" else "destinations"
            other = "destinations" if key == "sources" else "sources"
            chat = item["chat"]
            if chat in selected[key]:
                selected[key].remove(chat)
            else:
                if chat in selected[other]:
                    await query.answer("Channel yang sama tak boleh jadi source dan destination.", show_alert=True)
                    return
                selected[key].add(chat)
            page = index // _PAGE_SIZE
            await edit(query, _channel_text(user_id, path, page), _channel_menu(user_id, path, page))
            await query.answer()
            return
        if data.startswith("channels:noop:"):
            await query.answer()
            return
        if data == "channels:save":
            selected = _selection_for(user_id, path)
            if not selected["sources"] or not selected["destinations"]:
                await query.answer("Pilih sekurang-kurangnya satu source dan satu destination.", show_alert=True)
                return
            overlap = selected["sources"] & selected["destinations"]
            if overlap:
                await query.answer("Source dan destination tak boleh channel yang sama.", show_alert=True)
                return
            set_sources(sorted(selected["sources"]), path)
            set_destinations(sorted(selected["destinations"]), path)
            _request_mode(config, "run")
            await query.answer("Channel selection disimpan.", show_alert=True)
            await edit(query, _dashboard_text(config), _menu())
            start_live(user_id, query.message)
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
            "finder:view": (lambda _: "🔍 Original Media Finder\n\nFingerprint engine aktif. Gunakan command-line finder untuk carian terperinci sementara UI carian media dibina.", _back("smart:menu", "⬅️ Smart Center")),
            "duplicates:view": (lambda _: "👥 Duplicate Detector\n\nDuplicate fingerprint direkodkan oleh media engine. Paparan kumpulan duplicate akan ditambah pada release seterusnya.", _back("smart:menu", "⬅️ Smart Center")),
        }
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
                    await query.answer("Pilih source dan destination dalam Channel Manager dahulu.", show_alert=True)
                    return
                if {item["chat"] for item in sources} & {item["chat"] for item in destinations}:
                    await query.answer("Source dan destination bertindih. Betulkan dalam Channel Manager.", show_alert=True)
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
        if not (message.text or "").startswith("/start"):
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
