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
from app.dashboard_v2 import (
    dashboard_snapshot,
    delivery_matrix,
    format_bytes,
    format_eta,
    issue_center,
    source_library,
)
from app.db import Database
from app.destination_manager import (
    add_destination,
    get_sources,
    list_destinations,
    remove_destination,
    set_source,
)
from app.queue import MessageQueue
from app.telegram_client import load_accounts

_PENDING: dict[int, str] = {}
_LIVE_TASKS: dict[int, asyncio.Task[None]] = {}

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
            [("📊 Dashboard", "dashboard:view")],
            [("📚 Source Library", "sources:view"), ("🚚 Delivery Matrix", "matrix:view")],
            [("🚨 Issue Center", "issues:view"), ("📈 ETA & Storage", "capacity:view")],
            [("📥 Tukar Source", "source:set")],
            [("➕ Tambah Destination", "dest:add"), ("🗑 Buang Destination", "dest:remove")],
            [("📋 Lihat Setting", "settings:view")],
            [("📊 Live Status", "status:view"), ("⏹ Stop Current Job", "stop:current")],
            [("▶️ Sync Sekarang", "run:now")],
            [("🧰 Advanced Tools", "advanced:menu")],
        ]
    )


def _back(target: str = "menu", label: str = "⬅️ Menu") -> InlineKeyboardMarkup:
    return _buttons([[(label, target)]])


def _refresh_menu(callback: str, back: str = "menu") -> InlineKeyboardMarkup:
    return _buttons([[("🔄 Refresh", callback)], [("⬅️ Menu", back)]])


def _advanced_menu() -> InlineKeyboardMarkup:
    return _buttons(
        [
            [("🩺 Health Check", "advanced:health"), ("🛠 Repair Queue", "advanced:repair")],
            [("▶️ Resume Pending", "advanced:resume"), ("🆕 Sync New Posts", "advanced:sync")],
            [("🔍 Full Scan", "advanced:full"), ("📍 Checkpoints", "checkpoint:view")],
            [("⬅️ Menu", "menu")],
        ]
    )


def _health_menu() -> InlineKeyboardMarkup:
    return _buttons(
        [[("▶️ Run Check", "health:run"), ("🔄 Refresh", "health:refresh")], [("⬅️ Advanced Tools", "advanced:menu")]]
    )


def _repair_menu() -> InlineKeyboardMarkup:
    return _buttons(
        [
            [("🔄 Retry masalah sementara", "repair:retry:all")],
            [("🎞 Retry MEDIA_EMPTY", "repair:retry:media_empty"), ("🔑 Retry Permission", "repair:retry:permission")],
            [("🧩 Retry Peer ID", "repair:retry:peer_id"), ("📋 Lihat Butiran", "repair:details")],
            [("🔄 Refresh", "advanced:repair")],
            [("⬅️ Advanced Tools", "advanced:menu")],
        ]
    )


def _checkpoint_menu() -> InlineKeyboardMarkup:
    return _buttons(
        [[("🔄 Refresh", "checkpoint:refresh"), ("♻️ Reset + Full Scan", "checkpoint:reset:confirm")], [("⬅️ Advanced Tools", "advanced:menu")]]
    )


def _status_menu() -> InlineKeyboardMarkup:
    return _buttons(
        [[("🔄 Refresh", "status:refresh"), ("⏹ Stop Current Job", "stop:current")], [("⬅️ Menu", "menu")]]
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


def _extract_chat(message: Message) -> str:
    forwarded = getattr(message, "forward_from_chat", None)
    if forwarded:
        return f"@{forwarded.username}" if forwarded.username else str(forwarded.id)
    value = (message.text or message.caption or "").strip()
    if not value:
        raise ValueError("Hantar @username, link t.me, -100 ID, atau forward satu post channel.")
    return value


def _settings_text(path: Path) -> str:
    sources = get_sources(path)
    destinations = list_destinations(path)
    lines = ["⚙️ Setting Migration", "", f"Source: {sources[0].get('chat') if sources else 'Belum ditetapkan'}", "", "Destinations:"]
    if not destinations:
        lines.append("Belum ada destination")
    for index, destination in enumerate(destinations, 1):
        topic = f" (topic {destination['topic_id']})" if destination.get("topic_id") is not None else ""
        lines.append(f"{index}. {destination.get('chat', '')}{topic}")
    return "\n".join(lines)


def _dashboard_text(config: AppConfig) -> str:
    db = _database(config)
    try:
        data = dashboard_snapshot(db, config.queue.db_path)
    finally:
        db.close()
    q, s, d, v, t, storage = (
        data["queue"], data["sources"], data["destinations"], data["verification"], data["telemetry"], data["storage"]
    )
    issue_total = q["failed"] + q["skipped"] + d["paused"] + v["failed"]
    state = "🟢 Sihat" if issue_total == 0 else "🟠 Perlu perhatian"
    return "\n".join(
        [
            "📊 Migration Dashboard V2",
            "",
            f"System: {state}",
            f"Sources: {s['total']} · Live {s['live']} · Verified {s['verified']} · Issues {s['issues']}",
            f"Destinations tracked: {d['total']} · Paused {d['paused']}",
            "",
            "Queue",
            f"• Pending {q['pending']} · Active {q['downloading'] + q['uploading']}",
            f"• Completed {q['copied']} · Failed {q['failed']} · Skipped {q['skipped']}",
            "",
            "Verification",
            f"• Verified {v['verified']} · Repairing {v['repairing']} · Failed {v['failed']}",
            "",
            f"Speed: {format_bytes(t['speed_bps'])}/s · ETA: {format_eta(t['eta_seconds'])}",
            f"Storage: {storage.percent_used:.1f}% used · {format_bytes(storage.free_bytes)} free",
        ]
    )[:3900]


def _source_text(config: AppConfig) -> str:
    db = _database(config)
    try:
        rows = source_library(db)
    finally:
        db.close()
    lines = ["📚 Source Library", ""]
    if not rows:
        return "\n".join(lines + ["Belum ada source dalam registry."])
    icons = {"verified": "✅", "issues": "🚨", "in_progress": "🔄", "not_started": "⚪"}
    for row in rows[:25]:
        icon = icons.get(str(row.get("migration_state")), "•")
        live = " LIVE" if row.get("live_watch_enabled") else ""
        lines.extend(
            [
                f"{icon} {row.get('title') or row.get('source_chat_id')}{live}",
                f"   State: {row.get('migration_state')} · Access: {row.get('access_status')}",
                f"   Latest {row.get('latest_seen_message_id') or '-'} · Scanned {row.get('history_scanned_through') or '-'} · Verified {row.get('history_verified_through') or '-'}",
                "",
            ]
        )
    return "\n".join(lines)[:3900]


def _matrix_text(config: AppConfig) -> str:
    db = _database(config)
    try:
        rows = delivery_matrix(db)
    finally:
        db.close()
    lines = ["🚚 Delivery Matrix", ""]
    if not rows:
        return "\n".join(lines + ["Belum ada delivery direkodkan."])
    for row in rows[:25]:
        icon = "⏸" if row["paused"] else "✅" if row["issues"] == 0 else "⚠️"
        lines.extend(
            [
                f"{icon} Destination {row['dest_chat_id']}",
                f"   Total {row['total']} · Copied {row['copied']} · Active {row['active']} · Issues {row['issues']}",
                *([f"   Pause: {row.get('pause_reason') or row.get('last_error') or '-'}"] if row["paused"] else []),
                "",
            ]
        )
    return "\n".join(lines)[:3900]


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
            lines.extend([f"⏸ Destination {item['id']} paused", str(item["error"])[:180], ""])
        else:
            lines.extend(
                [
                    f"❌ Job #{item['id']} · {item['status']} · {item['media_type']}",
                    f"Source message {item['source_message_id']} · Attempts {item['attempts']}",
                    str(item["error"]).replace("\n", " ")[:180],
                    "",
                ]
            )
    return "\n".join(lines)[:3900]


def _capacity_text(config: AppConfig) -> str:
    db = _database(config)
    try:
        data = dashboard_snapshot(db, config.queue.db_path)
    finally:
        db.close()
    telemetry, storage = data["telemetry"], data["storage"]
    warning = "🚨 Kritikal" if storage.percent_used >= 90 else "⚠️ Tinggi" if storage.percent_used >= 80 else "✅ Sihat"
    return "\n".join(
        [
            "📈 ETA & Storage",
            "",
            f"Active telemetry jobs: {telemetry['active']}",
            f"Combined speed: {format_bytes(telemetry['speed_bps'])}/s",
            f"Current ETA: {format_eta(telemetry['eta_seconds'])}",
            "",
            f"Storage status: {warning}",
            f"Used: {format_bytes(storage.used_bytes)} / {format_bytes(storage.total_bytes)} ({storage.percent_used:.1f}%)",
            f"Free: {format_bytes(storage.free_bytes)}",
        ]
    )


def _status_text(config: AppConfig) -> str:
    status, counts = read_status(config), _queue_counts(config)
    phase = str(status.get("phase") or "idle").lower()
    labels = {"starting": "🟡 Starting", "waiting": "⚪ Waiting", "scanning": "🔎 Scanning", "scan_complete": "✅ Scan Complete", "processing": "🔄 Processing", "downloading": "⬇️ Downloading", "uploading": "⬆️ Uploading", "batch_pause": "⏸ Batch Pause", "stopping": "🛑 Stopping", "stopped": "⏹ Stopped", "health_check": "🩺 Health Check", "health_complete": "✅ Health Complete", "idle": "✅ Idle", "watching": "👀 Live Watcher", "error": "❌ Error"}
    lines = ["📊 Live Migration Status", "", f"Status: {labels.get(phase, phase.title())}"]
    for key, label in (("message", ""), ("source", "Source: "), ("destination_chat", "Destination: ")):
        if status.get(key):
            lines.append(f"{label}{status[key]}")
    if status.get("job_id") is not None:
        lines += ["", f"Current job: #{status['job_id']}", f"Media: {status.get('media_type') or '-'}", f"Source message: {status.get('source_message_id') or '-'}"]
    if status.get("current") is not None and status.get("total") is not None:
        lines.append(f"Progress: {status['current']}/{status['total']}")
    error = status.get("last_error") or status.get("error")
    if error:
        lines += ["", f"Last error: {str(error)[:500]}"]
    lines += ["", "Queue:", f"• Pending: {counts.get('pending', 0)}", f"• Active: {counts.get('downloading', 0) + counts.get('uploading', 0)}", f"• Completed: {counts.get('copied', 0)}", f"• Failed: {counts.get('failed', 0)}", f"• Skipped: {counts.get('skipped', 0)}", "", "Auto-refresh setiap 3 saat."]
    return "\n".join(lines)[:3900]


def _advanced_text(config: AppConfig) -> str:
    counts, status = _queue_counts(config), read_status(config)
    db = _database(config)
    try:
        checkpoints = len(checkpoint_rows(db))
    finally:
        db.close()
    return "\n".join(["🧰 Advanced Tools", "", f"Migration: {'sedang berjalan' if is_active_phase(status.get('phase')) else 'tidak aktif'}", f"Pending: {counts.get('pending', 0)}", f"Failed: {counts.get('failed', 0)}", f"Skipped: {counts.get('skipped', 0)}", f"Active checkpoints: {checkpoints}"])


def _health_text(config: AppConfig) -> str:
    report = load_health_report(config)
    if not report:
        return "🩺 Pre-flight Health Check\n\nBelum ada laporan. Tekan Run Check."
    lines = ["🩺 Pre-flight Health Check", "", f"Overall: {str(report.get('overall') or 'unknown').upper()}", f"Checked: {report.get('generated_at') or '-'} UTC", ""]
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
            lines = ["📋 Repair Queue Details", ""]
            for category, item in rows[:15]:
                lines += [f"#{item['id']} · {CATEGORY_LABELS[category]} · {item['media_type']}", f"Source message: {item['source_message_id']} · Attempts: {item['attempts']}", str(item['last_error']).replace("\n", " ")[:160], ""]
            return "\n".join(lines or ["Tiada isu."])[:3900]
        summary = repair_summary(db)
    finally:
        db.close()
    lines = ["🛠 Repair Queue", "", f"Jumlah job bermasalah: {sum(summary.values())}", ""]
    lines += [f"• {CATEGORY_LABELS[key]}: {summary.get(key, 0)}" for key in REPAIR_CATEGORIES]
    return "\n".join(lines)


def _checkpoint_text(config: AppConfig) -> str:
    db = _database(config)
    try:
        rows = checkpoint_rows(db)
    finally:
        db.close()
    lines = ["📍 Incremental Sync Checkpoints", ""]
    if not rows:
        return "\n".join(lines + ["Belum ada checkpoint rasmi."])
    for index, row in enumerate(rows, 1):
        lines += [f"{index}. Source {row['source_chat_id']}", f"   Last message ID: {row['last_scanned_message_id']}", f"   Mode: {row['last_scan_mode']} · Updated: {row['updated_at']} UTC", ""]
    return "\n".join(lines)[:3900]


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
                text = _status_text(config)
                if text != last:
                    try:
                        await message.edit_text(text, reply_markup=_status_menu())
                    except MessageNotModified:
                        pass
                    last = text
                await asyncio.sleep(3)
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
        _cancel_live(user_id)
        _PENDING.pop(user_id, None)
        await message.reply_text(_dashboard_text(config), reply_markup=_menu())

    @app.on_callback_query()
    async def callback_handler(_: Client, query: CallbackQuery) -> None:
        user_id = query.from_user.id if query.from_user else None
        if not _is_authorized(config, user_id):
            await reject(query=query)
            return
        assert user_id is not None
        data = query.data or ""
        if data not in {"status:view", "status:refresh", "stop:current"}:
            _cancel_live(user_id)

        pages: dict[str, tuple[Callable[[AppConfig], str], InlineKeyboardMarkup]] = {
            "dashboard:view": (_dashboard_text, _refresh_menu("dashboard:view")),
            "sources:view": (_source_text, _refresh_menu("sources:view")),
            "matrix:view": (_matrix_text, _refresh_menu("matrix:view")),
            "issues:view": (_issues_text, _buttons([[("🔄 Refresh", "issues:view"), ("🛠 Repair Queue", "advanced:repair")], [("⬅️ Menu", "menu")]])),
            "capacity:view": (_capacity_text, _refresh_menu("capacity:view")),
            "advanced:menu": (_advanced_text, _advanced_menu()),
            "advanced:health": (_health_text, _health_menu()),
            "health:refresh": (_health_text, _health_menu()),
            "advanced:repair": (_repair_text, _repair_menu()),
            "checkpoint:view": (_checkpoint_text, _checkpoint_menu()),
            "checkpoint:refresh": (_checkpoint_text, _checkpoint_menu()),
        }
        if data == "menu":
            _PENDING.pop(user_id, None)
            await edit(query, _dashboard_text(config), _menu())
            await query.answer()
            return
        if data in pages:
            fn, markup = pages[data]
            await edit(query, fn(config), markup)
            await query.answer("Dikemas kini" if data.endswith("refresh") else None)
            return
        if data == "repair:details":
            await edit(query, _repair_text(config, True), _repair_menu())
            await query.answer()
            return
        if data == "health:run":
            _request_mode(config, "health")
            await query.answer("Health check telah dijadualkan.", show_alert=True)
            return
        if data.startswith("repair:retry:"):
            if is_active_phase(read_status(config).get("phase")):
                await query.answer("Migration masih aktif. Stop atau tunggu selesai.", show_alert=True)
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
            await edit(query, _repair_text(config), _repair_menu())
            return
        if data == "checkpoint:reset:confirm":
            await edit(query, "♻️ Reset semua checkpoint?\n\nQueue lama tidak dipadam.", _buttons([[("✅ Ya, reset dan full scan", "checkpoint:reset:all")], [("❌ Batal", "checkpoint:view")]]))
            await query.answer()
            return
        if data == "checkpoint:reset:all":
            if is_active_phase(read_status(config).get("phase")):
                await query.answer("Migration masih aktif.", show_alert=True)
                return
            db = _database(config)
            try:
                removed = reset_all_checkpoints(db)
            finally:
                db.close()
            _request_mode(config, "run")
            await query.answer(f"{removed} checkpoint dibuang. Full Scan dijadualkan.", show_alert=True)
            return
        if data in {"advanced:resume", "advanced:full", "advanced:sync", "run:now"}:
            mode = {"advanced:resume": "process", "advanced:full": "run", "advanced:sync": "sync", "run:now": "sync"}[data]
            _request_mode(config, mode)
            await query.answer("Arahan telah dijadualkan.", show_alert=True)
            return
        if data in {"source:set", "dest:add"}:
            _PENDING[user_id] = "source" if data == "source:set" else "destination"
            await edit(query, "Hantar @username, link t.me, -100 ID, atau forward satu post channel.", _back())
            await query.answer()
            return
        if data == "settings:view":
            await edit(query, _settings_text(path), _back())
            await query.answer()
            return
        if data in {"status:view", "status:refresh"}:
            await edit(query, _status_text(config), _status_menu())
            start_live(user_id, query.message)
            await query.answer("Status dikemas kini" if data == "status:refresh" else None)
            return
        if data == "stop:current":
            _cancel_live(user_id)
            if is_active_phase(read_status(config).get("phase")):
                request_stop(config)
                write_status(config, "stopping", message="Arahan stop dihantar. Menunggu operasi semasa selesai.")
                await query.answer("Arahan stop dihantar.", show_alert=True)
            else:
                await query.answer("Tiada migration aktif.", show_alert=True)
            await edit(query, _status_text(config), _status_menu())
            start_live(user_id, query.message)
            return
        if data == "dest:remove":
            destinations = list_destinations(path)
            if not destinations:
                await query.answer("Belum ada destination", show_alert=True)
                return
            rows = [[(f"🗑 {index}. {item.get('chat', '')}"[:60], f"dest:delete:{index}")] for index, item in enumerate(destinations, 1)]
            rows.append([("⬅️ Menu", "menu")])
            await edit(query, "Pilih destination yang nak dibuang:", _buttons(rows))
            await query.answer()
            return
        if data.startswith("dest:delete:"):
            try:
                removed = remove_destination(int(data.rsplit(":", 1)[1]), path)
                _request_mode(config, "run")
                await query.answer(f"Dibuang: {removed.get('chat', '')}", show_alert=True)
                await edit(query, _settings_text(path), _back())
            except Exception as exc:
                await query.answer(str(exc), show_alert=True)
            return
        await query.answer()

    @app.on_message(filters.private, group=1)
    async def input_handler(_: Client, message: Message) -> None:
        user_id = message.from_user.id if message.from_user else None
        if not _is_authorized(config, user_id):
            await reject(message=message)
            return
        assert user_id is not None
        _cancel_live(user_id)
        pending = _PENDING.get(user_id)
        if not pending:
            if not (message.text or "").startswith("/start"):
                await message.reply_text(_dashboard_text(config), reply_markup=_menu())
            return
        try:
            chat = _extract_chat(message)
            saved = set_source(chat, path) if pending == "source" else add_destination(chat, None, path)
            _PENDING.pop(user_id, None)
            _request_mode(config, "run")
            label = "Source disimpan" if pending == "source" else "Destination ditambah"
            await message.reply_text(f"✅ {label}: {saved['chat']}", reply_markup=_menu())
        except Exception as exc:
            await message.reply_text(f"Tak berjaya: {exc}\n\nCuba hantar semula atau tekan Menu.", reply_markup=_back())

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
