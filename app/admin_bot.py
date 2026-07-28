from __future__ import annotations

import asyncio
import os
from contextlib import suppress
from pathlib import Path
from typing import Any

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
from app.control import (
    clear_stop,
    is_active_phase,
    read_status,
    request_stop,
    write_status,
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


def _menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📥 Tukar Source", callback_data="source:set")],
            [
                InlineKeyboardButton("➕ Tambah Destination", callback_data="dest:add"),
                InlineKeyboardButton("🗑 Buang Destination", callback_data="dest:remove"),
            ],
            [InlineKeyboardButton("📋 Lihat Setting", callback_data="settings:view")],
            [
                InlineKeyboardButton("📊 Live Status", callback_data="status:view"),
                InlineKeyboardButton("⏹ Stop Current Job", callback_data="stop:current"),
            ],
            [InlineKeyboardButton("▶️ Sync Sekarang", callback_data="run:now")],
            [InlineKeyboardButton("🧰 Advanced Tools", callback_data="advanced:menu")],
        ]
    )


def _back_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Menu", callback_data="menu")]])


def _advanced_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🩺 Health Check", callback_data="advanced:health"),
                InlineKeyboardButton("🛠 Repair Queue", callback_data="advanced:repair"),
            ],
            [
                InlineKeyboardButton("▶️ Resume Pending", callback_data="advanced:resume"),
                InlineKeyboardButton("🆕 Sync New Posts", callback_data="advanced:sync"),
            ],
            [
                InlineKeyboardButton("🔍 Full Scan", callback_data="advanced:full"),
                InlineKeyboardButton("📍 Checkpoints", callback_data="checkpoint:view"),
            ],
            [InlineKeyboardButton("⬅️ Menu", callback_data="menu")],
        ]
    )


def _health_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("▶️ Run Check", callback_data="health:run"),
                InlineKeyboardButton("🔄 Refresh", callback_data="health:refresh"),
            ],
            [InlineKeyboardButton("⬅️ Advanced Tools", callback_data="advanced:menu")],
        ]
    )


def _repair_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔄 Retry masalah sementara", callback_data="repair:retry:all")],
            [
                InlineKeyboardButton("🎞 Retry MEDIA_EMPTY", callback_data="repair:retry:media_empty"),
                InlineKeyboardButton("🔑 Retry Permission", callback_data="repair:retry:permission"),
            ],
            [
                InlineKeyboardButton("🧩 Retry Peer ID", callback_data="repair:retry:peer_id"),
                InlineKeyboardButton("📋 Lihat Butiran", callback_data="repair:details"),
            ],
            [InlineKeyboardButton("🔄 Refresh", callback_data="advanced:repair")],
            [InlineKeyboardButton("⬅️ Advanced Tools", callback_data="advanced:menu")],
        ]
    )


def _checkpoint_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🔄 Refresh", callback_data="checkpoint:refresh"),
                InlineKeyboardButton("♻️ Reset + Full Scan", callback_data="checkpoint:reset:confirm"),
            ],
            [InlineKeyboardButton("⬅️ Advanced Tools", callback_data="advanced:menu")],
        ]
    )


def _checkpoint_confirm_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Ya, reset dan full scan", callback_data="checkpoint:reset:all")],
            [InlineKeyboardButton("❌ Batal", callback_data="checkpoint:view")],
        ]
    )


def _status_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🔄 Refresh", callback_data="status:refresh"),
                InlineKeyboardButton("⏹ Stop Current Job", callback_data="stop:current"),
            ],
            [InlineKeyboardButton("⬅️ Menu", callback_data="menu")],
        ]
    )


def _authorized_ids(config: AppConfig) -> set[int]:
    result: set[int] = set()

    # config.yaml is the source of truth; accounts.json may not exist yet when
    # the control panel starts before the migration service has cached a session.
    for configured_id in config.telegram.admin_ids:
        try:
            result.add(int(configured_id))
        except (TypeError, ValueError):
            continue

    for account in load_accounts(config).values():
        try:
            result.add(int(account.get("id")))
        except (TypeError, ValueError, AttributeError):
            continue

    for value in os.getenv("ADMIN_USER_ID", "").split(","):
        value = value.strip()
        if value.isdigit():
            result.add(int(value))
    return result


def _is_authorized(config: AppConfig, user_id: int | None) -> bool:
    return user_id is not None and user_id in _authorized_ids(config)


def _extract_chat(message: Message) -> str:
    forwarded = getattr(message, "forward_from_chat", None)
    if forwarded:
        if forwarded.username:
            return f"@{forwarded.username}"
        return str(forwarded.id)

    value = (message.text or message.caption or "").strip()
    if not value:
        raise ValueError("Hantar @username, link t.me, -100 ID, atau forward satu post channel.")
    return value


def _request_mode(config: AppConfig, mode: str) -> None:
    clear_stop(config)
    request_run_mode(config, mode)


def _settings_text(config_path: Path) -> str:
    sources = get_sources(config_path)
    destinations = list_destinations(config_path)

    source_text = sources[0].get("chat") if sources else "Belum ditetapkan"
    lines = ["⚙️ Setting Migration", "", f"Source: {source_text}", "", "Destinations:"]
    if destinations:
        for index, destination in enumerate(destinations, start=1):
            text = f"{index}. {destination.get('chat', '')}"
            if destination.get("topic_id") is not None:
                text += f" (topic {destination['topic_id']})"
            lines.append(text)
    else:
        lines.append("Belum ada destination")
    return "\n".join(lines)


def _queue_counts(config: AppConfig) -> dict[str, int]:
    db = Database(config.queue.db_path)
    try:
        db.initialize()
        return MessageQueue(db, config).counts_by_status()
    finally:
        db.close()


def _with_database(config: AppConfig) -> Database:
    db = Database(config.queue.db_path)
    db.initialize()
    return db


def _status_text(config: AppConfig) -> str:
    status = read_status(config)
    counts = _queue_counts(config)
    phase = str(status.get("phase") or "idle").lower()
    labels = {
        "starting": "🟡 Starting",
        "waiting": "⚪ Waiting for Setting",
        "scanning": "🔎 Scanning",
        "scan_complete": "✅ Scan Complete",
        "processing": "🔄 Processing",
        "downloading": "⬇️ Downloading",
        "uploading": "⬆️ Uploading",
        "batch_pause": "⏸ Batch Pause",
        "stopping": "🛑 Stopping Safely",
        "stopped": "⏹ Stopped",
        "health_check": "🩺 Health Check",
        "health_complete": "✅ Health Check Complete",
        "idle": "✅ Idle",
        "error": "❌ Error",
    }

    lines = ["📊 Live Migration Status", "", f"Status: {labels.get(phase, phase.title())}"]
    message = status.get("message")
    if message:
        lines.append(str(message))

    mode = str(status.get("scan_mode") or status.get("cycle_mode") or "").lower()
    if mode:
        mode_label = {
            "incremental": "Incremental Sync",
            "full": "Full Scan",
            "process": "Resume Pending",
            "health": "Health Check",
        }.get(mode, mode.title())
        lines.append(f"Mode: {mode_label}")

    if status.get("job_id") is not None:
        lines.extend(
            [
                "",
                f"Current job: #{status['job_id']}",
                f"Media: {status.get('media_type') or '-'}",
                f"Source message: {status.get('source_message_id') or '-'}",
            ]
        )
    elif status.get("source"):
        lines.extend(["", f"Source: {status['source']}"])

    destination = status.get("destination_chat")
    if destination:
        lines.append(f"Destination: {destination}")

    current = status.get("current")
    total = status.get("total")
    if current is not None and total is not None:
        lines.append(f"Progress: {current}/{total}")

    scan_start = status.get("scan_start")
    scan_end = status.get("scan_end")
    if scan_start is not None and scan_end is not None:
        lines.append(f"Message range: {scan_start}-{scan_end}")
    if status.get("checkpoint") is not None:
        lines.append(f"Checkpoint: {status['checkpoint']}")
    if status.get("checkpoint_bootstrap"):
        lines.append("Checkpoint source: existing queue")

    batch_index = status.get("batch_index")
    batch_total = status.get("batch_total")
    if batch_index is not None and batch_total is not None:
        lines.append(f"Batch job: {batch_index}/{batch_total}")

    if status.get("last_error"):
        lines.extend(["", f"Last error: {str(status['last_error'])[:500]}"])
    elif status.get("error"):
        lines.extend(["", f"Error: {str(status['error'])[:500]}"])

    lines.extend(
        [
            "",
            "Queue:",
            f"• Pending: {counts.get('pending', 0)}",
            f"• Downloading: {counts.get('downloading', 0)}",
            f"• Uploading: {counts.get('uploading', 0)}",
            f"• Completed: {counts.get('copied', 0)}",
            f"• Failed: {counts.get('failed', 0)}",
            f"• Skipped: {counts.get('skipped', 0)}",
        ]
    )

    updated_at = status.get("updated_at")
    if updated_at:
        lines.extend(["", f"Updated: {updated_at} UTC"])
    lines.append("Auto-refresh setiap 3 saat.")
    return "\n".join(lines)


def _advanced_text(config: AppConfig) -> str:
    status = read_status(config)
    counts = _queue_counts(config)
    active = is_active_phase(status.get("phase"))
    db = _with_database(config)
    try:
        checkpoint_count = len(checkpoint_rows(db))
    finally:
        db.close()
    return "\n".join(
        [
            "🧰 Advanced Tools",
            "",
            f"Migration: {'sedang berjalan' if active else 'tidak aktif'}",
            f"Pending: {counts.get('pending', 0)}",
            f"Failed: {counts.get('failed', 0)}",
            f"Skipped: {counts.get('skipped', 0)}",
            f"Active checkpoints: {checkpoint_count}",
            "",
            "Sync New Posts membaca hanya ID selepas checkpoint terakhir.",
            "Full Scan membaca semula seluruh range tetapi queue kekal anti-duplicate.",
        ]
    )


def _health_text(config: AppConfig) -> str:
    report = load_health_report(config)
    status = read_status(config)
    if not report:
        lines = [
            "🩺 Pre-flight Health Check",
            "",
            "Belum ada laporan health check.",
            "Tekan Run Check untuk menguji session, source, destination, permission dan storage.",
        ]
        if str(status.get("phase")) == "health_check":
            lines.extend(["", "🟡 Health check sedang berjalan..."])
        return "\n".join(lines)

    overall_icon = {"pass": "✅", "warn": "⚠️", "fail": "❌"}.get(str(report.get("overall")), "ℹ️")
    lines = [
        "🩺 Pre-flight Health Check",
        "",
        f"Overall: {overall_icon} {str(report.get('overall') or 'unknown').upper()}",
        f"Checked: {report.get('generated_at') or '-'} UTC",
        "",
    ]
    for item in report.get("checks") or []:
        icon = {"pass": "✅", "warn": "⚠️", "fail": "❌"}.get(str(item.get("status")), "•")
        lines.append(f"{icon} {item.get('name')}: {item.get('detail')}")
    if str(status.get("phase")) == "health_check":
        lines.extend(["", "🟡 Health check baru sedang berjalan..."])
    return "\n".join(lines)[:3900]


def _repair_text(config: AppConfig) -> str:
    db = _with_database(config)
    try:
        summary = repair_summary(db)
    finally:
        db.close()
    total = sum(summary.values())
    lines = ["🛠 Repair Queue", "", f"Jumlah job bermasalah: {total}", ""]
    for category in REPAIR_CATEGORIES:
        lines.append(f"• {CATEGORY_LABELS[category]}: {summary.get(category, 0)}")
    lines.extend(
        [
            "",
            "Retry masalah sementara tidak menyentuh job Unsupported atau Source Missing.",
            "Permission hanya patut diretry selepas akses channel sudah dibetulkan.",
        ]
    )
    return "\n".join(lines)


def _repair_details_text(config: AppConfig) -> str:
    db = _with_database(config)
    try:
        rows: list[tuple[str, dict[str, Any]]] = []
        for category in REPAIR_CATEGORIES:
            for item in repair_samples(db, category, limit=5):
                rows.append((category, item))
        rows.sort(key=lambda pair: int(pair[1]["id"]), reverse=True)
    finally:
        db.close()

    lines = ["📋 Repair Queue Details", ""]
    if not rows:
        lines.append("Tiada failed/skipped job dengan error untuk dibaiki.")
    for category, item in rows[:15]:
        error = str(item["last_error"]).replace("\n", " ")[:160]
        lines.extend(
            [
                f"#{item['id']} · {CATEGORY_LABELS[category]} · {item['media_type']}",
                f"Source message: {item['source_message_id']} · Attempts: {item['attempts']}",
                f"{error}",
                "",
            ]
        )
    return "\n".join(lines)[:3900]


def _checkpoint_text(config: AppConfig) -> str:
    db = _with_database(config)
    try:
        rows = checkpoint_rows(db)
    finally:
        db.close()

    lines = ["📍 Incremental Sync Checkpoints", ""]
    if not rows:
        lines.extend(
            [
                "Belum ada checkpoint rasmi.",
                "Sync pertama akan bootstrap daripada message ID tertinggi yang sudah ada dalam queue.",
            ]
        )
        return "\n".join(lines)

    for index, row in enumerate(rows, start=1):
        topic = f" · topic {row['source_topic_id']}" if row.get("source_topic_id") else ""
        lines.extend(
            [
                f"{index}. Source {row['source_chat_id']}{topic}",
                f"   Last message ID: {row['last_scanned_message_id']}",
                f"   Mode: {row['last_scan_mode']} · Updated: {row['updated_at']} UTC",
                "",
            ]
        )
    lines.append("Sync seterusnya bermula selepas Last message ID.")
    return "\n".join(lines)[:3900]


def _cancel_live(user_id: int) -> None:
    task = _LIVE_TASKS.pop(user_id, None)
    if task and not task.done():
        task.cancel()


async def run_admin_bot(config: AppConfig, config_path: str | Path = "config.yaml") -> None:
    if not config.telegram.bot_enabled or not config.telegram.bot_token:
        raise ValueError("Uploader bot must be enabled before starting the control panel")

    path = Path(config_path).resolve()
    app = Client(
        name="manager_admin",
        api_id=config.telegram.api_id,
        api_hash=config.telegram.api_hash,
        bot_token=config.telegram.bot_token,
        in_memory=True,
    )

    async def reject(message: Message | None = None, query: CallbackQuery | None = None) -> None:
        text = "Akses ditolak. Bot ini hanya untuk pemilik user session."
        if query:
            await query.answer(text, show_alert=True)
        elif message:
            await message.reply_text(text)

    async def live_status_loop(user_id: int, message: Message) -> None:
        last_text: str | None = None
        try:
            while _LIVE_TASKS.get(user_id) is asyncio.current_task():
                text = _status_text(config)
                if text != last_text:
                    try:
                        await message.edit_text(text, reply_markup=_status_menu())
                    except MessageNotModified:
                        pass
                    last_text = text
                await asyncio.sleep(3)
        except asyncio.CancelledError:
            raise
        except Exception:
            return
        finally:
            if _LIVE_TASKS.get(user_id) is asyncio.current_task():
                _LIVE_TASKS.pop(user_id, None)

    def start_live_status(user_id: int, message: Message) -> None:
        _cancel_live(user_id)
        task = asyncio.create_task(live_status_loop(user_id, message))
        _LIVE_TASKS[user_id] = task

    @app.on_message(filters.private & filters.command("start"))
    async def start_handler(_: Client, message: Message) -> None:
        user_id = message.from_user.id if message.from_user else None
        if not _is_authorized(config, user_id):
            await reject(message=message)
            return
        assert user_id is not None
        _cancel_live(user_id)
        _PENDING.pop(user_id, None)
        await message.reply_text(
            "Migration Manager\n\nUbah source dan destination terus dari sini.",
            reply_markup=_menu(),
        )

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

        if data == "menu":
            _PENDING.pop(user_id, None)
            await query.message.edit_text(
                "Migration Manager\n\nUbah source dan destination terus dari sini.",
                reply_markup=_menu(),
            )
            await query.answer()
            return

        if data == "advanced:menu":
            await query.message.edit_text(_advanced_text(config), reply_markup=_advanced_menu())
            await query.answer()
            return

        if data == "advanced:health" or data == "health:refresh":
            try:
                await query.message.edit_text(_health_text(config), reply_markup=_health_menu())
            except MessageNotModified:
                pass
            await query.answer("Health report dikemas kini" if data == "health:refresh" else None)
            return

        if data == "health:run":
            status = read_status(config)
            _request_mode(config, "health")
            text = "Health check akan berjalan selepas cycle semasa selesai." if is_active_phase(
                status.get("phase")
            ) else "Health check akan mula secepat mungkin."
            await query.answer(text, show_alert=True)
            try:
                await query.message.edit_text(_health_text(config), reply_markup=_health_menu())
            except MessageNotModified:
                pass
            return

        if data == "advanced:repair":
            try:
                await query.message.edit_text(_repair_text(config), reply_markup=_repair_menu())
            except MessageNotModified:
                pass
            await query.answer()
            return

        if data == "repair:details":
            await query.message.edit_text(_repair_details_text(config), reply_markup=_repair_menu())
            await query.answer()
            return

        if data.startswith("repair:retry:"):
            status = read_status(config)
            if is_active_phase(status.get("phase")):
                await query.answer(
                    "Migration masih aktif. Stop atau tunggu selesai sebelum repair queue.",
                    show_alert=True,
                )
                return
            category = data.rsplit(":", 1)[1]
            db = _with_database(config)
            try:
                if category == "all":
                    revived = requeue_retryable_repairs(db)
                else:
                    revived = requeue_repair_category(db, category)
            finally:
                db.close()
            if revived:
                _request_mode(config, "process")
            await query.answer(
                f"{revived} job dikembalikan ke pending." if revived else "Tiada job sepadan untuk diretry.",
                show_alert=True,
            )
            await query.message.edit_text(_repair_text(config), reply_markup=_repair_menu())
            return

        if data in {"checkpoint:view", "checkpoint:refresh"}:
            try:
                await query.message.edit_text(_checkpoint_text(config), reply_markup=_checkpoint_menu())
            except MessageNotModified:
                pass
            await query.answer("Checkpoint dikemas kini" if data == "checkpoint:refresh" else None)
            return

        if data == "checkpoint:reset:confirm":
            await query.message.edit_text(
                "♻️ Reset semua checkpoint?\n\nSelepas reset, bot akan buat Full Scan dan bina checkpoint baru. Queue lama tidak dipadam.",
                reply_markup=_checkpoint_confirm_menu(),
            )
            await query.answer()
            return

        if data == "checkpoint:reset:all":
            status = read_status(config)
            if is_active_phase(status.get("phase")):
                await query.answer(
                    "Migration masih aktif. Stop atau tunggu selesai sebelum reset checkpoint.",
                    show_alert=True,
                )
                return
            db = _with_database(config)
            try:
                removed = reset_all_checkpoints(db)
            finally:
                db.close()
            _request_mode(config, "run")
            await query.answer(
                f"{removed} checkpoint dibuang. Full Scan telah dijadualkan.",
                show_alert=True,
            )
            await query.message.edit_text(_checkpoint_text(config), reply_markup=_checkpoint_menu())
            return

        if data == "advanced:resume":
            _request_mode(config, "process")
            await query.answer("Pending queue akan disambung tanpa scan semula.", show_alert=True)
            return

        if data == "advanced:full":
            _request_mode(config, "run")
            await query.answer("Full scan dan pemprosesan queue telah dijadualkan.", show_alert=True)
            return

        if data == "advanced:sync":
            _request_mode(config, "sync")
            await query.answer("Hanya post selepas checkpoint terakhir akan disync.", show_alert=True)
            return

        if data == "source:set":
            _PENDING[user_id] = "source"
            await query.message.edit_text(
                "Hantar source channel sekarang.\n\nBoleh hantar @username, link t.me, -100 ID, atau forward satu post channel.",
                reply_markup=_back_menu(),
            )
            await query.answer()
            return

        if data == "dest:add":
            _PENDING[user_id] = "destination"
            await query.message.edit_text(
                "Hantar destination channel sekarang.\n\nBoleh hantar @username, link t.me, -100 ID, atau forward satu post channel.",
                reply_markup=_back_menu(),
            )
            await query.answer()
            return

        if data == "settings:view":
            await query.message.edit_text(_settings_text(path), reply_markup=_back_menu())
            await query.answer()
            return

        if data in {"status:view", "status:refresh"}:
            try:
                await query.message.edit_text(_status_text(config), reply_markup=_status_menu())
            except MessageNotModified:
                pass
            start_live_status(user_id, query.message)
            await query.answer("Status dikemas kini" if data == "status:refresh" else None)
            return

        if data == "stop:current":
            _cancel_live(user_id)
            status = read_status(config)
            if is_active_phase(status.get("phase")):
                request_stop(config)
                write_status(
                    config,
                    "stopping",
                    message="Arahan stop dihantar. Menunggu operasi Telegram semasa selesai.",
                )
                await query.answer(
                    "Arahan stop dihantar. Bot akan berhenti selepas operasi semasa selesai.",
                    show_alert=True,
                )
            else:
                await query.answer("Tiada migration aktif untuk dihentikan.", show_alert=True)
            try:
                await query.message.edit_text(_status_text(config), reply_markup=_status_menu())
            except MessageNotModified:
                pass
            start_live_status(user_id, query.message)
            return

        if data == "dest:remove":
            destinations = list_destinations(path)
            if not destinations:
                await query.answer("Belum ada destination", show_alert=True)
                return
            rows: list[list[InlineKeyboardButton]] = []
            for index, destination in enumerate(destinations, start=1):
                label = f"🗑 {index}. {destination.get('chat', '')}"[:60]
                rows.append([InlineKeyboardButton(label, callback_data=f"dest:delete:{index}")])
            rows.append([InlineKeyboardButton("⬅️ Menu", callback_data="menu")])
            await query.message.edit_text(
                "Pilih destination yang nak dibuang:",
                reply_markup=InlineKeyboardMarkup(rows),
            )
            await query.answer()
            return

        if data.startswith("dest:delete:"):
            try:
                index = int(data.rsplit(":", 1)[1])
                removed = remove_destination(index, path)
                _request_mode(config, "run")
                await query.answer(f"Dibuang: {removed.get('chat', '')}", show_alert=True)
                await query.message.edit_text(_settings_text(path), reply_markup=_back_menu())
            except Exception as exc:
                await query.answer(str(exc), show_alert=True)
            return

        if data == "run:now":
            _request_mode(config, "sync")
            await query.answer("Sync post baru akan mula secepat mungkin.", show_alert=True)
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
                await message.reply_text("Pilih setting dekat bawah:", reply_markup=_menu())
            return

        try:
            chat = _extract_chat(message)
            if pending == "source":
                saved = set_source(chat, path)
                result = f"✅ Source disimpan: {saved['chat']}"
            else:
                saved = add_destination(chat, None, path)
                result = f"✅ Destination ditambah: {saved['chat']}"
            _PENDING.pop(user_id, None)
            _request_mode(config, "run")
            await message.reply_text(result, reply_markup=_menu())
        except Exception as exc:
            await message.reply_text(
                f"Tak berjaya: {exc}\n\nCuba hantar semula atau tekan Menu.",
                reply_markup=_back_menu(),
            )

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
