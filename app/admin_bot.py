from __future__ import annotations

import asyncio
import os
from contextlib import suppress
from pathlib import Path
from typing import Any

from pyrogram import Client, filters, idle
from pyrogram.errors import MessageNotModified
from pyrogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

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
            [InlineKeyboardButton("▶️ Run Sekarang", callback_data="run:now")],
        ]
    )


def _back_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Menu", callback_data="menu")]])


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


def _touch_run_now(config: AppConfig) -> None:
    path = config.queue.db_path.parent / "run_now"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()


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
        "idle": "✅ Idle",
        "error": "❌ Error",
    }

    lines = ["📊 Live Migration Status", "", f"Status: {labels.get(phase, phase.title())}"]
    message = status.get("message")
    if message:
        lines.append(str(message))

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
            await query.message.edit_text("Pilih destination yang nak dibuang:", reply_markup=InlineKeyboardMarkup(rows))
            await query.answer()
            return

        if data.startswith("dest:delete:"):
            try:
                index = int(data.rsplit(":", 1)[1])
                removed = remove_destination(index, path)
                _touch_run_now(config)
                await query.answer(f"Dibuang: {removed.get('chat', '')}", show_alert=True)
                await query.message.edit_text(_settings_text(path), reply_markup=_back_menu())
            except Exception as exc:
                await query.answer(str(exc), show_alert=True)
            return

        if data == "run:now":
            clear_stop(config)
            _touch_run_now(config)
            await query.answer("Migration akan mula secepat mungkin.", show_alert=True)
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
            clear_stop(config)
            _touch_run_now(config)
            await message.reply_text(result, reply_markup=_menu())
        except Exception as exc:
            await message.reply_text(f"Tak berjaya: {exc}\n\nCuba hantar semula atau tekan Menu.", reply_markup=_back_menu())

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
