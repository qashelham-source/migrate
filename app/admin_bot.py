from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from pyrogram import Client, filters, idle
from pyrogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from app.config import AppConfig
from app.destination_manager import (
    add_destination,
    get_sources,
    list_destinations,
    remove_destination,
    set_source,
)
from app.telegram_client import load_accounts


_PENDING: dict[int, str] = {}


def _menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📥 Tukar Source", callback_data="source:set")],
            [
                InlineKeyboardButton("➕ Tambah Destination", callback_data="dest:add"),
                InlineKeyboardButton("🗑 Buang Destination", callback_data="dest:remove"),
            ],
            [InlineKeyboardButton("📋 Lihat Setting", callback_data="settings:view")],
            [InlineKeyboardButton("▶️ Run Sekarang", callback_data="run:now")],
        ]
    )


def _back_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Menu", callback_data="menu")]])


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

    @app.on_message(filters.private & filters.command("start"))
    async def start_handler(_: Client, message: Message) -> None:
        user_id = message.from_user.id if message.from_user else None
        if not _is_authorized(config, user_id):
            await reject(message=message)
            return
        _PENDING.pop(int(user_id), None)
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
            _touch_run_now(config)
            await query.answer("Migration akan mula pada cycle seterusnya.", show_alert=True)
            return

        await query.answer()

    @app.on_message(filters.private, group=1)
    async def input_handler(_: Client, message: Message) -> None:
        user_id = message.from_user.id if message.from_user else None
        if not _is_authorized(config, user_id):
            await reject(message=message)
            return
        assert user_id is not None

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
            _touch_run_now(config)
            await message.reply_text(result, reply_markup=_menu())
        except Exception as exc:
            await message.reply_text(f"Tak berjaya: {exc}\n\nCuba hantar semula atau tekan Menu.", reply_markup=_back_menu())

    await app.start()
    try:
        await idle()
    finally:
        await app.stop()
