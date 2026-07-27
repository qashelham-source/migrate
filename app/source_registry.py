from __future__ import annotations

import asyncio
from typing import Any

from pyrogram import Client

from app.config import AppConfig
from app.control import write_status
from app.queue import MessageQueue


SUPPORTED_CHAT_TYPES = {"channel", "supergroup", "group"}


def normalized_chat_type(chat: Any) -> str:
    value = getattr(getattr(chat, "type", None), "value", None)
    if value is None:
        value = str(getattr(chat, "type", "unknown"))
    return str(value).split(".")[-1].lower()


async def refresh_source_registry(
    config: AppConfig,
    queue: MessageQueue,
    reader: Client,
    stop_event: asyncio.Event,
    logger: Any | None = None,
) -> int:
    """Index all accessible channel/group dialogs without changing configured sources."""
    discovered = 0
    write_status(
        config,
        "starting",
        message="Mengemaskini Source Registry daripada dialog Telegram...",
    )
    try:
        async for dialog in reader.get_dialogs():
            if stop_event.is_set():
                break
            chat = getattr(dialog, "chat", None)
            if chat is None:
                continue
            chat_type = normalized_chat_type(chat)
            if chat_type not in SUPPORTED_CHAT_TYPES:
                continue
            top_message = getattr(dialog, "top_message", None)
            latest_id = int(getattr(top_message, "id", 0) or 0) or None
            queue.register_source(
                source_chat_id=int(chat.id),
                title=str(getattr(chat, "title", None) or getattr(chat, "username", None) or chat.id),
                username=getattr(chat, "username", None),
                chat_type=chat_type,
                latest_seen_message_id=latest_id,
                access_status="ok",
            )
            discovered += 1
    except Exception as exc:
        if logger:
            logger.warning("Source Registry refresh stopped early: %s", exc)
        write_status(
            config,
            "starting",
            message="Source Registry dikemaskini sebahagian; dialog Telegram berhenti awal.",
            source_registry_count=discovered,
            source_registry_error=f"{exc.__class__.__name__}: {exc}"[:1000],
        )
        return discovered

    if logger:
        logger.info("Source Registry refreshed with %s accessible channel/group dialog(s)", discovered)
    write_status(
        config,
        "starting",
        message="Source Registry selesai dikemaskini.",
        source_registry_count=discovered,
    )
    return discovered
