from __future__ import annotations

import asyncio
import json
import random
import signal
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable

from pyrogram import Client
from pyrogram.errors import FloodWait, PhoneCodeExpired, PhoneCodeInvalid, SessionPasswordNeeded
from pyrogram.types import Message

from app.config import AppConfig, ChatSpec


@dataclass(frozen=True)
class ResolvedChat:
    chat_id: str
    topic_id: int | None
    title: str


class TelegramLimiter:
    def __init__(self, config: AppConfig, logger: Any | None = None) -> None:
        self.config = config
        self.logger = logger
        self._lock = asyncio.Lock()
        self._last_global = 0.0
        self._last_by_operation: dict[str, float] = {}

    async def wait(self, operation: str) -> None:
        async with self._lock:
            now = time.monotonic()
            global_wait = self.config.limits.global_min_delay_seconds - (now - self._last_global)
            op_wait = self.config.limits.delay_for(operation) - (
                now - self._last_by_operation.get(operation, 0.0)
            )
            delay = max(0.0, global_wait, op_wait)
            if delay > 0:
                await asyncio.sleep(delay)

            finished = time.monotonic()
            self._last_global = finished
            self._last_by_operation[operation] = finished

    async def call(
        self,
        operation: str,
        fn: Callable[..., Awaitable[Any]],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        while True:
            await self.wait(operation)
            try:
                return await fn(*args, **kwargs)
            except FloodWait as exc:
                extra_min = self.config.limits.floodwait_extra_min_seconds
                extra_max = self.config.limits.floodwait_extra_max_seconds
                wait = int(exc.value) + random.randint(extra_min, extra_max)
                if self.logger:
                    self.logger.warning("FloodWait from Telegram: sleeping %ss", wait)
                await asyncio.sleep(wait)


def install_stop_handlers(stop_event: asyncio.Event) -> None:
    def request_stop(*_: object) -> None:
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, request_stop)
        except (ValueError, OSError, AttributeError):
            continue


def make_user_client(config: AppConfig) -> Client:
    return Client(
        name=config.telegram.user_session,
        api_id=config.telegram.api_id,
        api_hash=config.telegram.api_hash,
        workdir=str(config.telegram.sessions_dir),
    )


def make_bot_client(config: AppConfig) -> Client | None:
    if not config.telegram.bot_enabled:
        return None
    if not config.telegram.bot_token:
        raise ValueError("telegram.bot.enabled is true, but telegram.bot.token is empty")
    return Client(
        name=config.telegram.bot_session_name,
        api_id=config.telegram.api_id,
        api_hash=config.telegram.api_hash,
        bot_token=config.telegram.bot_token,
        workdir=str(config.telegram.sessions_dir),
    )


def _accounts_path(config: AppConfig) -> Path:
    return config.telegram.sessions_dir / "accounts.json"


def load_accounts(config: AppConfig) -> dict[str, Any]:
    path = _accounts_path(config)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def save_accounts(config: AppConfig, accounts: dict[str, Any]) -> None:
    _accounts_path(config).write_text(json.dumps(accounts, indent=2), encoding="utf-8")


def update_account_cache(config: AppConfig, session_name: str, user: Any) -> None:
    accounts = load_accounts(config)
    accounts[session_name] = {
        "first_name": user.first_name or "",
        "last_name": user.last_name or "",
        "username": user.username or "None",
        "id": user.id,
    }
    save_accounts(config, accounts)


async def interactive_login(config: AppConfig, session_name: str | None = None) -> None:
    session = session_name or input("Session name: ").strip()
    if not session:
        raise ValueError("Session name cannot be empty")

    limiter = TelegramLimiter(config)
    client = Client(
        name=session,
        api_id=config.telegram.api_id,
        api_hash=config.telegram.api_hash,
        workdir=str(config.telegram.sessions_dir),
    )
    await client.connect()
    try:
        phone = input("Phone number with country code: ").strip()
        sent_code = await limiter.call("read", client.send_code, phone)
        code = input("Login code: ").strip()
        try:
            await limiter.call("read", client.sign_in, phone, sent_code.phone_code_hash, code)
        except SessionPasswordNeeded:
            password = input("Two-factor password: ").strip()
            await limiter.call("read", client.sign_in, password=password)

        me = await limiter.call("read", client.get_me)
        update_account_cache(config, session, me)
        print(f"Logged in as {me.first_name} ({me.id}); session saved as {session}.session")
    except PhoneCodeInvalid as exc:
        raise ValueError("Invalid login code") from exc
    except PhoneCodeExpired as exc:
        raise ValueError("Login code expired") from exc
    finally:
        await client.disconnect()


async def resolve_chat(client: Client, limiter: TelegramLimiter, spec: ChatSpec) -> ResolvedChat:
    chat = await limiter.call("resolve", client.get_chat, spec.chat)
    title = chat.title or chat.username or str(chat.id)
    return ResolvedChat(chat_id=str(chat.id), topic_id=spec.topic_id, title=title)


def message_media_type(message: Message) -> str:
    if message.video:
        return "video"
    if message.photo:
        return "photo"
    if message.document:
        return "document"
    if message.animation:
        return "animation"
    if message.audio:
        return "audio"
    if message.voice:
        return "voice"
    if message.video_note:
        return "video_note"
    if message.text or message.caption:
        return "text"
    return "unsupported"


def message_file_size(message: Message) -> int | None:
    media = _message_media_object(message)
    return int(getattr(media, "file_size", 0) or 0) if media else None


def message_unique_key(message: Message) -> str:
    media = _message_media_object(message)
    if media:
        return str(getattr(media, "file_unique_id", None) or getattr(media, "file_id", None) or "")
    return ""


def message_caption(message: Message) -> str | None:
    return message.caption or message.text or None


def message_is_empty(message: Message | None) -> bool:
    return message is None or bool(getattr(message, "empty", False))


def _message_media_object(message: Message) -> Any | None:
    for attr in ("video", "photo", "document", "animation", "audio", "voice", "video_note"):
        media = getattr(message, attr, None)
        if media:
            return media
    return None
