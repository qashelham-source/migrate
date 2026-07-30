from __future__ import annotations

import asyncio
import json
from contextlib import suppress
import os
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
    chat_id: int | str
    topic_id: int | None
    title: str


class TelegramLimiter:
    """Rate-limit Telegram operations without blocking unrelated pipeline stages.

    Global and per-operation timestamps are reserved while holding the scheduling
    lock, but the actual sleep happens after releasing it. An upload cooldown can
    therefore no longer prevent reads, downloads, or verification from scheduling.
    """

    def __init__(self, config: AppConfig, logger: Any | None = None) -> None:
        self.config = config
        self.logger = logger
        self._lock = asyncio.Lock()
        self._upload_lock = asyncio.Lock()
        self._last_global = 0.0
        self._last_by_operation: dict[str, float] = {}
        self._floodwait_until_by_operation: dict[str, float] = {}
        self._floodwait_events_by_operation: dict[str, int] = {}
        self._floodwait_seconds_by_operation: dict[str, int] = {}

    async def wait(self, operation: str) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                ready_at = max(
                    self._floodwait_until_by_operation.get(operation, 0.0),
                    self._last_global + self.config.limits.global_min_delay_seconds,
                    self._last_by_operation.get(operation, 0.0)
                    + self.config.limits.delay_for(operation),
                )
                delay = ready_at - now
                if delay <= 0:
                    self._last_global = now
                    self._last_by_operation[operation] = now
                    return
            await asyncio.sleep(delay)

    def floodwait_snapshot(self) -> dict[str, Any]:
        """Return lightweight limiter telemetry for status pages and diagnostics."""
        now = time.monotonic()
        operations = sorted(
            set(self._floodwait_until_by_operation)
            | set(self._floodwait_events_by_operation)
            | set(self._floodwait_seconds_by_operation)
        )
        details = {
            operation: {
                "events": self._floodwait_events_by_operation.get(operation, 0),
                "total_wait_seconds": self._floodwait_seconds_by_operation.get(operation, 0),
                "cooldown_remaining_seconds": max(
                    0,
                    int(self._floodwait_until_by_operation.get(operation, 0.0) - now),
                ),
            }
            for operation in operations
        }
        return {
            "floodwait_events": sum(item["events"] for item in details.values()),
            "floodwait_total_seconds": sum(item["total_wait_seconds"] for item in details.values()),
            "floodwait_operations": details,
        }

    async def _call_with_retry(
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
                wait = max(1, int(exc.value)) + random.randint(extra_min, extra_max)
                async with self._lock:
                    self._floodwait_until_by_operation[operation] = max(
                        self._floodwait_until_by_operation.get(operation, 0.0),
                        time.monotonic() + wait,
                    )
                    self._floodwait_events_by_operation[operation] = (
                        self._floodwait_events_by_operation.get(operation, 0) + 1
                    )
                    self._floodwait_seconds_by_operation[operation] = (
                        self._floodwait_seconds_by_operation.get(operation, 0) + wait
                    )
                if self.logger:
                    self.logger.warning(
                        "FloodWait from Telegram during %s: pausing only %s operations for %ss",
                        operation,
                        operation,
                        wait,
                    )

    async def call(
        self,
        operation: str,
        fn: Callable[..., Awaitable[Any]],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        # Pyrogram can upload several SaveBigFilePart chunks concurrently. Telegram may
        # throttle every chunk at once, creating a traceback storm and preventing useful
        # progress. Serialize top-level upload calls while allowing unrelated reads,
        # downloads and verification calls to continue under their own cooldowns.
        if operation == "upload":
            async with self._upload_lock:
                return await self._call_with_retry(operation, fn, *args, **kwargs)
        return await self._call_with_retry(operation, fn, *args, **kwargs)


def install_stop_handlers(stop_event: asyncio.Event) -> None:
    def request_stop(*_: object) -> None:
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, request_stop)
        except (ValueError, OSError, AttributeError):
            continue


async def start_client_with_floodwait(
    client: Client,
    *,
    label: str,
    logger: Any | None = None,
) -> None:
    """Start a Pyrogram client without converting Telegram's required wait into a restart loop."""
    while True:
        try:
            await client.start()
            return
        except FloodWait as exc:
            wait_seconds = max(1, int(exc.value)) + 1
            if logger:
                logger.warning(
                    "Telegram asked %s to wait %ss during startup; keeping this service alive",
                    label,
                    wait_seconds,
                )
            # A failed start can leave a partial connection behind. Clean it up
            # before waiting, but never let cleanup hide the original FloodWait.
            with suppress(Exception):
                await client.stop()
            await asyncio.sleep(wait_seconds)


def make_user_client(config: AppConfig) -> Client:
    return Client(
        name=config.telegram.user_session,
        api_id=config.telegram.api_id,
        api_hash=config.telegram.api_hash,
        workdir=str(config.telegram.sessions_dir),
        max_concurrent_transmissions=1,
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
        # The control-panel process is the only client that should receive
        # button callbacks. This uploader client only sends media.
        no_updates=True,
        max_concurrent_transmissions=1,
    )


def _accounts_path(config: AppConfig) -> Path:
    return config.telegram.sessions_dir / "accounts.json"


def load_accounts(config: AppConfig) -> dict[str, Any]:
    path = _accounts_path(config)
    if not path.exists():
        return {}
    try:
        decoded = json.loads(path.read_text(encoding="utf-8"))
        return decoded if isinstance(decoded, dict) else {}
    except (json.JSONDecodeError, OSError):
        corrupt = path.with_name(f"{path.name}.corrupt-{int(time.time())}")
        try:
            path.replace(corrupt)
        except OSError:
            pass
        return {}


def save_accounts(config: AppConfig, accounts: dict[str, Any]) -> None:
    path = _accounts_path(config)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.unlink(missing_ok=True)
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(accounts, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


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
        max_concurrent_transmissions=1,
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
            await limiter.call("read", client.check_password, password)

        me = await limiter.call("read", client.get_me)
        update_account_cache(config, session, me)
        print(f"Logged in as {me.first_name} ({me.id}); session saved as {session}.session")
    except PhoneCodeInvalid as exc:
        raise ValueError("Invalid login code") from exc
    except PhoneCodeExpired as exc:
        raise ValueError("Login code expired") from exc
    finally:
        await client.disconnect()


def telegram_peer(value: int | str) -> int | str:
    """Return numeric Telegram IDs as int; keep usernames and links as strings."""
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if text.lstrip("-").isdigit():
        return int(text)
    return text


async def resolve_chat(client: Client, limiter: TelegramLimiter, spec: ChatSpec) -> ResolvedChat:
    chat = await limiter.call("resolve", client.get_chat, telegram_peer(spec.chat))
    title = chat.title or chat.username or str(chat.id)
    return ResolvedChat(chat_id=int(chat.id), topic_id=spec.topic_id, title=title)


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
