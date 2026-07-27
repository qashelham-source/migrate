from __future__ import annotations

import shutil
from datetime import datetime, timezone
from typing import Any

from pyrogram import Client

from app.advanced import save_health_report
from app.config import AppConfig
from app.control import write_status
from app.queue import MessageQueue
from app.telegram_client import TelegramLimiter, message_is_empty, resolve_chat


def _status_name(value: Any) -> str:
    text = str(value or "").lower()
    return text.rsplit(".", 1)[-1]


def _member_can_post(member: Any) -> tuple[bool | None, str]:
    status = _status_name(getattr(member, "status", None))
    if status in {"owner", "creator"}:
        return True, status
    if status != "administrator":
        return False, status or "unknown"
    privileges = getattr(member, "privileges", None)
    can_post = getattr(privileges, "can_post_messages", None) if privileges is not None else None
    if can_post is False:
        return False, "administrator without post permission"
    return True, "administrator"


async def _has_readable_history(client: Client, chat_id: int | str) -> bool:
    async for message in client.get_chat_history(chat_id, limit=1):
        return not message_is_empty(message)
    return True


async def run_health_check(
    config: AppConfig,
    reader: Client,
    writer: Client,
    limiter: TelegramLimiter,
    queue: MessageQueue,
    *,
    reader_me: Any,
    writer_me: Any,
    logger: Any | None = None,
) -> dict[str, Any]:
    write_status(config, "health_check", message="Menjalankan pre-flight health check...")
    checks: list[dict[str, Any]] = []

    def add(name: str, status: str, detail: str, **extra: Any) -> None:
        item: dict[str, Any] = {"name": name, "status": status, "detail": detail}
        item.update(extra)
        checks.append(item)

    session_path = config.telegram.sessions_dir / f"{config.telegram.user_session}.session"
    add(
        "user_session",
        "pass" if session_path.exists() else "fail",
        f"{reader_me.first_name or 'Telegram user'} ({reader_me.id})"
        if session_path.exists()
        else f"Session file tidak dijumpai: {session_path.name}",
    )

    if writer is reader:
        add("writer", "warn", "Upload menggunakan user session")
    else:
        add("writer", "pass", f"{writer_me.first_name or 'Uploader bot'} ({writer_me.id})")

    sources = [spec for spec in config.sources if str(spec.chat or "").strip()]
    if not sources:
        add("source", "fail", "Source belum ditetapkan")
    for index, spec in enumerate(sources, start=1):
        try:
            resolved = await resolve_chat(reader, limiter, spec)
            readable = await limiter.call("read", _has_readable_history, reader, resolved.chat_id)
            add(
                f"source_{index}",
                "pass" if readable else "warn",
                f"{resolved.title} ({resolved.chat_id})" + (" boleh dibaca" if readable else " tiada history yang boleh disahkan"),
                chat_id=resolved.chat_id,
            )
        except Exception as exc:
            add(f"source_{index}", "fail", f"{spec.chat}: {exc.__class__.__name__}: {exc}")

    destinations = [spec for spec in config.destinations if str(spec.chat or "").strip()]
    if not destinations:
        add("destination", "fail", "Destination belum ditetapkan")
    for index, spec in enumerate(destinations, start=1):
        sending_client = writer
        sender = writer_me
        route = "bot" if writer is not reader else "user"
        try:
            resolved = await resolve_chat(sending_client, limiter, spec)
        except Exception as writer_error:
            if writer is reader:
                add(
                    f"destination_{index}",
                    "fail",
                    f"{spec.chat}: {writer_error.__class__.__name__}: {writer_error}",
                )
                continue
            try:
                resolved = await resolve_chat(reader, limiter, spec)
                sending_client = reader
                sender = reader_me
                route = "user fallback"
            except Exception as reader_error:
                add(
                    f"destination_{index}",
                    "fail",
                    f"{spec.chat}: bot={writer_error}; user={reader_error}",
                )
                continue

        try:
            member = await limiter.call(
                "resolve",
                sending_client.get_chat_member,
                resolved.chat_id,
                int(sender.id),
            )
            can_post, permission_detail = _member_can_post(member)
            status = "pass" if can_post is True else "fail" if can_post is False else "warn"
            detail = f"{resolved.title} ({resolved.chat_id}) melalui {route}: {permission_detail}"
            add(
                f"destination_{index}",
                status,
                detail,
                chat_id=resolved.chat_id,
                route=route,
            )
        except Exception as exc:
            add(
                f"destination_{index}",
                "warn",
                f"{resolved.title} ({resolved.chat_id}) boleh resolve melalui {route}, permission tidak dapat disahkan: {exc}",
                chat_id=resolved.chat_id,
                route=route,
            )

    usage = shutil.disk_usage(config.base_dir)
    free_gb = usage.free / (1024**3)
    free_ratio = usage.free / usage.total if usage.total else 0.0
    disk_ok = usage.free >= 1024**3 and free_ratio >= 0.05
    add(
        "storage",
        "pass" if disk_ok else "fail",
        f"{free_gb:.2f} GB kosong ({free_ratio * 100:.1f}%)",
        free_bytes=usage.free,
        total_bytes=usage.total,
    )

    counts = queue.counts_by_status()
    add(
        "queue",
        "pass",
        (
            f"pending={counts.get('pending', 0)}, completed={counts.get('copied', 0)}, "
            f"failed={counts.get('failed', 0)}, skipped={counts.get('skipped', 0)}"
        ),
        counts=counts,
    )

    overall = "fail" if any(item["status"] == "fail" for item in checks) else "warn" if any(
        item["status"] == "warn" for item in checks
    ) else "pass"
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "overall": overall,
        "checks": checks,
        "queue": counts,
    }
    save_health_report(config, report)
    write_status(
        config,
        "health_complete",
        message="Health check selesai.",
        health=overall,
        checks=len(checks),
        failed=sum(1 for item in checks if item["status"] == "fail"),
        warnings=sum(1 for item in checks if item["status"] == "warn"),
    )
    if logger:
        logger.info("Health check complete: overall=%s checks=%s", overall, len(checks))
    return report
