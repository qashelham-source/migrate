from __future__ import annotations

import asyncio
import json
import uuid
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from pyrogram import Client

from app.config import AppConfig
from app.db import utc_now
from app.telegram_client import (
    TelegramLimiter,
    message_file_unique_id,
    message_is_empty,
    message_media_type,
    resolve_chat,
)


_RESULT_FILE = "destination_duplicate_cleanup.json"
_SUPPORTED_MEDIA_TYPES = {"photo", "video", "document", "audio", "voice"}


@dataclass(frozen=True)
class DestinationDuplicateGroup:
    """Exact duplicate media found in one destination history."""

    dest_chat_id: str
    dest_title: str
    dest_topic_id: int | None
    file_unique_id: str
    media_type: str
    kept_message_id: int
    duplicate_message_ids: tuple[int, ...]

    @property
    def message_count(self) -> int:
        return len(self.duplicate_message_ids)

    def as_dict(self) -> dict[str, Any]:
        return {
            "dest_chat_id": self.dest_chat_id,
            "dest_title": self.dest_title,
            "dest_topic_id": self.dest_topic_id,
            "file_unique_id": self.file_unique_id,
            "media_type": self.media_type,
            "kept_message_id": self.kept_message_id,
            "duplicate_message_ids": list(self.duplicate_message_ids),
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "DestinationDuplicateGroup | None":
        try:
            duplicate_ids = tuple(int(item) for item in value.get("duplicate_message_ids") or [])
            kept_message_id = int(value["kept_message_id"])
        except (KeyError, TypeError, ValueError):
            return None
        if (
            not duplicate_ids
            or kept_message_id <= 0
            or any(message_id <= 0 for message_id in duplicate_ids)
        ):
            return None
        topic_id = value.get("dest_topic_id")
        try:
            normalized_topic = int(topic_id) if topic_id is not None else None
        except (TypeError, ValueError):
            return None
        file_unique_id = str(value.get("file_unique_id") or "").strip()
        dest_chat_id = str(value.get("dest_chat_id") or "").strip()
        if not file_unique_id or not dest_chat_id:
            return None
        return cls(
            dest_chat_id=dest_chat_id,
            dest_title=str(value.get("dest_title") or dest_chat_id),
            dest_topic_id=normalized_topic,
            file_unique_id=file_unique_id,
            media_type=str(value.get("media_type") or "media"),
            kept_message_id=kept_message_id,
            duplicate_message_ids=tuple(sorted(set(duplicate_ids))),
        )


@dataclass(frozen=True)
class DestinationDuplicatePlan:
    """Persisted destination-history scan output shared with the admin bot."""

    state: str
    scan_id: str
    requested_at: str | None = None
    started_at: str | None = None
    completed_at: str | None = None
    scanned_message_count: int = 0
    media_message_count: int = 0
    deleted_message_count: int = 0
    groups: tuple[DestinationDuplicateGroup, ...] = ()
    error: str | None = None

    @property
    def group_count(self) -> int:
        return len(self.groups)

    @property
    def message_count(self) -> int:
        return sum(group.message_count for group in self.groups)

    def as_dict(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "scan_id": self.scan_id,
            "requested_at": self.requested_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "scanned_message_count": self.scanned_message_count,
            "media_message_count": self.media_message_count,
            "deleted_message_count": self.deleted_message_count,
            "groups": [group.as_dict() for group in self.groups],
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "DestinationDuplicatePlan | None":
        state = str(value.get("state") or "").strip().lower()
        scan_id = str(value.get("scan_id") or "").strip()
        if state not in {"pending", "running", "ready", "failed", "cancelled", "completed"} or not scan_id:
            return None
        groups: list[DestinationDuplicateGroup] = []
        raw_groups = value.get("groups") or []
        if not isinstance(raw_groups, list):
            return None
        for raw_group in raw_groups:
            if not isinstance(raw_group, dict):
                return None
            group = DestinationDuplicateGroup.from_dict(raw_group)
            if group is None:
                return None
            groups.append(group)

        def count(name: str) -> int:
            try:
                return max(0, int(value.get(name) or 0))
            except (TypeError, ValueError):
                return 0

        return cls(
            state=state,
            scan_id=scan_id,
            requested_at=_optional_text(value.get("requested_at")),
            started_at=_optional_text(value.get("started_at")),
            completed_at=_optional_text(value.get("completed_at")),
            scanned_message_count=count("scanned_message_count"),
            media_message_count=count("media_message_count"),
            deleted_message_count=count("deleted_message_count"),
            groups=tuple(groups),
            error=_optional_text(value.get("error")),
        )


@dataclass(frozen=True)
class _HistoryRecord:
    message_id: int
    media_type: str


class DestinationScanCancelled(Exception):
    """Internal signal used to leave no deletable plan after a stop request."""


def _optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _plan_path(config: AppConfig) -> Path:
    path = config.queue.db_path.parent / _RESULT_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _save_plan(config: AppConfig, plan: DestinationDuplicatePlan) -> DestinationDuplicatePlan:
    path = _plan_path(config)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(plan.as_dict(), ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    temporary.replace(path)
    return plan


def request_destination_duplicate_scan(config: AppConfig) -> DestinationDuplicatePlan:
    """Queue a read-only destination-history audit for the manager session."""

    return _save_plan(
        config,
        DestinationDuplicatePlan(
            state="pending",
            scan_id=uuid.uuid4().hex,
            requested_at=utc_now(),
        ),
    )


def load_destination_duplicate_plan(config: AppConfig) -> DestinationDuplicatePlan | None:
    try:
        raw = json.loads(_plan_path(config).read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return None
    return DestinationDuplicatePlan.from_dict(raw) if isinstance(raw, dict) else None


def complete_destination_duplicate_cleanup(
    config: AppConfig,
    plan: DestinationDuplicatePlan,
    *,
    deleted_message_count: int,
    failures: Iterable[str] = (),
) -> DestinationDuplicatePlan:
    """Invalidate a consumed preview so deletion can never run from stale IDs."""

    failure_text = "; ".join(str(item) for item in failures if item)[:1000] or None
    return _save_plan(
        config,
        DestinationDuplicatePlan(
            state="completed",
            scan_id=plan.scan_id,
            requested_at=plan.requested_at,
            started_at=plan.started_at,
            completed_at=utc_now(),
            scanned_message_count=plan.scanned_message_count,
            media_message_count=plan.media_message_count,
            deleted_message_count=max(0, int(deleted_message_count)),
            error=failure_text,
        ),
    )


def _topic_id(message: Any) -> int | None:
    for name in ("reply_to_top_message_id", "reply_to_message_id"):
        value = getattr(message, name, None)
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            continue
    reply_to = getattr(message, "reply_to_message", None)
    try:
        return int(getattr(reply_to, "id", None)) if reply_to is not None else None
    except (TypeError, ValueError):
        return None


def _matches_configured_topic(message: Any, configured_topic_id: int | None) -> bool:
    return configured_topic_id is None or _topic_id(message) == int(configured_topic_id)


def _build_groups(
    records: dict[tuple[str, int, str], list[_HistoryRecord]],
    destinations: dict[tuple[str, int], tuple[str, int | None]],
) -> tuple[DestinationDuplicateGroup, ...]:
    groups: list[DestinationDuplicateGroup] = []
    for (chat_id, topic_key, file_unique_id), entries in records.items():
        ordered = sorted(entries, key=lambda entry: entry.message_id)
        if len(ordered) < 2:
            continue
        kept = ordered[0]
        duplicate_ids = tuple(entry.message_id for entry in ordered[1:])
        title, topic_id = destinations[(chat_id, topic_key)]
        groups.append(
            DestinationDuplicateGroup(
                dest_chat_id=chat_id,
                dest_title=title,
                dest_topic_id=topic_id,
                file_unique_id=file_unique_id,
                media_type=kept.media_type,
                kept_message_id=kept.message_id,
                duplicate_message_ids=duplicate_ids,
            )
        )
    return tuple(
        sorted(
            groups,
            key=lambda group: (
                group.dest_title.casefold(),
                int(group.dest_topic_id or 0),
                group.kept_message_id,
            ),
        )
    )


async def scan_destination_duplicate_history(
    config: AppConfig,
    reader: Client,
    limiter: TelegramLimiter,
    stop_event: asyncio.Event,
) -> DestinationDuplicatePlan:
    """Read configured destination histories and persist exact duplicate groups.

    This deliberately runs inside the single manager user session.  Opening a
    second Pyrogram client against that session caused session-lock/revocation
    problems in the past, while bot accounts cannot reliably enumerate channel
    history.
    """

    prior = load_destination_duplicate_plan(config)
    plan = DestinationDuplicatePlan(
        state="running",
        scan_id=prior.scan_id if prior and prior.state == "pending" else uuid.uuid4().hex,
        requested_at=prior.requested_at if prior and prior.state == "pending" else utc_now(),
        started_at=utc_now(),
    )
    _save_plan(config, plan)

    specs = [
        spec
        for spec in config.destinations
        if str(getattr(spec, "chat", "") or "").strip()
        and "destination_channel_or_-100_id" not in str(getattr(spec, "chat", "")).casefold()
    ]
    if not specs:
        return _save_plan(
            config,
            DestinationDuplicatePlan(
                state="failed",
                scan_id=plan.scan_id,
                requested_at=plan.requested_at,
                started_at=plan.started_at,
                completed_at=utc_now(),
                error="No configured destination is available to scan.",
            ),
        )

    records: dict[tuple[str, int, str], list[_HistoryRecord]] = defaultdict(list)
    destinations: dict[tuple[str, int], tuple[str, int | None]] = {}
    scanned = 0
    media_count = 0
    try:
        for spec in specs:
            if stop_event.is_set():
                raise DestinationScanCancelled
            resolved = await resolve_chat(reader, limiter, spec)
            chat_id = str(resolved.chat_id)
            raw_topic_id = getattr(spec, "topic_id", None)
            configured_topic_id = int(raw_topic_id) if raw_topic_id is not None else None
            topic_key = int(configured_topic_id or 0)
            destinations[(chat_id, topic_key)] = (str(resolved.title or chat_id), configured_topic_id)

            async for message in reader.get_chat_history(resolved.chat_id):
                if stop_event.is_set():
                    raise DestinationScanCancelled
                scanned += 1
                if message_is_empty(message) or not _matches_configured_topic(message, configured_topic_id):
                    continue
                media_type = message_media_type(message)
                file_unique_id = message_file_unique_id(message)
                if media_type not in _SUPPORTED_MEDIA_TYPES or not file_unique_id:
                    continue
                message_id = int(getattr(message, "id", 0) or 0)
                if message_id <= 0:
                    continue
                media_count += 1
                records[(chat_id, topic_key, file_unique_id)].append(
                    _HistoryRecord(message_id=message_id, media_type=media_type)
                )
                if scanned % 100 == 0:
                    _save_plan(
                        config,
                        DestinationDuplicatePlan(
                            state="running",
                            scan_id=plan.scan_id,
                            requested_at=plan.requested_at,
                            started_at=plan.started_at,
                            scanned_message_count=scanned,
                            media_message_count=media_count,
                        ),
                    )
                    await asyncio.sleep(0)

        ready = DestinationDuplicatePlan(
            state="ready",
            scan_id=plan.scan_id,
            requested_at=plan.requested_at,
            started_at=plan.started_at,
            completed_at=utc_now(),
            scanned_message_count=scanned,
            media_message_count=media_count,
            groups=_build_groups(records, destinations),
        )
        return _save_plan(config, ready)
    except DestinationScanCancelled:
        return _save_plan(
            config,
            DestinationDuplicatePlan(
                state="cancelled",
                scan_id=plan.scan_id,
                requested_at=plan.requested_at,
                started_at=plan.started_at,
                completed_at=utc_now(),
                scanned_message_count=scanned,
                media_message_count=media_count,
                error="Scan was stopped before a deletion preview was created.",
            ),
        )
    except Exception as exc:
        return _save_plan(
            config,
            DestinationDuplicatePlan(
                state="failed",
                scan_id=plan.scan_id,
                requested_at=plan.requested_at,
                started_at=plan.started_at,
                completed_at=utc_now(),
                scanned_message_count=scanned,
                media_message_count=media_count,
                error=f"{exc.__class__.__name__}: {str(exc)[:500]}",
            ),
        )
