from __future__ import annotations

import asyncio
import hashlib
import json
import tempfile
import uuid
from collections import defaultdict
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Iterable

from pyrogram import Client

from app.config import AppConfig
from app.db import utc_now
from app.telegram_client import (
    TelegramLimiter,
    message_file_unique_id,
    message_file_size,
    message_is_empty,
    message_media_type,
    resolve_chat,
    telegram_peer,
)


_RESULT_FILE = "destination_duplicate_cleanup.json"
_SUPPORTED_MEDIA_TYPES = {"photo", "video", "document", "audio", "voice"}
_PLAN_STATES = {
    "pending",
    "running",
    "ready",
    "delete_pending",
    "deleting",
    "failed",
    "cancelled",
    "delete_failed",
    "delete_cancelled",
    "completed",
}
_DELETE_BATCH_SIZE = 100


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
    match_kind: str = "telegram_fingerprint"

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
            "match_kind": self.match_kind,
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
        match_kind = str(value.get("match_kind") or "telegram_fingerprint").strip().lower()
        if match_kind not in {"telegram_fingerprint", "content_sha256"}:
            return None
        return cls(
            dest_chat_id=dest_chat_id,
            dest_title=str(value.get("dest_title") or dest_chat_id),
            dest_topic_id=normalized_topic,
            file_unique_id=file_unique_id,
            media_type=str(value.get("media_type") or "media"),
            kept_message_id=kept_message_id,
            duplicate_message_ids=tuple(sorted(set(duplicate_ids))),
            match_kind=match_kind,
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
    scan_mode: str = "fingerprint"
    content_candidate_count: int = 0
    content_hashed_count: int = 0
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
            "scan_mode": self.scan_mode,
            "content_candidate_count": self.content_candidate_count,
            "content_hashed_count": self.content_hashed_count,
            "groups": [group.as_dict() for group in self.groups],
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "DestinationDuplicatePlan | None":
        state = str(value.get("state") or "").strip().lower()
        scan_id = str(value.get("scan_id") or "").strip()
        if state not in _PLAN_STATES or not scan_id:
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

        scan_mode = str(value.get("scan_mode") or "fingerprint").strip().lower()
        if scan_mode not in {"fingerprint", "content"}:
            return None
        return cls(
            state=state,
            scan_id=scan_id,
            requested_at=_optional_text(value.get("requested_at")),
            started_at=_optional_text(value.get("started_at")),
            completed_at=_optional_text(value.get("completed_at")),
            scanned_message_count=count("scanned_message_count"),
            media_message_count=count("media_message_count"),
            deleted_message_count=count("deleted_message_count"),
            scan_mode=scan_mode,
            content_candidate_count=count("content_candidate_count"),
            content_hashed_count=count("content_hashed_count"),
            groups=tuple(groups),
            error=_optional_text(value.get("error")),
        )


@dataclass(frozen=True)
class _HistoryRecord:
    message_id: int
    media_type: str


@dataclass(frozen=True)
class _ContentCandidate:
    message_id: int
    media_type: str


class DestinationScanCancelled(Exception):
    """Internal signal used to leave no deletable plan after a stop request."""


class DestinationCleanupCancelled(Exception):
    """Internal signal used to stop an in-progress destructive cleanup safely."""


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


def request_destination_duplicate_scan(
    config: AppConfig,
    *,
    scan_mode: str = "fingerprint",
) -> DestinationDuplicatePlan:
    """Queue a read-only destination-history audit for the manager session."""

    normalized_mode = str(scan_mode).strip().lower()
    if normalized_mode not in {"fingerprint", "content"}:
        raise ValueError(f"Unsupported destination duplicate scan mode: {scan_mode}")

    return _save_plan(
        config,
        DestinationDuplicatePlan(
            state="pending",
            scan_id=uuid.uuid4().hex,
            requested_at=utc_now(),
            scan_mode=normalized_mode,
        ),
    )


def request_destination_duplicate_cleanup(config: AppConfig) -> DestinationDuplicatePlan | None:
    """Queue a previously reviewed cleanup for the manager user session.

    The control bot intentionally does not call Telegram's delete API.  It
    cannot reliably resolve private channel peers.  Instead it records the
    approval here and wakes the long-lived manager session that performed the
    scan and already has the destination peer available.
    """

    plan = load_destination_duplicate_plan(config)
    if plan is None or plan.state != "ready" or not plan.message_count:
        return None
    return _save_plan(
        config,
        replace(
            plan,
            state="delete_pending",
            completed_at=None,
            deleted_message_count=0,
            error=None,
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
            scan_mode=plan.scan_mode,
            content_candidate_count=plan.content_candidate_count,
            content_hashed_count=plan.content_hashed_count,
            error=failure_text,
        ),
    )


def _chunks(values: tuple[int, ...], size: int = _DELETE_BATCH_SIZE) -> Iterable[tuple[int, ...]]:
    for index in range(0, len(values), size):
        yield values[index:index + size]


def _delete_failure(group: DestinationDuplicateGroup, message_ids: tuple[int, ...], exc: Exception) -> str:
    shown_ids = ", ".join(str(item) for item in message_ids[:10])
    remainder = len(message_ids) - 10
    ids = f"{shown_ids}, … (+{remainder})" if remainder > 0 else shown_ids
    return f"{group.dest_title} ({ids}): {exc.__class__.__name__}: {str(exc)[:120]}"


async def delete_destination_duplicate_history(
    config: AppConfig,
    reader: Client,
    limiter: TelegramLimiter,
    stop_event: asyncio.Event,
) -> DestinationDuplicatePlan:
    """Delete the approved IDs through the same manager session that scanned them.

    Telegram message IDs never get reused inside a chat.  The persisted plan
    therefore remains an exact, bounded deletion request; it never scans or
    deletes source posts during cleanup.  A newly requested scan always
    replaces the plan before another cleanup can be approved.
    """

    prior = load_destination_duplicate_plan(config)
    if prior is None:
        return _save_plan(
            config,
            DestinationDuplicatePlan(
                state="delete_failed",
                scan_id=uuid.uuid4().hex,
                requested_at=utc_now(),
                completed_at=utc_now(),
                error="No reviewed destination duplicate scan is available.",
            ),
        )
    if prior.state != "delete_pending":
        return prior

    plan = _save_plan(
        config,
        replace(
            prior,
            state="deleting",
            completed_at=None,
            deleted_message_count=0,
            error=None,
        ),
    )
    deleted_messages = 0
    failures: list[str] = []
    resolved_peers: dict[str, int | str] = {}

    try:
        for group in plan.groups:
            if stop_event.is_set():
                raise DestinationCleanupCancelled

            peer = resolved_peers.get(group.dest_chat_id)
            if peer is None:
                try:
                    chat = await limiter.call(
                        "resolve",
                        reader.get_chat,
                        telegram_peer(group.dest_chat_id),
                    )
                    peer = telegram_peer(getattr(chat, "id", group.dest_chat_id))
                    resolved_peers[group.dest_chat_id] = peer
                except Exception as exc:
                    failures.append(
                        _delete_failure(group, group.duplicate_message_ids, exc)
                    )
                    continue

            for message_ids in _chunks(group.duplicate_message_ids):
                if stop_event.is_set():
                    raise DestinationCleanupCancelled
                try:
                    await limiter.call(
                        "delete",
                        reader.delete_messages,
                        chat_id=peer,
                        message_ids=list(message_ids),
                        revoke=True,
                    )
                except Exception as exc:
                    failures.append(_delete_failure(group, message_ids, exc))
                else:
                    deleted_messages += len(message_ids)

                plan = _save_plan(
                    config,
                    replace(
                        plan,
                        state="deleting",
                        deleted_message_count=deleted_messages,
                    ),
                )
                await asyncio.sleep(0)

        return complete_destination_duplicate_cleanup(
            config,
            plan,
            deleted_message_count=deleted_messages,
            failures=failures,
        )
    except DestinationCleanupCancelled:
        return _save_plan(
            config,
            DestinationDuplicatePlan(
                state="delete_cancelled",
                scan_id=plan.scan_id,
                requested_at=plan.requested_at,
                started_at=plan.started_at,
                completed_at=utc_now(),
                scanned_message_count=plan.scanned_message_count,
                media_message_count=plan.media_message_count,
                deleted_message_count=deleted_messages,
                error="Cleanup was stopped. Run a fresh scan before deleting again.",
            ),
        )
    except Exception as exc:
        return _save_plan(
            config,
            DestinationDuplicatePlan(
                state="delete_failed",
                scan_id=plan.scan_id,
                requested_at=plan.requested_at,
                started_at=plan.started_at,
                completed_at=utc_now(),
                scanned_message_count=plan.scanned_message_count,
                media_message_count=plan.media_message_count,
                deleted_message_count=deleted_messages,
                error=f"{exc.__class__.__name__}: {str(exc)[:500]}",
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
    *,
    match_kind: str = "telegram_fingerprint",
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
                match_kind=match_kind,
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


def _content_candidate_key(
    chat_id: str,
    topic_key: int,
    media_type: str,
    file_size: int,
) -> tuple[str, int, str, int]:
    return chat_id, topic_key, media_type, max(0, int(file_size))


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


async def _download_content_hash(
    message: Any,
    limiter: TelegramLimiter,
    directory: Path,
) -> str:
    target = directory / f"message-{int(getattr(message, 'id', 0) or 0)}-{uuid.uuid4().hex}"
    downloaded = await limiter.call("download", message.download, file_name=str(target))
    path = Path(downloaded or target)
    try:
        if not path.is_file():
            raise OSError("Telegram did not return a downloadable media file")
        return await asyncio.to_thread(_hash_file, path)
    finally:
        path.unlink(missing_ok=True)
        if path != target:
            target.unlink(missing_ok=True)


async def scan_destination_content_duplicates(
    config: AppConfig,
    reader: Client,
    limiter: TelegramLimiter,
    stop_event: asyncio.Event,
) -> DestinationDuplicatePlan:
    """Find byte-identical destination media when Telegram fingerprints differ.

    The fast detector stays the default.  This deeper detector only downloads
    media whose canonical type and exact byte size already match, then compares
    SHA-256 checksums.  It never treats a merely similar image/video as safe to
    delete.
    """

    prior = load_destination_duplicate_plan(config)
    use_prior = bool(prior and prior.state == "pending" and prior.scan_mode == "content")
    plan = DestinationDuplicatePlan(
        state="running",
        scan_id=prior.scan_id if use_prior else uuid.uuid4().hex,
        requested_at=prior.requested_at if use_prior else utc_now(),
        started_at=utc_now(),
        scan_mode="content",
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
            replace(
                plan,
                state="failed",
                completed_at=utc_now(),
                error="No configured destination is available to scan.",
            ),
        )

    candidates: dict[tuple[str, int, str, int], list[_ContentCandidate]] = defaultdict(list)
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
                file_size = int(message_file_size(message) or 0)
                message_id = int(getattr(message, "id", 0) or 0)
                if media_type not in _SUPPORTED_MEDIA_TYPES or file_size <= 0 or message_id <= 0:
                    continue
                media_count += 1
                candidates[_content_candidate_key(chat_id, topic_key, media_type, file_size)].append(
                    _ContentCandidate(message_id=message_id, media_type=media_type)
                )
                if scanned % 100 == 0:
                    _save_plan(
                        config,
                        replace(
                            plan,
                            scanned_message_count=scanned,
                            media_message_count=media_count,
                        ),
                    )
                    await asyncio.sleep(0)

        candidate_groups = {
            key: entries for key, entries in candidates.items() if len(entries) >= 2
        }
        candidate_count = sum(len(entries) for entries in candidate_groups.values())
        plan = _save_plan(
            config,
            replace(
                plan,
                scanned_message_count=scanned,
                media_message_count=media_count,
                content_candidate_count=candidate_count,
            ),
        )

        checksums: dict[tuple[str, int, str], list[_HistoryRecord]] = defaultdict(list)
        failures: list[str] = []
        hashed = 0
        with tempfile.TemporaryDirectory(prefix="destination-content-scan-", dir=_plan_path(config).parent) as raw_dir:
            directory = Path(raw_dir)
            for (chat_id, topic_key, media_type, _size), entries in candidate_groups.items():
                if stop_event.is_set():
                    raise DestinationScanCancelled
                message_ids = [entry.message_id for entry in entries]
                for index in range(0, len(message_ids), 100):
                    if stop_event.is_set():
                        raise DestinationScanCancelled
                    batch = message_ids[index:index + 100]
                    result = await limiter.call(
                        "read",
                        reader.get_messages,
                        telegram_peer(chat_id),
                        batch,
                    )
                    messages = result if isinstance(result, list) else [result]
                    by_id = {
                        int(getattr(message, "id", 0) or 0): message
                        for message in messages
                        if not message_is_empty(message)
                    }
                    for entry in entries[index:index + 100]:
                        if stop_event.is_set():
                            raise DestinationScanCancelled
                        message = by_id.get(entry.message_id)
                        if message is None:
                            failures.append(f"{chat_id} ({entry.message_id}): message is no longer available")
                            continue
                        try:
                            checksum = await _download_content_hash(message, limiter, directory)
                        except Exception as exc:
                            failures.append(
                                f"{chat_id} ({entry.message_id}): {exc.__class__.__name__}: {str(exc)[:120]}"
                            )
                            continue
                        hashed += 1
                        checksums[(chat_id, topic_key, checksum)].append(
                            _HistoryRecord(message_id=entry.message_id, media_type=entry.media_type)
                        )
                        if hashed % 10 == 0:
                            plan = _save_plan(
                                config,
                                replace(plan, content_hashed_count=hashed),
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
            scan_mode="content",
            content_candidate_count=candidate_count,
            content_hashed_count=hashed,
            groups=_build_groups(checksums, destinations, match_kind="content_sha256"),
            error=("; ".join(failures[:5])[:1000] or None),
        )
        return _save_plan(config, ready)
    except DestinationScanCancelled:
        return _save_plan(
            config,
            replace(
                plan,
                state="cancelled",
                completed_at=utc_now(),
                scanned_message_count=scanned,
                media_message_count=media_count,
                error="Content scan was stopped before a deletion preview was created.",
            ),
        )
    except Exception as exc:
        return _save_plan(
            config,
            replace(
                plan,
                state="failed",
                completed_at=utc_now(),
                scanned_message_count=scanned,
                media_message_count=media_count,
                error=f"{exc.__class__.__name__}: {str(exc)[:500]}",
            ),
        )
