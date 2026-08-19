from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from app.db import Database, utc_now
from app.skip_policy import DUPLICATE_CLEANUP_SKIP_MARKER


@dataclass(frozen=True)
class DuplicateCleanupCandidate:
    """One already-sent duplicate delivery that can safely be removed.

    The candidate is deliberately limited to exact Telegram media fingerprints
    delivered to the same destination (and forum topic, when applicable).  It
    never represents source messages or text-only fallback jobs.
    """

    job_id: int
    source_chat_id: str
    dest_chat_id: str
    dest_topic_id: int | None
    file_unique_key: str
    dest_message_ids: tuple[int, ...]
    kept_job_id: int
    kept_dest_message_ids: tuple[int, ...]

    @property
    def message_count(self) -> int:
        return len(self.dest_message_ids)


@dataclass(frozen=True)
class DuplicateCleanupPlan:
    """The read-only preview shown before Telegram messages are deleted."""

    candidates: tuple[DuplicateCleanupCandidate, ...]
    group_count: int

    @property
    def delivery_count(self) -> int:
        return len(self.candidates)

    @property
    def message_count(self) -> int:
        return sum(candidate.message_count for candidate in self.candidates)


@dataclass(frozen=True)
class _DeliveredRecord:
    job_id: int
    source_chat_id: str
    dest_chat_id: str
    dest_topic_id: int | None
    file_unique_key: str
    dest_message_ids: tuple[int, ...]

    @property
    def earliest_message_id(self) -> int:
        return min(self.dest_message_ids)


def _decode_message_ids(value: Any) -> tuple[int, ...]:
    """Return a unique, positive ID tuple or an empty tuple for unsafe rows."""

    try:
        decoded = json.loads(str(value or "[]"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return ()
    if not isinstance(decoded, list):
        return ()

    result: list[int] = []
    for raw_id in decoded:
        try:
            message_id = int(raw_id)
        except (TypeError, ValueError):
            return ()
        if message_id <= 0:
            return ()
        if message_id not in result:
            result.append(message_id)
    return tuple(result)


def plan_duplicate_delivery_cleanup(db: Database) -> DuplicateCleanupPlan:
    """Find exact duplicate deliveries without changing Telegram or SQLite.

    A ``copied`` row with stored destination message IDs is a completed
    delivery record: Telegram has already returned the messages that were
    sent.  Strong post-send verification is useful for migration health, but
    older successful deliveries may not have it.  The first message already
    present in the destination is retained; later full copies of the same
    fingerprint become candidates.
    """

    rows = db.query(
        """
        SELECT id, source_chat_id, dest_chat_id, dest_topic_id,
               file_unique_key, dest_message_ids
        FROM messages
        WHERE status = 'copied'
          AND file_unique_key != ''
          AND file_unique_key NOT LIKE 'messages:%'
          AND file_unique_key NOT LIKE 'repair:%'
          AND dest_message_ids IS NOT NULL
        ORDER BY dest_chat_id ASC,
                 COALESCE(dest_topic_id, 0) ASC,
                 file_unique_key ASC,
                 id ASC
        """
    )

    grouped: dict[tuple[str, int, str], list[_DeliveredRecord]] = {}
    for row in rows:
        message_ids = _decode_message_ids(row["dest_message_ids"])
        if not message_ids:
            continue
        topic_id = row["dest_topic_id"]
        record = _DeliveredRecord(
            job_id=int(row["id"]),
            source_chat_id=str(row["source_chat_id"]),
            dest_chat_id=str(row["dest_chat_id"]),
            dest_topic_id=int(topic_id) if topic_id is not None else None,
            file_unique_key=str(row["file_unique_key"]),
            dest_message_ids=message_ids,
        )
        key = (record.dest_chat_id, int(record.dest_topic_id or 0), record.file_unique_key)
        grouped.setdefault(key, []).append(record)

    candidates: list[DuplicateCleanupCandidate] = []
    group_count = 0
    for records in grouped.values():
        if len(records) < 2:
            continue
        ordered = sorted(records, key=lambda record: (record.earliest_message_id, record.job_id))
        kept = ordered[0]
        kept_ids = set(kept.dest_message_ids)
        safe_duplicates = [
            record
            for record in ordered[1:]
            # A partial overlap indicates corrupt or uncertain delivery
            # bookkeeping. Do not delete anything from such a row.
            if not kept_ids.intersection(record.dest_message_ids)
        ]
        if not safe_duplicates:
            continue
        group_count += 1
        for duplicate in safe_duplicates:
            candidates.append(
                DuplicateCleanupCandidate(
                    job_id=duplicate.job_id,
                    source_chat_id=duplicate.source_chat_id,
                    dest_chat_id=duplicate.dest_chat_id,
                    dest_topic_id=duplicate.dest_topic_id,
                    file_unique_key=duplicate.file_unique_key,
                    dest_message_ids=duplicate.dest_message_ids,
                    kept_job_id=kept.job_id,
                    kept_dest_message_ids=kept.dest_message_ids,
                )
            )

    return DuplicateCleanupPlan(candidates=tuple(candidates), group_count=group_count)


def duplicate_cleanup_reason(candidate: DuplicateCleanupCandidate) -> str:
    kept_ids = ", ".join(str(message_id) for message_id in candidate.kept_dest_message_ids)
    return (
        "Deleted duplicate delivery by admin cleanup; "
        f"kept job #{candidate.kept_job_id} (destination message {kept_ids})."
    )


def mark_duplicate_delivery_deleted(db: Database, candidate: DuplicateCleanupCandidate) -> bool:
    """Keep a durable anti-duplicate record after Telegram deletion succeeds.

    The row becomes an expected skip rather than being removed.  This preserves
    the source checkpoint and prevents a later full scan from sending the same
    media back into the destination.
    """

    cursor = db.execute(
        """
        UPDATE messages
        SET status = 'skipped',
            last_error = ?,
            next_retry_at = NULL,
            activity_phase = NULL,
            worker_id = NULL,
            lease_expires_at = NULL,
            updated_at = ?
        WHERE id = ?
          AND status = 'copied'
          AND source_chat_id = ?
          AND dest_chat_id = ?
          AND COALESCE(dest_topic_id, 0) = ?
          AND file_unique_key = ?
        """,
        (
            duplicate_cleanup_reason(candidate),
            utc_now(),
            candidate.job_id,
            candidate.source_chat_id,
            candidate.dest_chat_id,
            int(candidate.dest_topic_id or 0),
            candidate.file_unique_key,
        ),
    )
    return cursor.rowcount == 1


def is_duplicate_cleanup_reason(reason: str | None) -> bool:
    return DUPLICATE_CLEANUP_SKIP_MARKER in str(reason or "").casefold()
