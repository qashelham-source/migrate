from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from app.db import Database

_NORMAL_STALL_SECONDS = 15 * 60
_LARGE_FILE_BYTES = 512 * 1024 * 1024
_VERY_LARGE_FILE_BYTES = 2 * 1024 * 1024 * 1024
_HUGE_FILE_BYTES = 4 * 1024 * 1024 * 1024


@dataclass(frozen=True)
class StalledJob:
    id: int
    phase: str
    source_chat_id: str
    dest_chat_id: str
    source_message_id: int
    media_type: str
    file_size: int | None
    updated_at: str
    age_seconds: int
    threshold_seconds: int


def stall_threshold_seconds(phase: str, file_size: int | None) -> int:
    """Return a conservative no-activity threshold for a live job."""
    if str(phase).lower() == "verifying":
        return _NORMAL_STALL_SECONDS

    size = max(0, int(file_size or 0))
    if size >= _HUGE_FILE_BYTES:
        return 90 * 60
    if size >= _VERY_LARGE_FILE_BYTES:
        return 60 * 60
    if size >= _LARGE_FILE_BYTES:
        return 30 * 60
    return _NORMAL_STALL_SECONDS


def _parse_timestamp(value: object) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def format_duration(seconds: int) -> str:
    minutes = max(0, int(seconds)) // 60
    if minutes < 60:
        return f"{max(1, minutes)} minit"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}j {minutes}m" if minutes else f"{hours}j"


def stalled_job_message(job: StalledJob) -> str:
    label = {
        "downloading": "Muat turun",
        "uploading": "Muat naik",
        "verifying": "Pengesahan",
    }.get(job.phase, "Job")
    return f"{label} tidak menunjukkan aktiviti selama {format_duration(job.age_seconds)}."


def find_stalled_jobs(
    db: Database,
    *,
    now: datetime | None = None,
) -> list[StalledJob]:
    """Find active jobs whose heartbeat has stopped beyond their safe threshold."""
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    rows = db.query(
        """
        SELECT id, source_chat_id, dest_chat_id, source_message_id, media_type,
               file_size, status, activity_phase, updated_at
        FROM messages
        WHERE status IN ('downloading', 'uploading')
           OR activity_phase = 'verifying'
        ORDER BY updated_at ASC, id ASC
        """
    )

    stalled: list[StalledJob] = []
    for row in rows:
        phase = (
            "verifying"
            if str(row["activity_phase"] or "") == "verifying"
            else str(row["status"] or "")
        )
        updated = _parse_timestamp(row["updated_at"])
        if updated is None:
            continue
        age_seconds = max(0, int((current - updated).total_seconds()))
        file_size = int(row["file_size"]) if row["file_size"] is not None else None
        threshold_seconds = stall_threshold_seconds(phase, file_size)
        if age_seconds < threshold_seconds:
            continue
        stalled.append(
            StalledJob(
                id=int(row["id"]),
                phase=phase,
                source_chat_id=str(row["source_chat_id"]),
                dest_chat_id=str(row["dest_chat_id"]),
                source_message_id=int(row["source_message_id"]),
                media_type=str(row["media_type"] or "unknown"),
                file_size=file_size,
                updated_at=str(row["updated_at"] or ""),
                age_seconds=age_seconds,
                threshold_seconds=threshold_seconds,
            )
        )
    return stalled
