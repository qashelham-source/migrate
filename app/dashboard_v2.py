from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from app.db import Database


@dataclass(frozen=True)
class StorageSnapshot:
    total_bytes: int
    used_bytes: int
    free_bytes: int
    percent_used: float


def _table_exists(db: Database, name: str) -> bool:
    row = db.query_one(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (name,),
    )
    return row is not None


def _count(db: Database, sql: str, params: tuple[Any, ...] = ()) -> int:
    row = db.query_one(sql, params)
    if not row:
        return 0
    value = row[0]
    return int(value or 0)


def storage_snapshot(path: str | Path) -> StorageSnapshot:
    probe = Path(path)
    target = probe if probe.exists() else probe.parent
    usage = shutil.disk_usage(target)
    percent = (usage.used / usage.total * 100.0) if usage.total else 0.0
    return StorageSnapshot(
        total_bytes=int(usage.total),
        used_bytes=int(usage.used),
        free_bytes=int(usage.free),
        percent_used=percent,
    )


def format_bytes(value: int | float | None) -> str:
    amount = float(value or 0)
    units = ("B", "KB", "MB", "GB", "TB")
    for unit in units:
        if abs(amount) < 1024.0 or unit == units[-1]:
            return f"{amount:.1f} {unit}" if unit != "B" else f"{int(amount)} B"
        amount /= 1024.0
    return f"{amount:.1f} TB"


def format_eta(seconds: int | float | None) -> str:
    if seconds is None:
        return "not enough data"
    remaining = max(0, int(seconds))
    hours, remaining = divmod(remaining, 3600)
    minutes, secs = divmod(remaining, 60)
    if hours:
        return f"{hours}j {minutes}m"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"



def source_migration_progress(db: Database) -> list[dict[str, Any]]:
    """Summarize each source once per post/album, even with multiple destinations."""
    has_registry = _table_exists(db, "source_registry")
    title_select = (
        "COALESCE(NULLIF(sr.title, ''), totals.source_chat_id) AS title"
        if has_registry
        else "totals.source_chat_id AS title"
    )
    registry_join = (
        "LEFT JOIN source_registry sr ON sr.source_chat_id = totals.source_chat_id"
        if has_registry
        else ""
    )
    rows = db.query(
        f"""
        WITH source_items AS (
            SELECT
                m.source_chat_id,
                m.file_unique_key,
                COUNT(*) AS destination_count,
                SUM(CASE WHEN m.status = 'copied' THEN 1 ELSE 0 END) AS copied_destinations,
                SUM(CASE WHEN m.status IN ('pending', 'downloading', 'uploading') THEN 1 ELSE 0 END) AS active_destinations,
                SUM(
                    CASE
                        WHEN m.status = 'skipped'
                         AND LOWER(COALESCE(m.last_error, '')) LIKE '%filtered out by config%'
                        THEN 1 ELSE 0
                    END
                ) AS filtered_destinations
            FROM messages m
            WHERE m.file_unique_key NOT LIKE 'repair:%'
            GROUP BY m.source_chat_id, m.file_unique_key
        ), totals AS (
            SELECT
                source_chat_id,
                COUNT(*) AS total_items,
                SUM(
                    CASE WHEN filtered_destinations = destination_count THEN 1 ELSE 0 END
                ) AS filtered_items,
                SUM(
                    CASE
                        WHEN filtered_destinations != destination_count
                         AND copied_destinations = destination_count
                        THEN 1 ELSE 0
                    END
                ) AS copied_items,
                SUM(
                    CASE
                        WHEN filtered_destinations != destination_count
                         AND copied_destinations != destination_count
                         AND active_destinations > 0
                        THEN 1 ELSE 0
                    END
                ) AS active_items,
                SUM(
                    CASE
                        WHEN filtered_destinations != destination_count
                         AND copied_destinations != destination_count
                         AND active_destinations = 0
                        THEN 1 ELSE 0
                    END
                ) AS blocked_items
            FROM source_items
            GROUP BY source_chat_id
        )
        SELECT totals.*, {title_select}
        FROM totals
        {registry_join}
        ORDER BY blocked_items DESC, active_items DESC, title COLLATE NOCASE
        """
    )

    progress: list[dict[str, Any]] = []
    for row in rows:
        total_items = int(row["total_items"] or 0)
        filtered_items = int(row["filtered_items"] or 0)
        eligible_items = max(0, total_items - filtered_items)
        copied_items = int(row["copied_items"] or 0)
        active_items = int(row["active_items"] or 0)
        blocked_items = int(row["blocked_items"] or 0)
        percent = round((copied_items / eligible_items) * 100) if eligible_items else 0
        progress.append(
            {
                "source_chat_id": str(row["source_chat_id"]),
                "title": str(row["title"] or row["source_chat_id"]),
                "total_items": total_items,
                "filtered_items": filtered_items,
                "eligible_items": eligible_items,
                "copied_items": copied_items,
                "active_items": active_items,
                "blocked_items": blocked_items,
                "remaining_items": active_items + blocked_items,
                "percent": int(percent),
            }
        )
    return progress


def active_source_progress(
    status: Mapping[str, Any],
    source_progress: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Return only the source named by the live runtime status.

    Completed sources remain available to reports, but never leak back into the
    live dashboard simply because they are in the historical queue.
    """
    phase = str(status.get("phase") or "").strip().lower()
    if phase in {"", "idle", "watching", "stopped", "source_complete"}:
        return None

    source_chat = status.get("source_chat")
    if source_chat is not None:
        source_id = str(source_chat)
        for item in source_progress:
            if str(item.get("source_chat_id")) == source_id:
                return item

    source_name = str(status.get("source") or "").strip().casefold()
    if source_name:
        for item in source_progress:
            if str(item.get("title") or "").strip().casefold() == source_name:
                return item

    unfinished = [
        item
        for item in source_progress
        if int(item.get("active_items") or 0) > 0
        or int(item.get("blocked_items") or 0) > 0
        or int(item.get("remaining_items") or 0) > 0
    ]
    return unfinished[0] if len(unfinished) == 1 else None

def dashboard_snapshot(db: Database, storage_path: str | Path) -> dict[str, Any]:
    queue = {
        status: _count(db, "SELECT COUNT(*) FROM messages WHERE status = ?", (status,))
        for status in ("pending", "downloading", "uploading", "copied", "failed", "skipped")
    }

    sources = {"total": 0, "live": 0, "issues": 0, "verified": 0}
    if _table_exists(db, "source_registry"):
        row = db.query_one(
            """
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN live_watch_enabled = 1 THEN 1 ELSE 0 END) AS live,
                   SUM(CASE WHEN migration_state = 'issues' OR access_status != 'ok' THEN 1 ELSE 0 END) AS issues,
                   SUM(CASE WHEN migration_state = 'verified' THEN 1 ELSE 0 END) AS verified
            FROM source_registry
            """
        )
        if row:
            sources = {key: int(row[key] or 0) for key in sources}

    destinations = {"total": 0, "paused": 0}
    if _table_exists(db, "destination_health"):
        destinations["total"] = _count(db, "SELECT COUNT(*) FROM destination_health")
        destinations["paused"] = _count(
            db, "SELECT COUNT(*) FROM destination_health WHERE paused = 1"
        )

    verification = {"verified": 0, "repairing": 0, "failed": 0}
    if _table_exists(db, "verification_results"):
        for key, states in {
            "verified": ("verified", "verified_repaired"),
            "repairing": ("repairing",),
            "failed": ("failed",),
        }.items():
            placeholders = ",".join("?" for _ in states)
            verification[key] = _count(
                db,
                f"SELECT COUNT(*) FROM verification_results WHERE status IN ({placeholders})",
                states,
            )

    review = {
        "repair_failed": _count(
            db,
            """
            SELECT COUNT(*) FROM messages
            WHERE file_unique_key LIKE 'repair:%'
              AND (
                    status = 'failed'
                 OR (status = 'skipped'
                     AND LOWER(COALESCE(last_error, '')) NOT LIKE '%filtered out by config%')
              )
            """,
        ),
        "verification_failed": int(verification["failed"]),
    }
    review["total"] = review["repair_failed"] + review["verification_failed"]

    telemetry = {"active": 0, "speed_bps": 0.0, "eta_seconds": None}
    if _table_exists(db, "job_telemetry"):
        row = db.query_one(
            """
            SELECT COUNT(*) AS active,
                   COALESCE(SUM(speed_bps), 0) AS speed_bps,
                   MAX(eta_seconds) AS eta_seconds
            FROM job_telemetry
            WHERE completed_at IS NULL AND stage NOT IN ('completed', 'failed')
            """
        )
        if row:
            telemetry = {
                "active": int(row["active"] or 0),
                "speed_bps": float(row["speed_bps"] or 0),
                "eta_seconds": None if row["eta_seconds"] is None else float(row["eta_seconds"]),
            }

    return {
        "queue": queue,
        "sources": sources,
        "source_progress": source_migration_progress(db),
        "destinations": destinations,
        "verification": verification,
        "review": review,
        "telemetry": telemetry,
        "storage": storage_snapshot(storage_path),
    }


def issue_center(db: Database, limit: int = 20) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    issue_job_ids: set[int] = set()

    for row in db.query(
        """
        SELECT id, source_chat_id, dest_chat_id, source_message_id, media_type,
               status, attempts, last_error, updated_at
        FROM messages
        WHERE status IN ('failed', 'skipped')
          AND COALESCE(last_error, '') != ''
        ORDER BY id DESC
        LIMIT ?
        """,
        (max(1, int(limit)),),
    ):
        issue_job_ids.add(int(row["id"]))
        issues.append(
            {
                "kind": "job",
                "id": int(row["id"]),
                "source_chat_id": str(row["source_chat_id"]),
                "dest_chat_id": str(row["dest_chat_id"]),
                "source_message_id": int(row["source_message_id"]),
                "media_type": str(row["media_type"] or "unknown"),
                "status": str(row["status"]),
                "attempts": int(row["attempts"] or 0),
                "error": str(row["last_error"] or ""),
                "updated_at": str(row["updated_at"] or ""),
            }
        )

    if _table_exists(db, "verification_results"):
        for row in db.query(
            """
            SELECT m.id, m.source_chat_id, m.dest_chat_id, m.source_message_id,
                   m.media_type, vr.expected_count, vr.present_count,
                   vr.media_match, vr.caption_match, vr.size_match,
                   vr.missing_source_message_ids, vr.details, vr.checked_at
            FROM verification_results vr
            JOIN messages m ON m.id = vr.job_id
            WHERE vr.status = 'failed'
            ORDER BY vr.checked_at DESC, m.id DESC
            LIMIT ?
            """,
            (max(1, int(limit)),),
        ):
            job_id = int(row["id"])
            if job_id in issue_job_ids:
                continue
            expected = int(row["expected_count"] or 0)
            present = int(row["present_count"] or 0)
            reasons: list[str] = []
            if expected != present:
                reasons.append(f"destination has {present}/{expected} item(s)")
            if row["media_match"] == 0:
                reasons.append("media type mismatch")
            if row["caption_match"] == 0:
                reasons.append("caption mismatch")
            if row["size_match"] == 0:
                reasons.append("file size mismatch")
            try:
                missing_ids = json.loads(row["missing_source_message_ids"] or "[]")
            except (TypeError, ValueError, json.JSONDecodeError):
                missing_ids = []
            if isinstance(missing_ids, list) and missing_ids:
                reasons.append(f"missing source item(s): {', '.join(str(value) for value in missing_ids[:5])}")
            issues.append(
                {
                    "kind": "verification",
                    "id": job_id,
                    "source_chat_id": str(row["source_chat_id"]),
                    "dest_chat_id": str(row["dest_chat_id"]),
                    "source_message_id": int(row["source_message_id"]),
                    "media_type": str(row["media_type"] or "unknown"),
                    "status": "verification failed",
                    "error": "; ".join(reasons) or "Destination media failed strict verification",
                    "updated_at": str(row["checked_at"] or ""),
                }
            )

    if _table_exists(db, "destination_health"):
        for row in db.query(
            """
            SELECT dest_chat_id, pause_reason, last_error, updated_at
            FROM destination_health
            WHERE paused = 1
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (max(1, int(limit)),),
        ):
            issues.append(
                {
                    "kind": "destination",
                    "id": str(row["dest_chat_id"]),
                    "status": "paused",
                    "error": str(row["last_error"] or row["pause_reason"] or "paused"),
                    "updated_at": str(row["updated_at"] or ""),
                }
            )

    issues.sort(key=lambda item: item.get("updated_at", ""), reverse=True)
    return issues[: max(1, int(limit))]


def source_library(db: Database) -> list[dict[str, Any]]:
    if not _table_exists(db, "source_registry"):
        return []
    return [dict(row) for row in db.query(
        """
        SELECT source_chat_id, title, username, chat_type, latest_seen_message_id,
               history_scanned_through, history_verified_through, migration_state,
               live_watch_enabled, access_status, updated_at
        FROM source_registry
        ORDER BY CASE migration_state
                   WHEN 'issues' THEN 0
                   WHEN 'in_progress' THEN 1
                   WHEN 'verified' THEN 2
                   ELSE 3
                 END,
                 LOWER(title)
        """
    )]


def delivery_matrix(db: Database) -> list[dict[str, Any]]:
    rows = db.query(
        """
        SELECT dest_chat_id,
               COUNT(*) AS total,
               SUM(CASE WHEN status = 'copied' THEN 1 ELSE 0 END) AS copied,
               SUM(CASE WHEN status IN ('pending', 'downloading', 'uploading') THEN 1 ELSE 0 END) AS active,
               SUM(CASE WHEN status IN ('failed', 'skipped') THEN 1 ELSE 0 END) AS issues
        FROM messages
        GROUP BY dest_chat_id
        ORDER BY issues DESC, active DESC, dest_chat_id
        """
    )
    health: dict[str, dict[str, Any]] = {}
    if _table_exists(db, "destination_health"):
        health = {
            str(row["dest_chat_id"]): dict(row)
            for row in db.query("SELECT * FROM destination_health")
        }

    result: list[dict[str, Any]] = []
    for row in rows:
        dest_id = str(row["dest_chat_id"])
        state = health.get(dest_id, {})
        result.append(
            {
                "dest_chat_id": dest_id,
                "total": int(row["total"] or 0),
                "copied": int(row["copied"] or 0),
                "active": int(row["active"] or 0),
                "issues": int(row["issues"] or 0),
                "paused": bool(state.get("paused", 0)),
                "pause_reason": state.get("pause_reason"),
                "last_error": state.get("last_error"),
            }
        )
    return result
