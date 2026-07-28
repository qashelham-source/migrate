from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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
        return "belum cukup data"
    remaining = max(0, int(seconds))
    hours, remaining = divmod(remaining, 3600)
    minutes, secs = divmod(remaining, 60)
    if hours:
        return f"{hours}j {minutes}m"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


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
        "destinations": destinations,
        "verification": verification,
        "telemetry": telemetry,
        "storage": storage_snapshot(storage_path),
    }


def issue_center(db: Database, limit: int = 20) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []

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
