from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from sqlite3 import Row
from typing import Any

from app.config import AppConfig
from app.db import Database, utc_now
from app.release3_store import Release3Store


RUN_MODES = {"run", "sync", "process", "health"}
REPAIR_CATEGORIES = (
    "media_empty",
    "peer_id",
    "permission",
    "temporary",
    "source_missing",
    "unsupported",
    "needs_review",
    "other",
)


def runtime_path(config: AppConfig, name: str) -> Path:
    path = config.queue.db_path.parent / name
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def request_run_mode(config: AppConfig, mode: str) -> None:
    normalized = str(mode).strip().lower()
    if normalized not in RUN_MODES:
        raise ValueError(f"Unsupported run mode: {mode}")
    mode_path = runtime_path(config, "run_mode")
    temporary = mode_path.with_suffix(".tmp")
    temporary.write_text(normalized, encoding="utf-8")
    temporary.replace(mode_path)
    runtime_path(config, "run_now").touch()


def health_report_path(config: AppConfig) -> Path:
    return runtime_path(config, "health_report.json")


def save_health_report(config: AppConfig, report: dict[str, Any]) -> None:
    path = health_report_path(config)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(report, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    temporary.replace(path)


def load_health_report(config: AppConfig) -> dict[str, Any] | None:
    try:
        data = json.loads(health_report_path(config).read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def checkpoint_rows(db: Database) -> list[dict[str, Any]]:
    return [
        {
            "source_chat_id": str(row["source_chat_id"]),
            "source_topic_id": int(row["source_topic_key"]) or None,
            "last_scanned_message_id": int(row["last_scanned_message_id"]),
            "last_scan_mode": str(row["last_scan_mode"]),
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
        }
        for row in db.list_scan_checkpoints()
    ]


def reset_all_checkpoints(db: Database) -> int:
    return db.reset_scan_checkpoints()


def classify_repair_error(row: Row | dict[str, Any]) -> str:
    media_type = str(row["media_type"] or "").lower()
    error = str(row["last_error"] or "").lower()

    if any(
        marker in error
        for marker in (
            "destination result is unknown",
            "verify destination before retrying manually",
            "upload connection interrupted",
            "interrupted during upload",
        )
    ):
        return "needs_review"
    if media_type == "unsupported" or "filtered out by config" in error:
        return "unsupported"
    if "media_empty" in error or "mediaempty" in error:
        return "media_empty"
    if "peer id invalid" in error or "peer_id_invalid" in error:
        return "peer_id"
    if any(
        marker in error
        for marker in (
            "chatwriteforbidden",
            "chat_write_forbidden",
            "channelprivate",
            "channel_private",
            "channelinvalid",
            "channel_invalid",
            "not enough rights",
            "forbidden",
        )
    ):
        return "permission"
    if any(
        marker in error
        for marker in (
            "source messages missing",
            "message id invalid",
            "message_id_invalid",
            "message empty",
            "message_empty",
        )
    ):
        return "source_missing"
    if any(
        marker in error
        for marker in (
            "timeout",
            "timed out",
            "connection",
            "network",
            "floodwait",
            "flood wait",
            "rpc_call_fail",
            "server error",
            "temporarily unavailable",
        )
    ):
        return "temporary"
    return "other"


def repair_rows(db: Database) -> list[Row]:
    return db.query(
        """
        SELECT id, status, media_type, source_message_id, dest_chat_id, attempts, last_error, updated_at
        FROM messages
        WHERE status IN ('failed', 'skipped')
          AND COALESCE(last_error, '') <> ''
        ORDER BY updated_at DESC, id DESC
        """
    )


def repair_summary(db: Database) -> dict[str, int]:
    counts = Counter(classify_repair_error(row) for row in repair_rows(db))
    return {category: int(counts.get(category, 0)) for category in REPAIR_CATEGORIES}


def repair_samples(db: Database, category: str, limit: int = 10) -> list[dict[str, Any]]:
    normalized = str(category).strip().lower()
    if normalized not in REPAIR_CATEGORIES:
        raise ValueError(f"Unknown repair category: {category}")
    result: list[dict[str, Any]] = []
    for row in repair_rows(db):
        if classify_repair_error(row) != normalized:
            continue
        result.append(
            {
                "id": int(row["id"]),
                "status": str(row["status"]),
                "media_type": str(row["media_type"] or "unsupported"),
                "source_message_id": int(row["source_message_id"]),
                "dest_chat_id": str(row["dest_chat_id"]),
                "attempts": int(row["attempts"]),
                "last_error": str(row["last_error"] or ""),
            }
        )
        if len(result) >= max(1, int(limit)):
            break
    return result


def requeue_repair_category(db: Database, category: str) -> int:
    normalized = str(category).strip().lower()
    allowed = {"media_empty", "peer_id", "permission", "temporary", "other"}
    if normalized not in allowed:
        raise ValueError(f"Category cannot be requeued: {category}")

    matching = [row for row in repair_rows(db) if classify_repair_error(row) == normalized]
    ids = [int(row["id"]) for row in matching]
    if not ids:
        return 0
    placeholders = ",".join("?" for _ in ids)
    cursor = db.execute(
        f"""
        UPDATE messages
        SET status = 'pending',
            attempts = 0,
            last_error = NULL,
            next_retry_at = NULL,
            updated_at = ?
        WHERE id IN ({placeholders})
        """,
        (utc_now(), *ids),
    )
    if normalized in {"permission", "peer_id"}:
        store = Release3Store(db)
        store.initialize()
        for destination in {str(row["dest_chat_id"]) for row in matching}:
            store.resume_destination(destination)
    return int(cursor.rowcount)


def requeue_retryable_repairs(db: Database) -> int:
    """Return only errors that are safe to retry without risking duplicate uploads."""
    total = 0
    for category in ("media_empty", "peer_id", "temporary"):
        total += requeue_repair_category(db, category)
    return total
