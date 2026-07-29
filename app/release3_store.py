from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from sqlite3 import Row
from typing import Any

from app.db import Database, utc_now


TERMINAL_VERIFICATION_STATES = {"verified", "verified_repaired", "failed", "repairing"}


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _decode_list(value: Any) -> list[Any]:
    try:
        decoded = json.loads(value or "[]")
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return decoded if isinstance(decoded, list) else []


class Release3Store:
    """Additive Release 3 persistence layered on top of the existing queue DB."""

    def __init__(self, db: Database) -> None:
        self.db = db

    def initialize(self) -> None:
        self.db.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS source_registry (
                source_chat_id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                username TEXT,
                chat_type TEXT NOT NULL,
                latest_seen_message_id INTEGER,
                history_scanned_through INTEGER,
                history_verified_through INTEGER,
                migration_state TEXT NOT NULL DEFAULT 'not_started',
                live_watch_enabled INTEGER NOT NULL DEFAULT 0,
                access_status TEXT NOT NULL DEFAULT 'ok',
                discovered_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS verification_results (
                job_id INTEGER PRIMARY KEY,
                status TEXT NOT NULL,
                expected_count INTEGER NOT NULL DEFAULT 0,
                present_count INTEGER NOT NULL DEFAULT 0,
                media_match INTEGER,
                caption_match INTEGER,
                size_match INTEGER,
                missing_source_message_ids TEXT NOT NULL DEFAULT '[]',
                details TEXT NOT NULL DEFAULT '{}',
                checked_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS repair_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER,
                source_chat_id TEXT,
                dest_chat_id TEXT,
                action TEXT NOT NULL,
                reason TEXT,
                outcome TEXT NOT NULL DEFAULT 'recorded',
                details TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS destination_health (
                dest_chat_id TEXT PRIMARY KEY,
                paused INTEGER NOT NULL DEFAULT 0,
                pause_reason TEXT,
                last_error TEXT,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS job_telemetry (
                job_id INTEGER PRIMARY KEY,
                stage TEXT NOT NULL DEFAULT 'pending',
                route TEXT,
                bytes_total INTEGER,
                bytes_processed INTEGER NOT NULL DEFAULT 0,
                speed_bps REAL,
                eta_seconds REAL,
                started_at TEXT NOT NULL,
                stage_started_at TEXT NOT NULL,
                completed_at TEXT,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS repair_links (
                parent_job_id INTEGER NOT NULL,
                repair_job_id INTEGER NOT NULL UNIQUE,
                source_message_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (parent_job_id, source_message_id)
            );

            CREATE INDEX IF NOT EXISTS idx_verification_status
                ON verification_results(status, checked_at);
            CREATE INDEX IF NOT EXISTS idx_repair_actions_job
                ON repair_actions(job_id, created_at);
            CREATE INDEX IF NOT EXISTS idx_telemetry_stage
                ON job_telemetry(stage, updated_at);
            """
        )
        self.db.conn.commit()

    def upsert_source(
        self,
        *,
        source_chat_id: int | str,
        title: str,
        username: str | None,
        chat_type: str,
        latest_seen_message_id: int | None,
        access_status: str = "ok",
    ) -> None:
        now = utc_now()
        self.db.execute(
            """
            INSERT INTO source_registry (
                source_chat_id, title, username, chat_type, latest_seen_message_id,
                access_status, discovered_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_chat_id) DO UPDATE SET
                title = excluded.title,
                username = excluded.username,
                chat_type = excluded.chat_type,
                latest_seen_message_id = COALESCE(excluded.latest_seen_message_id, source_registry.latest_seen_message_id),
                access_status = excluded.access_status,
                updated_at = excluded.updated_at
            """,
            (
                str(source_chat_id),
                title or str(source_chat_id),
                username,
                chat_type,
                latest_seen_message_id,
                access_status,
                now,
                now,
            ),
        )

    def set_source_scan_progress(
        self,
        source_chat_id: int | str,
        scanned_through: int,
        *,
        live_watch_enabled: bool | None = None,
    ) -> None:
        fields = [
            "history_scanned_through = MAX(COALESCE(history_scanned_through, 0), ?)",
            "latest_seen_message_id = MAX(COALESCE(latest_seen_message_id, 0), ?)",
            "migration_state = 'in_progress'",
            "updated_at = ?",
        ]
        values: list[Any] = [int(scanned_through), int(scanned_through), utc_now()]
        if live_watch_enabled is not None:
            fields.append("live_watch_enabled = ?")
            values.append(1 if live_watch_enabled else 0)
        values.append(str(source_chat_id))
        self.db.execute(
            f"UPDATE source_registry SET {', '.join(fields)} WHERE source_chat_id = ?",
            values,
        )

    def set_live_watch(self, source_chat_id: int | str, enabled: bool) -> None:
        self.db.execute(
            "UPDATE source_registry SET live_watch_enabled = ?, updated_at = ? WHERE source_chat_id = ?",
            (1 if enabled else 0, utc_now(), str(source_chat_id)),
        )

    def list_sources(self) -> list[dict[str, Any]]:
        rows = self.db.query(
            """
            SELECT * FROM source_registry
            ORDER BY
                CASE migration_state
                    WHEN 'issues' THEN 0
                    WHEN 'in_progress' THEN 1
                    WHEN 'verified' THEN 2
                    ELSE 3
                END,
                LOWER(title), source_chat_id
            """
        )
        return [dict(row) for row in rows]

    def _source_ids_for_cleanup(self, source_chat_id: int | str) -> list[str]:
        requested_id = str(source_chat_id)
        source_ids = {requested_id}
        username = requested_id.lstrip("@").lower()
        if requested_id.startswith("@") and username:
            try:
                rows = self.db.query(
                    """
                    SELECT source_chat_id
                    FROM source_registry
                    WHERE LOWER(COALESCE(username, '')) IN (?, ?)
                    """,
                    (username, f"@{username}"),
                )
            except Exception:
                rows = []
            source_ids.update(str(row["source_chat_id"]) for row in rows)
        return sorted(source_ids)

    def _delete_source_jobs_locked(self, ids: list[str]) -> int:
        """Delete source jobs and their dependent records inside an open transaction."""
        placeholders = ", ".join("?" for _ in ids)
        source_filter = f"source_chat_id IN ({placeholders})"
        job_subquery = f"SELECT id FROM messages WHERE {source_filter}"
        conn = self.db.conn
        job_row = conn.execute(
            f"SELECT COUNT(*) AS count FROM messages WHERE {source_filter}",
            ids,
        ).fetchone()
        jobs = int(job_row["count"] if job_row else 0)
        conn.execute(
            f"""
            DELETE FROM repair_links
            WHERE parent_job_id IN ({job_subquery})
               OR repair_job_id IN ({job_subquery})
            """,
            (*ids, *ids),
        )
        conn.execute(
            f"DELETE FROM verification_results WHERE job_id IN ({job_subquery})",
            ids,
        )
        conn.execute(
            f"DELETE FROM job_telemetry WHERE job_id IN ({job_subquery})",
            ids,
        )
        conn.execute(
            f"""
            DELETE FROM repair_actions
            WHERE source_chat_id IN ({placeholders})
               OR job_id IN ({job_subquery})
            """,
            (*ids, *ids),
        )
        conn.execute(f"DELETE FROM messages WHERE {source_filter}", ids)
        return jobs

    def purge_source_jobs(self, source_chat_id: int | str) -> dict[str, int]:
        """Permanently remove one source's queue state without touching media cache."""
        ids = self._source_ids_for_cleanup(source_chat_id)
        placeholders = ", ".join("?" for _ in ids)
        source_filter = f"source_chat_id IN ({placeholders})"
        conn = self.db.conn
        try:
            conn.execute("BEGIN IMMEDIATE")
            checkpoint_row = conn.execute(
                f"SELECT COUNT(*) AS count FROM scan_checkpoints WHERE {source_filter}",
                ids,
            ).fetchone()
            checkpoints = int(checkpoint_row["count"] if checkpoint_row else 0)
            jobs = self._delete_source_jobs_locked(ids)
            conn.execute(f"DELETE FROM scan_checkpoints WHERE {source_filter}", ids)
            conn.execute(f"DELETE FROM source_registry WHERE {source_filter}", ids)
            conn.commit()
        except BaseException:
            conn.rollback()
            raise

        return {"jobs": jobs, "checkpoints": checkpoints, "sources": len(ids)}

    def clear_source_history(
        self,
        source_chat_id: int | str,
        latest_message_id: int,
        *,
        source_topic_id: int | None = None,
    ) -> dict[str, int]:
        """Drop existing work but keep the source active for posts added later.

        The special checkpoint marker prevents an in-flight full scan from
        rebuilding the old queue. A future incremental sync begins after the
        source's current latest post.
        """
        source_id = str(source_chat_id)
        ids = self._source_ids_for_cleanup(source_id)
        placeholders = ", ".join("?" for _ in ids)
        source_filter = f"source_chat_id IN ({placeholders})"
        checkpoint = max(0, int(latest_message_id))
        topic_key = int(source_topic_id or 0)
        now = utc_now()
        conn = self.db.conn
        try:
            conn.execute("BEGIN IMMEDIATE")
            checkpoint_row = conn.execute(
                f"SELECT COUNT(*) AS count FROM scan_checkpoints WHERE {source_filter}",
                ids,
            ).fetchone()
            checkpoints = int(checkpoint_row["count"] if checkpoint_row else 0)
            jobs = self._delete_source_jobs_locked(ids)
            conn.execute(f"DELETE FROM scan_checkpoints WHERE {source_filter}", ids)
            conn.execute(
                """
                INSERT INTO scan_checkpoints (
                    source_chat_id, source_topic_key, last_scanned_message_id,
                    last_scan_mode, created_at, updated_at
                ) VALUES (?, ?, ?, 'skip_history', ?, ?)
                ON CONFLICT(source_chat_id, source_topic_key) DO UPDATE SET
                    last_scanned_message_id = excluded.last_scanned_message_id,
                    last_scan_mode = excluded.last_scan_mode,
                    updated_at = excluded.updated_at
                """,
                (source_id, topic_key, checkpoint, now, now),
            )
            conn.commit()
        except BaseException:
            conn.rollback()
            raise

        return {"jobs": jobs, "checkpoints": checkpoints, "checkpoint": checkpoint}

    def history_clear_is_pending(
        self,
        source_chat_id: int | str,
        *,
        source_topic_id: int | None = None,
    ) -> bool:
        row = self.db.get_scan_checkpoint(source_chat_id, source_topic_id)
        return bool(row and str(row["last_scan_mode"]) == "skip_history")

    def recompute_source_state(self, source_chat_id: int | str) -> dict[str, Any]:
        source_id = str(source_chat_id)
        row = self.db.query_one(
            """
            SELECT
                MAX(source_message_id) AS highest,
                SUM(CASE WHEN status IN ('pending', 'downloading', 'uploading', 'failed') THEN 1 ELSE 0 END) AS open_jobs,
                SUM(CASE WHEN status = 'skipped' AND LOWER(COALESCE(last_error, '')) NOT LIKE '%filtered out by config%' THEN 1 ELSE 0 END) AS issue_jobs,
                SUM(CASE WHEN status = 'copied' AND verified_at IS NULL
                    AND NOT EXISTS (
                        SELECT 1 FROM verification_results vr
                        WHERE vr.job_id = messages.id
                          AND vr.status IN ('verified', 'verified_repaired')
                    ) THEN 1 ELSE 0 END) AS unverified_jobs
            FROM messages
            WHERE source_chat_id = ?
              AND file_unique_key NOT LIKE 'repair:%'
            """,
            (source_id,),
        )
        highest = int(row["highest"] or 0) if row else 0
        open_jobs = int(row["open_jobs"] or 0) if row else 0
        issue_jobs = int(row["issue_jobs"] or 0) if row else 0
        unverified_jobs = int(row["unverified_jobs"] or 0) if row else 0
        if issue_jobs:
            state = "issues"
        elif open_jobs or unverified_jobs:
            state = "in_progress"
        elif highest:
            state = "verified"
        else:
            state = "not_started"
        verified_through = highest if state == "verified" else None
        self.db.execute(
            """
            UPDATE source_registry
            SET migration_state = ?,
                history_verified_through = COALESCE(?, history_verified_through),
                updated_at = ?
            WHERE source_chat_id = ?
            """,
            (state, verified_through, utc_now(), source_id),
        )
        return {
            "source_chat_id": source_id,
            "migration_state": state,
            "history_verified_through": verified_through,
            "open_jobs": open_jobs,
            "issue_jobs": issue_jobs,
            "unverified_jobs": unverified_jobs,
        }

    def is_destination_paused(self, dest_chat_id: int | str) -> bool:
        row = self.db.query_one(
            "SELECT paused FROM destination_health WHERE dest_chat_id = ?",
            (str(dest_chat_id),),
        )
        return bool(row and int(row["paused"]))

    def pause_destination(self, dest_chat_id: int | str, reason: str, error: str | None = None) -> None:
        now = utc_now()
        self.db.execute(
            """
            INSERT INTO destination_health (dest_chat_id, paused, pause_reason, last_error, updated_at)
            VALUES (?, 1, ?, ?, ?)
            ON CONFLICT(dest_chat_id) DO UPDATE SET
                paused = 1,
                pause_reason = excluded.pause_reason,
                last_error = excluded.last_error,
                updated_at = excluded.updated_at
            """,
            (str(dest_chat_id), reason[:1000], (error or reason)[:4000], now),
        )

    def resume_destination(self, dest_chat_id: int | str) -> None:
        self.db.execute(
            """
            INSERT INTO destination_health (dest_chat_id, paused, updated_at)
            VALUES (?, 0, ?)
            ON CONFLICT(dest_chat_id) DO UPDATE SET
                paused = 0, pause_reason = NULL, last_error = NULL, updated_at = excluded.updated_at
            """,
            (str(dest_chat_id), utc_now()),
        )

    def log_repair(
        self,
        *,
        action: str,
        job_id: int | None = None,
        source_chat_id: int | str | None = None,
        dest_chat_id: int | str | None = None,
        reason: str | None = None,
        outcome: str = "recorded",
        details: dict[str, Any] | None = None,
    ) -> None:
        self.db.execute(
            """
            INSERT INTO repair_actions (
                job_id, source_chat_id, dest_chat_id, action, reason, outcome, details, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                str(source_chat_id) if source_chat_id is not None else None,
                str(dest_chat_id) if dest_chat_id is not None else None,
                action,
                reason[:4000] if reason else None,
                outcome,
                _json(details or {}),
                utc_now(),
            ),
        )

    def verification_status(self, job_id: int) -> str | None:
        row = self.db.query_one("SELECT status FROM verification_results WHERE job_id = ?", (job_id,))
        return str(row["status"]) if row else None

    def should_verify(self, job_id: int) -> bool:
        status = self.verification_status(job_id)
        return status not in TERMINAL_VERIFICATION_STATES

    def record_verification(
        self,
        *,
        job_id: int,
        status: str,
        expected_count: int,
        present_count: int,
        media_match: bool | None,
        caption_match: bool | None,
        size_match: bool | None,
        missing_source_message_ids: list[int] | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.db.execute(
            """
            INSERT INTO verification_results (
                job_id, status, expected_count, present_count, media_match,
                caption_match, size_match, missing_source_message_ids, details, checked_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(job_id) DO UPDATE SET
                status = excluded.status,
                expected_count = excluded.expected_count,
                present_count = excluded.present_count,
                media_match = excluded.media_match,
                caption_match = excluded.caption_match,
                size_match = excluded.size_match,
                missing_source_message_ids = excluded.missing_source_message_ids,
                details = excluded.details,
                checked_at = excluded.checked_at
            """,
            (
                int(job_id),
                status,
                int(expected_count),
                int(present_count),
                None if media_match is None else int(media_match),
                None if caption_match is None else int(caption_match),
                None if size_match is None else int(size_match),
                _json(missing_source_message_ids or []),
                _json(details or {}),
                utc_now(),
            ),
        )

    def link_repair(
        self,
        *,
        parent_job_id: int,
        repair_job_id: int,
        source_message_id: int,
    ) -> None:
        now = utc_now()
        self.db.execute(
            """
            INSERT INTO repair_links (
                parent_job_id, repair_job_id, source_message_id, status, created_at, updated_at
            ) VALUES (?, ?, ?, 'pending', ?, ?)
            ON CONFLICT(parent_job_id, source_message_id) DO UPDATE SET
                repair_job_id = excluded.repair_job_id,
                updated_at = excluded.updated_at
            """,
            (parent_job_id, repair_job_id, source_message_id, now, now),
        )

    def complete_repair_job(self, repair_job_id: int) -> int | None:
        row = self.db.query_one(
            "SELECT parent_job_id FROM repair_links WHERE repair_job_id = ?",
            (repair_job_id,),
        )
        if not row:
            return None
        parent_job_id = int(row["parent_job_id"])
        self.db.execute(
            "UPDATE repair_links SET status = 'verified', updated_at = ? WHERE repair_job_id = ?",
            (utc_now(), repair_job_id),
        )
        pending = self.db.query_one(
            "SELECT COUNT(*) AS count FROM repair_links WHERE parent_job_id = ? AND status != 'verified'",
            (parent_job_id,),
        )
        if pending and int(pending["count"]) == 0:
            current = self.db.query_one(
                "SELECT expected_count FROM verification_results WHERE job_id = ?",
                (parent_job_id,),
            )
            expected = int(current["expected_count"] or 0) if current else 0
            self.record_verification(
                job_id=parent_job_id,
                status="verified_repaired",
                expected_count=expected,
                present_count=expected,
                media_match=True,
                caption_match=True,
                size_match=True,
                details={"repair_completed": True},
            )
            self.db.set_status(parent_job_id, "copied", verified_at=utc_now())
        return parent_job_id

    def start_telemetry(self, job_id: int, bytes_total: int | None) -> None:
        now = utc_now()
        self.db.execute(
            """
            INSERT INTO job_telemetry (
                job_id, stage, bytes_total, bytes_processed, started_at,
                stage_started_at, updated_at
            ) VALUES (?, 'processing', ?, 0, ?, ?, ?)
            ON CONFLICT(job_id) DO UPDATE SET
                stage = 'processing',
                bytes_total = COALESCE(excluded.bytes_total, job_telemetry.bytes_total),
                bytes_processed = 0,
                speed_bps = NULL,
                eta_seconds = NULL,
                completed_at = NULL,
                started_at = excluded.started_at,
                stage_started_at = excluded.stage_started_at,
                updated_at = excluded.updated_at
            """,
            (job_id, bytes_total, now, now, now),
        )

    def update_telemetry(
        self,
        job_id: int,
        *,
        stage: str | None = None,
        route: str | None = None,
        bytes_processed: int | None = None,
        bytes_total: int | None = None,
        speed_bps: float | None = None,
        eta_seconds: float | None = None,
    ) -> None:
        row = self.db.query_one("SELECT stage FROM job_telemetry WHERE job_id = ?", (job_id,))
        if not row:
            self.start_telemetry(job_id, bytes_total)
            row = self.db.query_one("SELECT stage FROM job_telemetry WHERE job_id = ?", (job_id,))
        fields = ["updated_at = ?"]
        values: list[Any] = [utc_now()]
        if stage is not None:
            fields.append("stage = ?")
            values.append(stage)
            if not row or str(row["stage"]) != stage:
                fields.append("stage_started_at = ?")
                values.append(utc_now())
        for name, value in (
            ("route", route),
            ("bytes_processed", bytes_processed),
            ("bytes_total", bytes_total),
            ("speed_bps", speed_bps),
            ("eta_seconds", eta_seconds),
        ):
            if value is not None:
                fields.append(f"{name} = ?")
                values.append(value)
        values.append(job_id)
        self.db.execute(f"UPDATE job_telemetry SET {', '.join(fields)} WHERE job_id = ?", values)

    def finish_telemetry(self, job_id: int, *, stage: str, route: str | None = None) -> None:
        now = utc_now()
        self.db.execute(
            """
            UPDATE job_telemetry
            SET stage = ?, route = COALESCE(?, route), completed_at = ?,
                eta_seconds = 0, updated_at = ?
            WHERE job_id = ?
            """,
            (stage, route, now, now, job_id),
        )

    def telemetry_for_job(self, job_id: int) -> dict[str, Any]:
        row = self.db.query_one("SELECT * FROM job_telemetry WHERE job_id = ?", (job_id,))
        return dict(row) if row else {}

    def delivery_matrix(
        self,
        *,
        source_chat_id: int | str | None = None,
        source_message_id: int | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses = ["m.file_unique_key NOT LIKE 'repair:%'"]
        params: list[Any] = []
        if source_chat_id is not None:
            clauses.append("m.source_chat_id = ?")
            params.append(str(source_chat_id))
        if source_message_id is not None:
            clauses.append("m.source_message_id = ?")
            params.append(int(source_message_id))
        params.append(max(1, int(limit)) * 20)
        rows = self.db.query(
            f"""
            SELECT m.*, vr.status AS verification_status, dh.paused AS destination_paused
            FROM messages m
            LEFT JOIN verification_results vr ON vr.job_id = m.id
            LEFT JOIN destination_health dh ON dh.dest_chat_id = m.dest_chat_id
            WHERE {' AND '.join(clauses)}
            ORDER BY m.source_message_id DESC, m.id ASC
            LIMIT ?
            """,
            params,
        )
        grouped: dict[tuple[str, int, str], list[Row]] = defaultdict(list)
        for row in rows:
            grouped[(str(row["source_chat_id"]), int(row["source_message_id"]), str(row["file_unique_key"]))].append(row)

        result: list[dict[str, Any]] = []
        for (source_id, message_id, key), jobs in grouped.items():
            destinations: list[dict[str, Any]] = []
            for row in jobs:
                verification = str(row["verification_status"] or ("verified" if row["verified_at"] else "pending"))
                destinations.append(
                    {
                        "job_id": int(row["id"]),
                        "dest_chat_id": str(row["dest_chat_id"]),
                        "status": str(row["status"]),
                        "verification": verification,
                        "paused": bool(row["destination_paused"] or 0),
                        "attempts": int(row["attempts"]),
                        "last_error": row["last_error"],
                        "dest_message_ids": _decode_list(row["dest_message_ids"]),
                    }
                )
            statuses = {item["status"] for item in destinations}
            verifications = {item["verification"] for item in destinations}
            if any(item["paused"] for item in destinations) or statuses & {"failed", "skipped"} or "failed" in verifications:
                overall = "issues"
            elif "repairing" in verifications:
                overall = "repairing"
            elif statuses & {"pending", "downloading", "uploading"}:
                overall = "in_progress"
            elif all(item["status"] == "copied" and item["verification"] in {"verified", "verified_repaired"} for item in destinations):
                overall = "verified"
            else:
                overall = "awaiting_verification"
            first = jobs[0]
            result.append(
                {
                    "source_chat_id": source_id,
                    "source_message_id": message_id,
                    "file_unique_key": key,
                    "source_message_ids": _decode_list(first["source_message_ids"]),
                    "media_type": str(first["media_type"] or "unsupported"),
                    "file_size": first["file_size"],
                    "overall_status": overall,
                    "total_destinations": len(destinations),
                    "verified_destinations": sum(
                        1 for item in destinations if item["verification"] in {"verified", "verified_repaired"}
                    ),
                    "destinations": destinations,
                }
            )
            if len(result) >= limit:
                break
        return result
