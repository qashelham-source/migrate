from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from sqlite3 import Row
from typing import Any

from app.config import AppConfig
from app.db import Database, RecoverySummary, utc_now
from app.release3_store import Release3Store
from app.skip_policy import expected_skip_reason_sql
from app.telegram_client import telegram_peer


@dataclass(frozen=True)
class MessageJob:
    id: int
    source_chat_id: int | str
    source_message_id: int
    dest_chat_id: int | str
    status: str
    attempts: int
    last_error: str | None
    next_retry_at: str | None
    file_unique_key: str
    source_topic_id: int | None
    dest_topic_id: int | None
    media_group_id: str | None
    source_message_ids: list[int]
    dest_message_ids: list[int]
    media_type: str
    file_size: int | None
    caption: str | None
    worker_id: str | None = None
    lease_token: int = 0
    lease_expires_at: str | None = None

    @classmethod
    def from_row(cls, row: Row) -> "MessageJob":
        # Some lightweight callers (and migrations) pass a mapping that predates
        # lease columns.  SQLite rows always have them after initialize(), but
        # restoring old data must remain backwards-compatible.
        keys = set(row.keys())
        worker_id = row["worker_id"] if "worker_id" in keys else None
        lease_token = row["lease_token"] if "lease_token" in keys else 0
        lease_expires_at = row["lease_expires_at"] if "lease_expires_at" in keys else None
        return cls(
            id=int(row["id"]),
            source_chat_id=telegram_peer(row["source_chat_id"]),
            source_message_id=int(row["source_message_id"]),
            dest_chat_id=telegram_peer(row["dest_chat_id"]),
            status=str(row["status"]),
            attempts=int(row["attempts"]),
            last_error=row["last_error"],
            next_retry_at=row["next_retry_at"],
            file_unique_key=str(row["file_unique_key"]),
            source_topic_id=row["source_topic_id"],
            dest_topic_id=row["dest_topic_id"],
            media_group_id=row["media_group_id"],
            source_message_ids=json.loads(row["source_message_ids"]),
            dest_message_ids=json.loads(row["dest_message_ids"] or "[]"),
            media_type=str(row["media_type"] or "unsupported"),
            file_size=row["file_size"],
            caption=row["caption"],
            worker_id=str(worker_id) if worker_id else None,
            lease_token=int(lease_token or 0),
            lease_expires_at=lease_expires_at,
        )


@dataclass(frozen=True)
class MediaCacheEntry:
    file_unique_key: str
    bot_file_ids: list[str]
    media_types: list[str]


@dataclass(frozen=True)
class DuplicateMediaDelivery:
    job_id: int
    source_chat_id: int | str
    source_message_id: int
    status: str


class MessageQueue:
    def __init__(self, db: Database, config: AppConfig) -> None:
        self.db = db
        self.config = config
        self.release3 = Release3Store(db)
        self.release3.initialize()

    def enqueue(
        self,
        *,
        source_chat_id: int | str,
        source_message_id: int,
        dest_chat_id: int | str,
        file_unique_key: str,
        source_message_ids: list[int],
        source_topic_id: int | None,
        dest_topic_id: int | None,
        media_group_id: str | None,
        media_type: str,
        file_size: int | None,
        caption: str | None,
        status: str = "pending",
        last_error: str | None = None,
    ) -> bool:
        return self.db.enqueue_message(
            source_chat_id=str(source_chat_id),
            source_message_id=source_message_id,
            dest_chat_id=str(dest_chat_id),
            file_unique_key=file_unique_key,
            source_message_ids=source_message_ids,
            source_topic_id=source_topic_id,
            dest_topic_id=dest_topic_id,
            media_group_id=media_group_id,
            media_type=media_type,
            file_size=file_size,
            caption=caption,
            status=status,
            last_error=last_error,
        )

    def find_duplicate_media_delivery(
        self,
        *,
        source_chat_id: int | str,
        source_message_id: int,
        dest_chat_id: int | str,
        dest_topic_id: int | None,
        file_unique_key: str,
    ) -> DuplicateMediaDelivery | None:
        row = self.db.find_duplicate_media_delivery(
            source_chat_id=str(source_chat_id),
            source_message_id=source_message_id,
            dest_chat_id=str(dest_chat_id),
            dest_topic_id=dest_topic_id,
            file_unique_key=file_unique_key,
        )
        if not row:
            return None
        return DuplicateMediaDelivery(
            job_id=int(row["id"]),
            source_chat_id=telegram_peer(row["source_chat_id"]),
            source_message_id=int(row["source_message_id"]),
            status=str(row["status"]),
        )

    def enqueue_with_duplicate_detection(
        self,
        *,
        source_chat_id: int | str,
        source_message_id: int,
        dest_chat_id: int | str,
        file_unique_key: str,
        source_message_ids: list[int],
        source_topic_id: int | None,
        dest_topic_id: int | None,
        media_group_id: str | None,
        media_type: str,
        file_size: int | None,
        caption: str | None,
    ) -> tuple[bool, DuplicateMediaDelivery | None]:
        """Queue a media delivery, recording a skip when its fingerprint already exists."""
        duplicate = self.find_duplicate_media_delivery(
            source_chat_id=source_chat_id,
            source_message_id=source_message_id,
            dest_chat_id=dest_chat_id,
            dest_topic_id=dest_topic_id,
            file_unique_key=file_unique_key,
        )
        if duplicate:
            reason = (
                "Skipped duplicate media fingerprint already queued or delivered "
                f"for this destination (job #{duplicate.job_id}, "
                f"source message #{duplicate.source_message_id})."
            )
            inserted = self.enqueue(
                source_chat_id=source_chat_id,
                source_message_id=source_message_id,
                dest_chat_id=dest_chat_id,
                file_unique_key=file_unique_key,
                source_message_ids=source_message_ids,
                source_topic_id=source_topic_id,
                dest_topic_id=dest_topic_id,
                media_group_id=media_group_id,
                media_type=media_type,
                file_size=file_size,
                caption=caption,
                status="skipped",
                last_error=reason,
            )
            return inserted, duplicate

        return (
            self.enqueue(
                source_chat_id=source_chat_id,
                source_message_id=source_message_id,
                dest_chat_id=dest_chat_id,
                file_unique_key=file_unique_key,
                source_message_ids=source_message_ids,
                source_topic_id=source_topic_id,
                dest_topic_id=dest_topic_id,
                media_group_id=media_group_id,
                media_type=media_type,
                file_size=file_size,
                caption=caption,
            ),
            None,
        )

    def fetch_due(
        self,
        limit: int,
        source_chat_id: int | str | None = None,
    ) -> list[MessageJob]:
        source_clause = ""
        source_params: tuple[Any, ...] = ()
        if source_chat_id is not None:
            source_clause = " AND m.source_chat_id = ?"
            source_params = (str(source_chat_id),)
        rows = self.db.query(
            f"""
            SELECT m.*
            FROM messages m
            LEFT JOIN destination_health dh ON dh.dest_chat_id = m.dest_chat_id
            WHERE m.status = 'pending'
              AND (m.next_retry_at IS NULL OR m.next_retry_at <= ?)
              AND COALESCE(dh.paused, 0) = 0
              {source_clause}
            ORDER BY m.updated_at ASC, m.id ASC
            LIMIT ?
            """,
            (utc_now(), *source_params, max(1, int(limit))),
        )
        return [MessageJob.from_row(row) for row in rows]

    def fetch_for_verification(
        self,
        limit: int,
        source_chat_id: int | str | None = None,
    ) -> list[MessageJob]:
        source_clause = ""
        source_params: tuple[Any, ...] = ()
        if source_chat_id is not None:
            source_clause = " AND m.source_chat_id = ?"
            source_params = (str(source_chat_id),)
        rows = self.db.query(
            f"""
            SELECT m.*
            FROM messages m
            LEFT JOIN verification_results vr ON vr.job_id = m.id
            WHERE m.status = 'copied'
              AND m.dest_message_ids IS NOT NULL
              AND m.verified_at IS NULL
              AND (vr.status IS NULL OR vr.status NOT IN ('verified', 'verified_repaired', 'failed', 'repairing'))
              {source_clause}
            ORDER BY m.updated_at ASC, m.id ASC
            LIMIT ?
            """,
            (*source_params, max(1, int(limit))),
        )
        return [MessageJob.from_row(row) for row in rows]

    def claim_due(
        self,
        limit: int,
        source_chat_id: int | str | None = None,
        *,
        worker_id: str | None = None,
        lease_seconds: int | float = 120,
    ) -> list[MessageJob]:
        """Claim work before processing so a second worker cannot send the same job."""
        owner = worker_id or f"manual-{uuid.uuid4().hex}"
        jobs = [
            MessageJob.from_row(row)
            for row in self.db.claim_due_messages(
                limit,
                source_chat_id=source_chat_id,
                worker_id=owner,
                lease_seconds=lease_seconds,
                excluded_source_chat_ids=getattr(self.config, "source_blacklist", []),
            )
        ]
        for job in jobs:
            self.release3.start_telemetry(job.id, job.file_size)
        return jobs

    @staticmethod
    def _lease_kwargs(job: MessageJob) -> dict[str, Any]:
        if job.worker_id is None:
            return {}
        return {
            "expected_worker_id": job.worker_id,
            "expected_lease_token": job.lease_token,
        }

    def set_phase(
        self,
        job: MessageJob | int,
        status: str,
        *,
        lease_seconds: int | float | None = None,
    ) -> bool:
        if isinstance(job, MessageJob):
            updated = self.db.set_status(
                job.id,
                status,
                lease_seconds=lease_seconds,
                **self._lease_kwargs(job),
            )
            job_id = job.id
        else:
            updated = self.db.set_status(job, status)
            job_id = job
        if updated:
            self.release3.update_telemetry(job_id, stage=status)
        return updated

    def begin_verification(self, job_id: int) -> bool:
        return self.db.start_verification(job_id)

    def clear_activity_phase(self, job_id: int) -> bool:
        return self.db.clear_activity_phase(job_id)

    def touch_active_job(
        self,
        job: MessageJob | int,
        *,
        lease_seconds: int | float | None = None,
    ) -> bool:
        if isinstance(job, MessageJob):
            return self.db.touch_active_job(
                job.id,
                worker_id=job.worker_id,
                lease_token=job.lease_token,
                lease_seconds=lease_seconds,
            )
        return self.db.touch_active_job(job)

    def mark_copied(
        self,
        job: MessageJob | int,
        dest_message_ids: list[int],
        route: str | None = None,
    ) -> bool:
        if isinstance(job, MessageJob):
            updated = self.db.set_status(
                job.id,
                "copied",
                last_error="",
                dest_message_ids=dest_message_ids,
                **self._lease_kwargs(job),
            )
            job_id = job.id
        else:
            updated = self.db.set_status(job, "copied", last_error="", dest_message_ids=dest_message_ids)
            job_id = job
        if not updated:
            return False
        self.release3.finish_telemetry(job_id, stage="copied", route=route)
        return True

    def mark_skipped(self, job: MessageJob | int, reason: str) -> bool:
        if isinstance(job, MessageJob):
            updated = self.db.set_status(
                job.id,
                "skipped",
                last_error=reason,
                **self._lease_kwargs(job),
            )
            job_id = job.id
        else:
            updated = self.db.set_status(job, "skipped", last_error=reason)
            job_id = job
        if not updated:
            return False
        self.release3.finish_telemetry(job_id, stage="skipped")
        parent_job_id = self.release3.fail_repair_job(job_id, reason)
        row = self.db.query_one("SELECT source_chat_id FROM messages WHERE id = ?", (parent_job_id or job_id,))
        if row:
            self.release3.recompute_source_state(row["source_chat_id"])
        return True

    @staticmethod
    def is_repair_job(job: MessageJob) -> bool:
        return job.file_unique_key.startswith("repair:")

    def cancel_job(self, job: MessageJob, reason: str) -> str:
        """Remove a non-repair job from live work while retaining its anti-duplicate record."""
        cancellation = f"Cancelled by policy. {str(reason or 'Terminal job cancelled.')}"
        if not self.db.set_status(
            job.id,
            "skipped",
            last_error=cancellation,
            **self._lease_kwargs(job),
        ):
            return cancellation
        self.release3.finish_telemetry(job.id, stage="skipped")
        row = self.db.query_one("SELECT source_chat_id FROM messages WHERE id = ?", (job.id,))
        if row:
            self.release3.recompute_source_state(row["source_chat_id"])
        return cancellation

    def cancel_terminal_issues(self) -> int:
        """Hide old terminal non-repair jobs without allowing a later scan to recreate them."""
        expected_skip = expected_skip_reason_sql("last_error")
        rows = self.db.query(
            f"""
            SELECT *
            FROM messages
            WHERE status IN ('failed', 'skipped')
              AND file_unique_key NOT LIKE 'repair:%'
              AND NOT {expected_skip}
              AND LOWER(COALESCE(last_error, '')) NOT LIKE
                  'interrupted during upload; destination result is unknown%'
              AND LOWER(COALESCE(last_error, '')) NOT LIKE
                  'upload connection interrupted; destination result is unknown%'
            ORDER BY id ASC
            """
        )
        for row in rows:
            self.cancel_job(MessageJob.from_row(row), str(row["last_error"] or ""))
        return len(rows)

    def mark_verified(self, job_id: int) -> None:
        self.db.set_status(job_id, "copied", verified_at=utc_now())
        parent_job_id = self.release3.complete_repair_job(job_id)
        row = self.db.query_one("SELECT source_chat_id FROM messages WHERE id = ?", (parent_job_id or job_id,))
        if row:
            self.release3.recompute_source_state(row["source_chat_id"])

    def mark_failure(self, job: MessageJob, error: str, attempts: int) -> str:
        if attempts >= self.config.queue.max_attempts:
            if not self.is_repair_job(job):
                self.cancel_job(job, f"Final automatic retry failed. {error}")
                return "cancelled"
            if not self.db.set_status(job.id, "failed", last_error=error, **self._lease_kwargs(job)):
                return "lease_lost"
            self.release3.finish_telemetry(job.id, stage="failed")
            parent_job_id = self.release3.fail_repair_job(job.id, error)
            row = self.db.query_one(
                "SELECT source_chat_id FROM messages WHERE id = ?",
                (parent_job_id or job.id,),
            )
            if row:
                self.release3.recompute_source_state(row["source_chat_id"])
            return "failed"
        backoff = self._backoff_for_attempt(attempts)
        next_retry = (datetime.now(timezone.utc) + timedelta(seconds=backoff)).isoformat(timespec="seconds")
        if not self.db.set_status(
            job.id,
            "pending",
            last_error=error,
            next_retry_at=next_retry,
            **self._lease_kwargs(job),
        ):
            return "lease_lost"
        self.release3.update_telemetry(job.id, stage="pending")
        return "pending"

    def defer_download_transport_failure(self, job: MessageJob, error: str, attempts: int) -> str:
        """Retry a connection loss that happened before any destination upload.

        A download can safely be repeated, so transport interruptions here stay in
        the automatic retry loop even after the normal job-attempt limit.  The
        backoff remains bounded by the configured retry schedule.
        """
        backoff = self._backoff_for_attempt(attempts)
        next_retry = (datetime.now(timezone.utc) + timedelta(seconds=backoff)).isoformat(timespec="seconds")
        reason = f"Download connection interrupted; retrying automatically. {error}"
        if not self.db.set_status(
            job.id,
            "pending",
            last_error=reason,
            next_retry_at=next_retry,
            **self._lease_kwargs(job),
        ):
            return reason
        self.release3.update_telemetry(job.id, stage="pending")
        return reason

    def hold_uncertain_upload(self, job: MessageJob, error: str) -> str:
        """Hold an upload whose destination result cannot be proven safely."""
        reason = (
            "Upload connection interrupted; destination result is unknown. "
            f"Verify destination before retrying manually. {error}"
        )
        if not self.db.set_status(job.id, "failed", last_error=reason, **self._lease_kwargs(job)):
            return reason
        self.release3.finish_telemetry(job.id, stage="failed")
        parent_job_id = self.release3.fail_repair_job(job.id, reason)
        row = self.db.query_one(
            "SELECT source_chat_id FROM messages WHERE id = ?",
            (parent_job_id or job.id,),
        )
        if row:
            self.release3.recompute_source_state(row["source_chat_id"])
        return reason

    def enqueue_repair_item(self, parent: MessageJob, source_message_id: int) -> int | None:
        """Create or safely requeue one repair child for a missing destination item."""
        source_message_id = int(source_message_id)
        unique_key = f"repair:{parent.id}:{source_message_id}"
        existing = self.db.query_one(
            """
            SELECT id, status FROM messages
            WHERE source_chat_id = ? AND dest_chat_id = ? AND file_unique_key = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (str(parent.source_chat_id), str(parent.dest_chat_id), unique_key),
        )
        inserted = False
        if existing:
            repair_job_id = int(existing["id"])
            if str(existing["status"]) in {"failed", "skipped"}:
                self.db.execute(
                    """
                    UPDATE messages
                    SET status = 'pending', attempts = 0,
                        last_error = ?, next_retry_at = NULL,
                        dest_message_ids = NULL, verified_at = NULL,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    ("Repair requeued after review", utc_now(), repair_job_id),
                )
        else:
            inserted = self.enqueue(
                source_chat_id=parent.source_chat_id,
                source_message_id=source_message_id,
                dest_chat_id=parent.dest_chat_id,
                file_unique_key=unique_key,
                source_message_ids=[source_message_id],
                source_topic_id=parent.source_topic_id,
                dest_topic_id=parent.dest_topic_id,
                media_group_id=None,
                media_type=parent.media_type,
                file_size=None,
                caption=parent.caption if source_message_id == parent.source_message_ids[0] else None,
                status="pending",
                last_error="Repair missing destination item",
            )
            row = self.db.query_one(
                """
                SELECT id FROM messages
                WHERE source_chat_id = ? AND dest_chat_id = ? AND file_unique_key = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (str(parent.source_chat_id), str(parent.dest_chat_id), unique_key),
            )
            if not row:
                return None
            repair_job_id = int(row["id"])

        self.release3.link_repair(
            parent_job_id=parent.id,
            repair_job_id=repair_job_id,
            source_message_id=source_message_id,
        )
        if inserted:
            self.log_repair(
                action="repair_missing_item",
                job=parent,
                reason=f"Destination item for source message #{source_message_id} was missing",
                outcome="queued",
                details={"repair_job_id": repair_job_id, "source_message_id": source_message_id},
            )
        return repair_job_id

    def record_verification(self, **kwargs: Any) -> None:
        self.release3.record_verification(**kwargs)

    def pause_destination(self, job: MessageJob, reason: str, error: str | None = None) -> None:
        self.release3.pause_destination(job.dest_chat_id, reason, error)
        self.release3.recompute_source_state(job.source_chat_id)

    def resume_destination(self, dest_chat_id: int | str) -> bool:
        return self.release3.resume_destination(dest_chat_id)

    def log_repair(
        self,
        *,
        action: str,
        job: MessageJob | None = None,
        reason: str | None = None,
        outcome: str = "recorded",
        details: dict[str, Any] | None = None,
    ) -> None:
        self.release3.log_repair(
            action=action,
            job_id=job.id if job else None,
            source_chat_id=job.source_chat_id if job else None,
            dest_chat_id=job.dest_chat_id if job else None,
            reason=reason,
            outcome=outcome,
            details=details,
        )

    def update_telemetry(self, job_id: int, **kwargs: Any) -> None:
        self.release3.update_telemetry(job_id, **kwargs)

    def telemetry_for_job(self, job_id: int) -> dict[str, Any]:
        return self.release3.telemetry_for_job(job_id)

    def delivery_matrix(
        self,
        *,
        source_chat_id: int | str | None = None,
        source_message_id: int | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        return self.release3.delivery_matrix(
            source_chat_id=source_chat_id,
            source_message_id=source_message_id,
            limit=limit,
        )

    def register_source(
        self,
        *,
        source_chat_id: int | str,
        title: str,
        username: str | None,
        chat_type: str,
        latest_seen_message_id: int | None,
        access_status: str = "ok",
    ) -> None:
        self.release3.upsert_source(
            source_chat_id=source_chat_id,
            title=title,
            username=username,
            chat_type=chat_type,
            latest_seen_message_id=latest_seen_message_id,
            access_status=access_status,
        )

    def list_registered_sources(self) -> list[dict[str, Any]]:
        return self.release3.list_sources()

    def purge_source_jobs(self, source_chat_id: int | str) -> dict[str, int]:
        """Delete all persisted work for a source that was removed by an admin."""
        return self.release3.purge_source_jobs(source_chat_id)

    def clear_source_history(
        self,
        source_chat_id: int | str,
        latest_message_id: int,
        *,
        source_topic_id: int | None = None,
    ) -> dict[str, int]:
        """Forget old work while keeping the source ready for future posts."""
        return self.release3.clear_source_history(
            source_chat_id,
            latest_message_id,
            source_topic_id=source_topic_id,
        )

    def history_clear_is_pending(
        self,
        source_chat_id: int | str,
        *,
        source_topic_id: int | None = None,
    ) -> bool:
        return self.release3.history_clear_is_pending(
            source_chat_id,
            source_topic_id=source_topic_id,
        )

    def set_source_scan_progress(
        self,
        source_chat_id: int | str,
        scanned_through: int,
        *,
        live_watch_enabled: bool | None = None,
    ) -> None:
        self.release3.set_source_scan_progress(
            source_chat_id,
            scanned_through,
            live_watch_enabled=live_watch_enabled,
        )

    def set_live_watch(self, source_chat_id: int | str, enabled: bool) -> None:
        self.release3.set_live_watch(source_chat_id, enabled)

    def recompute_source_state(self, source_chat_id: int | str) -> dict[str, Any]:
        return self.release3.recompute_source_state(source_chat_id)

    def recover_in_progress(self) -> RecoverySummary:
        return self.db.recover_in_progress()

    def requeue_peer_id_errors(self) -> int:
        return self.db.requeue_peer_id_errors()

    def counts_by_status(self, source_chat_id: int | str | None = None) -> dict[str, int]:
        return self.db.counts_by_status(source_chat_id)

    def source_work_state(self, source_chat_id: int | str) -> dict[str, Any]:
        return self.db.source_work_state(source_chat_id)

    def get_scan_checkpoint(
        self,
        source_chat_id: int | str,
        source_topic_id: int | None = None,
    ) -> int | None:
        row = self.db.get_scan_checkpoint(source_chat_id, source_topic_id)
        return int(row["last_scanned_message_id"]) if row else None

    def set_scan_checkpoint(
        self,
        source_chat_id: int | str,
        source_topic_id: int | None,
        last_scanned_message_id: int,
        scan_mode: str,
    ) -> None:
        self.db.set_scan_checkpoint(
            source_chat_id,
            source_topic_id,
            last_scanned_message_id,
            scan_mode,
        )

    def get_destination_scan_checkpoint(
        self,
        source_chat_id: int | str,
        source_topic_id: int | None,
        dest_chat_id: int | str,
        dest_topic_id: int | None,
    ) -> int | None:
        row = self.db.get_destination_scan_checkpoint(
            source_chat_id,
            source_topic_id,
            dest_chat_id,
            dest_topic_id,
        )
        return int(row["last_scanned_message_id"]) if row else None

    def set_destination_scan_checkpoint(
        self,
        source_chat_id: int | str,
        source_topic_id: int | None,
        dest_chat_id: int | str,
        dest_topic_id: int | None,
        last_scanned_message_id: int,
        scan_mode: str,
    ) -> None:
        self.db.set_destination_scan_checkpoint(
            source_chat_id,
            source_topic_id,
            dest_chat_id,
            dest_topic_id,
            last_scanned_message_id,
            scan_mode,
        )

    def source_queue_highwater(self, source_chat_id: int | str) -> int | None:
        return self.db.source_queue_highwater(source_chat_id)

    def get_media_cache(self, file_unique_key: str) -> MediaCacheEntry | None:
        row = self.db.get_media_cache(file_unique_key)
        if not row:
            return None
        try:
            bot_file_ids = [str(value) for value in json.loads(row["bot_file_ids"])]
            media_types = [str(value) for value in json.loads(row["media_types"])]
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
        if not bot_file_ids or len(bot_file_ids) != len(media_types):
            return None
        return MediaCacheEntry(
            file_unique_key=str(row["file_unique_key"]),
            bot_file_ids=bot_file_ids,
            media_types=media_types,
        )

    def save_media_cache(
        self,
        file_unique_key: str,
        bot_file_ids: list[str],
        media_types: list[str],
    ) -> None:
        self.db.save_media_cache(file_unique_key, bot_file_ids, media_types)

    def delete_media_cache(self, file_unique_key: str) -> None:
        self.db.delete_media_cache(file_unique_key)

    def recover_cached_file_id_mismatches(self) -> int:
        """Safely retry historical jobs rejected before Telegram accepted an upload."""
        rows = self.db.query(
            """
            SELECT id, source_chat_id, file_unique_key
            FROM messages
            WHERE status IN ('failed', 'skipped')
              AND LOWER(COALESCE(last_error, '')) LIKE
                  'valueerror: expected % got % file id instead%'
            """
        )
        if not rows:
            return 0

        now = utc_now()
        try:
            self.db.conn.execute("BEGIN IMMEDIATE")
            for row in rows:
                self.db.conn.execute(
                    "DELETE FROM media_cache WHERE file_unique_key = ?",
                    (str(row["file_unique_key"]),),
                )
                self.db.conn.execute(
                    """
                    UPDATE messages
                    SET status = 'pending', attempts = 0, last_error = NULL,
                        next_retry_at = NULL, dest_message_ids = NULL,
                        verified_at = NULL, updated_at = ?
                    WHERE id = ?
                    """,
                    (now, int(row["id"])),
                )
            self.db.conn.commit()
        except BaseException:
            self.db.conn.rollback()
            raise

        for source_chat_id in {str(row["source_chat_id"]) for row in rows}:
            self.release3.recompute_source_state(source_chat_id)
        return len(rows)

    def media_cache_count(self) -> int:
        return self.db.media_cache_count()

    def _backoff_for_attempt(self, attempts: int) -> int:
        if not self.config.queue.retry_backoff_seconds:
            return 300
        index = min(max(attempts - 1, 0), len(self.config.queue.retry_backoff_seconds) - 1)
        return self.config.queue.retry_backoff_seconds[index]
