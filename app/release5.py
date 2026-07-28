from __future__ import annotations

import math
import os
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from app.db import Database, utc_now
from app.telemetry import StoragePolicy, storage_snapshot


@dataclass(frozen=True)
class StorageEstimate:
    queued_bytes: int
    unknown_jobs: int
    temporary_peak_bytes: int
    required_bytes: int
    usable_bytes: int
    safe: bool
    headroom_ratio: float


@dataclass(frozen=True)
class ParallelismDecision:
    workers: int
    reason: str
    free_ratio: float
    pending_destinations: int


@dataclass(frozen=True)
class SmartETA:
    seconds: float | None
    speed_bps: float | None
    confidence: float
    remaining_bytes: int
    sample_count: int


class Release5Store:
    """Additive Release 5 persistence. It never mutates migration queue rows."""

    def __init__(self, db: Database) -> None:
        self.db = db

    def initialize(self) -> None:
        self.db.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS shadow_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                status TEXT NOT NULL DEFAULT 'planned',
                pending_jobs INTEGER NOT NULL DEFAULT 0,
                destinations INTEGER NOT NULL DEFAULT 0,
                queued_bytes INTEGER NOT NULL DEFAULT 0,
                unknown_jobs INTEGER NOT NULL DEFAULT 0,
                required_bytes INTEGER NOT NULL DEFAULT 0,
                usable_bytes INTEGER NOT NULL DEFAULT 0,
                recommended_workers INTEGER NOT NULL DEFAULT 1,
                eta_seconds REAL,
                eta_confidence REAL NOT NULL DEFAULT 0,
                notes TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS performance_samples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                route TEXT,
                media_type TEXT,
                bytes_total INTEGER NOT NULL,
                duration_seconds REAL NOT NULL,
                speed_bps REAL NOT NULL,
                successful INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_performance_samples_recent
                ON performance_samples(successful, created_at DESC);
            """
        )
        self.db.conn.commit()

    def record_sample(
        self,
        *,
        bytes_total: int,
        duration_seconds: float,
        route: str | None = None,
        media_type: str | None = None,
        successful: bool = True,
    ) -> None:
        total = max(0, int(bytes_total))
        duration = max(0.001, float(duration_seconds))
        self.db.execute(
            """
            INSERT INTO performance_samples (
                route, media_type, bytes_total, duration_seconds, speed_bps,
                successful, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (route, media_type, total, duration, total / duration, int(successful), utc_now()),
        )

    def recent_speeds(self, limit: int = 50) -> list[float]:
        rows = self.db.query(
            """
            SELECT speed_bps FROM performance_samples
            WHERE successful = 1 AND speed_bps > 0
            ORDER BY id DESC LIMIT ?
            """,
            (max(1, int(limit)),),
        )
        return [float(row["speed_bps"]) for row in rows]

    def save_shadow_run(
        self,
        *,
        estimate: StorageEstimate,
        decision: ParallelismDecision,
        eta: SmartETA,
        pending_jobs: int,
        destinations: int,
        notes: str | None = None,
    ) -> int:
        cursor = self.db.execute(
            """
            INSERT INTO shadow_runs (
                pending_jobs, destinations, queued_bytes, unknown_jobs,
                required_bytes, usable_bytes, recommended_workers,
                eta_seconds, eta_confidence, notes, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(pending_jobs), int(destinations), estimate.queued_bytes,
                estimate.unknown_jobs, estimate.required_bytes, estimate.usable_bytes,
                decision.workers, eta.seconds, eta.confidence,
                notes[:4000] if notes else None, utc_now(),
            ),
        )
        return int(cursor.lastrowid)


def _pending_rows(db: Database) -> list[Any]:
    return db.query(
        """
        SELECT id, dest_chat_id, media_type, file_size
        FROM messages
        WHERE status = 'pending'
        ORDER BY id
        """
    )


def estimate_storage(
    db: Database,
    storage_path: str | Path,
    *,
    unknown_job_bytes: int = 256 * 1024 * 1024,
    simultaneous_downloads: int = 1,
    policy: StoragePolicy | None = None,
) -> StorageEstimate:
    rows = _pending_rows(db)
    known_sizes = [max(0, int(row["file_size"] or 0)) for row in rows if row["file_size"]]
    unknown_jobs = sum(1 for row in rows if not row["file_size"])
    queued_bytes = sum(known_sizes) + unknown_jobs * max(0, int(unknown_job_bytes))
    largest = sorted(known_sizes or [max(0, int(unknown_job_bytes))], reverse=True)
    peak_count = max(1, int(simultaneous_downloads))
    temporary_peak = sum(largest[:peak_count])
    selected = policy or StoragePolicy.from_environment()
    snapshot = storage_snapshot(Path(storage_path), selected)
    required = temporary_peak + max(64 * 1024 * 1024, int(temporary_peak * 0.10))
    usable = snapshot.usable_bytes
    headroom = (usable - required) / usable if usable > 0 else -1.0
    return StorageEstimate(
        queued_bytes=queued_bytes,
        unknown_jobs=unknown_jobs,
        temporary_peak_bytes=temporary_peak,
        required_bytes=required,
        usable_bytes=usable,
        safe=required <= usable and not snapshot.critical,
        headroom_ratio=headroom,
    )


def choose_safe_parallelism(
    db: Database,
    estimate: StorageEstimate,
    *,
    configured_max: int | None = None,
) -> ParallelismDecision:
    maximum = max(1, int(configured_max or os.getenv("MIGRATION_MAX_PARALLEL", "3")))
    rows = _pending_rows(db)
    destinations = len({str(row["dest_chat_id"]) for row in rows})
    if not rows:
        return ParallelismDecision(1, "queue_empty", 1.0, 0)
    free_ratio = max(-1.0, estimate.headroom_ratio)
    if not estimate.safe or free_ratio < 0.10:
        return ParallelismDecision(1, "storage_guard", free_ratio, destinations)
    workers = min(maximum, max(1, destinations))
    if free_ratio < 0.25:
        workers = min(workers, 2)
        reason = "limited_storage_headroom"
    else:
        reason = "destination_isolated_parallelism"
    return ParallelismDecision(workers, reason, free_ratio, destinations)


def smart_eta(
    db: Database,
    speeds: Iterable[float],
    *,
    fallback_speed_bps: float = 2 * 1024 * 1024,
) -> SmartETA:
    rows = _pending_rows(db)
    remaining = sum(max(0, int(row["file_size"] or 0)) for row in rows)
    values = sorted(float(value) for value in speeds if value and value > 0)
    if values:
        speed = statistics.median(values)
        spread = statistics.pstdev(values) / speed if len(values) > 1 and speed > 0 else 0.0
        sample_factor = min(1.0, len(values) / 12.0)
        confidence = max(0.1, min(0.99, sample_factor * (1.0 / (1.0 + spread))))
    else:
        speed = max(1.0, float(fallback_speed_bps))
        confidence = 0.15
    seconds = remaining / speed if remaining > 0 and speed > 0 else 0.0
    return SmartETA(seconds, speed, confidence, remaining, len(values))


def build_shadow_plan(
    db: Database,
    storage_path: str | Path,
    *,
    configured_max_workers: int | None = None,
    unknown_job_bytes: int = 256 * 1024 * 1024,
) -> dict[str, Any]:
    store = Release5Store(db)
    store.initialize()
    preliminary = estimate_storage(
        db,
        storage_path,
        unknown_job_bytes=unknown_job_bytes,
        simultaneous_downloads=max(1, int(configured_max_workers or 3)),
    )
    decision = choose_safe_parallelism(db, preliminary, configured_max=configured_max_workers)
    estimate = estimate_storage(
        db,
        storage_path,
        unknown_job_bytes=unknown_job_bytes,
        simultaneous_downloads=decision.workers,
    )
    eta = smart_eta(db, store.recent_speeds())
    rows = _pending_rows(db)
    destinations = len({str(row["dest_chat_id"]) for row in rows})
    warnings: list[str] = []
    if estimate.unknown_jobs:
        warnings.append(f"{estimate.unknown_jobs} job mempunyai saiz tidak diketahui")
    if not estimate.safe:
        warnings.append("storage tidak selamat untuk memulakan batch")
    run_id = store.save_shadow_run(
        estimate=estimate,
        decision=decision,
        eta=eta,
        pending_jobs=len(rows),
        destinations=destinations,
        notes="; ".join(warnings) or "shadow plan ready",
    )
    return {
        "run_id": run_id,
        "mode": "shadow",
        "read_only": True,
        "pending_jobs": len(rows),
        "destinations": destinations,
        "storage": estimate,
        "parallelism": decision,
        "eta": eta,
        "warnings": warnings,
    }
