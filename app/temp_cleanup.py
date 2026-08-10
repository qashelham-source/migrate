from __future__ import annotations

import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any


_ACTIVE_JOB_DIR = re.compile(r"job-\d+\Z")


@dataclass(frozen=True)
class TempReapSummary:
    """Result of removing abandoned download directories during manager startup."""

    scanned: int = 0
    deleted: int = 0
    freed_bytes: int = 0
    failed: int = 0


def _directory_bytes(path: Path) -> int:
    total = 0
    try:
        for item in path.rglob("*"):
            if not item.is_file():
                continue
            try:
                total += item.stat().st_size
            except OSError:
                continue
    except OSError:
        return total
    return total


def reap_abandoned_active_job_dirs(active_dir: Path, logger: Any | None = None) -> TempReapSummary:
    """Remove only stale ``active/job-<id>`` directories after queue recovery.

    The normal uploader cleanup runs in a ``finally`` block.  A force-stop or
    process crash skips that block, leaving local media behind.  This function
    is called by the single migration-manager process at startup, after active
    database jobs have been recovered, so every matching directory is from an
    interrupted run.  It deliberately leaves all non-job entries and symlinks
    untouched.
    """
    if not active_dir.is_dir():
        return TempReapSummary()

    scanned = deleted = freed_bytes = failed = 0
    try:
        candidates = list(active_dir.iterdir())
    except OSError as exc:
        if logger:
            logger.warning("Could not inspect active download directory %s: %s", active_dir, exc)
        return TempReapSummary(failed=1)

    for candidate in candidates:
        if (
            candidate.is_symlink()
            or not candidate.is_dir()
            or not _ACTIVE_JOB_DIR.fullmatch(candidate.name)
        ):
            continue
        scanned += 1
        candidate_bytes = _directory_bytes(candidate)
        try:
            shutil.rmtree(candidate)
        except FileNotFoundError:
            continue
        except OSError as exc:
            failed += 1
            if logger:
                logger.warning(
                    "Could not remove abandoned download directory %s: %s",
                    candidate,
                    exc,
                )
            continue
        deleted += 1
        freed_bytes += candidate_bytes

    summary = TempReapSummary(
        scanned=scanned,
        deleted=deleted,
        freed_bytes=freed_bytes,
        failed=failed,
    )
    if logger and (summary.deleted or summary.failed):
        logger.warning(
            "Startup temp cleanup: scanned=%s deleted=%s freed_bytes=%s failed=%s",
            summary.scanned,
            summary.deleted,
            summary.freed_bytes,
            summary.failed,
        )
    return summary
