from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Any


_original_rmtree = shutil.rmtree
_original_mkdir = Path.mkdir
_logger = logging.getLogger("migration.temp_cleanup")
_MAX_REMAINING_FILES = 20


def _directory_usage(path: Path) -> tuple[int, int]:
    file_count = 0
    total_bytes = 0
    try:
        for item in path.rglob("*"):
            if not item.is_file():
                continue
            file_count += 1
            try:
                total_bytes += item.stat().st_size
            except OSError:
                pass
    except OSError:
        pass
    return file_count, total_bytes


def _remaining_files(path: Path) -> list[str]:
    remaining: list[str] = []
    try:
        for item in path.rglob("*"):
            if not item.is_file():
                continue
            try:
                size = item.stat().st_size
            except OSError:
                size = -1
            try:
                relative = item.relative_to(path)
            except ValueError:
                relative = item
            remaining.append(f"{relative}:{size}")
            if len(remaining) >= _MAX_REMAINING_FILES:
                break
    except OSError as exc:
        remaining.append(f"<scan-error>:{exc}")
    return remaining


def _is_migration_temp_job(path: Path) -> bool:
    return path.name.startswith("job-") and path.parent.name == "active"


def audited_mkdir(self: Path, *args: Any, **kwargs: Any) -> None:
    existed = self.exists()
    _original_mkdir(self, *args, **kwargs)
    if not existed and _is_migration_temp_job(self):
        _logger.info("JOB_DIR_CREATED path=%s", self)


def audited_rmtree(path: str | bytes | Path, *args: Any, **kwargs: Any) -> None:
    target = Path(path)
    audit = _is_migration_temp_job(target)
    existed = target.exists() if audit else False
    file_count, total_bytes = _directory_usage(target) if existed else (0, 0)

    if audit:
        _logger.info(
            "TEMP_CLEANUP_START path=%s files=%s bytes=%s",
            target,
            file_count,
            total_bytes,
        )

    try:
        _original_rmtree(path, *args, **kwargs)
    except Exception:
        if audit:
            _logger.exception(
                "TEMP_CLEANUP_FAILED path=%s files=%s bytes=%s remaining=%s",
                target,
                file_count,
                total_bytes,
                _remaining_files(target),
            )
        raise

    if not audit:
        return
    if target.exists():
        remaining_count, remaining_bytes = _directory_usage(target)
        _logger.warning(
            "TEMP_CLEANUP_INCOMPLETE path=%s files_before=%s bytes_before=%s "
            "files_remaining=%s bytes_remaining=%s remaining=%s",
            target,
            file_count,
            total_bytes,
            remaining_count,
            remaining_bytes,
            _remaining_files(target),
        )
    elif existed:
        _logger.info(
            "TEMP_CLEANUP_DELETED path=%s files=%s bytes=%s",
            target,
            file_count,
            total_bytes,
        )
    else:
        _logger.info("TEMP_CLEANUP_NOT_FOUND path=%s", target)


Path.mkdir = audited_mkdir
shutil.rmtree = audited_rmtree
