from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Any


_original_rmtree = shutil.rmtree
_logger = logging.getLogger("migration.temp_cleanup")


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


def _is_migration_temp_job(path: Path) -> bool:
    return path.name.startswith("job-") and path.parent.name == "active"


def audited_rmtree(path: str | bytes | Path, *args: Any, **kwargs: Any) -> None:
    target = Path(path)
    audit = _is_migration_temp_job(target)
    existed = target.exists() if audit else False
    file_count, total_bytes = _directory_usage(target) if existed else (0, 0)

    try:
        _original_rmtree(path, *args, **kwargs)
    except Exception:
        if audit:
            _logger.exception(
                "TEMP_CLEANUP_FAILED path=%s files=%s bytes=%s",
                target,
                file_count,
                total_bytes,
            )
        raise

    if not audit:
        return
    if target.exists():
        _logger.warning(
            "TEMP_CLEANUP_INCOMPLETE path=%s files_before=%s bytes_before=%s",
            target,
            file_count,
            total_bytes,
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


shutil.rmtree = audited_rmtree
