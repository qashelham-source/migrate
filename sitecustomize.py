from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Any


_original_rmtree = shutil.rmtree
_original_mkdir = Path.mkdir
_logger = logging.getLogger("migration.temp_cleanup")
_callsite_logger = logging.getLogger("migration.call_site")
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


def _install_call_site_audit() -> None:
    try:
        from app.upload import Uploader
    except Exception:
        _callsite_logger.exception("CALLSITE_AUDIT_INSTALL_FAILED")
        return

    if getattr(Uploader, "_callsite_audit_installed", False):
        return

    original_download_and_upload = Uploader._download_and_upload
    original_download_one = Uploader._download_one
    original_upload_downloaded = Uploader._upload_downloaded

    async def audited_download_and_upload(
        self: Any,
        job: Any,
        messages: list[Any],
        stop_event: Any,
        on_phase: Any,
    ) -> Any:
        job_dir = self.config.downloads.active_dir / f"job-{job.id}"
        _callsite_logger.info(
            "CALLSITE_ENTER job=%s path=%s messages=%s",
            job.id,
            job_dir,
            len(messages),
        )
        try:
            return await original_download_and_upload(self, job, messages, stop_event, on_phase)
        except BaseException:
            _callsite_logger.exception("CALLSITE_ERROR job=%s path=%s", job.id, job_dir)
            raise
        finally:
            files, total_bytes = _directory_usage(job_dir) if job_dir.exists() else (0, 0)
            _callsite_logger.info(
                "CALLSITE_EXIT job=%s path=%s exists=%s files=%s bytes=%s",
                job.id,
                job_dir,
                job_dir.exists(),
                files,
                total_bytes,
            )

    async def audited_download_one(self: Any, message: Any, job_dir: Path) -> Path:
        _callsite_logger.info(
            "DOWNLOAD_START job_path=%s message=%s",
            job_dir,
            getattr(message, "id", None),
        )
        try:
            result = await original_download_one(self, message, job_dir)
        except BaseException:
            _callsite_logger.exception(
                "DOWNLOAD_FAILED job_path=%s message=%s",
                job_dir,
                getattr(message, "id", None),
            )
            raise
        size = result.stat().st_size if result.exists() else -1
        _callsite_logger.info(
            "DOWNLOAD_FINISHED job_path=%s message=%s file=%s bytes=%s",
            job_dir,
            getattr(message, "id", None),
            result,
            size,
        )
        return result

    async def audited_upload_downloaded(self: Any, job: Any, downloaded: list[Any]) -> Any:
        files = [str(path) for _, path in downloaded]
        _callsite_logger.info(
            "UPLOAD_START job=%s files=%s paths=%s",
            job.id,
            len(downloaded),
            files,
        )
        try:
            result = await original_upload_downloaded(self, job, downloaded)
        except BaseException:
            _callsite_logger.exception("UPLOAD_FAILED job=%s files=%s", job.id, len(downloaded))
            raise
        _callsite_logger.info("UPLOAD_FINISHED job=%s files=%s", job.id, len(downloaded))
        return result

    Uploader._download_and_upload = audited_download_and_upload
    Uploader._download_one = audited_download_one
    Uploader._upload_downloaded = audited_upload_downloaded
    Uploader._callsite_audit_installed = True
    _callsite_logger.info("CALLSITE_AUDIT_INSTALLED")


Path.mkdir = audited_mkdir
shutil.rmtree = audited_rmtree
_install_call_site_audit()
