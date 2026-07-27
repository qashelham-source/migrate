from __future__ import annotations


class JobError(Exception):
    """Base class for job processing failures."""


class PermanentJobError(JobError):
    """A job cannot succeed without user/config changes."""


class RetryableJobError(JobError):
    """A job may succeed later."""


def compact_error(exc: BaseException) -> str:
    message = f"{exc.__class__.__name__}: {exc}"
    return " ".join(message.split())[:4000]

