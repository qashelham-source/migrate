from __future__ import annotations

import os
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


GIB = 1024 * 1024 * 1024


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        return max(0, int(value))
    except ValueError:
        return default


@dataclass(frozen=True)
class StoragePolicy:
    reserve_bytes: int = 5 * GIB
    warning_free_bytes: int = 10 * GIB
    critical_free_bytes: int = 3 * GIB

    @classmethod
    def from_environment(cls) -> "StoragePolicy":
        return cls(
            reserve_bytes=_env_int("STORAGE_RESERVE_BYTES", 5 * GIB),
            warning_free_bytes=_env_int("STORAGE_WARNING_FREE_BYTES", 10 * GIB),
            critical_free_bytes=_env_int("STORAGE_CRITICAL_FREE_BYTES", 3 * GIB),
        )


@dataclass(frozen=True)
class StorageSnapshot:
    total_bytes: int
    used_bytes: int
    free_bytes: int
    reserve_bytes: int
    usable_bytes: int
    warning: bool
    critical: bool

    def has_capacity(self, required_bytes: int | None) -> bool:
        if not required_bytes or required_bytes <= 0:
            return not self.critical
        return not self.critical and int(required_bytes) <= self.usable_bytes

    def as_status(self) -> dict[str, Any]:
        return {
            "storage_total_bytes": self.total_bytes,
            "storage_used_bytes": self.used_bytes,
            "storage_free_bytes": self.free_bytes,
            "storage_reserve_bytes": self.reserve_bytes,
            "storage_usable_bytes": self.usable_bytes,
            "storage_warning": self.warning,
            "storage_critical": self.critical,
        }


def storage_snapshot(path: Path, policy: StoragePolicy | None = None) -> StorageSnapshot:
    selected = policy or StoragePolicy.from_environment()
    target = path
    while not target.exists() and target != target.parent:
        target = target.parent
    usage = shutil.disk_usage(target)
    free = int(usage.free)
    return StorageSnapshot(
        total_bytes=int(usage.total),
        used_bytes=int(usage.used),
        free_bytes=free,
        reserve_bytes=selected.reserve_bytes,
        usable_bytes=max(0, free - selected.reserve_bytes),
        warning=free <= selected.warning_free_bytes,
        critical=free <= selected.critical_free_bytes,
    )


def estimate_eta_seconds(total: int | None, current: int, elapsed_seconds: float) -> float | None:
    if not total or total <= 0 or current <= 0 or elapsed_seconds <= 0:
        return None
    if current >= total:
        return 0.0
    speed = current / elapsed_seconds
    if speed <= 0:
        return None
    return max(0.0, (total - current) / speed)


@dataclass
class ProgressMeter:
    total: int | None = None
    started_monotonic: float = 0.0
    current: int = 0

    def __post_init__(self) -> None:
        if self.started_monotonic <= 0:
            self.started_monotonic = time.monotonic()

    def update(self, current: int, total: int | None = None) -> dict[str, float | int | None]:
        self.current = max(0, int(current))
        if total is not None and total > 0:
            self.total = int(total)
        elapsed = max(0.001, time.monotonic() - self.started_monotonic)
        speed = self.current / elapsed if self.current > 0 else 0.0
        eta = estimate_eta_seconds(self.total, self.current, elapsed)
        return {
            "bytes_processed": self.current,
            "bytes_total": self.total,
            "speed_bps": speed,
            "eta_seconds": eta,
            "elapsed_seconds": elapsed,
        }
