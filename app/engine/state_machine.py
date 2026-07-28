from __future__ import annotations

from enum import StrEnum


class EngineState(StrEnum):
    DISCOVERED = "discovered"
    PLANNED = "planned"
    LEASED = "leased"
    DOWNLOADING = "downloading"
    DOWNLOADED = "downloaded"
    READY_TO_UPLOAD = "ready_to_upload"
    UPLOADING = "uploading"
    UPLOADED_UNCONFIRMED = "uploaded_unconfirmed"
    VERIFYING = "verifying"
    VERIFIED = "verified"
    COMMITTED = "committed"
    CLEANING = "cleaning"
    DONE = "done"
    WAITING_FLOODWAIT = "waiting_floodwait"
    WAITING_STORAGE = "waiting_storage"
    WAITING_DEPENDENCY = "waiting_dependency"
    RETRY_SCHEDULED = "retry_scheduled"
    PAUSED_DESTINATION = "paused_destination"
    NEEDS_RECONCILIATION = "needs_reconciliation"
    QUARANTINED = "quarantined"
    CANCELLED = "cancelled"
    FAILED_PERMANENT = "failed_permanent"


TERMINAL_STATES = {
    EngineState.DONE,
    EngineState.CANCELLED,
    EngineState.FAILED_PERMANENT,
}


_ALLOWED: dict[EngineState, set[EngineState]] = {
    EngineState.DISCOVERED: {EngineState.PLANNED, EngineState.CANCELLED},
    EngineState.PLANNED: {
        EngineState.LEASED,
        EngineState.WAITING_STORAGE,
        EngineState.WAITING_DEPENDENCY,
        EngineState.PAUSED_DESTINATION,
        EngineState.CANCELLED,
    },
    EngineState.LEASED: {
        EngineState.DOWNLOADING,
        EngineState.READY_TO_UPLOAD,
        EngineState.RETRY_SCHEDULED,
        EngineState.CANCELLED,
    },
    EngineState.DOWNLOADING: {
        EngineState.DOWNLOADED,
        EngineState.WAITING_FLOODWAIT,
        EngineState.WAITING_STORAGE,
        EngineState.RETRY_SCHEDULED,
        EngineState.FAILED_PERMANENT,
        EngineState.CANCELLED,
    },
    EngineState.DOWNLOADED: {
        EngineState.READY_TO_UPLOAD,
        EngineState.RETRY_SCHEDULED,
        EngineState.CANCELLED,
    },
    EngineState.READY_TO_UPLOAD: {
        EngineState.UPLOADING,
        EngineState.WAITING_FLOODWAIT,
        EngineState.PAUSED_DESTINATION,
        EngineState.RETRY_SCHEDULED,
        EngineState.CANCELLED,
    },
    EngineState.UPLOADING: {
        EngineState.UPLOADED_UNCONFIRMED,
        EngineState.VERIFYING,
        EngineState.NEEDS_RECONCILIATION,
        EngineState.WAITING_FLOODWAIT,
        EngineState.RETRY_SCHEDULED,
        EngineState.FAILED_PERMANENT,
        EngineState.CANCELLED,
    },
    EngineState.UPLOADED_UNCONFIRMED: {
        EngineState.VERIFYING,
        EngineState.NEEDS_RECONCILIATION,
        EngineState.QUARANTINED,
    },
    EngineState.NEEDS_RECONCILIATION: {
        EngineState.VERIFYING,
        EngineState.READY_TO_UPLOAD,
        EngineState.QUARANTINED,
        EngineState.FAILED_PERMANENT,
    },
    EngineState.VERIFYING: {
        EngineState.VERIFIED,
        EngineState.NEEDS_RECONCILIATION,
        EngineState.RETRY_SCHEDULED,
        EngineState.QUARANTINED,
    },
    EngineState.VERIFIED: {EngineState.COMMITTED},
    EngineState.COMMITTED: {EngineState.CLEANING, EngineState.DONE},
    EngineState.CLEANING: {EngineState.DONE, EngineState.RETRY_SCHEDULED},
    EngineState.WAITING_FLOODWAIT: {
        EngineState.LEASED,
        EngineState.READY_TO_UPLOAD,
        EngineState.CANCELLED,
    },
    EngineState.WAITING_STORAGE: {EngineState.PLANNED, EngineState.CANCELLED},
    EngineState.WAITING_DEPENDENCY: {EngineState.PLANNED, EngineState.CANCELLED},
    EngineState.RETRY_SCHEDULED: {
        EngineState.LEASED,
        EngineState.READY_TO_UPLOAD,
        EngineState.NEEDS_RECONCILIATION,
        EngineState.FAILED_PERMANENT,
        EngineState.CANCELLED,
    },
    EngineState.PAUSED_DESTINATION: {EngineState.PLANNED, EngineState.READY_TO_UPLOAD, EngineState.CANCELLED},
    EngineState.QUARANTINED: {
        EngineState.VERIFYING,
        EngineState.READY_TO_UPLOAD,
        EngineState.COMMITTED,
        EngineState.FAILED_PERMANENT,
        EngineState.CANCELLED,
    },
    EngineState.DONE: set(),
    EngineState.CANCELLED: set(),
    EngineState.FAILED_PERMANENT: set(),
}


LEGACY_TO_ENGINE_STATE: dict[str, EngineState] = {
    "pending": EngineState.PLANNED,
    "downloading": EngineState.DOWNLOADING,
    "uploading": EngineState.UPLOADING,
    "copied": EngineState.VERIFYING,
    "failed": EngineState.FAILED_PERMANENT,
    "skipped": EngineState.CANCELLED,
}


class InvalidStateTransition(ValueError):
    pass


def normalize_state(value: str | EngineState) -> EngineState:
    if isinstance(value, EngineState):
        return value
    try:
        return EngineState(value)
    except ValueError:
        try:
            return LEGACY_TO_ENGINE_STATE[value]
        except KeyError as exc:
            raise ValueError(f"Unknown engine state: {value}") from exc


def can_transition(current: str | EngineState, target: str | EngineState) -> bool:
    source = normalize_state(current)
    destination = normalize_state(target)
    return destination in _ALLOWED[source]


def require_transition(current: str | EngineState, target: str | EngineState) -> None:
    source = normalize_state(current)
    destination = normalize_state(target)
    if destination not in _ALLOWED[source]:
        raise InvalidStateTransition(f"Invalid engine transition: {source.value} -> {destination.value}")
