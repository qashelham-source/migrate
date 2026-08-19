from __future__ import annotations

DUPLICATE_CLEANUP_SKIP_MARKER = "deleted duplicate delivery by admin cleanup"


EXPECTED_SKIP_REASON_MARKERS: tuple[str, ...] = (
    "filtered out by config",
    "skipped duplicate media fingerprint",
    "above configured bot upload limit",
    "forwarding_only is enabled",
    "text message was empty",
    "cancelled by policy",
    DUPLICATE_CLEANUP_SKIP_MARKER,
)


def is_expected_skip_reason(reason: str | None) -> bool:
    """Return whether a retained skipped job is an intentional policy outcome."""

    normalized = str(reason or "").casefold()
    return any(marker in normalized for marker in EXPECTED_SKIP_REASON_MARKERS)


def expected_skip_reason_sql(error_column: str) -> str:
    """Build a safe internal SQL predicate for intentional skipped-job reasons."""

    normalized = f"LOWER(COALESCE({error_column}, ''))"
    clauses = [f"{normalized} LIKE '%{marker}%'" for marker in EXPECTED_SKIP_REASON_MARKERS]
    return "(" + " OR ".join(clauses) + ")"
