from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass
from typing import Any, Iterable, Mapping

from app.db import Database, utc_now

_TELEGRAM_LINK = re.compile(
    r"(?:https?://)?t\.me/(?:c/)?(?P<chat>[A-Za-z0-9_+-]+)/(?P<message>\d+)(?:\?.*)?$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class MediaDescriptor:
    media_type: str
    file_size: int | None = None
    duration: int | None = None
    width: int | None = None
    height: int | None = None
    mime_type: str | None = None
    file_name: str | None = None
    telegram_file_unique_id: str | None = None
    thumbnail_hash: str | None = None

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "MediaDescriptor":
        return cls(
            media_type=str(value.get("media_type") or "unknown").lower(),
            file_size=_positive_int(value.get("file_size")),
            duration=_positive_int(value.get("duration")),
            width=_positive_int(value.get("width")),
            height=_positive_int(value.get("height")),
            mime_type=_clean(value.get("mime_type")),
            file_name=_clean(value.get("file_name")),
            telegram_file_unique_id=_clean(value.get("telegram_file_unique_id")),
            thumbnail_hash=_clean(value.get("thumbnail_hash")),
        )

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "media_type": self.media_type,
            "file_size": self.file_size,
            "duration": self.duration,
            "width": self.width,
            "height": self.height,
            "mime_type": (self.mime_type or "").lower(),
            "telegram_file_unique_id": self.telegram_file_unique_id or "",
            "thumbnail_hash": (self.thumbnail_hash or "").lower(),
        }


@dataclass(frozen=True)
class MatchResult:
    fingerprint_id: int
    source_chat_id: str
    source_message_id: int
    media_type: str
    confidence: float
    reasons: tuple[str, ...]
    differences: tuple[str, ...]
    is_duplicate: bool

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _clean(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _positive_int(value: Any) -> int | None:
    try:
        result = int(value)
    except (TypeError, ValueError):
        return None
    return result if result >= 0 else None


def build_fingerprint(descriptor: MediaDescriptor | Mapping[str, Any]) -> str:
    """Build a stable SHA-256 fingerprint without caption or message ID."""
    item = descriptor if isinstance(descriptor, MediaDescriptor) else MediaDescriptor.from_mapping(descriptor)
    payload = json.dumps(item.canonical_payload(), sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def parse_telegram_reference(value: str) -> tuple[str, int] | None:
    text = str(value or "").strip()
    match = _TELEGRAM_LINK.search(text)
    if match:
        return match.group("chat"), int(match.group("message"))
    if text.isdigit():
        return "", int(text)
    return None


def initialize_media_finder(db: Database) -> None:
    """Add Release 7 tables without changing or deleting existing queue data."""
    db.conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS media_fingerprints (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fingerprint TEXT NOT NULL,
            source_chat_id TEXT NOT NULL,
            source_message_id INTEGER NOT NULL,
            media_type TEXT NOT NULL,
            file_size INTEGER,
            duration INTEGER,
            width INTEGER,
            height INTEGER,
            mime_type TEXT,
            file_name TEXT,
            telegram_file_unique_id TEXT,
            thumbnail_hash TEXT,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(source_chat_id, source_message_id)
        );
        CREATE INDEX IF NOT EXISTS idx_media_fingerprints_hash
            ON media_fingerprints(fingerprint);
        CREATE INDEX IF NOT EXISTS idx_media_fingerprints_file_unique
            ON media_fingerprints(telegram_file_unique_id);
        CREATE INDEX IF NOT EXISTS idx_media_fingerprints_shape
            ON media_fingerprints(media_type, file_size, duration, width, height);

        CREATE TABLE IF NOT EXISTS media_match_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query_fingerprint TEXT NOT NULL,
            matched_fingerprint_id INTEGER,
            confidence REAL NOT NULL,
            reasons_json TEXT NOT NULL,
            differences_json TEXT NOT NULL,
            is_duplicate INTEGER NOT NULL DEFAULT 0,
            searched_at TEXT NOT NULL,
            FOREIGN KEY(matched_fingerprint_id) REFERENCES media_fingerprints(id)
        );
        CREATE INDEX IF NOT EXISTS idx_media_match_history_query
            ON media_match_history(query_fingerprint, searched_at);
        """
    )
    db.conn.commit()


def index_media(
    db: Database,
    *,
    source_chat_id: int | str,
    source_message_id: int,
    descriptor: MediaDescriptor | Mapping[str, Any],
    metadata: Mapping[str, Any] | None = None,
) -> int:
    initialize_media_finder(db)
    item = descriptor if isinstance(descriptor, MediaDescriptor) else MediaDescriptor.from_mapping(descriptor)
    fingerprint = build_fingerprint(item)
    now = utc_now()
    db.execute(
        """
        INSERT INTO media_fingerprints (
            fingerprint, source_chat_id, source_message_id, media_type, file_size,
            duration, width, height, mime_type, file_name, telegram_file_unique_id,
            thumbnail_hash, metadata_json, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_chat_id, source_message_id) DO UPDATE SET
            fingerprint=excluded.fingerprint,
            media_type=excluded.media_type,
            file_size=excluded.file_size,
            duration=excluded.duration,
            width=excluded.width,
            height=excluded.height,
            mime_type=excluded.mime_type,
            file_name=excluded.file_name,
            telegram_file_unique_id=excluded.telegram_file_unique_id,
            thumbnail_hash=excluded.thumbnail_hash,
            metadata_json=excluded.metadata_json,
            updated_at=excluded.updated_at
        """,
        (
            fingerprint,
            str(source_chat_id),
            int(source_message_id),
            item.media_type,
            item.file_size,
            item.duration,
            item.width,
            item.height,
            item.mime_type,
            item.file_name,
            item.telegram_file_unique_id,
            item.thumbnail_hash,
            json.dumps(dict(metadata or {}), ensure_ascii=False, sort_keys=True),
            now,
            now,
        ),
    )
    row = db.query_one(
        "SELECT id FROM media_fingerprints WHERE source_chat_id = ? AND source_message_id = ?",
        (str(source_chat_id), int(source_message_id)),
    )
    if row is None:
        raise RuntimeError("Failed to index media fingerprint")
    return int(row["id"])


def _ratio_score(left: int | None, right: int | None, tolerance: float) -> tuple[float, bool]:
    if left is None or right is None:
        return 0.0, False
    if left == right:
        return 1.0, True
    denominator = max(abs(left), abs(right), 1)
    distance = abs(left - right) / denominator
    return max(0.0, 1.0 - distance / tolerance), True


def compare_descriptors(
    query: MediaDescriptor | Mapping[str, Any],
    candidate: MediaDescriptor | Mapping[str, Any],
) -> tuple[float, tuple[str, ...], tuple[str, ...]]:
    left = query if isinstance(query, MediaDescriptor) else MediaDescriptor.from_mapping(query)
    right = candidate if isinstance(candidate, MediaDescriptor) else MediaDescriptor.from_mapping(candidate)
    reasons: list[str] = []
    differences: list[str] = []
    weighted = 0.0
    possible = 0.0

    def exact(name: str, a: Any, b: Any, weight: float) -> None:
        nonlocal weighted, possible
        if a in (None, "") or b in (None, ""):
            return
        possible += weight
        if str(a).lower() == str(b).lower():
            weighted += weight
            reasons.append(f"Same {name}")
        else:
            differences.append(f"Different {name}")

    exact("media type", left.media_type, right.media_type, 12)
    exact("Telegram file unique ID", left.telegram_file_unique_id, right.telegram_file_unique_id, 35)
    exact("thumbnail hash", left.thumbnail_hash, right.thumbnail_hash, 18)
    exact("MIME type", left.mime_type, right.mime_type, 5)

    for name, a, b, weight, tolerance in (
        ("file size", left.file_size, right.file_size, 15, 0.03),
        ("duration", left.duration, right.duration, 8, 0.02),
        ("width", left.width, right.width, 3.5, 0.05),
        ("height", left.height, right.height, 3.5, 0.05),
    ):
        score, available = _ratio_score(a, b, tolerance)
        if not available:
            continue
        possible += weight
        weighted += weight * score
        if score >= 0.98:
            reasons.append(f"Same {name}")
        elif score >= 0.60:
            reasons.append(f"Similar {name}")
        else:
            differences.append(f"Different {name}")

    if possible == 0:
        return 0.0, tuple(reasons), tuple(differences)
    confidence = round(min(100.0, max(0.0, weighted / possible * 100.0)), 1)
    return confidence, tuple(reasons), tuple(differences)


def _row_descriptor(row: Mapping[str, Any]) -> MediaDescriptor:
    return MediaDescriptor.from_mapping(row)


def find_matches(
    db: Database,
    descriptor: MediaDescriptor | Mapping[str, Any],
    *,
    limit: int = 10,
    minimum_confidence: float = 55.0,
    record_history: bool = True,
) -> list[MatchResult]:
    initialize_media_finder(db)
    item = descriptor if isinstance(descriptor, MediaDescriptor) else MediaDescriptor.from_mapping(descriptor)
    fingerprint = build_fingerprint(item)
    rows = db.query(
        """
        SELECT * FROM media_fingerprints
        WHERE fingerprint = ?
           OR (telegram_file_unique_id IS NOT NULL AND telegram_file_unique_id = ?)
           OR (media_type = ? AND file_size BETWEEN ? AND ?)
        ORDER BY updated_at DESC
        LIMIT 250
        """,
        (
            fingerprint,
            item.telegram_file_unique_id,
            item.media_type,
            math.floor((item.file_size or 0) * 0.95),
            math.ceil((item.file_size or 0) * 1.05),
        ),
    )
    results: list[MatchResult] = []
    for row in rows:
        confidence, reasons, differences = compare_descriptors(item, _row_descriptor(row))
        if str(row["fingerprint"]) == fingerprint:
            confidence = max(confidence, 99.0)
            if "Same stable fingerprint" not in reasons:
                reasons = ("Same stable fingerprint", *reasons)
        if confidence < float(minimum_confidence):
            continue
        results.append(
            MatchResult(
                fingerprint_id=int(row["id"]),
                source_chat_id=str(row["source_chat_id"]),
                source_message_id=int(row["source_message_id"]),
                media_type=str(row["media_type"]),
                confidence=confidence,
                reasons=reasons,
                differences=differences,
                is_duplicate=confidence >= 95.0,
            )
        )
    results.sort(key=lambda result: (-result.confidence, result.fingerprint_id))
    results = results[: max(1, int(limit))]

    if record_history:
        for result in results:
            db.execute(
                """
                INSERT INTO media_match_history (
                    query_fingerprint, matched_fingerprint_id, confidence,
                    reasons_json, differences_json, is_duplicate, searched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fingerprint,
                    result.fingerprint_id,
                    result.confidence,
                    json.dumps(result.reasons, ensure_ascii=False),
                    json.dumps(result.differences, ensure_ascii=False),
                    int(result.is_duplicate),
                    utc_now(),
                ),
            )
    return results


def find_by_reference(db: Database, reference: str) -> dict[str, Any] | None:
    initialize_media_finder(db)
    parsed = parse_telegram_reference(reference)
    if parsed is None:
        return None
    chat, message_id = parsed
    if chat:
        row = db.query_one(
            """
            SELECT * FROM media_fingerprints
            WHERE source_message_id = ?
              AND (source_chat_id = ? OR source_chat_id = ?)
            ORDER BY updated_at DESC LIMIT 1
            """,
            (message_id, chat, f"@{chat}"),
        )
    else:
        row = db.query_one(
            "SELECT * FROM media_fingerprints WHERE source_message_id = ? ORDER BY updated_at DESC LIMIT 1",
            (message_id,),
        )
    return dict(row) if row else None


def duplicate_groups(db: Database, *, limit: int = 100) -> list[dict[str, Any]]:
    initialize_media_finder(db)
    rows = db.query(
        """
        SELECT fingerprint, COUNT(*) AS copies,
               MIN(source_chat_id) AS original_chat_id,
               MIN(source_message_id) AS original_message_id,
               GROUP_CONCAT(source_chat_id || ':' || source_message_id) AS locations
        FROM media_fingerprints
        GROUP BY fingerprint
        HAVING COUNT(*) > 1
        ORDER BY copies DESC, fingerprint ASC
        LIMIT ?
        """,
        (max(1, int(limit)),),
    )
    return [dict(row) for row in rows]


def media_finder_stats(db: Database) -> dict[str, Any]:
    initialize_media_finder(db)
    indexed = int(db.query_one("SELECT COUNT(*) AS count FROM media_fingerprints")["count"])
    unique_count = int(db.query_one("SELECT COUNT(DISTINCT fingerprint) AS count FROM media_fingerprints")["count"])
    duplicate_records = max(0, indexed - unique_count)
    history = int(db.query_one("SELECT COUNT(*) AS count FROM media_match_history")["count"])
    average = db.query_one("SELECT AVG(confidence) AS value FROM media_match_history")
    return {
        "indexed": indexed,
        "unique_fingerprints": unique_count,
        "duplicate_records": duplicate_records,
        "duplicate_rate": round((duplicate_records / indexed * 100.0) if indexed else 0.0, 1),
        "match_history": history,
        "average_confidence": round(float(average["value"] or 0.0), 1),
    }


def index_existing_queue(db: Database, *, limit: int | None = None) -> int:
    """Backfill safe metadata already present in the queue; no Telegram calls are made."""
    initialize_media_finder(db)
    sql = """
        SELECT source_chat_id, source_message_id, media_type, file_size, file_unique_key
        FROM messages
        WHERE media_type IS NOT NULL
        ORDER BY id ASC
    """
    params: Iterable[Any] = ()
    if limit is not None:
        sql += " LIMIT ?"
        params = (max(1, int(limit)),)
    created = 0
    for row in db.query(sql, params):
        before = db.query_one(
            "SELECT id FROM media_fingerprints WHERE source_chat_id = ? AND source_message_id = ?",
            (str(row["source_chat_id"]), int(row["source_message_id"])),
        )
        index_media(
            db,
            source_chat_id=row["source_chat_id"],
            source_message_id=int(row["source_message_id"]),
            descriptor=MediaDescriptor(
                media_type=str(row["media_type"] or "unknown"),
                file_size=_positive_int(row["file_size"]),
                telegram_file_unique_id=_clean(row["file_unique_key"]),
            ),
            metadata={"origin": "messages_backfill"},
        )
        if before is None:
            created += 1
    return created
