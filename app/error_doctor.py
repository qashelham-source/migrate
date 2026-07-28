from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from typing import Any

from app.db import Database, utc_now


@dataclass(frozen=True)
class Diagnosis:
    category: str
    severity: str
    confidence: float
    title: str
    explanation: str
    likely_cause: str
    recommended_actions: tuple[str, ...]
    retry_safe: bool
    pause_destination: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "severity": self.severity,
            "confidence": self.confidence,
            "title": self.title,
            "explanation": self.explanation,
            "likely_cause": self.likely_cause,
            "recommended_actions": list(self.recommended_actions),
            "retry_safe": self.retry_safe,
            "pause_destination": self.pause_destination,
        }


RULES: tuple[tuple[tuple[str, ...], Diagnosis], ...] = (
    (("floodwait", "flood wait", "too many requests"), Diagnosis(
        "rate_limit", "warning", 0.99, "Telegram rate limit",
        "Telegram meminta bot memperlahankan operasi buat sementara waktu.",
        "Terlalu banyak permintaan dihantar dalam tempoh singkat.",
        ("Tunggu tempoh FloodWait selesai.", "Kurangkan parallelism jika ralat berulang.", "Biarkan retry backoff berjalan secara automatik."),
        True,
    )),
    (("chatwriteforbidden", "chat_write_forbidden", "not enough rights", "forbidden"), Diagnosis(
        "permission", "critical", 0.98, "Tiada kebenaran menghantar",
        "Destination boleh dikenal pasti tetapi akaun penghantar tidak dibenarkan membuat post.",
        "Bot atau user session bukan admin, atau permission post telah dibuang.",
        ("Jadikan bot/user sebagai admin destination.", "Aktifkan permission Post Messages.", "Selepas akses dibaiki, retry kategori Permission."),
        False, True,
    )),
    (("channelprivate", "channel_private", "channelinvalid", "channel_invalid"), Diagnosis(
        "access", "critical", 0.97, "Channel tidak boleh diakses",
        "Session semasa tidak lagi boleh membuka source atau destination tersebut.",
        "Channel private, ID salah, akaun telah dibuang, atau channel sudah tidak wujud.",
        ("Semak semula -100 ID atau forward post channel.", "Pastikan user session masih menjadi ahli.", "Tambah semula destination selepas akses pulih."),
        False, True,
    )),
    (("peer id invalid", "peer_id_invalid"), Diagnosis(
        "peer_id", "warning", 0.96, "Telegram peer belum dikenali",
        "Telegram belum mempunyai access hash atau cache untuk ID channel tersebut.",
        "Dialog cache belum dimuatkan atau ID channel tidak tepat.",
        ("Pastikan ID bermula dengan -100.", "Forward satu post channel kepada bot manager.", "Restart service untuk warm dialog cache, kemudian retry."),
        True,
    )),
    (("media_empty", "mediaempty", "sendmultimedia"), Diagnosis(
        "media", "warning", 0.95, "Telegram memulangkan media kosong",
        "Telegram gagal menggunakan media asal atau media group dalam route semasa.",
        "Media dilindungi, file reference luput, atau album gagal dihantar sebagai satu kumpulan.",
        ("Gunakan download/upload fallback.", "Hantar album secara item individu jika perlu.", "Retry kategori MEDIA_EMPTY."),
        True,
    )),
    (("timeout", "timed out", "connection", "network", "temporarily unavailable", "server error"), Diagnosis(
        "network", "warning", 0.90, "Masalah rangkaian sementara",
        "Operasi Telegram terganggu sebelum selesai tetapi job biasanya selamat dicuba semula.",
        "Sambungan server tidak stabil atau Telegram mengalami gangguan sementara.",
        ("Biarkan automatic retry berjalan.", "Semak kestabilan rangkaian droplet.", "Kurangkan parallelism jika timeout berlaku serentak."),
        True,
    )),
    (("no space left", "disk full", "storage guard", "not enough space"), Diagnosis(
        "storage", "critical", 0.99, "Storage tidak mencukupi",
        "Server tidak mempunyai ruang selamat untuk memulakan atau menyiapkan pemindahan media.",
        "Ruang kosong berada di bawah reserve atau fail sementara terlalu besar.",
        ("Kosongkan folder downloads sementara.", "Tambah kapasiti disk.", "Jalankan Shadow Migration untuk anggaran peak storage."),
        True,
    )),
    (("message id invalid", "message_id_invalid", "source messages missing", "message_empty"), Diagnosis(
        "source_missing", "critical", 0.94, "Post source tidak lagi tersedia",
        "Message asal yang diperlukan tidak dapat dibaca daripada source.",
        "Post dipadam, ID tidak tepat, atau session kehilangan akses kepada history.",
        ("Semak post masih wujud dalam source.", "Pastikan session boleh membaca history.", "Buat Full Scan jika checkpoint tidak tepat."),
        False,
    )),
)


def diagnose_error(error: str | None, *, media_type: str | None = None, attempts: int = 0) -> Diagnosis:
    text = str(error or "").lower()
    for markers, diagnosis in RULES:
        if any(marker in text for marker in markers):
            confidence = max(0.50, diagnosis.confidence - min(max(attempts - 1, 0), 5) * 0.01)
            return Diagnosis(**{**diagnosis.__dict__, "confidence": round(confidence, 2)})
    if str(media_type or "").lower() == "unsupported" or "filtered out by config" in text:
        return Diagnosis(
            "unsupported", "info", 0.99, "Media ditapis oleh konfigurasi",
            "Job ini sengaja tidak dipindahkan kerana jenis media atau filter semasa.",
            "Tetapan migration tidak membenarkan kandungan tersebut.",
            ("Semak filter media dalam config.yaml.", "Aktifkan jenis media hanya jika memang diperlukan."),
            False,
        )
    return Diagnosis(
        "unknown", "warning", 0.45, "Ralat belum dikenal pasti",
        "Corak ralat ini belum cukup jelas untuk diagnosis automatik yang yakin.",
        "Punca mungkin datang daripada Telegram API, konfigurasi atau keadaan server.",
        ("Buka butiran error penuh.", "Semak log sekitar job tersebut.", "Cuba semula sekali jika operasi tidak merosakkan data."),
        True,
    )


def initialize_error_doctor(db: Database) -> None:
    db.conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS error_diagnoses (
            job_id INTEGER PRIMARY KEY,
            category TEXT NOT NULL,
            severity TEXT NOT NULL,
            confidence REAL NOT NULL,
            title TEXT NOT NULL,
            explanation TEXT NOT NULL,
            likely_cause TEXT NOT NULL,
            recommended_actions TEXT NOT NULL,
            retry_safe INTEGER NOT NULL,
            pause_destination INTEGER NOT NULL DEFAULT 0,
            diagnosed_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_error_diagnoses_category
            ON error_diagnoses(category, severity, diagnosed_at);
        """
    )
    db.conn.commit()


def diagnose_job(db: Database, job_id: int) -> dict[str, Any] | None:
    initialize_error_doctor(db)
    row = db.query_one(
        "SELECT id, media_type, attempts, last_error FROM messages WHERE id = ?",
        (int(job_id),),
    )
    if not row:
        return None
    diagnosis = diagnose_error(row["last_error"], media_type=row["media_type"], attempts=int(row["attempts"] or 0))
    data = diagnosis.as_dict()
    db.execute(
        """
        INSERT INTO error_diagnoses (
            job_id, category, severity, confidence, title, explanation,
            likely_cause, recommended_actions, retry_safe, pause_destination, diagnosed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(job_id) DO UPDATE SET
            category=excluded.category, severity=excluded.severity,
            confidence=excluded.confidence, title=excluded.title,
            explanation=excluded.explanation, likely_cause=excluded.likely_cause,
            recommended_actions=excluded.recommended_actions,
            retry_safe=excluded.retry_safe,
            pause_destination=excluded.pause_destination,
            diagnosed_at=excluded.diagnosed_at
        """,
        (
            int(job_id), data["category"], data["severity"], data["confidence"], data["title"],
            data["explanation"], data["likely_cause"], json.dumps(data["recommended_actions"], ensure_ascii=False),
            int(data["retry_safe"]), int(data["pause_destination"]), utc_now(),
        ),
    )
    return {"job_id": int(job_id), **data}


def diagnose_open_issues(db: Database, limit: int = 100) -> list[dict[str, Any]]:
    rows = db.query(
        """
        SELECT id FROM messages
        WHERE status IN ('failed', 'skipped') AND COALESCE(last_error, '') <> ''
        ORDER BY updated_at DESC LIMIT ?
        """,
        (max(1, int(limit)),),
    )
    return [result for row in rows if (result := diagnose_job(db, int(row["id"]))) is not None]


def explain_performance(db: Database) -> dict[str, Any]:
    telemetry_exists = db.query_one("SELECT 1 FROM sqlite_master WHERE type='table' AND name='job_telemetry'")
    rows = db.query(
        """
        SELECT stage, speed_bps, eta_seconds, bytes_total, bytes_processed
        FROM job_telemetry
        WHERE speed_bps IS NOT NULL
        ORDER BY updated_at DESC LIMIT 100
        """
    ) if telemetry_exists else []
    speeds = [float(row["speed_bps"] or 0) for row in rows if float(row["speed_bps"] or 0) > 0]
    counts = Counter(str(row["stage"] or "unknown") for row in rows)
    queue = {str(row["status"]): int(row["count"]) for row in db.query("SELECT status, COUNT(*) AS count FROM messages GROUP BY status")}
    avg_speed = sum(speeds) / len(speeds) if speeds else 0.0
    active = queue.get("downloading", 0) + queue.get("uploading", 0)
    pending = queue.get("pending", 0)
    failed = queue.get("failed", 0) + queue.get("skipped", 0)

    if not speeds:
        state, reason = "learning", "Belum cukup sampel pemindahan untuk menerangkan prestasi dengan yakin."
    elif failed > max(3, queue.get("copied", 0) // 10):
        state, reason = "degraded", "Kadar job bermasalah tinggi berbanding job selesai."
    elif pending > 0 and active == 0:
        state, reason = "blocked", "Queue masih ada tetapi tiada transfer aktif; destination pause, retry timer atau storage guard mungkin menahan job."
    elif active > 0:
        state, reason = "working", "Migration sedang memproses queue dengan telemetry aktif."
    else:
        state, reason = "healthy", "Tiada petunjuk bottleneck utama pada sampel semasa."

    suggestions: list[str] = []
    if pending > 20 and active <= 1:
        suggestions.append("Pertimbangkan safe parallelism selepas Shadow Migration mengesahkan storage mencukupi.")
    if failed:
        suggestions.append("Buka AI Error Doctor untuk kumpulan ralat terbesar sebelum menaikkan parallelism.")
    if not suggestions:
        suggestions.append("Kekalkan tetapan semasa dan kumpulkan lebih banyak sampel untuk ETA lebih tepat.")

    return {
        "state": state,
        "reason": reason,
        "average_speed_bps": avg_speed,
        "sample_count": len(speeds),
        "queue": queue,
        "stage_counts": dict(counts),
        "suggestions": suggestions,
    }
