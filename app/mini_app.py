from __future__ import annotations

import hashlib
import hmac
import json
import re
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, quote, urlsplit
from urllib.request import Request, urlopen

from app.admin_auth import is_authorized
from app.bot_token_recovery import (
    bot_id_from_token,
    effective_bot_token,
    save_runtime_bot_token,
)
from app.config import AppConfig
from app.control import is_active_phase, read_status
from app.dashboard_v2 import (
    active_source_progress,
    dashboard_snapshot,
    format_bytes,
    format_eta,
    issue_center,
    source_library,
)
from app.db import Database
from app.destination_manager import get_sources, set_sources
from app.queue import MessageQueue


class MiniAppAuthError(ValueError):
    """Raised when Telegram Mini App launch data cannot be trusted."""


class BotTokenRecoveryError(ValueError):
    """Raised for a safe, user-facing bot-token recovery failure."""


_CHAT_ID_PATTERN = re.compile(r"(?<!\w)-100\d{5,}")


def validate_init_data(
    init_data: str,
    bot_token: str,
    max_age_seconds: int,
    *,
    now: int | None = None,
) -> dict[str, Any]:
    """Validate Telegram WebApp initData without logging its sensitive contents."""
    if not init_data or len(init_data) > 16_384:
        raise MiniAppAuthError("Missing or oversized Telegram launch data")

    try:
        pairs = parse_qsl(init_data, keep_blank_values=True, strict_parsing=True)
    except ValueError as exc:
        raise MiniAppAuthError("Malformed Telegram launch data") from exc

    values: dict[str, str] = {}
    for key, value in pairs:
        if not key or key in values:
            raise MiniAppAuthError("Malformed Telegram launch data")
        values[key] = value

    received_hash = values.pop("hash", "")
    if not received_hash or not bot_token:
        raise MiniAppAuthError("Telegram launch data is incomplete")

    data_check_string = "\n".join(f"{key}={values[key]}" for key in sorted(values))
    secret_key = hmac.new(
        b"WebAppData",
        bot_token.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    expected_hash = hmac.new(
        secret_key,
        data_check_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(expected_hash, received_hash):
        raise MiniAppAuthError("Telegram launch data did not verify")

    try:
        auth_date = int(values["auth_date"])
    except (KeyError, TypeError, ValueError) as exc:
        raise MiniAppAuthError("Telegram launch data is missing auth_date") from exc

    current_time = int(time.time()) if now is None else int(now)
    if auth_date > current_time + 60 or current_time - auth_date > int(max_age_seconds):
        raise MiniAppAuthError("Telegram launch data has expired")

    try:
        user = json.loads(values["user"])
        user_id = int(user["id"])
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise MiniAppAuthError("Telegram launch data is missing a valid user") from exc

    if not isinstance(user, dict) or user_id <= 0:
        raise MiniAppAuthError("Telegram launch data is missing a valid user")
    user["id"] = user_id
    return user


def _verify_bot_token_with_telegram(token: str, expected_bot_id: int) -> None:
    """Ensure a submitted token is live and belongs to the configured bot.

    ``initData`` validation alone cannot validate a newly supplied secret: a
    caller that knows an arbitrary string could manufacture an HMAC for it.
    Telegram's getMe response proves that the token is real before we accept
    its initData signature.
    """
    endpoint = "https://api.telegram.org/bot" + quote(token, safe=":") + "/getMe"
    request = Request(endpoint, headers={"Accept": "application/json"})
    try:
        with urlopen(request, timeout=12) as response:  # noqa: S310 - fixed Telegram API origin
            raw = response.read(65_537)
    except (HTTPError, URLError, TimeoutError, OSError):
        raise BotTokenRecoveryError("Token bot tidak dapat disahkan dengan Telegram.") from None

    if len(raw) > 65_536:
        raise BotTokenRecoveryError("Token bot tidak dapat disahkan dengan Telegram.")
    try:
        payload = json.loads(raw.decode("utf-8"))
        actual_bot_id = int(payload["result"]["id"])
    except (TypeError, ValueError, KeyError, UnicodeDecodeError, json.JSONDecodeError):
        raise BotTokenRecoveryError("Token bot tidak sah atau telah dibatalkan.") from None
    if payload.get("ok") is not True or actual_bot_id != expected_bot_id:
        raise BotTokenRecoveryError("Token itu bukan milik bot kawalan ini.")


def replace_bot_token(
    config: AppConfig,
    *,
    token: str,
    init_data: str,
) -> None:
    """Validate and atomically activate a replacement token from the Mini App."""
    candidate = str(token or "").strip()
    expected_bot_id = bot_id_from_token(
        effective_bot_token(config.telegram.bot_token, config.telegram.sessions_dir)
    )
    candidate_bot_id = bot_id_from_token(candidate)
    if expected_bot_id is None or candidate_bot_id is None:
        raise BotTokenRecoveryError("Format token bot tidak sah.")
    if candidate_bot_id != expected_bot_id:
        raise BotTokenRecoveryError("Token itu bukan milik bot kawalan ini.")

    _verify_bot_token_with_telegram(candidate, expected_bot_id)
    try:
        user = validate_init_data(
            init_data,
            candidate,
            config.mini_app.auth_max_age_seconds,
        )
    except MiniAppAuthError:
        raise BotTokenRecoveryError("Sahkan semula melalui Mini App bot ini, kemudian cuba lagi.") from None
    if not is_authorized(config, int(user["id"])):
        raise BotTokenRecoveryError("Akaun Telegram ini bukan admin yang dibenarkan.")

    try:
        save_runtime_bot_token(config.telegram.sessions_dir, candidate)
    except OSError:
        raise BotTokenRecoveryError("Token tidak dapat disimpan. Cuba semula sebentar lagi.") from None


def _title(value: Any, chat: str) -> str:
    candidate = _CHAT_ID_PATTERN.sub("", str(value or "")).strip()
    if candidate and candidate != str(chat):
        return candidate
    if str(chat).startswith("@"):
        return str(chat)
    return "Channel belum discan"


def _handle(record: dict[str, Any] | None) -> str | None:
    username = str((record or {}).get("username") or "").strip().lstrip("@")
    return f"@{username}" if username else None


def _state_label(phase: str) -> str:
    labels = {
        "idle": "Sedia",
        "watching": "Menunggu kerja baru",
        "starting": "Menyiapkan",
        "scanning": "Mengimbas",
        "processing": "Memproses",
        "downloading": "Memuat turun",
        "uploading": "Memuat naik",
        "verifying": "Mengesahkan media",
        "job_stalled": "Job Stalled",
        "batch_pause": "Rehat antara batch",
        "waiting_retry": "Menunggu cuba semula",
        "stopping": "Sedang berhenti",
        "stopped": "Dihentikan",
        "source_complete": "Sumber selesai",
        "queued": "Menunggu mula",
        "blocked": "Perlu perhatian",
        "error": "Perlu perhatian",
    }
    return labels.get(phase, "Sedia")


def _redact_chat_ids(value: Any, limit: int = 220) -> str:
    text = _CHAT_ID_PATTERN.sub("channel", str(value or "").strip())
    return text[:limit] if text else "Perlu semakan dalam bot."


def _source_record(
    chat: str,
    records_by_id: dict[str, dict[str, Any]],
    records_by_handle: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    return records_by_id.get(chat) or records_by_handle.get(chat.casefold())


def _source_state(progress: dict[str, Any] | None, active_source_id: str) -> tuple[str, str]:
    if progress and active_source_id and str(progress.get("source_chat_id") or "") == active_source_id:
        return "running", "Sedang berjalan"
    if progress:
        eligible = int(progress.get("eligible_items") or 0)
        copied = int(progress.get("copied_items") or 0)
        remaining = int(progress.get("remaining_items") or 0)
        blocked = int(progress.get("blocked_items") or 0)
        if eligible and copied >= eligible and not remaining:
            return "complete", "Selesai"
        if blocked:
            return "attention", "Perlu semakan"
    return "waiting", "Menunggu giliran"


def dashboard_payload(config: AppConfig, config_path: str | Path) -> dict[str, Any]:
    """Build a public-safe dashboard; it deliberately excludes raw Telegram IDs."""
    db = Database(config.queue.db_path)
    try:
        db.initialize()
        MessageQueue(db, config)
        snapshot = dashboard_snapshot(db, config.downloads.root)
        status = read_status(config)
        source_rows = source_library(db)
        records_by_id = {str(item["source_chat_id"]): item for item in source_rows}
        records_by_handle = {
            f"@{str(item['username']).lstrip('@')}".casefold(): item
            for item in source_rows
            if str(item.get("username") or "").strip()
        }
        progress_rows = snapshot["source_progress"]
        progress_by_id = {str(item["source_chat_id"]): item for item in progress_rows}
        active = active_source_progress(status, progress_rows)
        active_id = str(active["source_chat_id"]) if active else ""
        configured_sources = get_sources(config_path)

        sources: list[dict[str, Any]] = []
        for position, item in enumerate(configured_sources):
            chat = str(item.get("chat") or "").strip()
            record = _source_record(chat, records_by_id, records_by_handle)
            progress = progress_by_id.get(str((record or {}).get("source_chat_id") or chat))
            if progress is None and chat == active_id:
                progress = active
            state, state_label = _source_state(progress, active_id)
            title = _title((record or {}).get("title") or (progress or {}).get("title"), chat)
            sources.append(
                {
                    "position": position,
                    "title": title,
                    "handle": _handle(record),
                    "state": state,
                    "state_label": state_label,
                    "progress": (
                        {
                            "percent": int(progress.get("percent") or 0),
                            "copied": int(progress.get("copied_items") or 0),
                            "eligible": int(progress.get("eligible_items") or 0),
                            "remaining": int(progress.get("remaining_items") or 0),
                        }
                        if progress
                        else None
                    ),
                    "can_move_up": position > 0,
                    "can_move_down": position < len(configured_sources) - 1,
                }
            )

        active_payload = None
        if active:
            active_chat = str(active.get("source_chat_id") or "")
            active_record = records_by_id.get(active_chat)
            active_payload = {
                "title": _title((active_record or {}).get("title") or active.get("title"), active_chat),
                "percent": int(active.get("percent") or 0),
                "copied": int(active.get("copied_items") or 0),
                "eligible": int(active.get("eligible_items") or 0),
                "remaining": int(active.get("remaining_items") or 0),
                "active": int(active.get("active_items") or 0),
            }

        issues = issue_center(db, limit=12)
        safe_issues = []
        for issue in issues[:5]:
            source_chat = str(issue.get("source_chat_id") or "")
            source = records_by_id.get(source_chat)
            safe_issues.append(
                {
                    "source": _title((source or {}).get("title"), source_chat),
                    "status": str(issue.get("status") or "Perlu semakan"),
                    "error": _redact_chat_ids(issue.get("error")),
                }
            )

        queue = snapshot["queue"]
        telemetry = snapshot["telemetry"]
        storage = snapshot["storage"]
        phase = str(status.get("phase") or "idle").strip().lower()
        health = snapshot["health"]
        stalled_jobs = list(health.get("stalled_jobs") or [])
        if stalled_jobs:
            state = {
                "phase": "job_stalled",
                "label": _state_label("job_stalled"),
                "active": False,
                "tone": "danger",
            }
        else:
            state = {
                "phase": phase,
                "label": _state_label(phase),
                "active": is_active_phase(phase),
                "tone": "active" if is_active_phase(phase) else "waiting",
            }
        eta = format_eta(telemetry.get("eta_seconds"))
        if eta == "not enough data":
            eta = "Belum cukup data"

        return {
            "updated_at": status.get("updated_at"),
            "state": state,
            "summary": {
                "pending": int(queue.get("pending") or 0),
                "active": int(telemetry.get("active") or 0),
                "copied": int(queue.get("copied") or 0),
                "issues": len(issues),
                "stalled": len(stalled_jobs),
                "speed": format_bytes(float(telemetry.get("speed_bps") or 0)) + "/s",
                "eta": eta,
                "storage_free": format_bytes(storage.free_bytes),
            },
            "active": active_payload,
            "sources": sources,
            "issues": safe_issues,
        }
    finally:
        db.close()


def move_source(config_path: str | Path, index: int, direction: str) -> None:
    sources = get_sources(config_path)
    if direction not in {"up", "down"}:
        raise ValueError("Direction must be up or down")
    target = index - 1 if direction == "up" else index + 1
    if not 0 <= index < len(sources) or not 0 <= target < len(sources):
        raise ValueError("Source cannot be moved further")
    sources[index], sources[target] = sources[target], sources[index]
    set_sources(sources, config_path)


def _handler_for(config: AppConfig, config_path: str | Path) -> type[BaseHTTPRequestHandler]:
    static_root = Path(__file__).with_name("mini_app_static").resolve()

    class MiniAppHandler(BaseHTTPRequestHandler):
        server_version = "MigrationMiniApp/1.0"

        def log_message(self, _format: str, *_args: Any) -> None:
            # initData can be present in requests; never write request details to logs.
            return

        def _send_json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            self.send_response(int(status))
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Referrer-Policy", "no-referrer")
            self.end_headers()
            self.wfile.write(body)

        def _send_html(self, body: bytes) -> None:
            self.send_response(int(HTTPStatus.OK))
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header(
                "Content-Security-Policy",
                "default-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; "
                "script-src 'self' 'unsafe-inline' https://telegram.org; connect-src 'self';",
            )
            self.end_headers()
            self.wfile.write(body)

        def _require_admin(self) -> bool:
            try:
                user = validate_init_data(
                    self.headers.get("X-Telegram-Init-Data", ""),
                    effective_bot_token(config.telegram.bot_token, config.telegram.sessions_dir),
                    config.mini_app.auth_max_age_seconds,
                )
            except MiniAppAuthError:
                self._send_json(HTTPStatus.UNAUTHORIZED, {"error": "Akses Telegram tidak sah."})
                return False
            if not is_authorized(config, int(user["id"])):
                self._send_json(HTTPStatus.FORBIDDEN, {"error": "Akaun ini bukan admin bot."})
                return False
            return True

        def _read_json(self) -> dict[str, Any] | None:
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                self._send_json(HTTPStatus.BAD_REQUEST, {"error": "Permintaan tidak sah."})
                return None
            if length <= 0 or length > 4096:
                self._send_json(HTTPStatus.BAD_REQUEST, {"error": "Permintaan tidak sah."})
                return None
            try:
                body = json.loads(self.rfile.read(length).decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                self._send_json(HTTPStatus.BAD_REQUEST, {"error": "Permintaan tidak sah."})
                return None
            if not isinstance(body, dict):
                self._send_json(HTTPStatus.BAD_REQUEST, {"error": "Permintaan tidak sah."})
                return None
            return body

        def do_GET(self) -> None:  # noqa: N802
            path = urlsplit(self.path).path
            if path == "/health":
                self._send_json(HTTPStatus.OK, {"ok": True, "enabled": config.mini_app.enabled})
                return
            if not config.mini_app.enabled:
                self._send_json(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "Mini App belum diaktifkan."})
                return
            if path in {"/", "/index.html"}:
                try:
                    self._send_html((static_root / "index.html").read_bytes())
                except OSError:
                    self._send_json(HTTPStatus.NOT_FOUND, {"error": "Paparan Mini App tidak ditemui."})
                return
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "Tidak ditemui."})

        def do_POST(self) -> None:  # noqa: N802
            if not config.mini_app.enabled:
                self._send_json(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "Mini App belum diaktifkan."})
                return
            path = urlsplit(self.path).path
            if path == "/api/bot-token/recover":
                body = self._read_json()
                if body is None:
                    return
                token = body.get("bot_token")
                if not isinstance(token, str) or len(token) > 512:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"error": "Token bot tidak sah."})
                    return
                try:
                    replace_bot_token(
                        config,
                        token=token,
                        init_data=self.headers.get("X-Telegram-Init-Data", ""),
                    )
                except BotTokenRecoveryError as exc:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                    return
                except OSError:
                    self._send_json(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "Cuba semula sebentar lagi."})
                    return
                self._send_json(
                    HTTPStatus.OK,
                    {"ok": True, "message": "Token disahkan. Admin bot sedang disambungkan semula."},
                )
                return
            if not self._require_admin():
                return

            try:
                if path == "/api/dashboard":
                    self._send_json(HTTPStatus.OK, dashboard_payload(config, config_path))
                    return
                if path == "/api/queue/move":
                    body = self._read_json()
                    if body is None:
                        return
                    index = body.get("index")
                    direction = body.get("direction")
                    if not isinstance(index, int) or not isinstance(direction, str):
                        self._send_json(HTTPStatus.BAD_REQUEST, {"error": "Permintaan tidak sah."})
                        return
                    move_source(config_path, index, direction)
                    self._send_json(HTTPStatus.OK, dashboard_payload(config, config_path))
                    return
            except ValueError as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return
            except OSError:
                self._send_json(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "Cuba semula sebentar lagi."})
                return
            except Exception:
                self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "Tidak dapat memuatkan dashboard."})
                return

            self._send_json(HTTPStatus.NOT_FOUND, {"error": "Tidak ditemui."})

    return MiniAppHandler


def run_mini_app_server(config: AppConfig, config_path: str | Path = "config.yaml") -> None:
    """Run the local Mini App server. A reverse proxy must provide public HTTPS."""
    server = ThreadingHTTPServer(
        (config.mini_app.host, config.mini_app.port),
        _handler_for(config, config_path),
    )
    server.daemon_threads = True
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        server.server_close()
