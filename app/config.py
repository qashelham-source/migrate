from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from dotenv import load_dotenv
import yaml


ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::([^}]*))?\}")
_TRUE_VALUES = {"1", "true", "yes", "on"}
_FALSE_VALUES = {"0", "false", "no", "off"}


def _expand_env(value: Any) -> Any:
    if isinstance(value, str):
        return ENV_PATTERN.sub(lambda match: os.getenv(match.group(1), match.group(2) or ""), value)
    if isinstance(value, list):
        return [_expand_env(item) for item in value]
    if isinstance(value, dict):
        return {key: _expand_env(item) for key, item in value.items()}
    return value


def _as_bool(value: Any, default: bool = False, *, name: str = "value") -> bool:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False
    raise ValueError(f"{name} must be a boolean value")


def _as_int(value: Any, default: int | None = None, *, name: str = "value") -> int | None:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer") from exc


def _as_float(value: Any, default: float, *, name: str = "value") -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a number") from exc


def _positive_int(value: Any, default: int, *, name: str) -> int:
    parsed = int(_as_int(value, default, name=name) or 0)
    if parsed <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return parsed


def _non_negative_float(value: Any, default: float, *, name: str) -> float:
    parsed = _as_float(value, default, name=name)
    if parsed < 0:
        raise ValueError(f"{name} cannot be negative")
    return parsed


def _path(base_dir: Path, value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()


def _https_url(value: Any, *, name: str) -> str:
    url = str(value or "").strip()
    if not url:
        return ""
    parsed = urlparse(url)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username
        or parsed.password
        or parsed.fragment
    ):
        raise ValueError(f"{name} must be a public HTTPS URL without credentials or fragment")
    return url.rstrip("/")


def _admin_ids(value: Any) -> tuple[int, ...]:
    values: list[Any]
    if value is None or value == "":
        values = []
    elif isinstance(value, (list, tuple, set)):
        values = list(value)
    else:
        values = str(value).split(",")

    for raw in os.getenv("ADMIN_USER_ID", "").split(","):
        if raw.strip():
            values.append(raw)

    result: list[int] = []
    for item in values:
        text = str(item).strip()
        if not text:
            continue
        try:
            parsed = int(text)
        except ValueError as exc:
            raise ValueError("telegram.admin_ids and ADMIN_USER_ID must contain numeric Telegram user IDs") from exc
        if parsed <= 0:
            raise ValueError("Telegram admin user IDs must be positive integers")
        if parsed not in result:
            result.append(parsed)
    return tuple(result)


@dataclass(frozen=True)
class ChatSpec:
    chat: str
    topic_id: int | None = None
    start_id: int | None = None
    end_id: int | None = None

    @classmethod
    def from_config(cls, value: Any) -> "ChatSpec":
        if isinstance(value, dict):
            message_range = value.get("message_range") or {}
            chat = str(value.get("chat") or "").strip()
            if not chat:
                raise ValueError("migration chat entries require a non-empty chat value")
            start_id = _as_int(value.get("start_id", message_range.get("start")), name="message range start")
            end_id = _as_int(value.get("end_id", message_range.get("end")), name="message range end")
            if start_id is not None and start_id <= 0:
                raise ValueError("message range start must be greater than zero")
            if end_id is not None and end_id <= 0:
                raise ValueError("message range end must be greater than zero")
            if start_id is not None and end_id is not None and start_id > end_id:
                raise ValueError("message range start cannot be greater than end")
            return cls(
                chat=chat,
                topic_id=_as_int(value.get("topic_id"), name="topic_id"),
                start_id=start_id,
                end_id=end_id,
            )

        raw = str(value).strip()
        if not raw:
            raise ValueError("migration chat entries cannot be empty")
        if ":" in raw and not raw.startswith("@"):
            chat, topic = raw.rsplit(":", 1)
            if topic.isdigit():
                return cls(chat=chat, topic_id=int(topic))
        return cls(chat=raw)


@dataclass(frozen=True)
class TelegramConfig:
    api_id: int
    api_hash: str
    user_session: str
    sessions_dir: Path
    load_dialogs_on_start: bool
    bot_enabled: bool
    bot_token: str
    bot_session_name: str
    use_bot_for_uploads: bool
    admin_ids: tuple[int, ...] = ()


@dataclass(frozen=True)
class MiniAppConfig:
    enabled: bool
    public_url: str
    host: str
    port: int
    auth_max_age_seconds: int


@dataclass(frozen=True)
class TransferConfig:
    include_videos: bool
    include_photos: bool
    include_text: bool
    include_documents: bool
    hide_sender: bool
    drop_caption: bool
    prefer_copy: bool
    forwarding_only: bool
    save_to_local: bool
    max_bot_upload_bytes: int


@dataclass(frozen=True)
class QueueConfig:
    db_path: Path
    max_attempts: int
    retry_backoff_seconds: list[int]
    record_skipped: bool


@dataclass(frozen=True)
class LimitsConfig:
    global_min_delay_seconds: float
    resolve_delay_seconds: float
    read_delay_seconds: float
    download_delay_seconds: float
    copy_delay_seconds: float
    upload_delay_seconds: float
    verify_delay_seconds: float
    get_messages_chunk_size: int
    floodwait_extra_min_seconds: int
    floodwait_extra_max_seconds: int

    def delay_for(self, operation: str) -> float:
        return {
            "resolve": self.resolve_delay_seconds,
            "read": self.read_delay_seconds,
            "download": self.download_delay_seconds,
            "copy": self.copy_delay_seconds,
            "upload": self.upload_delay_seconds,
            "verify": self.verify_delay_seconds,
        }.get(operation, self.global_min_delay_seconds)


@dataclass(frozen=True)
class BatchConfig:
    size: int
    pause_between_batches_seconds: int
    idle_sleep_seconds: int


@dataclass(frozen=True)
class DownloadsConfig:
    root: Path
    active_dir: Path
    failed_dir: Path
    completed_dir: Path
    keep_failed: bool
    keep_completed: bool


@dataclass(frozen=True)
class LoggingConfig:
    level: str
    file: Path | None


@dataclass(frozen=True)
class AppConfig:
    base_dir: Path
    telegram: TelegramConfig
    mini_app: MiniAppConfig
    transfer: TransferConfig
    queue: QueueConfig
    limits: LimitsConfig
    batch: BatchConfig
    downloads: DownloadsConfig
    logging: LoggingConfig
    sources: list[ChatSpec] = field(default_factory=list)
    destinations: list[ChatSpec] = field(default_factory=list)
    source_blacklist: list[str] = field(default_factory=list)
    # Owner's local UTC offset in whole hours (e.g. 8 for UTC+8 / Malaysia).
    # Set via config.yaml:  display: { timezone_hours: 8 }
    display_timezone_hours: int = 8

    def ensure_directories(self) -> None:
        self.telegram.sessions_dir.mkdir(parents=True, exist_ok=True)
        self.queue.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.downloads.root.mkdir(parents=True, exist_ok=True)
        self.downloads.active_dir.mkdir(parents=True, exist_ok=True)
        self.downloads.failed_dir.mkdir(parents=True, exist_ok=True)
        self.downloads.completed_dir.mkdir(parents=True, exist_ok=True)
        if self.logging.file:
            self.logging.file.parent.mkdir(parents=True, exist_ok=True)


def load_config(path: str | Path = "config.yaml") -> AppConfig:
    load_dotenv()
    config_path = Path(path).resolve()
    base_dir = config_path.parent

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}
    if not isinstance(raw, dict):
        raise ValueError("config.yaml must contain a YAML object")
    raw = _expand_env(raw)

    telegram = raw.get("telegram") or {}
    transfer = raw.get("transfer") or {}
    queue = raw.get("queue") or {}
    limits = raw.get("limits") or {}
    batch = raw.get("batch") or {}
    downloads = raw.get("downloads") or {}
    logging_cfg = raw.get("logging") or {}
    migration = raw.get("migration") or {}
    mini_app = raw.get("mini_app") or {}
    bot = telegram.get("bot") or {}
    include = transfer.get("include") or {}
    native_copy = transfer.get("native_copy") or {}

    api_id = int(_as_int(telegram.get("api_id") or os.getenv("API_ID"), 0, name="telegram.api_id") or 0)
    api_hash = str(telegram.get("api_hash") or os.getenv("API_HASH") or "").strip()
    bot_token = str(bot.get("token") or os.getenv("BOT_TOKEN") or "").strip()
    bot_enabled = _as_bool(bot.get("enabled"), bool(bot_token), name="telegram.bot.enabled")

    if api_id <= 0:
        raise ValueError("telegram.api_id is required and must be greater than zero")
    if not api_hash:
        raise ValueError("telegram.api_hash is required in config.yaml or API_HASH environment variable")
    if bot_enabled and not bot_token:
        raise ValueError("telegram.bot.token is required when telegram.bot.enabled is true")

    mini_app_url = _https_url(
        mini_app.get("public_url") or os.getenv("MINI_APP_URL"),
        name="mini_app.public_url",
    )
    mini_app_enabled = _as_bool(
        mini_app.get("enabled", os.getenv("MINI_APP_ENABLED")),
        bool(mini_app_url),
        name="mini_app.enabled",
    )
    if mini_app_enabled and not mini_app_url:
        raise ValueError("mini_app.public_url is required when mini_app.enabled is true")
    if mini_app_enabled and not bot_enabled:
        raise ValueError("telegram.bot.enabled must be true when mini_app.enabled is true")
    mini_app_host = str(mini_app.get("host") or os.getenv("MINI_APP_HOST") or "0.0.0.0").strip()
    if not mini_app_host:
        raise ValueError("mini_app.host cannot be empty")
    mini_app_port = _positive_int(
        mini_app.get("port") or os.getenv("MINI_APP_PORT"),
        8080,
        name="mini_app.port",
    )
    mini_app_auth_max_age_seconds = _positive_int(
        mini_app.get("auth_max_age_seconds") or os.getenv("MINI_APP_AUTH_MAX_AGE_SECONDS"),
        3600,
        name="mini_app.auth_max_age_seconds",
    )

    root = _path(base_dir, str(downloads.get("root", "downloads")))
    active_dir = _path(base_dir, str(downloads.get("active_dir", root / "active")))
    failed_dir = _path(base_dir, str(downloads.get("failed_dir", root / "failed")))
    completed_dir = _path(base_dir, str(downloads.get("completed_dir", root / "completed")))
    log_file = logging_cfg.get("file")

    max_attempts = _positive_int(queue.get("max_attempts"), 4, name="queue.max_attempts")
    retry_backoff_seconds = [int(value) for value in queue.get("retry_backoff_seconds", [300, 600, 1800])]
    if not retry_backoff_seconds or any(value < 0 for value in retry_backoff_seconds):
        raise ValueError("queue.retry_backoff_seconds must contain one or more non-negative integers")

    floodwait_min = int(_as_int(limits.get("floodwait_extra_min_seconds"), 5, name="limits.floodwait_extra_min_seconds") or 0)
    floodwait_max = int(_as_int(limits.get("floodwait_extra_max_seconds"), 20, name="limits.floodwait_extra_max_seconds") or 0)
    if floodwait_min < 0 or floodwait_max < 0 or floodwait_min > floodwait_max:
        raise ValueError("FloodWait padding must be non-negative and min cannot exceed max")

    logging_level = str(logging_cfg.get("level", "INFO")).upper()
    if logging_level not in {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}:
        raise ValueError("logging.level must be CRITICAL, ERROR, WARNING, INFO, or DEBUG")

    display_cfg = raw.get("display") or {}
    display_tz_raw = display_cfg.get("timezone_hours", 8)
    try:
        display_timezone_hours = int(display_tz_raw)
    except (TypeError, ValueError):
        raise ValueError("display.timezone_hours must be an integer (e.g. 8 for UTC+8)")
    if not (-12 <= display_timezone_hours <= 14):
        raise ValueError("display.timezone_hours must be between -12 and +14")

    sources = [ChatSpec.from_config(item) for item in migration.get("sources", [])]
    destinations = [ChatSpec.from_config(item) for item in migration.get("destinations", [])]
    source_blacklist = [
        chat
        for chat in (
            str(item.get("chat") if isinstance(item, dict) else item).strip()
            for item in migration.get("source_blacklist", []) or []
        )
        if chat
    ]
    if any(source.topic_id is not None for source in sources):
        raise ValueError(
            "migration.sources topic_id is not supported; refusing to scan an entire forum by accident"
        )

    return AppConfig(
        base_dir=base_dir,
        telegram=TelegramConfig(
            api_id=api_id,
            api_hash=api_hash,
            user_session=str(telegram.get("user_session") or "user"),
            sessions_dir=_path(base_dir, str(telegram.get("sessions_dir", "sessions"))),
            load_dialogs_on_start=_as_bool(
                telegram.get("load_dialogs_on_start"), True, name="telegram.load_dialogs_on_start"
            ),
            bot_enabled=bot_enabled,
            bot_token=bot_token,
            bot_session_name=str(bot.get("session_name") or "uploader_bot"),
            use_bot_for_uploads=_as_bool(
                bot.get("use_for_uploads"), True, name="telegram.bot.use_for_uploads"
            ),
            admin_ids=_admin_ids(telegram.get("admin_ids")),
        ),
        mini_app=MiniAppConfig(
            enabled=mini_app_enabled,
            public_url=mini_app_url,
            host=mini_app_host,
            port=mini_app_port,
            auth_max_age_seconds=mini_app_auth_max_age_seconds,
        ),
        transfer=TransferConfig(
            include_videos=_as_bool(include.get("videos"), True, name="transfer.include.videos"),
            include_photos=_as_bool(include.get("photos"), True, name="transfer.include.photos"),
            include_text=_as_bool(include.get("text"), True, name="transfer.include.text"),
            include_documents=_as_bool(
                include.get("documents"), False, name="transfer.include.documents"
            ),
            hide_sender=_as_bool(transfer.get("hide_sender"), True, name="transfer.hide_sender"),
            drop_caption=_as_bool(transfer.get("drop_caption"), False, name="transfer.drop_caption"),
            prefer_copy=_as_bool(native_copy.get("enabled"), True, name="transfer.native_copy.enabled"),
            forwarding_only=_as_bool(native_copy.get("only"), False, name="transfer.native_copy.only"),
            save_to_local=_as_bool(transfer.get("save_to_local"), False, name="transfer.save_to_local"),
            max_bot_upload_bytes=_positive_int(
                transfer.get("max_bot_upload_bytes"), 2_000 * 1024 * 1024, name="transfer.max_bot_upload_bytes"
            ),
        ),
        queue=QueueConfig(
            db_path=_path(base_dir, str(queue.get("db_path", "data/migration.sqlite3"))),
            max_attempts=max_attempts,
            retry_backoff_seconds=retry_backoff_seconds,
            record_skipped=_as_bool(queue.get("record_skipped"), True, name="queue.record_skipped"),
        ),
        limits=LimitsConfig(
            global_min_delay_seconds=_non_negative_float(
                limits.get("global_min_delay_seconds"), 1.0, name="limits.global_min_delay_seconds"
            ),
            resolve_delay_seconds=_non_negative_float(
                limits.get("resolve_delay_seconds"), 2.0, name="limits.resolve_delay_seconds"
            ),
            read_delay_seconds=_non_negative_float(
                limits.get("read_delay_seconds"), 2.0, name="limits.read_delay_seconds"
            ),
            download_delay_seconds=_non_negative_float(
                limits.get("download_delay_seconds"), 5.0, name="limits.download_delay_seconds"
            ),
            copy_delay_seconds=_non_negative_float(
                limits.get("copy_delay_seconds"), 10.0, name="limits.copy_delay_seconds"
            ),
            upload_delay_seconds=_non_negative_float(
                limits.get("upload_delay_seconds"), 30.0, name="limits.upload_delay_seconds"
            ),
            verify_delay_seconds=_non_negative_float(
                limits.get("verify_delay_seconds"), 2.0, name="limits.verify_delay_seconds"
            ),
            get_messages_chunk_size=_positive_int(
                limits.get("get_messages_chunk_size"), 100, name="limits.get_messages_chunk_size"
            ),
            floodwait_extra_min_seconds=floodwait_min,
            floodwait_extra_max_seconds=floodwait_max,
        ),
        batch=BatchConfig(
            size=_positive_int(batch.get("size"), 25, name="batch.size"),
            pause_between_batches_seconds=int(
                _non_negative_float(
                    batch.get("pause_between_batches_seconds"),
                    1800,
                    name="batch.pause_between_batches_seconds",
                )
            ),
            idle_sleep_seconds=int(
                _non_negative_float(batch.get("idle_sleep_seconds"), 30, name="batch.idle_sleep_seconds")
            ),
        ),
        downloads=DownloadsConfig(
            root=root,
            active_dir=active_dir,
            failed_dir=failed_dir,
            completed_dir=completed_dir,
            keep_failed=_as_bool(downloads.get("keep_failed"), False, name="downloads.keep_failed"),
            keep_completed=_as_bool(
                downloads.get("keep_completed"), False, name="downloads.keep_completed"
            ),
        ),
        logging=LoggingConfig(
            level=logging_level,
            file=_path(base_dir, str(log_file)) if log_file else None,
        ),
        sources=sources,
        destinations=destinations,
        source_blacklist=source_blacklist,
        display_timezone_hours=display_timezone_hours,
    )
