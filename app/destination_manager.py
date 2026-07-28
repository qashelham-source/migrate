from __future__ import annotations

import re
from pathlib import Path
from threading import Lock
from typing import Any, Iterable

import yaml


_CONFIG_LOCK = Lock()
_TME_RE = re.compile(r"^(?:https?://)?t\.me/(.+?)/?$", re.IGNORECASE)


def _load_yaml(config_path: str | Path) -> tuple[Path, dict[str, Any]]:
    path = Path(config_path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError("config.yaml must contain a YAML object")
    return path, data


def _save_yaml(path: Path, data: dict[str, Any]) -> None:
    backup = path.with_suffix(path.suffix + ".bak")
    if path.exists():
        backup.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(data, handle, allow_unicode=True, sort_keys=False)
        handle.flush()
    temporary.replace(path)


def _is_placeholder(chat: str) -> bool:
    value = str(chat).lower()
    return "source_channel_or_-100_id" in value or "destination_channel_or_-100_id" in value


def normalize_chat(chat: str | int) -> str:
    value = str(chat).strip()
    if not value:
        raise ValueError("Channel cannot be empty")

    match = _TME_RE.match(value)
    if match:
        path = match.group(1).split("?", 1)[0].strip("/")
        parts = [part for part in path.split("/") if part]
        if not parts:
            raise ValueError("Invalid Telegram link")
        if parts[0] == "c" and len(parts) >= 2 and parts[1].isdigit():
            return f"-100{parts[1]}"
        if parts[0].startswith("+") or parts[0] == "joinchat":
            raise ValueError("Invite links are not supported. Forward a post or send the -100 channel ID.")
        return f"@{parts[0].lstrip('@')}"

    if value.startswith("@") or value.lstrip("-").isdigit():
        return value
    return f"@{value}"


def _normalised_items(values: Iterable[str | int | dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, int | None]] = set()
    for raw in values:
        if isinstance(raw, dict):
            chat = normalize_chat(raw.get("chat", ""))
            topic_id = raw.get("topic_id")
        else:
            chat = normalize_chat(raw)
            topic_id = None
        key = (chat.lower(), int(topic_id) if topic_id is not None else None)
        if key in seen:
            continue
        seen.add(key)
        item: dict[str, Any] = {"chat": chat}
        if topic_id is not None:
            item["topic_id"] = int(topic_id)
        result.append(item)
    return result


def get_sources(config_path: str | Path = "config.yaml") -> list[dict[str, Any]]:
    with _CONFIG_LOCK:
        _, data = _load_yaml(config_path)
        sources = (data.get("migration") or {}).get("sources") or []
        result: list[dict[str, Any]] = []
        for source in sources:
            item = dict(source) if isinstance(source, dict) else {"chat": str(source)}
            item["chat"] = str(item.get("chat") or "")
            if item["chat"] and not _is_placeholder(item["chat"]):
                result.append(item)
        return result


def set_sources(chats: Iterable[str | int | dict[str, Any]], config_path: str | Path = "config.yaml") -> list[dict[str, Any]]:
    items = _normalised_items(chats)
    if not items:
        raise ValueError("Choose at least one source")
    with _CONFIG_LOCK:
        path, data = _load_yaml(config_path)
        data.setdefault("migration", {})["sources"] = items
        _save_yaml(path, data)
    return items


def set_source(chat: str, config_path: str | Path = "config.yaml") -> dict[str, Any]:
    return set_sources([chat], config_path)[0]


def list_destinations(config_path: str | Path = "config.yaml") -> list[dict[str, Any]]:
    with _CONFIG_LOCK:
        _, data = _load_yaml(config_path)
        destinations = (data.get("migration") or {}).get("destinations") or []
        result: list[dict[str, Any]] = []
        for destination in destinations:
            item = dict(destination) if isinstance(destination, dict) else {"chat": str(destination)}
            item["chat"] = str(item.get("chat") or "")
            if item["chat"] and not _is_placeholder(item["chat"]):
                result.append(item)
        return result


def set_destinations(
    destinations: Iterable[str | int | dict[str, Any]],
    config_path: str | Path = "config.yaml",
) -> list[dict[str, Any]]:
    items = _normalised_items(destinations)
    if not items:
        raise ValueError("Choose at least one destination")
    with _CONFIG_LOCK:
        path, data = _load_yaml(config_path)
        data.setdefault("migration", {})["destinations"] = items
        _save_yaml(path, data)
    return items


def add_destination(
    chat: str,
    topic_id: int | None = None,
    config_path: str | Path = "config.yaml",
) -> dict[str, Any]:
    existing = list_destinations(config_path)
    item: dict[str, Any] = {"chat": chat}
    if topic_id is not None:
        if topic_id <= 0:
            raise ValueError("Topic ID must be greater than zero")
        item["topic_id"] = topic_id
    normalised = _normalised_items([*existing, item])
    if len(normalised) == len(existing):
        raise ValueError("Destination already exists")
    set_destinations(normalised, config_path)
    return normalised[-1]


def remove_destination(index: int, config_path: str | Path = "config.yaml") -> dict[str, Any]:
    destinations = list_destinations(config_path)
    if not destinations:
        raise ValueError("No destinations configured")
    if index < 1 or index > len(destinations):
        raise ValueError(f"Destination number must be between 1 and {len(destinations)}")
    removed = destinations.pop(index - 1)
    if destinations:
        set_destinations(destinations, config_path)
    else:
        with _CONFIG_LOCK:
            path, data = _load_yaml(config_path)
            data.setdefault("migration", {})["destinations"] = []
            _save_yaml(path, data)
    return removed
