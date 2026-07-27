from __future__ import annotations

import re
from pathlib import Path
from threading import Lock
from typing import Any

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
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(data, handle, allow_unicode=True, sort_keys=False)
    temporary.replace(path)


def normalize_chat(chat: str) -> str:
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


def get_sources(config_path: str | Path = "config.yaml") -> list[dict[str, Any]]:
    with _CONFIG_LOCK:
        _, data = _load_yaml(config_path)
        sources = (data.get("migration") or {}).get("sources") or []
        result: list[dict[str, Any]] = []
        for source in sources:
            if isinstance(source, dict):
                item = dict(source)
                item["chat"] = str(item.get("chat") or "")
                result.append(item)
            else:
                result.append({"chat": str(source)})
        return result


def set_source(chat: str, config_path: str | Path = "config.yaml") -> dict[str, Any]:
    normalized_chat = normalize_chat(chat)
    with _CONFIG_LOCK:
        path, data = _load_yaml(config_path)
        migration = data.setdefault("migration", {})
        item: dict[str, Any] = {"chat": normalized_chat}
        migration["sources"] = [item]
        _save_yaml(path, data)
        return item


def list_destinations(config_path: str | Path = "config.yaml") -> list[dict[str, Any]]:
    with _CONFIG_LOCK:
        _, data = _load_yaml(config_path)
        destinations = (data.get("migration") or {}).get("destinations") or []
        result: list[dict[str, Any]] = []
        for destination in destinations:
            if isinstance(destination, dict):
                item = dict(destination)
                item["chat"] = str(item.get("chat") or "")
                result.append(item)
            else:
                result.append({"chat": str(destination)})
        return result


def add_destination(
    chat: str,
    topic_id: int | None = None,
    config_path: str | Path = "config.yaml",
) -> dict[str, Any]:
    normalized_chat = normalize_chat(chat)
    if topic_id is not None and topic_id <= 0:
        raise ValueError("Topic ID must be greater than zero")

    with _CONFIG_LOCK:
        path, data = _load_yaml(config_path)
        destinations = data.setdefault("migration", {}).setdefault("destinations", [])

        for destination in destinations:
            if isinstance(destination, dict):
                existing_chat = normalize_chat(str(destination.get("chat") or ""))
                existing_topic = destination.get("topic_id")
            else:
                existing_chat = normalize_chat(str(destination))
                existing_topic = None
            if existing_chat.lower() == normalized_chat.lower() and existing_topic == topic_id:
                raise ValueError("Destination already exists")

        item: dict[str, Any] = {"chat": normalized_chat}
        if topic_id is not None:
            item["topic_id"] = topic_id
        destinations.append(item)
        _save_yaml(path, data)
        return item


def remove_destination(
    index: int,
    config_path: str | Path = "config.yaml",
) -> dict[str, Any]:
    with _CONFIG_LOCK:
        path, data = _load_yaml(config_path)
        destinations = data.setdefault("migration", {}).setdefault("destinations", [])
        if not destinations:
            raise ValueError("No destinations configured")
        if index < 1 or index > len(destinations):
            raise ValueError(f"Destination number must be between 1 and {len(destinations)}")
        removed = destinations.pop(index - 1)
        _save_yaml(path, data)
        return dict(removed) if isinstance(removed, dict) else {"chat": str(removed)}
