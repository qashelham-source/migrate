from __future__ import annotations

from pathlib import Path
from threading import Lock
from typing import Any

import yaml


_CONFIG_LOCK = Lock()


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
    chat = str(chat).strip()
    if not chat:
        raise ValueError("Destination cannot be empty")
    if chat.startswith("@") or chat.lstrip("-").isdigit():
        return chat
    return f"@{chat}"


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
