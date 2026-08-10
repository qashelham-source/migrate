from __future__ import annotations

import hashlib
import hmac
import json
import time
from pathlib import Path
from urllib.parse import urlencode

import pytest

from app.admin_bot import _menu
from app.bot_token_recovery import bot_id_from_token, load_runtime_bot_token, save_runtime_bot_token
from app.config import load_config
from app.db import Database
from app.destination_manager import get_sources
import app.mini_app as mini_app
from app.mini_app import MiniAppAuthError, dashboard_payload, move_source, replace_bot_token, validate_init_data
from app.queue import MessageQueue


BOT_TOKEN = "123456:mini-app-test-token"
SOURCE_ONE = "-1002843617976"
SOURCE_TWO = "-1002044036103"


def _write_config(tmp_path: Path, *, mini_app: str = "") -> Path:
    path = tmp_path / "config.yaml"
    path.write_text(
        f"""
telegram:
  api_id: 1
  api_hash: "test-hash"
  user_session: "user"
  admin_ids: [1234]
  bot:
    enabled: true
    token: "{BOT_TOKEN}"
{mini_app}
migration:
  sources:
    - chat: "{SOURCE_ONE}"
    - chat: "{SOURCE_TWO}"
  destinations:
    - chat: "-1001678732307"
queue:
  db_path: "data/migration.sqlite3"
downloads:
  root: "downloads"
""".lstrip(),
        encoding="utf-8",
    )
    return path


def _init_data(*, auth_date: int, user_id: int = 1234, token: str = BOT_TOKEN) -> str:
    values = {
        "auth_date": str(auth_date),
        "query_id": "AAEAAAE",
        "user": json.dumps({"id": user_id, "first_name": "Admin"}, separators=(",", ":")),
    }
    check = "\n".join(f"{key}={values[key]}" for key in sorted(values))
    secret = hmac.new(b"WebAppData", token.encode("utf-8"), hashlib.sha256).digest()
    values["hash"] = hmac.new(secret, check.encode("utf-8"), hashlib.sha256).hexdigest()
    return urlencode(values)


def test_validate_init_data_accepts_valid_and_rejects_tampering() -> None:
    init_data = _init_data(auth_date=10_000)

    assert validate_init_data(init_data, BOT_TOKEN, 300, now=10_100)["id"] == 1234

    with pytest.raises(MiniAppAuthError):
        validate_init_data(init_data.replace("Admin", "Other"), BOT_TOKEN, 300, now=10_100)

    with pytest.raises(MiniAppAuthError):
        validate_init_data(init_data, BOT_TOKEN, 60, now=10_100)


def test_runtime_token_override_is_used_on_next_config_load(tmp_path: Path) -> None:
    path = _write_config(tmp_path)
    replacement = "123456:replacement-token-123456"

    save_runtime_bot_token(tmp_path / "sessions", replacement)

    loaded = load_config(path)
    assert loaded.telegram.bot_token == replacement
    assert load_runtime_bot_token(loaded.telegram.sessions_dir) == replacement
    assert bot_id_from_token(replacement) == 123456


def test_token_recovery_requires_same_bot_and_authorized_mini_app(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _write_config(tmp_path)
    config = load_config(path)
    replacement = "123456:replacement-token-123456"
    checked: list[tuple[str, int]] = []

    monkeypatch.setattr(
        mini_app,
        "_verify_bot_token_with_telegram",
        lambda token, bot_id: checked.append((token, bot_id)),
    )

    replace_bot_token(
        config,
        token=replacement,
        init_data=_init_data(auth_date=int(time.time()), token=replacement),
    )

    assert checked == [(replacement, 123456)]
    assert load_runtime_bot_token(config.telegram.sessions_dir) == replacement

    with pytest.raises(mini_app.BotTokenRecoveryError, match="bukan milik"):
        replace_bot_token(
            config,
            token="654321:replacement-token-123456",
            init_data=_init_data(auth_date=int(time.time()), token=replacement),
        )


def test_mini_app_requires_a_public_https_url(tmp_path: Path) -> None:
    path = _write_config(
        tmp_path,
        mini_app="""
mini_app:
  enabled: true
  public_url: "http://dashboard.example.test"
""",
    )

    with pytest.raises(ValueError, match="HTTPS"):
        load_config(path)


def test_admin_menu_shows_mini_app_only_for_enabled_config(tmp_path: Path) -> None:
    path = _write_config(
        tmp_path,
        mini_app="""
mini_app:
  enabled: true
  public_url: "https://migration.example.test"
""",
    )

    button = _menu(load_config(path)).inline_keyboard[0][0]
    assert button.web_app is not None
    assert button.web_app.url == "https://migration.example.test"


def test_dashboard_hides_raw_source_ids_and_moves_queue(tmp_path: Path) -> None:
    path = _write_config(tmp_path)
    config = load_config(path)
    db = Database(config.queue.db_path)
    try:
        db.initialize()
        queue = MessageQueue(db, config)
        queue.register_source(
            source_chat_id=SOURCE_ONE,
            title="Awek Bigo Mango",
            username="awekbigo",
            chat_type="channel",
            latest_seen_message_id=100,
        )
        queue.register_source(
            source_chat_id=SOURCE_TWO,
            title="Archive Dua",
            username="archive_dua",
            chat_type="channel",
            latest_seen_message_id=20,
        )
    finally:
        db.close()

    payload = dashboard_payload(config, path)
    serialized = json.dumps(payload)

    assert payload["sources"][0]["title"] == "Awek Bigo Mango"
    assert payload["sources"][0]["handle"] == "@awekbigo"
    assert SOURCE_ONE not in serialized
    assert SOURCE_TWO not in serialized

    move_source(path, 1, "up")
    assert [source["chat"] for source in get_sources(path)] == [SOURCE_TWO, SOURCE_ONE]

    with pytest.raises(ValueError):
        move_source(path, 0, "up")
