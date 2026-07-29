import sqlite3
from pathlib import Path

import pytest

from app.admin_bot import _snapshot_session_database


def _read_label(path: Path) -> str:
    with sqlite3.connect(path) as database:
        return str(database.execute("SELECT value FROM session_data WHERE key = 'label'").fetchone()[0])


def test_snapshot_copies_session_without_changing_live_database(tmp_path: Path) -> None:
    source = tmp_path / "migration-user.session"
    destination = tmp_path / "scan" / "temporary.session"
    with sqlite3.connect(source) as database:
        database.execute("CREATE TABLE session_data (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        database.execute("INSERT INTO session_data (key, value) VALUES ('label', 'live-session')")

    _snapshot_session_database(source, destination)

    assert _read_label(destination) == "live-session"
    assert _read_label(source) == "live-session"


def test_snapshot_requires_existing_session(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="Telegram session was not found"):
        _snapshot_session_database(tmp_path / "missing.session", tmp_path / "temporary.session")
