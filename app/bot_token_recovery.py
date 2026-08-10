"""Safe runtime storage for replacing a revoked Telegram bot token.

The Compose environment is immutable for a running container.  A small file in
the already-shared sessions volume lets the web control panel hand a validated
replacement token to the admin-bot container without requiring Docker socket
access or a host-side ``.env`` edit.
"""
from __future__ import annotations

import os
import re
from pathlib import Path


_TOKEN_PATTERN = re.compile(r"^(?P<bot_id>[1-9]\d{4,}):(?P<secret>[A-Za-z0-9_-]{16,})$")
_RUNTIME_TOKEN_FILENAME = ".runtime_bot_token"


def bot_id_from_token(token: str) -> int | None:
    """Return the bot's immutable numeric ID, without retaining its secret."""
    match = _TOKEN_PATTERN.fullmatch(str(token or "").strip())
    if not match:
        return None
    try:
        return int(match.group("bot_id"))
    except ValueError:
        return None


def runtime_token_path(sessions_dir: str | Path) -> Path:
    return Path(sessions_dir) / _RUNTIME_TOKEN_FILENAME


def load_runtime_bot_token(sessions_dir: str | Path) -> str:
    """Read a previously validated override, never raising on a bad file."""
    try:
        token = runtime_token_path(sessions_dir).read_text(encoding="utf-8").strip()
    except OSError:
        return ""
    return token if bot_id_from_token(token) is not None else ""


def effective_bot_token(default_token: str, sessions_dir: str | Path) -> str:
    """Prefer a validated runtime replacement over the static Compose value."""
    return load_runtime_bot_token(sessions_dir) or str(default_token or "").strip()


def save_runtime_bot_token(sessions_dir: str | Path, token: str) -> None:
    """Atomically persist a token with owner-only file permissions.

    The caller must validate the token with Telegram before this function is
    invoked.  Keeping the write primitive separate makes that requirement easy
    to test and prevents it from ever logging the secret.
    """
    token = str(token or "").strip()
    if bot_id_from_token(token) is None:
        raise ValueError("Format token bot tidak sah.")

    target = runtime_token_path(sessions_dir)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    try:
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(token)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        os.chmod(target, 0o600)
    except Exception:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        raise
