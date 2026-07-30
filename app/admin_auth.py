from __future__ import annotations

from app.config import AppConfig
from app.telegram_client import load_accounts


def authorized_ids(config: AppConfig) -> set[int]:
    """Return explicit admin IDs with the same narrow first-run fallback as the bot."""
    configured = {
        int(user_id)
        for user_id in config.telegram.admin_ids
        if isinstance(user_id, int) or str(user_id).strip().isdigit()
    }
    if configured:
        return configured

    active_account = load_accounts(config).get(config.telegram.user_session)
    try:
        return {int(active_account["id"])}
    except (KeyError, TypeError, ValueError):
        return set()


def is_authorized(config: AppConfig, user_id: int | None) -> bool:
    return user_id is not None and user_id in authorized_ids(config)
