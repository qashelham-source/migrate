from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from app.config import load_config
from app.db import Database
from app.release3_store import Release3Store


INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_messages_pending_order ON messages(status, next_retry_at, updated_at, id)",
    "CREATE INDEX IF NOT EXISTS idx_messages_verification_order ON messages(status, verified_at, updated_at, id)",
    "CREATE INDEX IF NOT EXISTS idx_messages_destination_status ON messages(dest_chat_id, status, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_messages_media_group ON messages(media_group_id, status)",
    "CREATE INDEX IF NOT EXISTS idx_repair_actions_lookup ON repair_actions(job_id, action, outcome)",
    "CREATE INDEX IF NOT EXISTS idx_verification_results_status ON verification_results(status, checked_at)",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply idempotent SQLite performance tuning")
    parser.add_argument("--config", default="config.yaml")
    return parser.parse_args()


def initialize_schema(path: Path) -> None:
    """Create every table required by the optimizer on a fresh installation."""
    database = Database(path)
    try:
        database.initialize()
        Release3Store(database).initialize()
    finally:
        database.close()


def optimize(path: Path) -> None:
    initialize_schema(path)
    connection = sqlite3.connect(path, timeout=30.0)
    try:
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA temp_store=MEMORY")
        for statement in INDEXES:
            connection.execute(statement)
        connection.execute("PRAGMA optimize")
        connection.commit()
    finally:
        connection.close()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    config.ensure_directories()
    optimize(config.queue.db_path)
    print(f"Database performance tuning applied: {config.queue.db_path}")


if __name__ == "__main__":
    main()
