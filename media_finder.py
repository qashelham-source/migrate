from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.config import load_config
from app.db import Database
from app.media_finder import (
    duplicate_groups,
    find_by_reference,
    index_existing_queue,
    initialize_media_finder,
    media_finder_stats,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Release 7 Original Media Finder")
    parser.add_argument("command", choices=("init", "backfill", "stats", "duplicates", "find"))
    parser.add_argument("value", nargs="?", help="Telegram link or message ID for find")
    parser.add_argument("--config", default="config.yaml", help="Path to YAML config")
    parser.add_argument("--limit", type=int, default=100, help="Maximum records")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(Path(args.config))
    config.ensure_directories()
    db = Database(config.queue.db_path)
    db.initialize()
    initialize_media_finder(db)
    try:
        if args.command == "init":
            print("Media Finder database is ready")
        elif args.command == "backfill":
            created = index_existing_queue(db, limit=args.limit)
            print(f"Indexed {created} existing queue record(s)")
        elif args.command == "stats":
            print(json.dumps(media_finder_stats(db), indent=2, ensure_ascii=False))
        elif args.command == "duplicates":
            print(json.dumps(duplicate_groups(db, limit=args.limit), indent=2, ensure_ascii=False))
        elif args.command == "find":
            if not args.value:
                raise SystemExit("Usage: python3 media_finder.py find <Telegram link or message ID>")
            result = find_by_reference(db, args.value)
            print(json.dumps(result, indent=2, ensure_ascii=False) if result else "No indexed media found")
    finally:
        db.close()


if __name__ == "__main__":
    main()
