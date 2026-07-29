from __future__ import annotations

import argparse
import json
import sqlite3
import tempfile
from pathlib import Path
from typing import Any

from app.config import load_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migration Manager reliability health check")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--json", action="store_true")
    parser.add_argument(
        "--deep",
        action="store_true",
        help="run SQLite quick_check; intended for maintenance, not the frequent container probe",
    )
    return parser.parse_args()


def check_writable(path: Path) -> tuple[bool, str]:
    try:
        path.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(prefix=".health-", dir=path, delete=True):
            pass
        return True, "writable"
    except OSError as exc:
        return False, f"not writable: {exc}"


def check_database(path: Path, *, deep: bool = False) -> tuple[bool, str]:
    if not path.exists():
        return False, "database file missing"
    try:
        uri = f"file:{path.resolve()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True, timeout=2.0)
        try:
            connection.execute("SELECT 1").fetchone()
            connection.execute("SELECT COUNT(*) FROM messages").fetchone()
            if deep:
                row = connection.execute("PRAGMA quick_check").fetchone()
                result = str(row[0]) if row else "no result"
                if result.lower() != "ok":
                    return False, f"quick_check={result}"
                return True, "quick_check=ok"
        finally:
            connection.close()
        return True, "readable"
    except (sqlite3.Error, OSError) as exc:
        return False, f"database check failed: {exc}"


def run(config_path: str, *, deep: bool = False) -> dict[str, Any]:
    checks: dict[str, dict[str, Any]] = {}
    try:
        config = load_config(config_path)
    except Exception as exc:
        return {
            "healthy": False,
            "checks": {"config": {"ok": False, "detail": f"{exc.__class__.__name__}: {exc}"}},
        }

    checks["config"] = {"ok": True, "detail": "loaded"}
    db_ok, db_detail = check_database(config.queue.db_path, deep=deep)
    checks["database"] = {"ok": db_ok, "detail": db_detail}

    for name, path in {
        "sessions": config.telegram.sessions_dir,
        "data": config.queue.db_path.parent,
        "downloads_active": config.downloads.active_dir,
        "downloads_failed": config.downloads.failed_dir,
        "downloads_completed": config.downloads.completed_dir,
    }.items():
        ok, detail = check_writable(path)
        checks[name] = {"ok": ok, "detail": detail}

    required_env = {
        "API_ID": bool(config.telegram.api_id),
        "API_HASH": bool(config.telegram.api_hash),
    }
    if config.telegram.bot_enabled:
        required_env["BOT_TOKEN"] = bool(config.telegram.bot_token)
    missing = [name for name, present in required_env.items() if not present]
    checks["credentials"] = {
        "ok": not missing,
        "detail": "present" if not missing else f"missing: {', '.join(missing)}",
    }

    return {"healthy": all(item["ok"] for item in checks.values()), "checks": checks}


def main() -> None:
    args = parse_args()
    result = run(args.config, deep=args.deep)
    if args.json:
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    else:
        for name, item in result["checks"].items():
            marker = "OK" if item["ok"] else "FAIL"
            print(f"[{marker}] {name}: {item['detail']}")
        print("HEALTHY" if result["healthy"] else "UNHEALTHY")
    raise SystemExit(0 if result["healthy"] else 1)


if __name__ == "__main__":
    main()
