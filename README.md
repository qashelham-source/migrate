# Telegram Restricted Content Migrator

A resumable, queue-based Telegram migration service for content that you are authorized to access and migrate. It separates scanning, processing, verification, repair, and live monitoring so large migrations do not depend on one fragile process.

## Safety and ownership

Use this project only for content you own or have permission to access, copy, and publish. Telegram credentials, bot tokens, session files, database files, and downloaded media must be treated as secrets or sensitive data.

## Core capabilities

- SQLite-backed durable queue with crash recovery
- sequential multi-source processing
- incremental checkpoints and live source watching
- native Telegram copy/forward with download-and-upload fallback
- album/media-group handling
- reusable bot `file_id` cache
- strong destination verification and item-level repair
- destination pause controls and Issue Center reporting
- Telegram admin control panel
- FloodWait-aware operation pacing
- storage pressure protection and progress telemetry
- Docker deployment with health checks, backups, and rollback

## Requirements

- Python 3.11
- a Telegram API ID and API hash
- a logged-in Telegram user session with access to the source
- a bot token when bot uploads or the control panel are enabled
- numeric Telegram user IDs authorized to use the control panel

## Install locally

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
cp config.example.yaml config.yaml
```

Create `.env`:

```env
API_ID=123456
API_HASH=your_api_hash
BOT_TOKEN=123456:your_bot_token
ADMIN_USER_ID=123456789
```

`ADMIN_USER_ID` accepts a comma-separated list. The same IDs may be placed in `telegram.admin_ids` inside `config.yaml`.

Protect local secrets:

```bash
chmod 600 .env config.yaml
```

## Configuration

The example configuration contains validated defaults. Invalid booleans, negative delays, empty required credentials, invalid retry ranges, and reversed FloodWait padding ranges cause startup to fail with a clear error.

Important sections:

```yaml
telegram:
  api_id: "${API_ID}"
  api_hash: "${API_HASH}"
  user_session: "user"
  admin_ids: [123456789]
  bot:
    enabled: true
    token: "${BOT_TOKEN}"
    use_for_uploads: true

migration:
  sources: []
  destinations: []

queue:
  db_path: "data/migration.sqlite3"
  max_attempts: 4
  retry_backoff_seconds: [300, 600, 1800]

batch:
  size: 100
  pause_between_batches_seconds: 120
```

Sources and destinations can be managed from the Telegram control panel. A destination may include a forum topic ID; queue deduplication treats different topics as different delivery targets.

## First login

```bash
python main.py login --session user
```

The session is stored under `sessions/`. Never commit or share session files.

## Commands

```bash
python main.py admin                 # private Telegram control panel
python main.py health                # Telegram-aware health report
python main.py scan                  # full scan into the durable queue
python main.py sync                  # scan only after the latest checkpoint
python main.py process               # process runnable queued work
python main.py verify                # verify copied destination messages
python main.py run                   # full scan, process, and verify
python main.py serve                 # live watcher and service loop
python main.py stats                 # queue and cache counts
python main.py recover               # recover interrupted queue state safely
python main.py list-destinations
python main.py add-destination @channel [topic_id]
python main.py remove-destination <number>
```

`python bot.py ...` remains a compatibility wrapper around `main.py`.

## Queue and recovery behavior

Queue states are `pending`, `downloading`, `uploading`, `copied`, `failed`, and `skipped`.

- interrupted downloads return to `pending`;
- interrupted uploads are held as failed because the destination result is uncertain;
- pending work is claimed atomically to prevent duplicate sends by multiple workers;
- destination permission failures pause that destination;
- verification mismatches are recorded for review or item-level repair;
- schema initialization never changes job states.

Legacy `SendMultiMedia MEDIA_EMPTY` failures are not silently requeued during startup. Retry them through the repair controls after reviewing the destination.

## Telegram control panel

Start it with:

```bash
python main.py admin
```

Only IDs from `telegram.admin_ids`, `ADMIN_USER_ID`, or the cached owner session are accepted. The panel supports source and destination management, live status, stop requests, health checks, repair queue controls, checkpoint reset, full scans, and incremental sync.

## Docker

Prepare files and directories:

```bash
cp config.example.yaml config.yaml
mkdir -p sessions data downloads/active downloads/failed downloads/completed
chmod 600 config.yaml .env
```

Build and start:

```bash
docker compose config
docker compose up -d --build
```

The image uses a multi-stage build and runs as a non-root user. Compose drops Linux capabilities, enables `no-new-privileges`, uses a read-only root filesystem, and mounts only the required writable runtime paths.

Set `APP_UID` and `APP_GID` when the host directories belong to a different service account:

```env
APP_UID=10001
APP_GID=10001
```

## Production deployment

`deploy.sh` performs a health-gated deployment:

1. acquires a deployment lock;
2. preserves `config.yaml` and `.env`;
3. creates a consistent SQLite online backup;
4. fetches and builds the new revision while the old container is still running;
5. replaces the service;
6. waits for a healthy state;
7. restores the previous Git revision automatically if health validation fails.

Run as the deployment user:

```bash
APP_DIR=/opt/migration-manager ./deploy.sh
```

Backups are stored under `backups/`; the ten newest are retained.

## Health and database maintenance

Frequent container probes use a lightweight database read. A deeper SQLite integrity check is available for maintenance windows:

```bash
python reliability_check.py --config config.yaml --deep
```

Apply schema initialization and performance indexes safely:

```bash
python optimize_database.py --config config.yaml
```

The optimizer supports an empty database and creates all required base and Release 3 tables before adding indexes.

## Backup and restore

Stop the service before a manual restore. Restore the database together with its matching configuration and ensure the runtime user owns the restored files.

Example manual backup using Python's SQLite online backup API:

```bash
python - <<'PY'
import sqlite3
source = sqlite3.connect('data/migration.sqlite3')
destination = sqlite3.connect('migration-backup.sqlite3')
source.backup(destination)
destination.close()
source.close()
PY
```

Do not copy only the main SQLite file while the service is writing unless you use the online backup API.

## Testing and release gates

```bash
pip install -r requirements.txt -r requirements-dev.txt
python -m ruff check --select E9,F63,F7,F82 app tests main.py bot.py
python -m pytest -q
python -m pip_audit --requirement requirements.txt
```

CI also performs:

- Python compilation and the complete test suite
- secret scanning
- Docker Compose validation
- a fresh-database startup smoke test in a read-only container
- high/critical container vulnerability scanning
- clean artifact packaging

Dependabot monitors Python, Docker, and GitHub Actions dependencies.

## Security incident response

Immediately rotate `API_HASH`, `BOT_TOKEN`, affected Telegram sessions, and any exposed credentials if a secret enters Git history or an artifact. Deleting the file in a later commit does not remove it from history.

History rewriting is intentionally not automated by `deploy.sh`. Follow `SECURITY.md`, coordinate a maintenance window, use `git filter-repo`, force-push all affected refs, and require every clone to re-clone afterward.

## Project layout

```text
app/                         application, queue, uploader, verifier, dashboard
.github/workflows/           test, security, image scan, and packaging gate
data/                        SQLite database and rotating log
downloads/                   controlled active/failed/completed work areas
sessions/                    Telegram sessions and private account cache
tests/                       regression and fault-injection coverage
config.example.yaml          validated example configuration
docker-compose.yml           hardened service definition
optimize_database.py         schema bootstrap and performance tuning
reliability_check.py         lightweight/deep health checks
deploy.sh                    backup, health validation, and rollback
```
