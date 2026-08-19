# Telegram Restricted Content Migrator

A resumable, queue-based Telegram migration service for content that you are authorized to access and migrate. It separates scanning, processing, verification, repair, and live monitoring so large migrations do not depend on one fragile process.

## Safety and ownership

Use this project only for content you own or have permission to access, copy, and publish. Telegram credentials, bot tokens, session files, database files, and downloaded media must be treated as secrets or sensitive data.

## Core capabilities

- SQLite-backed durable queue with crash recovery
- sequential multi-source processing
- incremental checkpoints and live source watching
- native Telegram copy/forward with download-and-upload fallback
- filter-consistent album/media-group handling
- fast per-destination duplicate detection using Telegram media fingerprints
- confirmed cleanup of already-sent exact duplicate media copies
- reusable bot `file_id` cache
- strong destination verification and item-level repair
- fail-safe destination readiness checks and Issue Center reporting
- Telegram admin control panel and authenticated Mini App dashboard
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

mini_app:
  enabled: false
  public_url: "${MINI_APP_URL}"
  host: "0.0.0.0"
  port: 8080

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

Sources and destinations can be managed from the Telegram control panel. A destination may include a forum topic ID; queue deduplication treats different topics as different delivery targets. Source forum-topic filtering is intentionally rejected at startup rather than risk scanning an entire forum when only one topic was intended.

## First login

```bash
python main.py login --session user
```

The session is stored under `sessions/`. Never commit or share session files.

## Recover a revoked bot token without SSH

When the control bot token is revoked or expires, the migration worker and web
dashboard stay separate. Open the existing **Open Mini Dashboard** button in
the bot chat, then use **Masukkan token bot baharu**. The dashboard accepts a
replacement only after it verifies all of the following:

- Telegram accepts the token via `getMe`;
- the token belongs to the same configured bot; and
- the Mini App was opened by an authorized admin Telegram account.

The validated replacement is kept in the protected shared `sessions` volume,
so the crash-looping `migration-admin` container picks it up automatically on
its next restart. The token is never returned by the dashboard. This recovery
flow requires the Mini App to be enabled and reachable over its configured
HTTPS URL.

## Commands

```bash
python main.py admin                 # private Telegram control panel
python main.py web                   # local Mini App HTTP server (behind HTTPS proxy)
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
- a terminal repair failure changes the parent verification to `failed` for review instead of leaving it stuck in `repairing`;
- schema initialization never changes job states;
- deliberate skips—such as configured filters, same-destination duplicates, media above the bot upload cap, and terminal non-repair failures cancelled by policy—remain recorded for checkpoints but do not block source advancement or appear in Issue Center;
- uploads with an unknown destination result and failed repair jobs stay visible for review rather than being silently cancelled.

Legacy `SendMultiMedia MEDIA_EMPTY` failures are not silently requeued during startup. Retry them through the repair controls after reviewing the destination.

## Fast media duplicate detector

The fast detector compares Telegram's media fingerprint (`file_unique_id`) before a new job is queued. When the same single media item or complete album is already pending, in progress, or copied to the **same destination and topic**, the later source post is recorded as `skipped` with a reason that identifies the original job. This prevents a restart or repeated repost from producing another copy.

Different destinations and topics remain independent. Text messages and repair jobs are not compared by this detector. It does not download every file or calculate a SHA-256 checksum, so it remains lightweight; a file re-uploaded to Telegram with a different fingerprint can still be copied.

For duplicates created before the detector was enabled, open **Smart Center → Duplicate Detector → Clean Sent Copies**. The panel first shows a read-only preview, then requires a separate delete confirmation. It only deletes tracked destination messages with saved IDs and the exact same media fingerprint in the same destination and topic; it keeps the oldest sent copy, never touches source posts, and records the removed copy as an expected skip so a later full scan does not send it again. Stop an active migration before cleanup, and ensure the control bot has permission to delete messages in that destination.

## Telegram control panel

Start it with:

```bash
python main.py admin
```

Only explicit IDs from `telegram.admin_ids` and `ADMIN_USER_ID` are accepted. On a first-run setup with no explicit admin ID, only the configured active user session may bootstrap access; unrelated entries in the cached account list never become admins. The panel supports source and destination management, live status, stop requests, health checks, repair queue controls, checkpoint reset, full scans, and incremental sync.

## Mini App dashboard

When `mini_app.enabled` is true and `mini_app.public_url` is a public HTTPS URL, the private bot dashboard shows **Open Mini Dashboard**. The Mini App is designed for live status and source-queue ordering on a phone; destructive actions such as deleting a source, clearing history, and checkpoint reset remain in the chat panel with their existing confirmations.

The browser never gets access merely because it sends a Telegram user ID. Every API request carries Telegram `initData`, which the server validates with the bot token and checks against the same admin allow-list as the chat bot. The dashboard omits raw channel IDs, bot tokens, and session data.

Set the public URL in `.env` and enable the section in `config.yaml`:

```env
MINI_APP_URL=https://migration.example.com
```

```yaml
mini_app:
  enabled: true
  public_url: "${MINI_APP_URL}"
```

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

The image uses a multi-stage build and runs as a non-root user. Compose drops Linux capabilities, enables `no-new-privileges`, uses a read-only root filesystem, and mounts only the required writable runtime paths. The control panel can still update the writable `config.yaml` bind mount: it stages the replacement in the container's `/tmp` tmpfs and copies it back safely when an atomic rename is unavailable.

Set `APP_UID` and `APP_GID` when the host directories belong to a different service account:

```env
APP_UID=10001
APP_GID=10001
```

### Mini App HTTPS proxy

Compose binds the Mini App port to `127.0.0.1` only. Put a TLS reverse proxy in front of it; do not expose the container port directly to the internet. For example, with Caddy and a DNS name pointed at the server:

```caddyfile
migration.example.com {
  reverse_proxy 127.0.0.1:8080
}
```

Use the same HTTPS address in `MINI_APP_URL`. Once the admin bot restarts, the button appears in its dashboard. Keep `mini_app.enabled: false` until the proxy and DNS are ready.

## Production deployment

`deploy.sh` performs a health-gated deployment:

1. acquires a deployment lock;
2. fetches `origin/main`, resets the checkout, and re-executes the newly fetched script so stale deployment logic cannot continue in memory;
3. preserves `config.yaml` and `.env`;
4. validates and fixes runtime ownership for the configured non-root UID/GID;
5. creates a consistent SQLite online backup with `python3`;
6. builds the new revision while the old container is still running;
7. replaces the service and waits for a healthy state;
8. restores the previous Git revision and matching database snapshot automatically if health validation fails.

Always invoke the deployment entrypoint through Bash. This works even when an older checkout has lost the executable mode bit:

```bash
cd /opt/migration-manager
APP_DIR=/opt/migration-manager APP_UID=10001 APP_GID=10001 bash ./deploy.sh
```

The script requires Git, Python 3, Docker, and Docker Compose v2. Run it as root, or set `APP_UID` and `APP_GID` to the numeric IDs of the deployment user. `config.yaml` is kept at mode `600` and owned by the same UID/GID used by the container so the non-root process can read and update it. Before updating, take a backup: the deployment script preserves the file and rollback restores the matching revision.

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
python3 - <<'PY'
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
bash -n deploy.sh
python -m ruff check --select E9,F63,F7,F82 app tests main.py bot.py
python -m pytest -q
python -m pip_audit --requirement requirements.txt
```

CI also performs:

- deployment script syntax and executable-mode validation
- Python compilation and the complete test suite
- secret scanning
- Docker Compose validation
- a fresh-database startup smoke test in a read-only container
- high/critical container vulnerability scanning
- clean artifact packaging

Dependabot monitors Python, Docker, and GitHub Actions dependencies.

## Security incident response

Immediately rotate `API_HASH`, `BOT_TOKEN`, affected Telegram sessions, and any exposed credentials if a secret enters Git history or an artifact. Deleting the file in a later commit does not remove it from history.

This repository's legacy ZIP was deleted from the current tree but must still be treated as present in reachable history until the documented rewrite and verification are complete. History rewriting is intentionally not automated by `deploy.sh`: follow `SECURITY.md`, coordinate a maintenance window, use `git filter-repo`, force-push all affected refs only after explicit owner approval, and require every clone to re-clone afterward.

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
deploy.sh                    self-refreshing backup/deploy/rollback entrypoint
```
