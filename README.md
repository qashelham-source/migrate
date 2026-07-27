# Telegram Restricted Content Migrator

A queue-based Telegram migration tool for moving large batches of channel media without running one giant fragile loop. It separates scanning, processing, and verification, stores all jobs in SQLite, uses one global Telegram API limiter, handles `FloodWait`, supports album jobs, and can upload to your destination through a bot account.

Use it only for content you are allowed to access and migrate.

## What Changed

- SQLite queue in `data/migration.sqlite3`
- resumable `messages` table with `pending`, `downloading`, `uploading`, `copied`, `failed`, and `skipped` states
- separate phases: scan source IDs, process pending jobs, verify destination posts
- one shared rate limiter for Telegram calls
- `FloodWait` sleeps for Telegram's wait plus extra random padding
- batch processing with long pauses between batches
- album/media group detection and one queue job per album
- optional bot upload mode: user session reads, bot session posts
- retry backoff with a maximum attempt count
- controlled download cleanup under `downloads/active`, `downloads/failed`, and `downloads/completed`
- graceful Ctrl+C handling

## Install

```bash
pip install -r requirements.txt
```

Create or edit `config.yaml`. Secrets can be placed directly in YAML, but using `.env` is cleaner:

```env
API_ID=123456
API_HASH=your_api_hash
BOT_TOKEN=123456:your_bot_token
```

The app reads `.env`, then expands values like `${API_ID}` in `config.yaml`.

## Configure

Important fields in `config.yaml`:

```yaml
telegram:
  user_session: "tnabil"
  bot:
    enabled: true
    token: "${BOT_TOKEN}"
    use_for_uploads: true

migration:
  sources:
    - chat: "@source_channel_or_-100_id"
      message_range:
        start: 1
        end: 2000
  destinations:
    - chat: "@destination_channel_or_-100_id"

limits:
  read_delay_seconds: 2
  download_delay_seconds: 5
  upload_delay_seconds: 30

batch:
  size: 25
  pause_between_batches_seconds: 1800
```

Set the bot as an admin in your destination channel when `telegram.bot.use_for_uploads` is enabled. The user session still reads the source because bots often cannot access old source history.

## Commands

Create a user session:

```bash
python main.py login --session tnabil
```

Scan source message IDs into SQLite:

```bash
python main.py scan
```

Process pending jobs in configured batches:

```bash
python main.py process
```

Verify copied destination messages:

```bash
python main.py verify
```

Run scan and process sequentially:

```bash
python main.py run
```

Show queue counts:

```bash
python main.py stats
```

Recover jobs that were left as `downloading` or `uploading` after a crash:

```bash
python main.py recover
```

`python bot.py ...` still works as a wrapper around `main.py`.

## Project Structure

```text
app/
  config.py
  db.py
  telegram_client.py
  scanner.py
  queue.py
  worker.py
  upload.py
  errors.py
  logging.py
data/
  migration.sqlite3
downloads/
  active/
  failed/
  completed/
config.yaml
main.py
```

## Queue Behavior

The `messages` table stores:

- `source_chat_id`
- `source_message_id`
- `dest_chat_id`
- `status`
- `attempts`
- `last_error`
- `next_retry_at`
- `file_unique_key`
- `created_at`
- `updated_at`

Extra columns keep album IDs, source message ID lists, destination message IDs, and verification timestamps.

Retries use `queue.retry_backoff_seconds`; after `queue.max_attempts`, repeated failures become `failed`. Filtered messages become `skipped` when `queue.record_skipped` is true.

