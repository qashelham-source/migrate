# Release 9.0 — Reliability Foundation

Release 9.0 adds a non-destructive runtime health check for Migration Manager.

## Checks

- configuration loads successfully
- SQLite `PRAGMA quick_check` returns `ok`
- the `messages` table is readable
- session, data, and download directories are writable
- required Telegram credentials are present

Docker Compose now reports container health every 60 seconds. The health check is read-only for the migration database and does not open a Telegram session, so it does not compete with the running Pyrogram clients.

Manual check:

```bash
python reliability_check.py --config config.yaml
```

Machine-readable output:

```bash
python reliability_check.py --config config.yaml --json
```
