# Release 10.0 — Performance

Release 10.0 improves queue and database responsiveness for large migrations.

## Changes

- Adds idempotent SQLite indexes for pending-job selection, verification scans, destination status lookups, media-group queries, and repair history lookups.
- Applies `WAL`, `synchronous=NORMAL`, `busy_timeout=30000`, and in-memory temporary storage before services start.
- Runs `PRAGMA optimize` at startup so SQLite can refresh query-planner statistics.
- Keeps all existing queue states, album ordering, verification logic, and Telegram upload behavior unchanged.

## Compatibility

No destructive migration is performed. All indexes use `CREATE INDEX IF NOT EXISTS`, so repeated deployments are safe.
