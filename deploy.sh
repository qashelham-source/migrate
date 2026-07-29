#!/usr/bin/env bash
set -Eeuo pipefail

APP_DIR="${APP_DIR:-/opt/migration-manager}"
PROJECT_NAME="${COMPOSE_PROJECT_NAME:-migration-manager}"
APP_UID="${APP_UID:-10001}"
APP_GID="${APP_GID:-10001}"
HEALTH_TIMEOUT_SECONDS="${HEALTH_TIMEOUT_SECONDS:-180}"
TMP_DIR="$(mktemp -d)"
LOCK_DIR="${APP_DIR}/.deploy.lock"
BACKUP_ROOT="${APP_DIR}/backups"
BACKUP_DIR="${BACKUP_ROOT}/$(date -u +%Y%m%dT%H%M%SZ)"
PREVIOUS_SHA=""
DEPLOYED_SHA=""

cleanup() {
  rm -rf "$TMP_DIR" "$LOCK_DIR"
}
trap cleanup EXIT

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "Another deployment is already running: $LOCK_DIR" >&2
  exit 1
fi

cd "$APP_DIR"
PREVIOUS_SHA="$(git rev-parse HEAD)"
mkdir -p "$BACKUP_DIR"

# Preserve server-only configuration before replacing tracked project files.
[ -f config.yaml ] && cp -a config.yaml "$TMP_DIR/config.yaml"
[ -f .env ] && cp -a .env "$TMP_DIR/.env"
[ -f config.yaml ] && cp -a config.yaml "$BACKUP_DIR/config.yaml"
[ -f .env ] && cp -a .env "$BACKUP_DIR/.env"

# Use SQLite's online backup API so WAL-backed databases are copied consistently.
if [ -f data/migration.sqlite3 ]; then
  python - "$APP_DIR/data/migration.sqlite3" "$BACKUP_DIR/migration.sqlite3" <<'PY'
import sqlite3
import sys

source_path, destination_path = sys.argv[1:]
source = sqlite3.connect(source_path, timeout=30)
destination = sqlite3.connect(destination_path)
try:
    source.backup(destination)
finally:
    destination.close()
    source.close()
PY
fi

restore_local_files() {
  [ -f "$TMP_DIR/config.yaml" ] && cp -a "$TMP_DIR/config.yaml" config.yaml
  [ -f "$TMP_DIR/.env" ] && cp -a "$TMP_DIR/.env" .env
  rm -f config.yaml.bak config.yaml.tmp sessions/accounts.json.tmp
  mkdir -p sessions data downloads/active downloads/failed downloads/completed
  chmod 600 config.yaml 2>/dev/null || true
  [ -f .env ] && chmod 600 .env || true
  if [ "$(id -u)" -eq 0 ]; then
    chown -R "$APP_UID:$APP_GID" sessions data downloads
    chown "$APP_UID:$APP_GID" config.yaml 2>/dev/null || true
    [ -f .env ] && chown "$APP_UID:$APP_GID" .env || true
  fi
}

wait_for_health() {
  local deadline=$((SECONDS + HEALTH_TIMEOUT_SECONDS))
  local status
  while [ "$SECONDS" -lt "$deadline" ]; do
    status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' migration-manager 2>/dev/null || true)"
    case "$status" in
      healthy|running)
        return 0
        ;;
      unhealthy|exited|dead)
        return 1
        ;;
    esac
    sleep 5
  done
  return 1
}

rollback() {
  echo "Deployment failed health validation. Rolling back to $PREVIOUS_SHA." >&2
  docker compose -p "$PROJECT_NAME" logs --no-color --tail=200 migration-manager >&2 || true
  docker compose -p "$PROJECT_NAME" down --remove-orphans || true
  git reset --hard "$PREVIOUS_SHA"
  restore_local_files
  if [ -f "$BACKUP_DIR/migration.sqlite3" ]; then
    cp -a "$BACKUP_DIR/migration.sqlite3" data/migration.sqlite3
    rm -f data/migration.sqlite3-wal data/migration.sqlite3-shm
    if [ "$(id -u)" -eq 0 ]; then
      chown "$APP_UID:$APP_GID" data/migration.sqlite3
    fi
  fi
  docker compose -p "$PROJECT_NAME" config >/dev/null
  docker compose -p "$PROJECT_NAME" up -d --build --remove-orphans
  echo "Rollback deployment started from $PREVIOUS_SHA with its database snapshot." >&2
}

git fetch origin main
git reset --hard origin/main
DEPLOYED_SHA="$(git rev-parse HEAD)"
restore_local_files

export APP_UID APP_GID

docker compose -p "$PROJECT_NAME" config >/dev/null
# Build while the current container is still serving traffic.
docker compose -p "$PROJECT_NAME" build
docker compose -p "$PROJECT_NAME" up -d --remove-orphans

if ! wait_for_health; then
  rollback
  exit 1
fi

# Retain the ten newest deployment backups.
find "$BACKUP_ROOT" -mindepth 1 -maxdepth 1 -type d -printf '%T@ %p\n' 2>/dev/null \
  | sort -nr \
  | awk 'NR > 10 {sub(/^[^ ]+ /, ""); print}' \
  | xargs -r rm -rf --

echo
echo "Deployment complete: $DEPLOYED_SHA"
echo "Backup: $BACKUP_DIR"
docker compose -p "$PROJECT_NAME" ps
