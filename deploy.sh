#!/usr/bin/env bash
set -Eeuo pipefail

APP_DIR="${APP_DIR:-/opt/migration-manager}"
PROJECT_NAME="${COMPOSE_PROJECT_NAME:-migration-manager}"
APP_UID="${APP_UID:-10001}"
APP_GID="${APP_GID:-10001}"
HEALTH_TIMEOUT_SECONDS="${HEALTH_TIMEOUT_SECONDS:-180}"
LOCK_DIR="${APP_DIR}/.deploy.lock"
BACKUP_ROOT="${APP_DIR}/backups"

fail() {
  echo "Deployment error: $*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "Required command not found: $1"
}

cleanup() {
  if [ -n "${TMP_DIR:-}" ]; then
    rm -rf "$TMP_DIR"
  fi
  if [ "${DEPLOY_LOCK_HELD:-0}" = "1" ]; then
    rm -rf "$LOCK_DIR"
  fi
}
trap cleanup EXIT

[[ "$APP_UID" =~ ^[0-9]+$ ]] || fail "APP_UID must be numeric"
[[ "$APP_GID" =~ ^[0-9]+$ ]] || fail "APP_GID must be numeric"

require_command git
require_command docker
require_command python3

docker compose version >/dev/null 2>&1 || fail "Docker Compose v2 is required"

cd "$APP_DIR"
[ -d .git ] || fail "$APP_DIR is not a Git checkout"

if [ "${DEPLOY_LOCK_HELD:-0}" != "1" ]; then
  if ! mkdir "$LOCK_DIR" 2>/dev/null; then
    fail "Another deployment is already running: $LOCK_DIR"
  fi
  export DEPLOY_LOCK_HELD=1
fi

# Refresh the checkout first, then re-exec the script from the fetched revision.
# This prevents an old in-memory deploy script from continuing after it replaces itself.
if [ "${DEPLOY_BOOTSTRAPPED:-0}" != "1" ]; then
  current_sha="$(git rev-parse HEAD)"
  git fetch origin main
  target_sha="$(git rev-parse origin/main)"
  export DEPLOY_PREVIOUS_SHA="$current_sha"
  git reset --hard "$target_sha"
  export DEPLOY_BOOTSTRAPPED=1
  exec bash "$APP_DIR/deploy.sh"
fi

PREVIOUS_SHA="${DEPLOY_PREVIOUS_SHA:-$(git rev-parse HEAD)}"
DEPLOYED_SHA="$(git rev-parse HEAD)"
TMP_DIR="$(mktemp -d)"
BACKUP_DIR="${BACKUP_ROOT}/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$BACKUP_DIR"

[ -f config.yaml ] || fail "Missing $APP_DIR/config.yaml"

# Preserve server-only configuration and keep a deployment snapshot.
cp -a config.yaml "$TMP_DIR/config.yaml"
cp -a config.yaml "$BACKUP_DIR/config.yaml"
if [ -f .env ]; then
  cp -a .env "$TMP_DIR/.env"
  cp -a .env "$BACKUP_DIR/.env"
fi

# Use SQLite's online backup API so WAL-backed databases are copied consistently.
if [ -f data/migration.sqlite3 ]; then
  python3 - "$APP_DIR/data/migration.sqlite3" "$BACKUP_DIR/migration.sqlite3" <<'PY'
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

prepare_runtime_files() {
  cp -a "$TMP_DIR/config.yaml" config.yaml
  if [ -f "$TMP_DIR/.env" ]; then
    cp -a "$TMP_DIR/.env" .env
  fi
  rm -f config.yaml.bak config.yaml.tmp sessions/accounts.json.tmp
  mkdir -p sessions data downloads/active downloads/failed downloads/completed

  if [ "$(id -u)" -eq 0 ]; then
    chown "$APP_UID:$APP_GID" config.yaml
    chown -R "$APP_UID:$APP_GID" sessions data downloads
  elif [ "$(id -u)" -ne "$APP_UID" ]; then
    fail "Run as root or set APP_UID/APP_GID to the deployment user's numeric IDs"
  fi

  chmod 600 config.yaml
  [ -f .env ] && chmod 600 .env

  config_owner="$(stat -c '%u:%g' config.yaml)"
  [ "$config_owner" = "$APP_UID:$APP_GID" ] \
    || fail "config.yaml owner is $config_owner; expected $APP_UID:$APP_GID"
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
  prepare_runtime_files
  if [ -f "$BACKUP_DIR/migration.sqlite3" ]; then
    cp -a "$BACKUP_DIR/migration.sqlite3" data/migration.sqlite3
    rm -f data/migration.sqlite3-wal data/migration.sqlite3-shm
    if [ "$(id -u)" -eq 0 ]; then
      chown "$APP_UID:$APP_GID" data/migration.sqlite3
    fi
  fi
  docker compose -p "$PROJECT_NAME" config >/dev/null
  docker compose -p "$PROJECT_NAME" up -d --build --remove-orphans
  if ! wait_for_health; then
    fail "Rollback also failed health validation; inspect container logs immediately"
  fi
  echo "Rollback complete: $PREVIOUS_SHA" >&2
}

prepare_runtime_files
export APP_UID APP_GID

# Make local invocation convenient even when an older checkout lost the Git mode bit.
chmod 755 "$APP_DIR/deploy.sh" 2>/dev/null || true

docker compose -p "$PROJECT_NAME" config >/dev/null
# Build while the current healthy container is still serving traffic.
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
