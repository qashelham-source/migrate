#!/usr/bin/env bash
set -Eeuo pipefail

APP_DIR="${APP_DIR:-/opt/migration-manager}"
PROJECT_NAME="${COMPOSE_PROJECT_NAME:-migration-manager}"
TMP_DIR="$(mktemp -d)"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

cd "$APP_DIR"

# Keep server-only configuration while replacing tracked project files.
[ -f config.yaml ] && cp -a config.yaml "$TMP_DIR/config.yaml"
[ -f .env ] && cp -a .env "$TMP_DIR/.env"

if docker compose version >/dev/null 2>&1; then
  docker compose -p "$PROJECT_NAME" down --remove-orphans || true
fi

git fetch origin main
git reset --hard origin/main

[ -f "$TMP_DIR/config.yaml" ] && cp -a "$TMP_DIR/config.yaml" config.yaml
[ -f "$TMP_DIR/.env" ] && cp -a "$TMP_DIR/.env" .env

mkdir -p sessions data downloads/active downloads/failed downloads/completed

if [ ! -f config.yaml ]; then
  cp config.example.yaml config.yaml
  echo "config.yaml was created from the example. Edit it, then run ./deploy.sh again."
  exit 2
fi

chmod 600 config.yaml
[ -f .env ] && chmod 600 .env

docker compose -p "$PROJECT_NAME" config >/dev/null
docker compose -p "$PROJECT_NAME" up -d --build --remove-orphans

echo
echo "Deployment complete."
docker compose -p "$PROJECT_NAME" ps
