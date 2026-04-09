#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

docker build -t chess-elo-api "$REPO_DIR/apps/api"
docker build -t chess-elo-content-web "$REPO_DIR/apps/content-web"

if command -v docker-compose >/dev/null 2>&1; then
  docker-compose -f "$SCRIPT_DIR/docker-compose.yml" up -d
elif docker compose version >/dev/null 2>&1; then
  docker compose -f "$SCRIPT_DIR/docker-compose.yml" up -d
else
  echo "Docker Compose is not installed. Install either 'docker-compose' or the 'docker compose' plugin." >&2
  exit 1
fi
