#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if command -v docker-compose >/dev/null 2>&1; then
  docker-compose -f "$SCRIPT_DIR/docker-compose.yml" down
elif docker compose version >/dev/null 2>&1; then
  docker compose -f "$SCRIPT_DIR/docker-compose.yml" down
else
  echo "Docker Compose is not installed. Install either 'docker-compose' or the 'docker compose' plugin." >&2
  exit 1
fi
