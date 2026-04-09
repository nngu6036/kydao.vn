#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if command -v docker-compose >/dev/null 2>&1; then
  docker-compose -f "$SCRIPT_DIR/docker-compose.yml" up -d --build
else
  docker compose -f "$SCRIPT_DIR/docker-compose.yml" up -d --build
fi
