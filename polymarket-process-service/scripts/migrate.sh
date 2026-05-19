#!/usr/bin/env bash
set -euo pipefail

DOCKER_BIN="${DOCKER:-$(command -v docker 2>/dev/null || echo /opt/homebrew/bin/docker)}"
"${DOCKER_BIN}" compose -p polymarket-bot exec -T postgres psql -U postgres -d polymarket_bot < migrations/001_init.sql
