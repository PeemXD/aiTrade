#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FETCH_DATA_DIR="${ROOT_DIR}/polymarket-fetch-data-service"
PROCESS_DIR="${ROOT_DIR}/polymarket-process-service"
TMP_DIR="${ROOT_DIR}/tmp"
COMPOSE_PROJECT="${COMPOSE_PROJECT:-polymarket-bot}"
export PATH="/Applications/Docker.app/Contents/Resources/bin:${PATH}"
GO_BIN="${GO:-$(command -v go 2>/dev/null || echo /opt/homebrew/bin/go)}"
DOCKER_BIN="${DOCKER:-$(command -v docker 2>/dev/null || echo /opt/homebrew/bin/docker)}"

ensure_file() {
  local src="$1"
  local dst="$2"
  if [ ! -f "${dst}" ]; then
    cp "${src}" "${dst}"
  fi
}

ensure_env_line() {
  local file="$1"
  local line="$2"
  local key="${line%%=*}"
  if ! grep -q "^${key}=" "${file}"; then
    printf '\n%s\n' "${line}" >> "${file}"
  fi
}

ensure_file "${ROOT_DIR}/configs/config.example.env" "${ROOT_DIR}/.env"
ensure_file "${FETCH_DATA_DIR}/configs/config.example.env" "${FETCH_DATA_DIR}/.env"
ensure_file "${PROCESS_DIR}/configs/config.example.env" "${PROCESS_DIR}/.env"

for env_file in "${ROOT_DIR}/.env" "${FETCH_DATA_DIR}/.env" "${PROCESS_DIR}/.env"; do
  ensure_env_line "${env_file}" "LIVE_AUTO_START=true"
  ensure_env_line "${env_file}" "LIVE_LOOP_INTERVAL_SECONDS=60"
  ensure_env_line "${env_file}" "LIVE_RUN_ONCE_PUBLISH_PIPELINE_EVENTS=false"
done

set -a
# shellcheck disable=SC1091
source "${ROOT_DIR}/.env"
set +a

mkdir -p "${TMP_DIR}"
touch "${TMP_DIR}/fetch-data.log" "${TMP_DIR}/process.log"

echo "Starting local infrastructure"
"${DOCKER_BIN}" compose -p "${COMPOSE_PROJECT}" -f "${ROOT_DIR}/docker-compose.yml" up -d postgres redis kafka kafka-ui

echo "Waiting for PostgreSQL and Kafka"
until "${DOCKER_BIN}" compose -p "${COMPOSE_PROJECT}" -f "${ROOT_DIR}/docker-compose.yml" exec -T postgres pg_isready -U postgres -d polymarket_bot >/dev/null 2>&1; do
  sleep 1
done
until "${DOCKER_BIN}" compose -p "${COMPOSE_PROJECT}" -f "${ROOT_DIR}/docker-compose.yml" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do
  sleep 1
done

echo "Running migrations"
"${DOCKER_BIN}" compose -p "${COMPOSE_PROJECT}" -f "${ROOT_DIR}/docker-compose.yml" exec -T postgres psql -U postgres -d polymarket_bot < "${PROCESS_DIR}/migrations/001_init.sql" >/dev/null

echo "Starting polymarket-fetch-data-service and polymarket-process-service"
echo "Kafka UI: http://localhost:8081"
echo "Process API: http://localhost:8080"
echo "Dashboard SSE: curl -N http://localhost:8080/api/v1/dashboard/stream"
echo "Logs:"
echo "  ${TMP_DIR}/fetch-data.log"
echo "  ${TMP_DIR}/process.log"

(
  cd "${FETCH_DATA_DIR}"
  exec "${GO_BIN}" run ./cmd/app
) > "${TMP_DIR}/fetch-data.log" 2>&1 &
FETCH_PID="$!"

(
  cd "${PROCESS_DIR}"
  exec "${GO_BIN}" run ./cmd/app
) > "${TMP_DIR}/process.log" 2>&1 &
PROCESS_PID="$!"

terminate_tree() {
  local pid="$1"
  local child
  for child in $(pgrep -P "${pid}" 2>/dev/null || true); do
    terminate_tree "${child}"
  done
  kill "${pid}" 2>/dev/null || true
}

cleanup() {
  if [ "${CLEANED_UP:-0}" = "1" ]; then
    return
  fi
  CLEANED_UP=1
  echo
  echo "Stopping local Go services"
  if [ -n "${FETCH_PID:-}" ]; then
    terminate_tree "${FETCH_PID}"
    wait "${FETCH_PID}" 2>/dev/null || true
  fi
  if [ -n "${PROCESS_PID:-}" ]; then
    terminate_tree "${PROCESS_PID}"
    wait "${PROCESS_PID}" 2>/dev/null || true
  fi
  if [ -n "${TAIL_PID:-}" ]; then
    kill "${TAIL_PID}" 2>/dev/null || true
  fi
}

handle_signal() {
  cleanup
  exit 0
}

trap handle_signal INT TERM
trap cleanup EXIT

tail -f "${TMP_DIR}/fetch-data.log" "${TMP_DIR}/process.log" &
TAIL_PID="$!"

STATUS=0
while true; do
  if ! kill -0 "${FETCH_PID}" 2>/dev/null; then
    wait "${FETCH_PID}" || STATUS="$?"
    break
  fi
  if ! kill -0 "${PROCESS_PID}" 2>/dev/null; then
    wait "${PROCESS_PID}" || STATUS="$?"
    break
  fi
  sleep 1
done
exit "${STATUS}"
