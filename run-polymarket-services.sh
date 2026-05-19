#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FETCH_DATA_DIR="${ROOT_DIR}/polymarket-fetch-data-service"
PROCESS_DIR="${ROOT_DIR}/polymarket-process-service"
TMP_DIR="${ROOT_DIR}/tmp"
GO_BIN="${GO:-$(command -v go 2>/dev/null || echo /opt/homebrew/bin/go)}"

mkdir -p "${TMP_DIR}"
if [ -f "${ROOT_DIR}/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/.env"
  set +a
fi

echo "Starting polymarket-fetch-data-service and polymarket-process-service"
echo "Kafka: ${KAFKA_BROKERS:-localhost:29092}"
echo "Logs:"
echo "  ${TMP_DIR}/fetch-data.log"
echo "  ${TMP_DIR}/process.log"

(
  cd "${FETCH_DATA_DIR}"
  "${GO_BIN}" run ./cmd/app
) > "${TMP_DIR}/fetch-data.log" 2>&1 &
FETCH_PID="$!"

(
  cd "${PROCESS_DIR}"
  "${GO_BIN}" run ./cmd/app
) > "${TMP_DIR}/process.log" 2>&1 &
PROCESS_PID="$!"

cleanup() {
  kill "${FETCH_PID}" "${PROCESS_PID}" 2>/dev/null || true
}
trap cleanup INT TERM EXIT

tail -f "${TMP_DIR}/fetch-data.log" "${TMP_DIR}/process.log" &
TAIL_PID="$!"

wait -n "${FETCH_PID}" "${PROCESS_PID}"
STATUS="$?"
kill "${TAIL_PID}" 2>/dev/null || true
exit "${STATUS}"
