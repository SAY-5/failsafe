#!/usr/bin/env bash
# Run the compose stack, kill upstream containers repeatedly under load,
# and fail if any client-visible request failed.
set -euo pipefail
cd "$(dirname "$0")/.."

DURATION=${DURATION:-45}
RPS=${RPS:-150}
KILL_INTERVAL=${KILL_INTERVAL:-8}
RESTART_AFTER=${RESTART_AFTER:-3}
CONCURRENCY=${CONCURRENCY:-64}
PY=${PY:-.venv/bin/python}
COMPOSE="docker compose -f deploy/docker-compose.yml"
OUT=chaos/out
mkdir -p "$OUT"

cleanup() {
  kill "${KILLER_PID:-}" 2>/dev/null || true
  echo "heartbeat: collecting gateway logs and tearing down"
  $COMPOSE logs gateway > "$OUT/gateway.log" 2>&1 || true
  $COMPOSE down -t 2 >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "heartbeat: building image and starting gateway + 3 upstream replicas"
$COMPOSE up -d --build --wait --wait-timeout 180 gateway upstream-1 upstream-2 upstream-3

echo "heartbeat: waiting for gateway readiness"
for _ in $(seq 1 60); do
  if curl -fsS http://localhost:8080/readyz >/dev/null 2>&1; then break; fi
  sleep 1
done
curl -fsS http://localhost:8080/readyz; echo

# Let every replica pass a health check before the chaos starts.
sleep 2

chaos/kill.sh compose "$DURATION" "$KILL_INTERVAL" "$RESTART_AFTER" "$OUT/kills.log" &
KILLER_PID=$!

"$PY" -m chaos.run \
  --target http://localhost:8080 \
  --rps "$RPS" --duration "$DURATION" --concurrency "$CONCURRENCY" \
  --metrics-url http://localhost:8080/metrics \
  --kill-log "$OUT/kills.log" --json "$OUT/summary.json" --fail-on-errors | tee "$OUT/summary.txt"

wait "$KILLER_PID" 2>/dev/null || true
