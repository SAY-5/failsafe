#!/usr/bin/env bash
# Kill a random upstream replica repeatedly for DURATION seconds.
#   chaos/kill.sh compose DURATION [INTERVAL] [RESTART_AFTER] [LOG]
#   chaos/kill.sh k8s     DURATION [INTERVAL] [unused]        [LOG]
# compose mode SIGKILLs a container and starts it again after RESTART_AFTER seconds.
# k8s mode force-deletes a pod, then waits for the Deployment to be fully ready again.
set -euo pipefail

MODE=${1:?compose|k8s}
DURATION=${2:?seconds}
INTERVAL=${3:-8}
RESTART_AFTER=${4:-3}
LOG=${5:-chaos/out/kills.log}
COMPOSE_FILE=${COMPOSE_FILE:-deploy/docker-compose.yml}
NAMESPACE=${NAMESPACE:-failsafe}

mkdir -p "$(dirname "$LOG")"
: > "$LOG"

stamp() { date -u +%H:%M:%S; }
log() { printf '%s\t%s\t%s\n' "$(stamp)" "$1" "$2" | tee -a "$LOG"; }

end=$((SECONDS + DURATION))
# First kill lands early so the run exercises failover from the start.
sleep 3
while [ $SECONDS -lt $end ]; do
  case "$MODE" in
    compose)
      n=$((RANDOM % 3 + 1))
      target="upstream-$n"
      docker compose -f "$COMPOSE_FILE" kill "$target" >/dev/null 2>&1 || true
      log kill "$target"
      sleep "$RESTART_AFTER"
      docker compose -f "$COMPOSE_FILE" start "$target" >/dev/null 2>&1 || true
      log start "$target"
      ;;
    k8s)
      pods=($(kubectl -n "$NAMESPACE" get pods -l app=upstream \
        --field-selector=status.phase=Running -o jsonpath='{.items[*].metadata.name}'))
      if [ ${#pods[@]} -eq 0 ]; then sleep 1; continue; fi
      target=${pods[$((RANDOM % ${#pods[@]}))]}
      kubectl -n "$NAMESPACE" delete pod "$target" --grace-period=0 --force --wait=false \
        >/dev/null 2>&1 || true
      log kill "$target"
      # One pod at a time: wait until the Deployment replaced it before the next kill,
      # otherwise the experiment measures total outage rather than failover.
      for _ in $(seq 1 60); do
        want=$(kubectl -n "$NAMESPACE" get deploy upstream -o jsonpath='{.spec.replicas}')
        have=$(kubectl -n "$NAMESPACE" get deploy upstream -o jsonpath='{.status.readyReplicas}')
        [ "${have:-0}" = "$want" ] && break
        sleep 1
      done
      ;;
    *) echo "unknown mode $MODE" >&2; exit 2 ;;
  esac
  jitter=$((RANDOM % 5 - 2))
  wait_for=$((INTERVAL + jitter))
  [ $wait_for -lt 2 ] && wait_for=2
  sleep "$wait_for"
done
