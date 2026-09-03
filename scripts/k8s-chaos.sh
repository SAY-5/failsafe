#!/usr/bin/env bash
# Create a kind cluster, deploy the gateway and upstream, kill upstream pods
# under load and assert that no client-visible request failed.
set -euo pipefail
cd "$(dirname "$0")/.."

CLUSTER=${CLUSTER:-failsafe}
NS=failsafe
DURATION=${DURATION:-45}
RPS=${RPS:-150}
KILL_INTERVAL=${KILL_INTERVAL:-8}
CONCURRENCY=${CONCURRENCY:-64}
KEEP_CLUSTER=${KEEP_CLUSTER:-0}
SKIP_BUILD=${SKIP_BUILD:-0}
OUT=chaos/out
mkdir -p "$OUT"

for tool in kind kubectl docker; do
  command -v "$tool" >/dev/null || { echo "missing $tool" >&2; exit 2; }
done

cleanup() {
  kill "${KILLER_PID:-}" 2>/dev/null || true
  kubectl -n "$NS" logs deploy/gateway --all-containers --prefix > "$OUT/k8s-gateway.log" 2>&1 || true
  if [ "$KEEP_CLUSTER" != "1" ]; then
    echo "heartbeat: deleting kind cluster $CLUSTER"
    kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  echo "heartbeat: creating kind cluster $CLUSTER"
  kind create cluster --name "$CLUSTER" --wait 120s
fi
kubectl config use-context "kind-$CLUSTER" >/dev/null

if [ "$SKIP_BUILD" != "1" ]; then
  echo "heartbeat: building failsafe:dev"
  docker buildx build --load -t failsafe:dev . >/dev/null
fi
echo "heartbeat: loading image into kind"
kind load docker-image failsafe:dev --name "$CLUSTER"

echo "heartbeat: applying manifests"
kubectl apply -k deploy/k8s
kubectl -n "$NS" rollout status deploy/upstream --timeout=180s
kubectl -n "$NS" rollout status deploy/gateway --timeout=180s
kubectl -n "$NS" get pods -o wide

# Give the gateways one health-check round so every replica is marked healthy.
sleep 3
for pod in $(kubectl -n "$NS" get pods -l app=gateway -o jsonpath='{.items[*].metadata.name}'); do
  kubectl -n "$NS" exec "$pod" -- python -c \
    "import urllib.request; print('$pod', urllib.request.urlopen('http://127.0.0.1:8080/readyz').read().decode())"
done

METRICS_ARGS=()
for ip in $(kubectl -n "$NS" get pods -l app=gateway -o jsonpath='{.items[*].status.podIP}'); do
  METRICS_ARGS+=(--metrics-url "http://$ip:8080/metrics")
done

kubectl -n "$NS" delete pod loadgen --ignore-not-found --wait=true >/dev/null
echo "heartbeat: starting load generator pod ($RPS rps for ${DURATION}s)"
kubectl -n "$NS" run loadgen --image=failsafe:dev --image-pull-policy=Never --restart=Never \
  --command -- python -m chaos.run --target http://gateway:8080 \
  --rps "$RPS" --duration "$DURATION" --concurrency "$CONCURRENCY" \
  "${METRICS_ARGS[@]}" --fail-on-errors
kubectl -n "$NS" wait --for=jsonpath='{.status.phase}'=Running pod/loadgen --timeout=60s

NAMESPACE=$NS chaos/kill.sh k8s "$DURATION" "$KILL_INTERVAL" 0 "$OUT/k8s-kills.log" &
KILLER_PID=$!

echo "heartbeat: waiting for the load generator to finish"
for _ in $(seq 1 $((DURATION + 90))); do
  phase=$(kubectl -n "$NS" get pod loadgen -o jsonpath='{.status.phase}')
  [ "$phase" = "Succeeded" ] || [ "$phase" = "Failed" ] && break
  sleep 1
done
wait "$KILLER_PID" 2>/dev/null || true

kubectl -n "$NS" logs loadgen | tee "$OUT/k8s-summary.txt"
echo "kill timeline (UTC):"
sed 's/^/  /' "$OUT/k8s-kills.log"
kubectl -n "$NS" get pods -l app=upstream

result=$(grep '^CHAOS_RESULT ' "$OUT/k8s-summary.txt" | tail -1 | sed 's/^CHAOS_RESULT //')
failed=$(printf '%s' "$result" | python3 -c 'import json,sys; print(json.load(sys.stdin)["client_failed_requests"])')
kills=$(grep -c $'\tkill\t' "$OUT/k8s-kills.log" || true)
echo "k8s chaos: pods killed=$kills client-visible failed requests=$failed"
if [ "$failed" != "0" ] || [ "$phase" != "Succeeded" ]; then
  echo "FAIL: client-visible failures during pod kills" >&2
  exit 1
fi
echo "PASS: zero client-visible failures across $kills pod kills"
