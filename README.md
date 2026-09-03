# FailSafe

Resilient API gateway in Python. FailSafe sits in front of a set of upstream
replicas and keeps client requests succeeding while those replicas are being
rate limited, timing out, crashing or being killed outright.

* **Token-bucket rate limiting** per API key or client IP, with exact refill
  math and `Retry-After` on `429`.
* **Circuit breakers** per upstream replica: closed, open, half-open, with a
  failure-rate window and bounded probes.
* **Retries with exponential backoff and jitter** for idempotent requests, and
  **failover across replicas** so a dead pod is skipped instead of surfaced.
* **Active health checks** plus replica discovery from the Kubernetes
  EndpointSlice API (or headless DNS).
* **Prometheus metrics** for every decision the gateway makes, with a
  provisioned Grafana dashboard.
* **Kubernetes manifests** with liveness and readiness probes and zero
  unavailable pods during rollouts.
* **Chaos suite** that kills containers or pods under load and asserts that the
  number of client-visible failed requests is zero. It runs on every push.

Built with FastAPI, httpx, uvicorn and prometheus-client on Python 3.12.

## Architecture

```
                          +--------------------------------------------------+
                          |                  FailSafe gateway                |
   clients                |                                                  |
     |   GET /orders/42   |  route match -> token bucket -> forwarder        |
     +------------------->|                    |               |             |
     <-------------------+|                   429     retry + backoff        |
        200 (always)      |                            failover across       |
                          |                            healthy replicas      |
                          |                                |                 |
                          |   health checker ---+   breaker per replica      |
                          |   (active probes,   |          |                 |
                          |    DNS discovery)   v          v                 |
                          +--------------------[replica pool]----------------+
                                        |            |            |
                                   upstream-1   upstream-2   upstream-3
                                     /health      (killed)     /health
                          +--------------------------------------------------+
                          |  /metrics  -> Prometheus -> Grafana dashboard    |
                          |  /healthz  -> liveness    /readyz -> readiness   |
                          +--------------------------------------------------+
```

`ARCHITECTURE.md` explains each stage and why pod kills stay invisible.

## Quick start

```bash
make setup      # uv venv + dependencies
make lint test  # ruff + 60 tests, including an in-process failover test
                # (1200 requests while one replica is killed and another hangs)
```

Run the whole stack with three upstream replicas, Prometheus and Grafana:

```bash
docker compose -f deploy/docker-compose.yml --profile observability up --build
curl -s localhost:8080/orders/42 -H 'X-API-Key: demo'
curl -s localhost:8080/metrics | grep failsafe_
open http://localhost:3000/d/failsafe     # dashboard, anonymous admin
```

## Chaos demo: `make chaos`

`make chaos` (alias `make demo`) builds the image, starts the gateway with
three upstream replicas, drives 150 requests per second through the gateway
for 45 seconds and, while that runs, SIGKILLs a random upstream container
every few seconds and restarts it. The load generator counts every
client-visible outcome; the run fails if any request did not get a 2xx.

Summary block from a run on a laptop (Docker Desktop, single gateway process):

```
================================================================
FailSafe chaos summary
================================================================
target                        http://localhost:8080
duration / target rps         45.0s / 150.0 rps (achieved 150.0 rps)
total requests                6751
successful (2xx)              6751
rate limited (429)            0
client-visible failed         0   <- must be 0 (gateway counter: 0)
retries (gateway)             5  {'connect': 5}
failovers (gateway)           5
breaker transitions           0  {}
latency ms p50 / p95 / p99    3.2 / 5.1 / 7.8  (max 1012.1)
kills                         4
kill timeline (UTC):
  05:16:30	kill	upstream-2
  05:16:33	start	upstream-2
  05:16:44	kill	upstream-2
  05:16:47	start	upstream-2
  05:16:54	kill	upstream-2
  05:16:57	start	upstream-2
  05:17:03	kill	upstream-3
  05:17:06	start	upstream-3
================================================================
```

Five requests were in flight to a container at the moment it was killed;
each one was retried on another replica and the client saw a 200. The
gateway log shows the matching `-> unhealthy` / `-> healthy` transitions for
every kill. Tunables: `DURATION`, `RPS`, `KILL_INTERVAL`, `RESTART_AFTER`,
`CONCURRENCY` (environment variables), for example
`DURATION=120 KILL_INTERVAL=5 make chaos`.

## Kubernetes chaos: `make k8s-chaos`

`scripts/k8s-chaos.sh` creates a kind cluster, loads the image, applies
`deploy/k8s` (2 gateway pods with liveness and readiness probes, 3 upstream
pods behind a headless Service, `RollingUpdate` with `maxUnavailable: 0`),
runs the load generator as a pod inside the cluster and force-deletes a random
upstream pod at random intervals for the whole run. It exits non-zero unless
`client_failed_requests` is 0. Set `KEEP_CLUSTER=1` to inspect the cluster
afterwards and `SKIP_BUILD=1` to reuse an existing `failsafe:dev` image.

## Configuration

The gateway reads `routes.yaml` (path from `--config` or `$FAILSAFE_CONFIG`).
`${VAR}` references are expanded from the environment.

```yaml
health_check:
  interval_seconds: 1.0        # active probe period
  timeout_seconds: 0.5
  unhealthy_threshold: 1       # failed probes before a replica is pulled
  healthy_threshold: 1         # passed probes before it is restored

upstreams:
  orders:
    replicas: [http://upstream-1:9000, http://upstream-2:9000]   # static list
    # kubernetes:                # or the ready endpoints of a Service
    #   service: upstream
    #   namespace: failsafe      # defaults to the pod's own namespace
    # dns: upstream.failsafe.svc.cluster.local.   # or headless DNS (TTL-bound)
    # port: 9000
    health_path: /health

routes:
  - prefix: /orders            # longest prefix wins
    upstream: orders
    timeout_seconds: 2.0       # per attempt
    connect_timeout_seconds: 0.5
    strip_prefix: false
    rate_limit:                # omit or set to null to disable
      capacity: 200
      refill_per_second: 400
      key_header: X-API-Key    # falls back to client IP
    retry:
      max_attempts: 4
      base_delay_ms: 10
      max_delay_ms: 150
      idempotent_post: false   # true: treat POST as retryable on this route
      retry_on_status: [500, 502, 503, 504]
    breaker:
      window: 20
      failure_ratio: 0.5
      min_requests: 5
      consecutive_failures: 3
      open_seconds: 3.0
      half_open_max: 2
```

Retry rules: GET, HEAD, PUT, DELETE and OPTIONS are retried on connection
errors, timeouts, dropped connections and the listed statuses. POST and PATCH
are only retried after connection errors (nothing reached the upstream),
unless the route sets `idempotent_post` or the request carries an
`Idempotency-Key` header.

## Endpoints

| path       | purpose                                                       |
|------------|---------------------------------------------------------------|
| `/*`       | proxied according to `routes`                                 |
| `/healthz` | liveness: the process is serving                              |
| `/readyz`  | readiness: at least one upstream replica is healthy (503 otherwise) |
| `/metrics` | Prometheus exposition                                         |

Proxied responses carry `X-Failsafe-Upstream` with the replica that served
them; rate-limited responses carry `Retry-After` and `X-RateLimit-Limit`.

## Metrics

| metric | labels | meaning |
|--------|--------|---------|
| `failsafe_requests_total` | route, upstream, status | final status per request and the replica that produced it |
| `failsafe_request_latency_seconds` | route | end-to-end latency histogram including retries |
| `failsafe_rate_limited_total` | route | requests rejected with 429 |
| `failsafe_breaker_state` | upstream | 0 closed, 1 half-open, 2 open |
| `failsafe_breaker_transitions_total` | upstream, from_state, to_state | breaker state changes |
| `failsafe_retries_total` | route, reason | retried attempts by failure kind (connect, timeout, read, status) |
| `failsafe_failovers_total` | from_upstream, to_upstream | attempts moved to another replica |
| `failsafe_upstream_healthy` | upstream | 1 when the replica passes health checks |
| `failsafe_client_failed_requests_total` | route | 5xx returned to a client after exhausting retries; must stay 0 in chaos |
| `failsafe_inflight_requests` | route | requests currently being proxied |

The Grafana dashboard in `monitoring/grafana/dashboards/failsafe.json` shows
RPS by status, latency percentiles, rate limiting, retries and failovers,
breaker state, upstream health and the client-failed counter.

## Layout

```
failsafe/           gateway package (config, ratelimit, breaker, retry, upstreams, proxy, app)
example_upstream/   orders service with env and runtime failure injection
chaos/              load generator (run.py) and kill script (kill.sh)
scripts/            compose-chaos.sh, k8s-chaos.sh
deploy/             docker-compose.yml, k8s/ (kustomize)
monitoring/         prometheus.yml, grafana provisioning and dashboard
tests/              pytest suite
```

## License

MIT
