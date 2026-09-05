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
* **Adaptive concurrency limits** per replica (AIMD driven by observed
  latency) that steer requests away from a saturated replica and shed with
  `503` + `Retry-After` only when every replica is full.
* **Hedged reads**: an idempotent request that is slower than the route's
  observed p95 gets a second attempt on another replica; the first answer
  wins and the loser is cancelled.
* **End-to-end deadlines**: `X-Request-Timeout` / `X-Request-Deadline` (or a
  route default) bound the whole request, are propagated to upstreams with
  the remaining budget, and stop retries that could not finish in time.
* **Canary routing**: a weighted share of a route's traffic goes to a named
  subset of replicas, a header can force either side, and canary traffic
  falls back to the stable replicas when the canary cannot take it.
* **Outlier ejection**: a replica whose error rate or latency stands out from
  its peers is taken out of rotation for an escalating cool-down, while a
  service-wide failure ejects nobody.
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
make lint test  # ruff + 101 tests, including an in-process failover test
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

Result on a fresh kind cluster (Docker Desktop VM with 2 CPUs, so the run used
`RPS=60`; the pod kills are the point, not the throughput):

```
================================================================
FailSafe chaos summary
================================================================
target                        http://gateway:8080
duration / target rps         45.0s / 60.0 rps (achieved 60.0 rps)
total requests                2701
successful (2xx)              2701
rate limited (429)            0
client-visible failed         0   <- must be 0 (gateway counter: 0)
retries (gateway)             4  {'connect': 4}
failovers (gateway)           4
breaker transitions           0  {}
latency ms p50 / p95 / p99    3.3 / 8.5 / 14.4  (max 38.9)
================================================================
kill timeline (UTC):
  06:44:17	kill	upstream-77d888f65c-h2trr
  06:44:29	kill	upstream-77d888f65c-5f9js
  06:44:43	kill	upstream-77d888f65c-88q6f
  06:44:57	kill	upstream-77d888f65c-rczvw
k8s chaos: pods killed=4 client-visible failed requests=0
PASS: zero client-visible failures across 4 pod kills
```

Each kill produced exactly one connect-phase retry: the request in flight to
the deleted pod moved to a live replica and the client got its 200. Replicas
are discovered from the EndpointSlice API, so the replacement pod entered
rotation as soon as its readiness probe passed; the first run of this script
used headless DNS and showed why that is not good enough (see
`ARCHITECTURE.md`).

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
    concurrency:               # omit to disable adaptive per-replica limits
      initial: 32              # in-flight slots each replica starts with
      min_limit: 4
      max_limit: 512
      backoff_ratio: 0.9       # multiplicative decrease on a drop or latency spike
      rtt_tolerance: 2.5       # spike = window average above no-load RTT x tolerance
      window: 10               # samples per adjustment
      probe_interval: 200      # samples between no-load RTT re-estimates
    hedge:                     # omit to disable hedged reads
      percentile: 95           # fire a second attempt once the first exceeds p95
      min_samples: 50          # samples needed before the percentile is trusted
      # after_ms: 25           # fixed delay instead of the percentile
    # deadline_seconds: 5.0    # default end-to-end budget per request
    canary:                    # omit to spread traffic evenly
      replicas: [http://upstream-3:9000]       # URLs or host:port labels
      weight: 0.1              # share of requests sent to the canary subset
      header: X-Canary         # 1/true forces canary, 0/false forces stable
    outlier:                   # omit to disable outlier ejection
      window: 100              # recent outcomes kept per replica
      min_requests: 20         # samples a replica needs before it is judged
      error_ratio: 0.2         # error rate needed, and 3x the peers' median
      latency_factor: 3.0      # mean latency above 3x the peers' median (and 50 ms)
      max_ejection_ratio: 0.5  # never eject more than half the pool
      base_ejection_seconds: 30
      max_ejection_seconds: 300
```

Retry rules: GET, HEAD, PUT, DELETE and OPTIONS are retried on connection
errors, timeouts, dropped connections and the listed statuses. POST and PATCH
are only retried after connection errors (nothing reached the upstream),
unless the route sets `idempotent_post` or the request carries an
`Idempotency-Key` header. The same idempotency rule decides whether a request
may be hedged.

Deadlines: a client sends `X-Request-Timeout: 1.5` (seconds of budget) or
`X-Request-Deadline: 1725300000.250` (Unix epoch seconds); the tighter of the
two and the route's `deadline_seconds` wins. Every attempt's timeout is
clamped to the remaining budget, both headers are rewritten with the
remaining budget before the request reaches the upstream, and a retry whose
backoff would end past the deadline is not started: the client gets `504`
with `deadline exceeded`, counted in `failsafe_deadline_exceeded_total`.

## Endpoints

| path       | purpose                                                       |
|------------|---------------------------------------------------------------|
| `/*`       | proxied according to `routes`                                 |
| `/healthz` | liveness: the process is serving                              |
| `/readyz`  | readiness: at least one upstream replica is healthy (503 otherwise) |
| `/metrics` | Prometheus exposition                                         |

Proxied responses carry `X-Failsafe-Upstream` with the replica that served
them; rate-limited responses carry `Retry-After` and `X-RateLimit-Limit`.
When every replica is at its concurrency limit the gateway answers `503`
with `{"error": "overloaded"}` and a `Retry-After` derived from the replicas'
no-load latency; a replica that is merely slow is skipped, not shed.

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
| `failsafe_concurrency_limit` | upstream | adaptive in-flight limit currently granted to the replica |
| `failsafe_concurrency_inflight` | upstream | calls in flight to the replica as counted by its limiter |
| `failsafe_load_shed_total` | route | requests answered 503 because every replica was at its limit |
| `failsafe_hedges_total` | route | hedge attempts fired |
| `failsafe_hedge_wins_total` | route | requests answered by the hedge rather than the first attempt |
| `failsafe_hedge_delay_seconds` | route | current hedge delay (observed latency percentile) |
| `failsafe_deadline_exceeded_total` | route | requests answered 504 because the deadline ran out |
| `failsafe_canary_requests_total` | route, canary | requests routed to the canary subset (`true`) or the stable replicas |
| `failsafe_outlier_ejections_total` | upstream, reason | replicas ejected from rotation (`errors`, `latency`, `manual`) |
| `failsafe_upstream_ejected` | upstream | 1 while the replica is ejected |

The Grafana dashboard in `monitoring/grafana/dashboards/failsafe.json` shows
RPS by status, latency percentiles, rate limiting, retries and failovers,
breaker state, upstream health and the client-failed counter.

## Layout

```
failsafe/           gateway package (config, ratelimit, breaker, concurrency, hedge,
                    retry, upstreams, proxy, app)
example_upstream/   orders service with env and runtime failure injection
chaos/              load generator (run.py) and kill script (kill.sh)
scripts/            compose-chaos.sh, k8s-chaos.sh
deploy/             docker-compose.yml, k8s/ (kustomize)
monitoring/         prometheus.yml, grafana provisioning and dashboard
tests/              pytest suite
```

## Releases

### v4.0.0: canary routing and outlier ejection

A route can name a canary subset of replicas (`canary.replicas`, by URL or
host:port label) and a `weight`; that share of requests is served by the
subset, everything else avoids it, and either side can be forced with the
configured header. Canary traffic that finds no canary replica available is
served by the stable ones, so a dead canary costs nothing. Every replica now
keeps a window of recent outcomes; on each health-check round a replica whose
error rate is at least `error_ratio` and three times its peers' median, or
whose mean latency is three times its peers' median (and above a floor), is
ejected for `base_ejection_seconds` times its ejection count, capped, and
never beyond `max_ejection_ratio` of the pool. Responses served by a canary
carry `x-failsafe-canary: 1`. New metrics: `failsafe_canary_requests_total`,
`failsafe_outlier_ejections_total`, `failsafe_upstream_ejected`. 13 new tests
(101 total).

### v3.0.0: request hedging and deadline propagation

Idempotent requests are hedged: once the first attempt has run longer than
the route's observed latency percentile (`hedge.percentile`, or a fixed
`hedge.after_ms`), a second attempt starts on another replica with a free
slot, the first successful answer is relayed and the loser is cancelled
without touching its breaker. Clients can bound a request end to end with
`X-Request-Timeout` or `X-Request-Deadline`; routes can set a default
`deadline_seconds`. The budget clamps every attempt timeout, is forwarded to
upstreams as both headers, and stops retries that would finish late. New
metrics: `failsafe_hedges_total`, `failsafe_hedge_wins_total`,
`failsafe_hedge_delay_seconds`, `failsafe_deadline_exceeded_total`. 13 new
tests (88 total).

### v2.0.0: adaptive concurrency limits

Every replica now carries an adaptive in-flight limit (`failsafe/concurrency.py`).
The limit grows by one per window of calls that complete within the replica's
no-load latency while the replica is at least half busy, and is cut by
`backoff_ratio` on a timeout, reset, 5xx or a window whose average latency
exceeds `rtt_tolerance` times the no-load RTT. The pool skips replicas that are
at their limit, so a request is only shed (`503`, `Retry-After`) when no
healthy replica has a free slot. New metrics: `failsafe_concurrency_limit`,
`failsafe_concurrency_inflight`, `failsafe_load_shed_total`; the chaos summary
prints the shed count. 13 new tests (75 total).

### v1.0.0: initial release

Token-bucket rate limiting, per-replica circuit breakers, retries with
full-jitter backoff and replica failover, active health checks, EndpointSlice
discovery, Prometheus metrics with a Grafana dashboard, compose and kind chaos
suites. 62 tests.

## License

MIT
