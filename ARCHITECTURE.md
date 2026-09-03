# Architecture

FailSafe is a single-process async reverse proxy. Every client request passes
through four stages, each backed by a small, independently tested module.

```
client ──> route match ──> token bucket ──> forward (retry + failover) ──> upstream replica
                              │                 │
                              429               ├── breaker per replica
                                                ├── health state per replica
                                                └── Prometheus counters
```

## Routing and configuration

`routes.yaml` declares upstreams (a static replica list, or a DNS name for a
Kubernetes headless Service) and routes (a path prefix mapped to an upstream
with a timeout, rate limit, retry policy and breaker policy). The loader in
`failsafe/config.py` builds frozen dataclasses and validates every value at
startup so a bad file fails fast instead of at request time. Longest prefix
wins when several routes match.

## Token bucket rate limiting (`ratelimit.py`)

Each client key (the `X-API-Key` header, otherwise the client IP) gets a
bucket with `capacity` tokens refilled continuously at `refill_per_second`.
Refill is computed lazily from a monotonic clock:

```
tokens = min(capacity, tokens + (now - last) * rate)
```

so there is no timer per bucket and the math is exact regardless of how often
the bucket is touched. A request costs one token. When the bucket is empty the
gateway returns `429` with `Retry-After` set to the ceiling of
`deficit / rate` seconds, which is the earliest moment a retry can succeed.
Buckets are protected by a lock (never held across an await) so the limiter is
correct under threads and under asyncio. Idle keys are evicted so an attacker
spraying random keys cannot grow memory without bound.

## Circuit breaker (`breaker.py`)

One breaker per upstream replica, three states:

* **closed**: calls flow; outcomes are recorded in a sliding window of the
  last `window` results and in a consecutive-failure counter.
* **open**: entered when `consecutive_failures` failures happen in a row, or
  when the window holds at least `min_requests` outcomes and the failure ratio
  reaches `failure_ratio`. Every call is refused for `open_seconds`.
* **half-open**: entered automatically after `open_seconds`. At most
  `half_open_max` probe calls are admitted. One probe failure reopens the
  breaker and restarts the timer; when all probes succeed the breaker closes
  and its window is cleared.

The state transition hook feeds `failsafe_breaker_state` and
`failsafe_breaker_transitions_total`. Because the breaker is per replica, an
open breaker simply removes that replica from rotation; the route stays up on
the remaining replicas.

## Retries and failover (`retry.py`, `proxy.py`)

An attempt can fail in four ways and each is classified:

| kind      | meaning                                         | retried for non-idempotent requests |
|-----------|-------------------------------------------------|-------------------------------------|
| connect   | connection refused, DNS failure, connect timeout | yes: no bytes reached the upstream  |
| timeout   | read timeout after the request was sent          | no                                  |
| read      | connection dropped mid-response                  | no                                  |
| status    | upstream answered 500/502/503/504                | no                                  |

GET, HEAD, PUT, DELETE and OPTIONS are idempotent by definition. POST and
PATCH are treated as idempotent only when the route sets
`retry.idempotent_post: true` or the client sends an `Idempotency-Key` header,
in which case the upstream is expected to deduplicate (the example upstream
does). Attempts are bounded by `max_attempts`; each retry sleeps for a
full-jitter exponential backoff `uniform(0, min(max_delay, base * 2^(n-1)))`.

Failover is what turns a retry into a useful retry. The forwarder asks the
pool for a replica it has not tried yet on every attempt, and the pool skips
replicas that are unhealthy or whose breaker is open. A connection failure also
marks the replica unhealthy immediately, so at most the requests already in
flight to a dead replica ever observe the failure, and each of them is moved to
a live replica within one backoff interval. That is why a pod kill is
invisible: the client never sees the connection error, only a slightly slower
response.

If every replica has been tried and attempts remain, the pool is consulted
again without exclusions so a single-replica upstream still gets its retries.
When nothing is available the gateway returns `503` and increments
`failsafe_client_failed_requests_total`, the counter the chaos runs assert on.

## Upstream pool and health checks (`upstreams.py`)

The pool keeps one `Replica` per address with a health flag, hysteresis
counters and its breaker. Selection is round-robin over available replicas.
`HealthChecker` runs one probe round before the gateway reports ready and then
every `interval_seconds`, hitting each replica's health path; a replica
becomes unhealthy after `unhealthy_threshold` failures and healthy again
after `healthy_threshold` successes.

With `dns:` configured the pool resolves the name on every round. Kubernetes
headless Services publish one A record per ready pod, so new pods are
discovered as soon as their readiness probe passes and deleted pods vanish
from the set. Newly discovered replicas start healthy because the Service
already vouched for them; the passive connection-failure path corrects that
within a single request if the record is stale.

## Kubernetes deployment

* Gateway: 2 replicas, `RollingUpdate` with `maxUnavailable: 0`, liveness on
  `/healthz` (process alive) and readiness on `/readyz` (at least one healthy
  upstream replica). A gateway with no reachable upstream is taken out of the
  Service instead of returning errors.
* Upstream: 3 replicas behind a headless Service with readiness and liveness
  probes on `/health`, so DNS only ever lists pods that can serve.
* Containers run as a non-root user with all capabilities dropped and a
  read-only root filesystem for the gateway.

## Chaos method

`chaos/run.py` drives a fixed request rate through the gateway with a mix of
GET and idempotent POST requests and records every client-visible outcome:
2xx, 429, other statuses and transport exceptions. It scrapes the gateway's
`/metrics` before and after to report retries, failovers and breaker
transitions, and prints a summary with p50/p95/p99 latency.

`chaos/kill.sh` runs alongside it. In compose mode it SIGKILLs a random
upstream container every few seconds and restarts it shortly after; in
Kubernetes mode it force-deletes a random upstream pod and lets the Deployment
replace it. `make chaos` and `scripts/k8s-chaos.sh` wire the two together and
exit non-zero if `client_failed_requests` is anything but zero. Both run in CI
on every push.
