# Architecture

FailSafe is a single-process async reverse proxy. Every client request passes
through four stages, each backed by a small, independently tested module.

```
client ──> route match ──> token bucket ──> forward (retry + failover) ──> upstream replica
                              │                 │
                              429               ├── breaker per replica
                                                ├── concurrency limit per replica (503 when all full)
                                                ├── hedge after p95 on another replica
                                                ├── deadline budget (504 when exhausted)
                                                ├── canary subset first, stable fallback
                                                ├── outlier-ejected replicas skipped
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

## Adaptive concurrency limits (`concurrency.py`)

Each replica has an `AdaptiveLimiter` that bounds how many calls may be in
flight to it. The forwarder acquires a slot when the pool picks the replica
and releases it with the attempt's outcome and round-trip time. The bound is
tuned with additive increase / multiplicative decrease:

* the smallest RTT seen is the no-load estimate; every `probe_interval`
  samples it is reset to the current sample so a service that became slower
  for good is not penalised forever;
* every `window` successful samples are judged on their average: above
  `no_load_rtt * rtt_tolerance` the limit is cut to `floor(limit * backoff_ratio)`,
  otherwise, if the window ran at least half the limit in flight, the limit
  grows by one. Judging the average rather than each call keeps ordinary
  jitter from shrinking the limit;
* a timeout, reset or retryable 5xx cuts the limit immediately; a refused
  connection says nothing about load and only frees the slot.

`pick()` skips replicas without a free slot before it consults the breaker, so
a replica that is slow but alive stops receiving new work while its siblings
carry it. Only when every healthy replica is full does the gateway answer
`503` with `Retry-After` set to one no-load RTT (at least a second once
rounded); that outcome is counted in `failsafe_load_shed_total`, not in
`failsafe_client_failed_requests_total`, because the request was never
attempted. The limit and in-flight count are exported per replica.

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

## Hedged requests (`hedge.py`, `proxy.py`)

Tail latency is usually one slow replica, not a slow service. For idempotent
requests the forwarder runs the first attempt as a task and waits for the
route's hedge delay; if the attempt is still running it picks another replica
(one with a free concurrency slot that has not been tried) and starts a second
attempt. Whichever returns a non-retryable response first wins, the other
task is cancelled, and the cancelled attempt only frees its limiter slot: its
outcome is unknown, so the breaker is not told anything. If both fail the
request falls into the ordinary retry loop with two attempts spent. At most
one hedge is fired per request.

The delay comes from a `LatencyTracker` per route, a sliding window of
successful attempt latencies with a cached nearest-rank percentile. Until
`min_samples` have been seen no hedge fires; a percentile that is not below
the attempt timeout also disables hedging, since the hedge would never start
before the first attempt gave up. `hedge.after_ms` replaces the percentile
with a fixed delay.

## Deadlines (`proxy.py`)

`X-Request-Timeout` (seconds) or `X-Request-Deadline` (epoch seconds) from
the client, and `deadline_seconds` from the route, are combined into one
monotonic deadline per request; the tightest wins and a malformed header is a
`400`. Each attempt's total timeout is clamped to the remaining budget and
the upstream receives both headers rewritten with what is left, so a chain of
services shares one budget instead of multiplying timeouts. Before a retry the
forwarder checks that the backoff would end before the deadline; otherwise it
stops and answers `504 deadline exceeded`, which is counted in
`failsafe_deadline_exceeded_total` and in the client-failed counter.

## Canary routing (`proxy.py`, `upstreams.py`)

The forwarder decides once per request whether it is canary traffic: the
configured header wins when present (`1`/`true`/`yes` or `0`/`false`/`no`),
otherwise a draw against `canary.weight`. The decision becomes a preferred
subset of replica URLs handed to `pool.pick(prefer=...)`: canary requests
prefer the canary replicas, stable requests prefer everything else, and in
both cases the other side is used only when no preferred replica is healthy,
admitted by its breaker and under its concurrency limit. Retries and hedges
keep the same preference. A response served by a canary replica carries
`x-failsafe-canary: 1` so a client or a dashboard can split by cohort.

## Outlier ejection (`upstreams.py`)

Each replica keeps a window of its last `outlier.window` outcomes: success or
error (non-connect failures and retryable statuses) with the attempt latency.
After every health-check round the pool first readmits replicas whose
cool-down expired, then judges every non-ejected replica that has at least
`min_requests` samples, provided at least `min_replicas` of them exist. A
replica is ejected for errors when its error rate reaches `error_ratio` and
`error_factor` times the median error rate of its peers, or for latency when
its mean latency is above `min_latency_seconds` and `latency_factor` times
the peers' median. Comparing against peers is what separates a bad replica
from a bad day: when every replica fails alike, nobody is ejected and the
breakers handle it. Ejection lasts `base_ejection_seconds` times the
replica's ejection count, capped at `max_ejection_seconds`, and the pool
never ejects more than `max_ejection_ratio` of its replicas, worst first.
An ejected replica stays healthy and keeps its breaker; it simply is not
picked. `eject` and `readmit` are also callable directly for manual control.

## Upstream pool and health checks (`upstreams.py`)

The pool keeps one `Replica` per address with a health flag, hysteresis
counters and its breaker. Selection is round-robin over available replicas.
`HealthChecker` runs one probe round before the gateway reports ready and then
every `interval_seconds`, hitting each replica's health path; a replica
becomes unhealthy after `unhealthy_threshold` failures and healthy again
after `healthy_threshold` successes.

Replica discovery runs at the start of every round. With `kubernetes:`
configured the pool lists the Service's EndpointSlices through the API server
(service account token, namespaced read-only Role) and keeps exactly the
addresses whose `ready` condition is true. This reflects a replaced pod within
about a second. `dns:` is also supported and resolves a headless Service name,
but cluster DNS caches those answers (kubeadm and kind ship CoreDNS with a 30
second TTL), which is too slow when pods are being replaced under load; the
first kind chaos run with DNS discovery showed the gateway lagging 20 to 30
seconds behind the real pod set. Newly discovered replicas start healthy
because the Service already vouched for them; the passive connection-failure
path corrects that within a single request if an entry is stale.

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
