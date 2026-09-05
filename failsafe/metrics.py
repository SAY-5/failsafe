"""Prometheus metrics exported by the gateway."""

from __future__ import annotations

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

from failsafe.breaker import State

REQUESTS = Counter(
    "failsafe_requests_total",
    "Requests handled by the gateway, by route, upstream replica and final status.",
    ["route", "upstream", "status"],
)
LATENCY = Histogram(
    "failsafe_request_latency_seconds",
    "End-to-end latency of gateway requests including retries.",
    ["route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)
RATE_LIMITED = Counter(
    "failsafe_rate_limited_total",
    "Requests rejected with 429 by the token bucket.",
    ["route"],
)
BREAKER_STATE = Gauge(
    "failsafe_breaker_state",
    "Circuit breaker state per upstream replica: 0 closed, 1 half-open, 2 open.",
    ["upstream"],
)
BREAKER_TRANSITIONS = Counter(
    "failsafe_breaker_transitions_total",
    "Circuit breaker state transitions.",
    ["upstream", "from_state", "to_state"],
)
RETRIES = Counter(
    "failsafe_retries_total",
    "Retried attempts, by route and failure reason.",
    ["route", "reason"],
)
FAILOVERS = Counter(
    "failsafe_failovers_total",
    "Attempts moved from one replica to another after a failure.",
    ["from_upstream", "to_upstream"],
)
UPSTREAM_HEALTHY = Gauge(
    "failsafe_upstream_healthy",
    "1 when the replica passes health checks, 0 otherwise.",
    ["upstream"],
)
CLIENT_FAILED = Counter(
    "failsafe_client_failed_requests_total",
    "Requests for which the gateway returned a 5xx to the client "
    "after exhausting retries and replicas. Must stay 0 during chaos runs.",
    ["route"],
)
INFLIGHT = Gauge(
    "failsafe_inflight_requests",
    "Requests currently being proxied.",
    ["route"],
)
CONCURRENCY_LIMIT = Gauge(
    "failsafe_concurrency_limit",
    "Adaptive in-flight limit currently granted to the replica.",
    ["upstream"],
)
CONCURRENCY_INFLIGHT = Gauge(
    "failsafe_concurrency_inflight",
    "Calls currently in flight to the replica, as counted by its limiter.",
    ["upstream"],
)
HEDGES = Counter(
    "failsafe_hedges_total",
    "Hedge attempts fired because the first attempt exceeded the hedge delay.",
    ["route"],
)
HEDGE_WINS = Counter(
    "failsafe_hedge_wins_total",
    "Requests whose response came from the hedge attempt rather than the first one.",
    ["route"],
)
HEDGE_DELAY = Gauge(
    "failsafe_hedge_delay_seconds",
    "Delay after which a hedge is fired on the route (observed latency percentile).",
    ["route"],
)
DEADLINE_EXCEEDED = Counter(
    "failsafe_deadline_exceeded_total",
    "Requests answered 504 because the end-to-end deadline ran out before a retry could help.",
    ["route"],
)
OUTLIER_EJECTIONS = Counter(
    "failsafe_outlier_ejections_total",
    "Replicas ejected from rotation, by reason (errors, latency, manual).",
    ["upstream", "reason"],
)
UPSTREAM_EJECTED = Gauge(
    "failsafe_upstream_ejected",
    "1 while the replica is ejected from rotation, 0 otherwise.",
    ["upstream"],
)
CANARY_REQUESTS = Counter(
    "failsafe_canary_requests_total",
    "Requests routed to the canary subset (canary=true) or to the stable replicas.",
    ["route", "canary"],
)
UPSTREAM_DRAINING = Gauge(
    "failsafe_upstream_draining",
    "1 while an operator has drained the replica (no new requests), 0 otherwise.",
    ["upstream"],
)
ADMIN_ACTIONS = Counter(
    "failsafe_admin_actions_total",
    "Operator actions taken through the admin API.",
    ["action"],
)
LOAD_SHED = Counter(
    "failsafe_load_shed_total",
    "Requests answered 503 because every replica was at its concurrency limit.",
    ["route"],
)

_STATE_VALUE = {State.CLOSED: 0, State.HALF_OPEN: 1, State.OPEN: 2}


def set_breaker_state(upstream: str, state: State) -> None:
    BREAKER_STATE.labels(upstream=upstream).set(_STATE_VALUE[state])


def record_transition(upstream: str, old: State, new: State) -> None:
    BREAKER_TRANSITIONS.labels(upstream=upstream, from_state=old.value, to_state=new.value).inc()
    set_breaker_state(upstream, new)


def set_concurrency(upstream: str, limit: int, inflight: int) -> None:
    CONCURRENCY_LIMIT.labels(upstream=upstream).set(limit)
    CONCURRENCY_INFLIGHT.labels(upstream=upstream).set(inflight)


def render() -> tuple[bytes, str]:
    return generate_latest(), CONTENT_TYPE_LATEST
