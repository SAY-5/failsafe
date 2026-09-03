"""Forwarding of a single request with retries, backoff, replica failover,
hedged attempts and end-to-end deadlines."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass

import httpx
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from failsafe import metrics
from failsafe.config import RouteConfig
from failsafe.hedge import LatencyTracker, hedge_delay
from failsafe.ratelimit import retry_after_header
from failsafe.retry import FailureKind, RetryPolicy, is_idempotent
from failsafe.upstreams import Replica, UpstreamPool

log = logging.getLogger("failsafe.proxy")

DEADLINE_HEADER = "x-request-deadline"  # absolute, seconds since the Unix epoch
TIMEOUT_HEADER = "x-request-timeout"  # relative, seconds of budget left

HOP_BY_HOP = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
        "host",
        "content-length",
        "accept-encoding",
    }
)
RESPONSE_STRIP = HOP_BY_HOP | {"content-encoding", "date", "server"}


class BadDeadline(ValueError):
    pass


def _classify(exc: httpx.HTTPError) -> FailureKind:
    if isinstance(exc, httpx.ConnectError | httpx.ConnectTimeout):
        return FailureKind.CONNECT
    if isinstance(exc, httpx.TimeoutException):
        return FailureKind.TIMEOUT
    return FailureKind.READ


def _forward_headers(request: Request, trust_proxy: bool) -> dict[str, str]:
    out = {k: v for k, v in request.headers.items() if k.lower() not in HOP_BY_HOP}
    client_ip = request.client.host if request.client else "unknown"
    prior = request.headers.get("x-forwarded-for") if trust_proxy else None
    out["x-forwarded-for"] = f"{prior}, {client_ip}" if prior else client_ip
    out["x-forwarded-proto"] = request.url.scheme
    out["x-forwarded-host"] = request.headers.get("host", "")
    out["accept-encoding"] = "identity"
    return out


def parse_deadline(headers: Mapping[str, str], now_wall: float) -> float | None:
    """Seconds from now until the client's deadline, from either header (the tighter wins)."""
    budget: float | None = None
    raw = headers.get(DEADLINE_HEADER)
    if raw:
        try:
            budget = float(raw) - now_wall
        except ValueError as exc:
            raise BadDeadline(f"{DEADLINE_HEADER} must be epoch seconds") from exc
    raw = headers.get(TIMEOUT_HEADER)
    if raw:
        try:
            timeout = float(raw)
        except ValueError as exc:
            raise BadDeadline(f"{TIMEOUT_HEADER} must be seconds") from exc
        if timeout < 0:
            raise BadDeadline(f"{TIMEOUT_HEADER} must be >= 0")
        budget = timeout if budget is None else min(budget, timeout)
    return budget


@dataclass
class Attempt:
    replica: Replica
    resp: httpx.Response | None = None
    kind: FailureKind | None = None
    error: str = ""
    latency: float = 0.0

    @property
    def failed(self) -> bool:
        return self.kind is not None


class Forwarder:
    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        trust_proxy: bool = False,
        rng: random.Random | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.client = client
        self.trust_proxy = trust_proxy
        self.rng = rng or random.Random()
        self.clock = clock
        self._trackers: dict[str, LatencyTracker] = {}

    def tracker(self, route: RouteConfig) -> LatencyTracker:
        t = self._trackers.get(route.prefix)
        if t is None:
            window = route.hedge.window if route.hedge is not None else 1000
            t = self._trackers[route.prefix] = LatencyTracker(window)
        return t

    async def forward(
        self,
        route: RouteConfig,
        pool: UpstreamPool,
        policy: RetryPolicy,
        request: Request,
        body: bytes,
    ) -> Response:
        method = request.method.upper()
        idempotent = is_idempotent(
            method, request.headers, idempotent_post=route.retry.idempotent_post
        )
        try:
            budget = parse_deadline(request.headers, time.time())
        except BadDeadline as exc:
            return JSONResponse({"error": "bad deadline", "detail": str(exc)}, status_code=400)
        if route.deadline_seconds is not None:
            budget = (
                route.deadline_seconds if budget is None else min(budget, route.deadline_seconds)
            )
        deadline = None if budget is None else self.clock() + budget

        headers = _forward_headers(request, self.trust_proxy)
        path = request.url.path
        if route.strip_prefix:
            path = path[len(route.prefix.rstrip("/")) :] or "/"
        if request.url.query:
            path = f"{path}?{request.url.query}"

        tried: set[str] = set()
        prev: Replica | None = None
        attempt = 0
        hedged = False
        while True:
            if deadline is not None and self.clock() >= deadline:
                return self._deadline_exceeded(route, prev, attempt)
            replica = pool.pick(exclude=tried)
            if replica is None and tried:
                replica = pool.pick()  # every replica was tried once; allow a second pass
            if replica is None:
                if pool.saturated():
                    return self._shed(route, pool)
                return self._fail(route, prev, 503, "no healthy upstream replica")
            attempt += 1
            tried.add(replica.url)
            if prev is not None and prev is not replica:
                metrics.FAILOVERS.labels(from_upstream=prev.label, to_upstream=replica.label).inc()

            args = (route, pool, policy, method, path, headers, body, deadline)
            delay = self._hedge_delay(route) if idempotent and not hedged else None
            if delay is not None:
                result, fired = await self._hedged(replica, delay, tried, *args)
                if fired:
                    attempt += 1
                    hedged = True
            else:
                result = await self._attempt(replica, *args)
            replica = result.replica

            if not result.failed:
                assert result.resp is not None
                return self._relay(route, replica, result.resp)
            assert result.kind is not None
            if policy.should_retry(attempt, result.kind, idempotent):
                backoff = policy.backoff(attempt, self.rng)
                if deadline is not None and self.clock() + backoff >= deadline:
                    return self._deadline_exceeded(route, replica, attempt)
                metrics.RETRIES.labels(route=route.prefix, reason=result.kind.value).inc()
                if backoff > 0:
                    await asyncio.sleep(backoff)
                prev = replica
                continue
            if result.resp is not None:
                return self._relay(route, replica, result.resp)
            status = 504 if result.kind is FailureKind.TIMEOUT else 502
            return self._fail(route, replica, status, result.error)

    # ---- attempts ---------------------------------------------------------

    async def _attempt(
        self,
        replica: Replica,
        route: RouteConfig,
        pool: UpstreamPool,
        policy: RetryPolicy,
        method: str,
        path: str,
        headers: dict[str, str],
        body: bytes,
        deadline: float | None,
    ) -> Attempt:
        """One request to one replica. Reports the outcome to the pool; never raises
        for transport errors. Cancellation (a lost hedge) frees the replica's slot."""
        total = route.timeout_seconds
        headers = dict(headers)
        if deadline is not None:
            remaining = max(0.0, deadline - self.clock())
            total = min(total, remaining)
            headers[DEADLINE_HEADER] = f"{time.time() + remaining:.3f}"
            headers[TIMEOUT_HEADER] = f"{remaining:.3f}"
        timeout = httpx.Timeout(total, connect=min(total, route.connect_timeout_seconds))

        sent = time.perf_counter()
        try:
            resp = await self.client.request(
                method, replica.url + path, content=body, headers=headers, timeout=timeout
            )
        except httpx.HTTPError as exc:
            kind = _classify(exc)
            pool.report_failure(
                replica,
                connection_failed=kind is FailureKind.CONNECT,
                latency=time.perf_counter() - sent,
            )
            return Attempt(replica, kind=kind, error=f"{kind.value}: {exc!s}"[:200])
        except asyncio.CancelledError:
            pool.report_cancelled(replica)
            raise
        latency = time.perf_counter() - sent
        if policy.retryable_status(resp.status_code):
            pool.report_failure(replica, connection_failed=False, latency=latency)
            return Attempt(replica, resp=resp, kind=FailureKind.STATUS, latency=latency)
        pool.report_success(replica, latency)
        self.tracker(route).record(latency)
        return Attempt(replica, resp=resp, latency=latency)

    def _hedge_delay(self, route: RouteConfig) -> float | None:
        h = route.hedge
        if h is None:
            return None
        delay = hedge_delay(
            self.tracker(route),
            after_ms=h.after_ms,
            percentile=h.percentile,
            min_samples=h.min_samples,
            attempt_timeout=route.timeout_seconds,
        )
        if delay is not None:
            metrics.HEDGE_DELAY.labels(route=route.prefix).set(delay)
        return delay

    async def _hedged(
        self, primary: Replica, delay: float, tried: set[str], *args
    ) -> tuple[Attempt, bool]:
        """Run the attempt on `primary`; if it is still running after `delay`, start a
        second attempt on another replica and return whichever succeeds first. The
        loser is cancelled. Returns (result, hedge_fired)."""
        route, pool = args[0], args[1]
        first = asyncio.create_task(self._attempt(primary, *args))
        done, _ = await asyncio.wait({first}, timeout=delay)
        if done:
            return first.result(), False
        other = pool.pick(exclude=tried)
        if other is None:
            return await first, False
        tried.add(other.url)
        metrics.HEDGES.labels(route=route.prefix).inc()
        second = asyncio.create_task(self._attempt(other, *args))

        pending = {first, second}
        winner: Attempt | None = None
        while pending and winner is None:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if not task.result().failed:
                    winner = task.result()
                    break
        for task in pending:
            task.cancel()
        for task in pending:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        if winner is None:
            return second.result(), True
        if winner.replica is other:
            metrics.HEDGE_WINS.labels(route=route.prefix).inc()
        return winner, True

    # ---- responses --------------------------------------------------------

    @staticmethod
    def _policy_for(route: RouteConfig) -> RetryPolicy:
        r = route.retry
        return RetryPolicy(
            max_attempts=r.max_attempts,
            base_delay=r.base_delay_ms / 1000.0,
            max_delay=r.max_delay_ms / 1000.0,
            retry_on_status=frozenset(r.retry_on_status),
        )

    @staticmethod
    def _relay(route: RouteConfig, replica: Replica, resp: httpx.Response) -> Response:
        metrics.REQUESTS.labels(
            route=route.prefix, upstream=replica.label, status=str(resp.status_code)
        ).inc()
        if resp.status_code >= 500:
            metrics.CLIENT_FAILED.labels(route=route.prefix).inc()
        headers = {k: v for k, v in resp.headers.items() if k.lower() not in RESPONSE_STRIP}
        headers["x-failsafe-upstream"] = replica.label
        return Response(content=resp.content, status_code=resp.status_code, headers=headers)

    @staticmethod
    def _shed(route: RouteConfig, pool: UpstreamPool) -> Response:
        """Every replica that could serve the call is at its concurrency limit."""
        metrics.LOAD_SHED.labels(route=route.prefix).inc()
        metrics.REQUESTS.labels(route=route.prefix, upstream="-", status="503").inc()
        return JSONResponse(
            {"error": "overloaded", "detail": "all replicas at concurrency limit"},
            status_code=503,
            headers={"Retry-After": retry_after_header(pool.retry_after())},
        )

    @staticmethod
    def _deadline_exceeded(route: RouteConfig, replica: Replica | None, attempts: int) -> Response:
        metrics.DEADLINE_EXCEEDED.labels(route=route.prefix).inc()
        return Forwarder._fail(
            route, replica, 504, f"deadline exceeded after {attempts} attempt(s)"
        )

    @staticmethod
    def _fail(route: RouteConfig, replica: Replica | None, status: int, detail: str) -> Response:
        label = replica.label if replica else "-"
        metrics.REQUESTS.labels(route=route.prefix, upstream=label, status=str(status)).inc()
        metrics.CLIENT_FAILED.labels(route=route.prefix).inc()
        log.warning("route %s failed with %s: %s", route.prefix, status, detail)
        return JSONResponse({"error": "upstream unavailable", "detail": detail}, status_code=status)


def client_key(request: Request, header: str, headers: Mapping[str, str], trust_proxy: bool) -> str:
    api_key = headers.get(header.lower())
    if api_key:
        return f"key:{api_key}"
    if trust_proxy:
        xff = headers.get("x-forwarded-for")
        if xff:
            return f"ip:{xff.split(',')[0].strip()}"
    return f"ip:{request.client.host if request.client else 'unknown'}"
