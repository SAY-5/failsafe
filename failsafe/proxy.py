"""Forwarding of a single request with retries, backoff and replica failover."""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Mapping

import httpx
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from failsafe import metrics
from failsafe.config import RouteConfig
from failsafe.retry import FailureKind, RetryPolicy, is_idempotent
from failsafe.upstreams import Replica, UpstreamPool

log = logging.getLogger("failsafe.proxy")

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


class Forwarder:
    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        trust_proxy: bool = False,
        rng: random.Random | None = None,
    ) -> None:
        self.client = client
        self.trust_proxy = trust_proxy
        self.rng = rng or random.Random()

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
        headers = _forward_headers(request, self.trust_proxy)
        path = request.url.path
        if route.strip_prefix:
            path = path[len(route.prefix.rstrip("/")) :] or "/"
        if request.url.query:
            path = f"{path}?{request.url.query}"
        timeout = httpx.Timeout(route.timeout_seconds, connect=min(route.timeout_seconds, 1.0))

        tried: set[str] = set()
        prev: Replica | None = None
        attempt = 0
        while True:
            replica = pool.pick(exclude=tried)
            if replica is None and tried:
                replica = pool.pick()  # every replica was tried once; allow a second pass
            if replica is None:
                return self._fail(route, prev, 503, "no healthy upstream replica")
            attempt += 1
            tried.add(replica.url)
            if prev is not None and prev is not replica:
                metrics.FAILOVERS.labels(from_upstream=prev.label, to_upstream=replica.label).inc()

            try:
                resp = await self.client.request(
                    method, replica.url + path, content=body, headers=headers, timeout=timeout
                )
            except httpx.HTTPError as exc:
                kind = _classify(exc)
                pool.report_failure(replica, connection_failed=kind is FailureKind.CONNECT)
                if policy.should_retry(attempt, kind, idempotent):
                    await self._retry(route, kind, attempt)
                    prev = replica
                    continue
                status = 504 if kind is FailureKind.TIMEOUT else 502
                return self._fail(route, replica, status, f"{kind.value}: {exc!s}"[:200])

            if policy.retryable_status(resp.status_code):
                pool.report_failure(replica, connection_failed=False)
                if policy.should_retry(attempt, FailureKind.STATUS, idempotent):
                    await self._retry(route, FailureKind.STATUS, attempt)
                    prev = replica
                    continue
                return self._relay(route, replica, resp)

            pool.report_success(replica)
            return self._relay(route, replica, resp)

    async def _retry(self, route: RouteConfig, kind: FailureKind, attempt: int) -> None:
        metrics.RETRIES.labels(route=route.prefix, reason=kind.value).inc()
        delay = RetryPolicy.backoff(self._policy_for(route), attempt, self.rng)
        if delay > 0:
            await asyncio.sleep(delay)

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
