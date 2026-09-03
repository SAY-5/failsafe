"""Gateway assembly: config -> pools, limiters, policies, ASGI app."""

from __future__ import annotations

import contextlib
import logging
import random
import time
from collections.abc import AsyncIterator, Callable

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from failsafe import metrics
from failsafe.config import GatewayConfig, load
from failsafe.proxy import Forwarder, client_key
from failsafe.ratelimit import RateLimiter, retry_after_header
from failsafe.retry import RetryPolicy
from failsafe.upstreams import HealthChecker, UpstreamPool, build_pools

log = logging.getLogger("failsafe")

ALL_METHODS = ["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]


class Gateway:
    def __init__(
        self,
        config: GatewayConfig,
        *,
        clock: Callable[[], float] = time.monotonic,
        rng: random.Random | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.config = config
        self.clock = clock
        self.rng = rng or random.Random()
        breaker_by_upstream = {}
        concurrency_by_upstream = {}
        for route in config.routes:
            breaker_by_upstream.setdefault(route.upstream, route.breaker)
            concurrency_by_upstream.setdefault(route.upstream, route.concurrency)
        self.pools: dict[str, UpstreamPool] = build_pools(
            config.upstreams,
            breaker_by_upstream,
            config.health_check,
            concurrency_by_upstream=concurrency_by_upstream,
            clock=clock,
        )
        self.limiters: dict[str, RateLimiter] = {
            r.prefix: RateLimiter(
                r.rate_limit.capacity, r.rate_limit.refill_per_second, clock=clock
            )
            for r in config.routes
            if r.rate_limit is not None
        }
        self.policies: dict[str, RetryPolicy] = {
            r.prefix: Forwarder._policy_for(r) for r in config.routes
        }
        self._client = client
        self._owns_client = client is None
        self.forwarder: Forwarder | None = None
        self.checker: HealthChecker | None = None

    async def startup(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(
                limits=httpx.Limits(max_connections=2000, max_keepalive_connections=500),
                follow_redirects=False,
            )
        self.forwarder = Forwarder(
            self._client,
            trust_proxy=self.config.trust_proxy_headers,
            rng=self.rng,
            clock=self.clock,
        )
        self.checker = HealthChecker(self.pools, self.config.health_check)
        await self.checker.start()
        log.info(
            "gateway ready: %d routes, %d upstreams, %d replicas",
            len(self.config.routes),
            len(self.pools),
            sum(len(p.replicas) for p in self.pools.values()),
        )

    async def shutdown(self) -> None:
        if self.checker is not None:
            await self.checker.stop()
            self.checker = None
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    def ready(self) -> bool:
        return any(p.healthy_count() > 0 for p in self.pools.values())

    async def handle(self, request: Request) -> Response:
        route = self.config.match(request.url.path)
        if route is None:
            return JSONResponse({"error": "no route"}, status_code=404)

        limiter = self.limiters.get(route.prefix)
        if limiter is not None:
            assert route.rate_limit is not None
            key = client_key(
                request,
                route.rate_limit.key_header,
                request.headers,
                self.config.trust_proxy_headers,
            )
            decision = limiter.check(key)
            if not decision.allowed:
                metrics.RATE_LIMITED.labels(route=route.prefix).inc()
                metrics.REQUESTS.labels(route=route.prefix, upstream="-", status="429").inc()
                return JSONResponse(
                    {"error": "rate limited"},
                    status_code=429,
                    headers={
                        "Retry-After": retry_after_header(decision.retry_after),
                        "X-RateLimit-Limit": str(route.rate_limit.capacity),
                        "X-RateLimit-Remaining": "0",
                    },
                )

        assert self.forwarder is not None, "gateway not started"
        body = await request.body()
        started = time.perf_counter()
        inflight = metrics.INFLIGHT.labels(route=route.prefix)
        inflight.inc()
        try:
            return await self.forwarder.forward(
                route, self.pools[route.upstream], self.policies[route.prefix], request, body
            )
        finally:
            inflight.dec()
            metrics.LATENCY.labels(route=route.prefix).observe(time.perf_counter() - started)


def create_app(config: GatewayConfig | None = None, gateway: Gateway | None = None) -> FastAPI:
    gw = gateway or Gateway(config or load())

    @contextlib.asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        await gw.startup()
        try:
            yield
        finally:
            await gw.shutdown()

    app = FastAPI(title="FailSafe", docs_url=None, redoc_url=None, lifespan=lifespan)
    app.state.gateway = gw

    @app.get("/healthz", include_in_schema=False)
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz", include_in_schema=False)
    async def readyz() -> JSONResponse:
        detail = {
            name: {"healthy": p.healthy_count(), "total": len(p.replicas)}
            for name, p in gw.pools.items()
        }
        ok = gw.ready()
        return JSONResponse({"ready": ok, "upstreams": detail}, status_code=200 if ok else 503)

    @app.get("/metrics", include_in_schema=False)
    async def metrics_endpoint() -> Response:
        payload, content_type = metrics.render()
        return Response(payload, media_type=content_type)

    @app.api_route("/{path:path}", methods=ALL_METHODS, include_in_schema=False)
    async def proxy(path: str, request: Request) -> Response:
        return await gw.handle(request)

    return app
