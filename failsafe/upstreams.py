"""Upstream replica pools: selection, passive failure marking, active health checks,
and DNS-based discovery for Kubernetes headless Services."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import socket
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from urllib.parse import urlsplit

import httpx

from failsafe import metrics
from failsafe.breaker import CircuitBreaker, State
from failsafe.config import BreakerConfig, HealthCheckConfig, UpstreamConfig

log = logging.getLogger("failsafe.upstreams")


@dataclass
class Replica:
    url: str
    breaker: CircuitBreaker
    healthy: bool = False
    consecutive_ok: int = 0
    consecutive_fail: int = 0
    last_checked: float = 0.0
    label: str = field(init=False)

    def __post_init__(self) -> None:
        parts = urlsplit(self.url)
        self.label = parts.netloc or self.url
        metrics.UPSTREAM_HEALTHY.labels(upstream=self.label).set(int(self.healthy))
        metrics.set_breaker_state(self.label, self.breaker.state)

    @property
    def available(self) -> bool:
        return self.healthy and self.breaker.state is not State.OPEN


class UpstreamPool:
    """All replicas of one logical upstream service."""

    def __init__(
        self,
        cfg: UpstreamConfig,
        breaker_cfg: BreakerConfig | None = None,
        health_cfg: HealthCheckConfig | None = None,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.cfg = cfg
        self.name = cfg.name
        self.breaker_cfg = breaker_cfg or BreakerConfig()
        self.health_cfg = health_cfg or HealthCheckConfig()
        self.clock = clock
        self._replicas: dict[str, Replica] = {}
        self._rr = 0
        for url in cfg.replicas:
            self._add(url.rstrip("/"))

    # ---- membership -------------------------------------------------------

    def _make_breaker(self, label: str) -> CircuitBreaker:
        b = self.breaker_cfg
        return CircuitBreaker(
            label,
            window=b.window,
            failure_ratio=b.failure_ratio,
            min_requests=b.min_requests,
            consecutive_failures=b.consecutive_failures,
            open_seconds=b.open_seconds,
            half_open_max=b.half_open_max,
            clock=self.clock,
            on_transition=lambda cb, old, new: metrics.record_transition(cb.name, old, new),
        )

    def _add(self, url: str, healthy: bool = False) -> Replica:
        label = urlsplit(url).netloc or url
        r = Replica(url=url, breaker=self._make_breaker(label), healthy=healthy)
        self._replicas[url] = r
        return r

    def _remove(self, url: str) -> None:
        r = self._replicas.pop(url, None)
        if r is not None:
            metrics.UPSTREAM_HEALTHY.labels(upstream=r.label).set(0)

    @property
    def replicas(self) -> list[Replica]:
        return list(self._replicas.values())

    def get(self, url: str) -> Replica | None:
        return self._replicas.get(url)

    def healthy_count(self) -> int:
        return sum(1 for r in self._replicas.values() if r.healthy)

    def available_count(self) -> int:
        return sum(1 for r in self._replicas.values() if r.available)

    # ---- selection --------------------------------------------------------

    def pick(self, exclude: frozenset[str] | set[str] = frozenset()) -> Replica | None:
        """Round-robin over healthy replicas whose breaker admits the call.

        `breaker.allow()` is only invoked on the replica actually returned, so a
        half-open breaker's probe budget is consumed by real attempts only.
        """
        reps = self.replicas
        n = len(reps)
        for i in range(n):
            r = reps[(self._rr + i) % n]
            if r.url in exclude or not r.healthy:
                continue
            if r.breaker.allow():
                self._rr = (self._rr + i + 1) % n
                return r
        return None

    # ---- passive signals from the proxy ------------------------------------

    def report_success(self, replica: Replica) -> None:
        replica.breaker.record_success()

    def report_failure(self, replica: Replica, *, connection_failed: bool) -> None:
        replica.breaker.record_failure()
        if connection_failed and replica.healthy:
            self._set_health(replica, False)

    # ---- health -----------------------------------------------------------

    def _set_health(self, replica: Replica, healthy: bool) -> None:
        if replica.healthy != healthy:
            replica.healthy = healthy
            metrics.UPSTREAM_HEALTHY.labels(upstream=replica.label).set(int(healthy))
            log.info(
                "upstream %s replica %s -> %s",
                self.name,
                replica.label,
                "healthy" if healthy else "unhealthy",
            )
        if healthy:
            replica.consecutive_fail = 0
            replica.consecutive_ok = 0
        else:
            replica.consecutive_ok = 0
            replica.consecutive_fail = max(replica.consecutive_fail, 1)

    def observe_check(self, replica: Replica, ok: bool) -> None:
        replica.last_checked = self.clock()
        hc = self.health_cfg
        if ok:
            replica.consecutive_ok += 1
            replica.consecutive_fail = 0
            if not replica.healthy and replica.consecutive_ok >= hc.healthy_threshold:
                self._set_health(replica, True)
        else:
            replica.consecutive_fail += 1
            replica.consecutive_ok = 0
            if replica.healthy and replica.consecutive_fail >= hc.unhealthy_threshold:
                self._set_health(replica, False)

    async def refresh_dns(self) -> None:
        """Resolve the headless Service name and reconcile the replica set."""
        if not self.cfg.dns:
            return
        loop = asyncio.get_running_loop()
        try:
            infos = await loop.getaddrinfo(self.cfg.dns, self.cfg.port, type=socket.SOCK_STREAM)
        except OSError as exc:
            log.warning("dns lookup for %s failed: %s", self.cfg.dns, exc)
            return
        found: set[str] = set()
        for family, _t, _p, _c, sockaddr in infos:
            host = sockaddr[0]
            if family == socket.AF_INET6:
                host = f"[{host}]"
            found.add(f"{self.cfg.scheme}://{host}:{self.cfg.port}")
        for url in found - self._replicas.keys():
            # A headless Service only lists ready endpoints, so start optimistic.
            self._add(url, healthy=True)
            log.info("upstream %s discovered replica %s", self.name, url)
        for url in list(self._replicas.keys() - found):
            self._remove(url)
            log.info("upstream %s dropped replica %s", self.name, url)


class HealthChecker:
    """Periodically probes every replica's health path."""

    def __init__(
        self,
        pools: dict[str, UpstreamPool],
        cfg: HealthCheckConfig,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.pools = pools
        self.cfg = cfg
        self._client = client
        self._owns_client = client is None
        self._task: asyncio.Task[None] | None = None

    async def __aenter__(self) -> HealthChecker:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    async def start(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.cfg.timeout_seconds)
        await self.check_all()
        self._task = asyncio.create_task(self._loop(), name="failsafe-healthcheck")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(self.cfg.interval_seconds)
            try:
                await self.check_all()
            except Exception:  # pragma: no cover - defensive; never kill the loop
                log.exception("health check round failed")

    async def check_all(self) -> None:
        for pool in self.pools.values():
            await pool.refresh_dns()
        jobs = [
            self.check_replica(pool, replica)
            for pool in self.pools.values()
            for replica in pool.replicas
        ]
        if jobs:
            await asyncio.gather(*jobs)

    async def check_replica(self, pool: UpstreamPool, replica: Replica) -> bool:
        assert self._client is not None
        url = replica.url + pool.cfg.health_path
        try:
            resp = await self._client.get(url, timeout=self.cfg.timeout_seconds)
            ok = 200 <= resp.status_code < 300
        except (httpx.HTTPError, OSError):
            ok = False
        pool.observe_check(replica, ok)
        return ok


def build_pools(
    upstreams: dict[str, UpstreamConfig],
    breaker_by_upstream: dict[str, BreakerConfig],
    health_cfg: HealthCheckConfig,
    *,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, UpstreamPool]:
    return {
        name: UpstreamPool(cfg, breaker_by_upstream.get(name), health_cfg, clock=clock)
        for name, cfg in upstreams.items()
    }
