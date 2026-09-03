"""Upstream replica pools: selection, passive failure marking, active health checks,
and replica discovery through DNS or the Kubernetes EndpointSlice API."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import socket
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from failsafe import metrics
from failsafe.breaker import CircuitBreaker, State
from failsafe.concurrency import AdaptiveLimiter
from failsafe.config import BreakerConfig, ConcurrencyConfig, HealthCheckConfig, UpstreamConfig

log = logging.getLogger("failsafe.upstreams")

SERVICE_ACCOUNT_DIR = Path("/var/run/secrets/kubernetes.io/serviceaccount")


class KubernetesEndpoints:
    """Reads ready pod addresses for a Service from the EndpointSlice API.

    DNS answers for headless Services are cached (kubeadm and kind ship CoreDNS
    with a 30 second TTL), which is far too slow when pods are being replaced.
    The EndpointSlice API reflects readiness within a second, so this is the
    discovery path used in the Kubernetes deployment.
    """

    def __init__(
        self,
        service: str,
        namespace: str | None = None,
        *,
        client: httpx.AsyncClient | None = None,
        api_base: str | None = None,
        token: str | None = None,
    ) -> None:
        self.service = service
        self.namespace = namespace or self._read(SERVICE_ACCOUNT_DIR / "namespace") or "default"
        host = os.environ.get("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
        port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
        self.api_base = api_base or f"https://{host}:{port}"
        self._token = token if token is not None else self._read(SERVICE_ACCOUNT_DIR / "token")
        self._client = client

    @staticmethod
    def _read(path: Path) -> str | None:
        try:
            return path.read_text().strip()
        except OSError:
            return None

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            ca = SERVICE_ACCOUNT_DIR / "ca.crt"
            self._client = httpx.AsyncClient(
                base_url=self.api_base,
                verify=str(ca) if ca.exists() else True,
                timeout=2.0,
            )
        return self._client

    async def ready_addresses(self, port_name: str | None = None) -> set[str] | None:
        """Return ready pod IPs, or None when the API could not be queried."""
        client = self._get_client()
        headers = {"Authorization": f"Bearer {self._token}"} if self._token else {}
        url = (
            f"/apis/discovery.k8s.io/v1/namespaces/{self.namespace}/endpointslices"
            f"?labelSelector=kubernetes.io/service-name%3D{self.service}"
        )
        try:
            resp = await client.get(url, headers=headers)
            resp.raise_for_status()
            doc = resp.json()
        except (httpx.HTTPError, ValueError) as exc:
            log.warning(
                "endpointslice lookup for %s/%s failed: %s", self.namespace, self.service, exc
            )
            return None
        found: set[str] = set()
        for item in doc.get("items", []):
            if port_name and not any(p.get("name") == port_name for p in item.get("ports", [])):
                continue
            for ep in item.get("endpoints", []):
                cond = ep.get("conditions", {})
                if cond.get("ready") is False or cond.get("serving") is False:
                    continue
                found.update(ep.get("addresses", []))
        return found

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None


@dataclass
class Replica:
    url: str
    breaker: CircuitBreaker
    limiter: AdaptiveLimiter | None = None
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
        if self.limiter is not None:
            metrics.set_concurrency(self.label, self.limiter.limit, self.limiter.inflight)

    @property
    def available(self) -> bool:
        return self.healthy and self.breaker.state is not State.OPEN

    @property
    def has_capacity(self) -> bool:
        return self.limiter is None or self.limiter.has_capacity()


class UpstreamPool:
    """All replicas of one logical upstream service."""

    def __init__(
        self,
        cfg: UpstreamConfig,
        breaker_cfg: BreakerConfig | None = None,
        health_cfg: HealthCheckConfig | None = None,
        concurrency_cfg: ConcurrencyConfig | None = None,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.cfg = cfg
        self.name = cfg.name
        self.breaker_cfg = breaker_cfg or BreakerConfig()
        self.health_cfg = health_cfg or HealthCheckConfig()
        self.concurrency_cfg = concurrency_cfg
        self.clock = clock
        self._replicas: dict[str, Replica] = {}
        self._rr = 0
        self.k8s: KubernetesEndpoints | None = (
            KubernetesEndpoints(cfg.kubernetes_service, cfg.kubernetes_namespace)
            if cfg.kubernetes_service
            else None
        )
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

    def _make_limiter(self, label: str) -> AdaptiveLimiter | None:
        c = self.concurrency_cfg
        if c is None:
            return None
        return AdaptiveLimiter(
            label,
            initial=c.initial,
            min_limit=c.min_limit,
            max_limit=c.max_limit,
            backoff_ratio=c.backoff_ratio,
            rtt_tolerance=c.rtt_tolerance,
            window=c.window,
            probe_interval=c.probe_interval,
            on_update=lambda lim: metrics.set_concurrency(lim.name, lim.limit, lim.inflight),
        )

    def _add(self, url: str, healthy: bool = False) -> Replica:
        label = urlsplit(url).netloc or url
        r = Replica(
            url=url,
            breaker=self._make_breaker(label),
            limiter=self._make_limiter(label),
            healthy=healthy,
        )
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
        """Round-robin over healthy replicas with spare concurrency whose breaker admits the call.

        `breaker.allow()` is only invoked on the replica actually returned, so a
        half-open breaker's probe budget is consumed by real attempts only. The
        returned replica has one limiter slot acquired; `report_success` or
        `report_failure` releases it.
        """
        reps = self.replicas
        n = len(reps)
        for i in range(n):
            r = reps[(self._rr + i) % n]
            if r.url in exclude or not r.healthy or not r.has_capacity:
                continue
            if r.breaker.allow():
                if r.limiter is not None:
                    r.limiter.acquire()
                self._rr = (self._rr + i + 1) % n
                return r
        return None

    def saturated(self, exclude: frozenset[str] | set[str] = frozenset()) -> bool:
        """True when a replica could serve the call but its concurrency limit is full."""
        return any(
            r.url not in exclude and r.available and not r.has_capacity for r in self.replicas
        )

    def retry_after(self) -> float:
        waits = [r.limiter.retry_after() for r in self.replicas if r.limiter is not None]
        return min(waits) if waits else 1.0

    # ---- passive signals from the proxy ------------------------------------

    def report_success(self, replica: Replica, latency: float | None = None) -> None:
        replica.breaker.record_success()
        if replica.limiter is not None:
            replica.limiter.release(latency)

    def report_failure(
        self, replica: Replica, *, connection_failed: bool, latency: float | None = None
    ) -> None:
        replica.breaker.record_failure()
        if replica.limiter is not None:
            # A refused connection says nothing about load; anything later is backpressure.
            replica.limiter.release(None, dropped=not connection_failed)
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

    async def refresh(self) -> None:
        """Reconcile the replica set from the configured discovery source."""
        if self.k8s is not None:
            hosts = await self.k8s.ready_addresses()
        elif self.cfg.dns:
            hosts = await self._resolve_dns()
        else:
            return
        if hosts is None:
            return  # lookup failed: keep what we have, health checks still run
        self._reconcile({self._url_for(h) for h in hosts})

    async def refresh_dns(self) -> None:
        await self.refresh()

    def _url_for(self, host: str) -> str:
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        return f"{self.cfg.scheme}://{host}:{self.cfg.port}"

    async def _resolve_dns(self) -> set[str] | None:
        assert self.cfg.dns is not None
        loop = asyncio.get_running_loop()
        try:
            infos = await loop.getaddrinfo(self.cfg.dns, self.cfg.port, type=socket.SOCK_STREAM)
        except OSError as exc:
            log.warning("dns lookup for %s failed: %s", self.cfg.dns, exc)
            return None
        return {sockaddr[0] for _f, _t, _p, _c, sockaddr in infos}

    def _reconcile(self, found: set[str]) -> None:
        for url in found - self._replicas.keys():
            # Discovery only lists ready endpoints, so start optimistic.
            self._add(url, healthy=True)
            log.info("upstream %s discovered replica %s", self.name, url)
        for url in list(self._replicas.keys() - found):
            self._remove(url)
            log.info("upstream %s dropped replica %s", self.name, url)

    async def aclose(self) -> None:
        if self.k8s is not None:
            await self.k8s.aclose()


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
        for pool in self.pools.values():
            await pool.aclose()

    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(self.cfg.interval_seconds)
            try:
                await self.check_all()
            except Exception:  # pragma: no cover - defensive; never kill the loop
                log.exception("health check round failed")

    async def check_all(self) -> None:
        for pool in self.pools.values():
            await pool.refresh()
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
    concurrency_by_upstream: dict[str, ConcurrencyConfig | None] | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, UpstreamPool]:
    concurrency_by_upstream = concurrency_by_upstream or {}
    return {
        name: UpstreamPool(
            cfg,
            breaker_by_upstream.get(name),
            health_cfg,
            concurrency_by_upstream.get(name),
            clock=clock,
        )
        for name, cfg in upstreams.items()
    }
