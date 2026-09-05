"""Upstream replica pools: selection, passive failure marking, active health checks,
and replica discovery through DNS or the Kubernetes EndpointSlice API."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import os
import socket
import statistics
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from failsafe import metrics
from failsafe.breaker import CircuitBreaker, State
from failsafe.concurrency import AdaptiveLimiter
from failsafe.config import (
    BreakerConfig,
    ConcurrencyConfig,
    HealthCheckConfig,
    OutlierConfig,
    UpstreamConfig,
)

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
    clock: Callable[[], float] = time.monotonic
    stats_window: int = 100
    ejected_until: float = 0.0
    ejections: int = 0
    draining: bool = False
    label: str = field(init=False)
    stats: deque[tuple[bool, float]] = field(init=False)  # (ok, latency) recent outcomes

    def __post_init__(self) -> None:
        parts = urlsplit(self.url)
        self.label = parts.netloc or self.url
        self.stats = deque(maxlen=self.stats_window)
        metrics.UPSTREAM_HEALTHY.labels(upstream=self.label).set(int(self.healthy))
        metrics.UPSTREAM_EJECTED.labels(upstream=self.label).set(0)
        metrics.UPSTREAM_DRAINING.labels(upstream=self.label).set(0)
        metrics.set_breaker_state(self.label, self.breaker.state)
        if self.limiter is not None:
            metrics.set_concurrency(self.label, self.limiter.limit, self.limiter.inflight)

    @property
    def ejected(self) -> bool:
        return self.ejected_until > self.clock()

    @property
    def available(self) -> bool:
        return (
            self.healthy
            and not self.draining
            and self.breaker.state is not State.OPEN
            and not self.ejected
        )

    def snapshot(self) -> dict[str, object]:
        """Operator-facing view of the replica for the admin API."""
        return {
            "label": self.label,
            "url": self.url,
            "healthy": self.healthy,
            "available": self.available,
            "draining": self.draining,
            "breaker": self.breaker.state.value,
            "ejected": self.ejected,
            "ejected_for_seconds": max(0.0, self.ejected_until - self.clock())
            if self.ejected
            else 0.0,
            "ejections": self.ejections,
            "limit": None if self.limiter is None else self.limiter.limit,
            "inflight": None if self.limiter is None else self.limiter.inflight,
            "samples": len(self.stats),
            "error_rate": round(self.error_rate, 4),
            "mean_latency": round(self.mean_latency, 4),
        }

    @property
    def error_rate(self) -> float:
        return sum(1 for ok, _ in self.stats if not ok) / len(self.stats) if self.stats else 0.0

    @property
    def mean_latency(self) -> float:
        return statistics.fmean(lat for _, lat in self.stats) if self.stats else 0.0

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
        outlier_cfg: OutlierConfig | None = None,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.cfg = cfg
        self.name = cfg.name
        self.breaker_cfg = breaker_cfg or BreakerConfig()
        self.health_cfg = health_cfg or HealthCheckConfig()
        self.concurrency_cfg = concurrency_cfg
        self.outlier_cfg = outlier_cfg
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
            clock=self.clock,
            stats_window=self.outlier_cfg.window if self.outlier_cfg else 100,
        )
        self._replicas[url] = r
        return r

    def _remove(self, url: str) -> None:
        r = self._replicas.pop(url, None)
        if r is not None:
            metrics.UPSTREAM_HEALTHY.labels(upstream=r.label).set(0)
            metrics.UPSTREAM_EJECTED.labels(upstream=r.label).set(0)
            metrics.UPSTREAM_DRAINING.labels(upstream=r.label).set(0)

    def resolve(self, entries: tuple[str, ...] | list[str]) -> set[str]:
        """URLs of the replicas named by URL or host:port label."""
        wanted = {e.rstrip("/") for e in entries}
        return {r.url for r in self._replicas.values() if r.url in wanted or r.label in wanted}

    @property
    def replicas(self) -> list[Replica]:
        return list(self._replicas.values())

    def get(self, url: str) -> Replica | None:
        return self._replicas.get(url)

    def get_by_label(self, label: str) -> Replica | None:
        return next((r for r in self._replicas.values() if r.label == label), None)

    def snapshot(self) -> dict[str, object]:
        return {
            "healthy": self.healthy_count(),
            "available": self.available_count(),
            "total": len(self._replicas),
            "replicas": [r.snapshot() for r in self._replicas.values()],
        }

    def set_draining(self, replica: Replica, draining: bool) -> None:
        """Stop (or resume) sending new requests to a replica; health checks continue."""
        if replica.draining == draining:
            return
        replica.draining = draining
        metrics.UPSTREAM_DRAINING.labels(upstream=replica.label).set(int(draining))
        log.info(
            "upstream %s replica %s %s",
            self.name,
            replica.label,
            "draining" if draining else "undrained",
        )

    def healthy_count(self) -> int:
        return sum(1 for r in self._replicas.values() if r.healthy)

    def available_count(self) -> int:
        return sum(1 for r in self._replicas.values() if r.available)

    # ---- selection --------------------------------------------------------

    def pick(
        self,
        exclude: frozenset[str] | set[str] = frozenset(),
        prefer: set[str] | None = None,
    ) -> Replica | None:
        """Round-robin over healthy, non-ejected replicas with spare concurrency whose
        breaker admits the call.

        With `prefer` (a set of URLs, for canary routing) the preferred replicas
        are tried first and the rest only when none of them can take the call.
        `breaker.allow()` is only invoked on the replica actually returned, so a
        half-open breaker's probe budget is consumed by real attempts only. The
        returned replica has one limiter slot acquired; `report_success` or
        `report_failure` releases it.
        """
        if prefer is None:
            return self._pick(exclude, None)
        r = self._pick(exclude, prefer)
        if r is None:
            r = self._pick(exclude, {x.url for x in self.replicas} - prefer)
        return r

    def _pick(self, exclude: frozenset[str] | set[str], subset: set[str] | None) -> Replica | None:
        reps = self.replicas
        n = len(reps)
        for i in range(n):
            r = reps[(self._rr + i) % n]
            if subset is not None and r.url not in subset:
                continue
            if r.url in exclude or not r.available or not r.has_capacity:
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
        if latency is not None:
            replica.stats.append((True, latency))

    def report_cancelled(self, replica: Replica) -> None:
        """An attempt was abandoned (hedge lost): free the slot, record no outcome."""
        if replica.limiter is not None:
            replica.limiter.release(None)

    def report_failure(
        self, replica: Replica, *, connection_failed: bool, latency: float | None = None
    ) -> None:
        replica.breaker.record_failure()
        if replica.limiter is not None:
            # A refused connection says nothing about load; anything later is backpressure.
            replica.limiter.release(None, dropped=not connection_failed)
        if connection_failed and replica.healthy:
            self._set_health(replica, False)
        elif not connection_failed:
            replica.stats.append((False, latency or 0.0))

    # ---- outlier ejection -------------------------------------------------

    def eject(
        self, replica: Replica, seconds: float | None = None, *, reason: str = "manual"
    ) -> None:
        """Take the replica out of rotation for `seconds` (default: the configured
        cool-down times the number of ejections so far, capped)."""
        replica.ejections += 1
        if seconds is None:
            o = self.outlier_cfg or OutlierConfig()
            seconds = min(o.max_ejection_seconds, o.base_ejection_seconds * replica.ejections)
        replica.ejected_until = self.clock() + seconds
        replica.stats.clear()
        metrics.OUTLIER_EJECTIONS.labels(upstream=replica.label, reason=reason).inc()
        metrics.UPSTREAM_EJECTED.labels(upstream=replica.label).set(1)
        log.warning(
            "upstream %s replica %s ejected for %.1fs (%s, ejection #%d)",
            self.name,
            replica.label,
            seconds,
            reason,
            replica.ejections,
        )

    def readmit(self, replica: Replica) -> None:
        if replica.ejected_until == 0.0:
            return
        replica.ejected_until = 0.0
        replica.stats.clear()
        metrics.UPSTREAM_EJECTED.labels(upstream=replica.label).set(0)
        log.info("upstream %s replica %s readmitted", self.name, replica.label)

    def detect_outliers(self) -> list[Replica]:
        """Readmit replicas whose cool-down expired, then eject the ones whose error
        rate or mean latency stands out from their peers. Returns the newly ejected."""
        now = self.clock()
        for r in self.replicas:
            if r.ejected_until and r.ejected_until <= now:
                self.readmit(r)
        o = self.outlier_cfg
        if o is None:
            return []
        judged = [r for r in self.replicas if not r.ejected and len(r.stats) >= o.min_requests]
        if len(judged) < o.min_replicas:
            return []
        budget = math.floor(len(self.replicas) * o.max_ejection_ratio) - sum(
            1 for r in self.replicas if r.ejected
        )
        errors = {r.url: r.error_rate for r in judged}
        latencies = {r.url: r.mean_latency for r in judged}
        ejected: list[Replica] = []
        for r in sorted(judged, key=lambda x: (errors[x.url], latencies[x.url]), reverse=True):
            if budget <= 0:
                break
            peer_err = statistics.median(errors[p.url] for p in judged if p is not r)
            peer_lat = statistics.median(latencies[p.url] for p in judged if p is not r)
            if errors[r.url] >= o.error_ratio and errors[r.url] >= o.error_factor * peer_err:
                self.eject(r, reason="errors")
            elif (
                latencies[r.url] >= o.min_latency_seconds
                and latencies[r.url] >= o.latency_factor * peer_lat
            ):
                self.eject(r, reason="latency")
            else:
                continue
            ejected.append(r)
            budget -= 1
        return ejected

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
        for pool in self.pools.values():
            pool.detect_outliers()

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
    outlier_by_upstream: dict[str, OutlierConfig | None] | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, UpstreamPool]:
    concurrency_by_upstream = concurrency_by_upstream or {}
    outlier_by_upstream = outlier_by_upstream or {}
    return {
        name: UpstreamPool(
            cfg,
            breaker_by_upstream.get(name),
            health_cfg,
            concurrency_by_upstream.get(name),
            outlier_by_upstream.get(name),
            clock=clock,
        )
        for name, cfg in upstreams.items()
    }
