"""Configuration model for the gateway, loaded from a YAML file."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG_PATH = Path(__file__).with_name("routes.yaml")


@dataclass(frozen=True)
class RateLimitConfig:
    capacity: int = 100
    refill_per_second: float = 50.0
    key_header: str = "X-API-Key"

    def __post_init__(self) -> None:
        if self.capacity < 1:
            raise ValueError("rate_limit.capacity must be >= 1")
        if self.refill_per_second <= 0:
            raise ValueError("rate_limit.refill_per_second must be > 0")


@dataclass(frozen=True)
class RetryConfig:
    max_attempts: int = 3
    base_delay_ms: float = 20.0
    max_delay_ms: float = 250.0
    idempotent_post: bool = False
    retry_on_status: tuple[int, ...] = (500, 502, 503, 504)

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("retry.max_attempts must be >= 1")
        if self.base_delay_ms < 0 or self.max_delay_ms < self.base_delay_ms:
            raise ValueError("retry delays must satisfy 0 <= base <= max")


@dataclass(frozen=True)
class BreakerConfig:
    window: int = 20
    failure_ratio: float = 0.5
    min_requests: int = 5
    consecutive_failures: int = 5
    open_seconds: float = 5.0
    half_open_max: int = 2

    def __post_init__(self) -> None:
        if self.window < 1 or self.min_requests < 1 or self.half_open_max < 1:
            raise ValueError("breaker window, min_requests and half_open_max must be >= 1")
        if not 0 < self.failure_ratio <= 1:
            raise ValueError("breaker.failure_ratio must be in (0, 1]")


@dataclass(frozen=True)
class ConcurrencyConfig:
    """Adaptive per-replica concurrency limit (AIMD driven by observed latency)."""

    initial: int = 20
    min_limit: int = 2
    max_limit: int = 1000
    backoff_ratio: float = 0.9
    rtt_tolerance: float = 2.0
    window: int = 10
    probe_interval: int = 200

    def __post_init__(self) -> None:
        if self.min_limit < 1 or self.max_limit < self.min_limit:
            raise ValueError("concurrency limits must satisfy 1 <= min_limit <= max_limit")
        if not self.min_limit <= self.initial <= self.max_limit:
            raise ValueError("concurrency.initial must be within [min_limit, max_limit]")
        if not 0 < self.backoff_ratio < 1:
            raise ValueError("concurrency.backoff_ratio must be in (0, 1)")
        if self.rtt_tolerance <= 1:
            raise ValueError("concurrency.rtt_tolerance must be > 1")
        if self.window < 1 or self.probe_interval < 1:
            raise ValueError("concurrency.window and probe_interval must be >= 1")


@dataclass(frozen=True)
class HedgeConfig:
    """Hedged requests: a second attempt on another replica once the first one
    has taken longer than the route's observed latency percentile."""

    after_ms: float | None = None  # fixed delay; None derives it from `percentile`
    percentile: float = 95.0
    min_samples: int = 20
    window: int = 1000

    def __post_init__(self) -> None:
        if self.after_ms is not None and self.after_ms < 0:
            raise ValueError("hedge.after_ms must be >= 0")
        if not 0 < self.percentile < 100:
            raise ValueError("hedge.percentile must be in (0, 100)")
        if self.min_samples < 1 or self.window < self.min_samples:
            raise ValueError("hedge window must satisfy 1 <= min_samples <= window")


@dataclass(frozen=True)
class CanaryConfig:
    """Send a weighted share of a route's traffic to a subset of replicas."""

    replicas: tuple[str, ...]  # replica URLs or host:port labels
    weight: float = 0.1  # share of requests routed to the canary subset
    header: str | None = None  # request header that forces canary (1/true) or stable (0/false)

    def __post_init__(self) -> None:
        if not self.replicas:
            raise ValueError("canary.replicas must list at least one replica")
        if not 0 <= self.weight <= 1:
            raise ValueError("canary.weight must be in [0, 1]")


@dataclass(frozen=True)
class OutlierConfig:
    """Eject a replica whose error rate or latency stands out from its peers."""

    window: int = 100  # recent outcomes kept per replica
    min_requests: int = 20  # samples a replica needs before it is judged
    min_replicas: int = 2  # replicas with enough samples needed to have peers
    error_ratio: float = 0.2  # error rate a replica must reach to be ejected
    error_factor: float = 3.0  # and exceed this multiple of the peers' median error rate
    latency_factor: float = 3.0  # mean latency above this multiple of the peers' median
    min_latency_seconds: float = 0.05  # and above this floor, so noise is never an outlier
    max_ejection_ratio: float = 0.5  # never eject more than this share of the pool
    base_ejection_seconds: float = 30.0  # cool-down, multiplied by the ejection count
    max_ejection_seconds: float = 300.0

    def __post_init__(self) -> None:
        if self.window < 1 or self.min_requests < 1 or self.min_replicas < 2:
            raise ValueError("outlier window and min_requests must be >= 1, min_replicas >= 2")
        if self.min_requests > self.window:
            raise ValueError("outlier.min_requests must be <= window")
        if not 0 < self.error_ratio <= 1 or self.error_factor < 1 or self.latency_factor < 1:
            raise ValueError("outlier ratios must satisfy 0 < error_ratio <= 1, factors >= 1")
        if not 0 < self.max_ejection_ratio <= 1:
            raise ValueError("outlier.max_ejection_ratio must be in (0, 1]")
        if (
            self.base_ejection_seconds <= 0
            or self.max_ejection_seconds < self.base_ejection_seconds
        ):
            raise ValueError("outlier ejection seconds must satisfy 0 < base <= max")


@dataclass(frozen=True)
class HealthCheckConfig:
    interval_seconds: float = 2.0
    timeout_seconds: float = 1.0
    unhealthy_threshold: int = 1
    healthy_threshold: int = 1


@dataclass(frozen=True)
class UpstreamConfig:
    name: str
    replicas: tuple[str, ...] = ()
    dns: str | None = None
    kubernetes_service: str | None = None
    kubernetes_namespace: str | None = None
    port: int = 80
    scheme: str = "http"
    health_path: str = "/health"

    def __post_init__(self) -> None:
        if not self.replicas and not self.dns and not self.kubernetes_service:
            raise ValueError(f"upstream {self.name!r} needs replicas, dns or kubernetes.service")


@dataclass(frozen=True)
class RouteConfig:
    prefix: str
    upstream: str
    timeout_seconds: float = 2.0
    connect_timeout_seconds: float = 0.5
    strip_prefix: bool = False
    rate_limit: RateLimitConfig | None = field(default_factory=RateLimitConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    breaker: BreakerConfig = field(default_factory=BreakerConfig)
    concurrency: ConcurrencyConfig | None = None
    hedge: HedgeConfig | None = None
    deadline_seconds: float | None = None  # default end-to-end budget per request
    canary: CanaryConfig | None = None
    outlier: OutlierConfig | None = None

    def __post_init__(self) -> None:
        if not self.prefix.startswith("/"):
            raise ValueError(f"route prefix must start with '/': {self.prefix!r}")
        if self.timeout_seconds <= 0 or self.connect_timeout_seconds <= 0:
            raise ValueError("route timeouts must be > 0")
        if self.deadline_seconds is not None and self.deadline_seconds <= 0:
            raise ValueError("route deadline_seconds must be > 0")


@dataclass(frozen=True)
class GatewayConfig:
    routes: tuple[RouteConfig, ...]
    upstreams: dict[str, UpstreamConfig]
    health_check: HealthCheckConfig = field(default_factory=HealthCheckConfig)
    trust_proxy_headers: bool = False
    admin_token: str | None = None  # bearer token for /admin; None disables the admin API

    def __post_init__(self) -> None:
        if self.admin_token is not None and len(self.admin_token) < 16:
            raise ValueError("admin_token must be at least 16 characters")
        for route in self.routes:
            if route.upstream not in self.upstreams:
                raise ValueError(
                    f"route {route.prefix} references unknown upstream {route.upstream!r}"
                )

    def match(self, path: str) -> RouteConfig | None:
        """Longest-prefix match on the request path."""
        best: RouteConfig | None = None
        for route in self.routes:
            p = route.prefix
            hit = path == p or p == "/" or path.startswith(p.rstrip("/") + "/")
            if hit and (best is None or len(p) > len(best.prefix)):
                best = route
        return best


def _expand(value: Any) -> Any:
    if isinstance(value, str):
        return os.path.expandvars(value)
    if isinstance(value, list):
        return [_expand(v) for v in value]
    if isinstance(value, dict):
        return {k: _expand(v) for k, v in value.items()}
    return value


def from_dict(raw: dict[str, Any]) -> GatewayConfig:
    raw = _expand(raw)
    upstreams: dict[str, UpstreamConfig] = {}
    for name, u in (raw.get("upstreams") or {}).items():
        k8s = u.get("kubernetes") or {}
        upstreams[name] = UpstreamConfig(
            name=name,
            replicas=tuple(u.get("replicas") or ()),
            dns=u.get("dns"),
            kubernetes_service=k8s.get("service"),
            kubernetes_namespace=k8s.get("namespace"),
            port=int(u.get("port", 80)),
            scheme=u.get("scheme", "http"),
            health_path=u.get("health_path", "/health"),
        )

    routes: list[RouteConfig] = []
    for r in raw.get("routes") or []:
        rl = r.get("rate_limit", {})
        cc = r.get("concurrency")
        hd = r.get("hedge")
        dl = r.get("deadline_seconds")
        cn = r.get("canary")
        ol = r.get("outlier")
        routes.append(
            RouteConfig(
                prefix=r["prefix"],
                upstream=r["upstream"],
                timeout_seconds=float(r.get("timeout_seconds", 2.0)),
                connect_timeout_seconds=float(r.get("connect_timeout_seconds", 0.5)),
                strip_prefix=bool(r.get("strip_prefix", False)),
                rate_limit=None if rl is None else RateLimitConfig(**rl),
                retry=RetryConfig(**_tuplify(r.get("retry", {}), "retry_on_status")),
                breaker=BreakerConfig(**r.get("breaker", {})),
                concurrency=None if cc is None else ConcurrencyConfig(**cc),
                hedge=None if hd is None else HedgeConfig(**hd),
                deadline_seconds=None if dl is None else float(dl),
                canary=None if cn is None else CanaryConfig(**_tuplify(cn, "replicas")),
                outlier=None if ol is None else OutlierConfig(**ol),
            )
        )

    hc = raw.get("health_check") or {}
    return GatewayConfig(
        routes=tuple(routes),
        upstreams=upstreams,
        health_check=HealthCheckConfig(**hc),
        trust_proxy_headers=bool(raw.get("trust_proxy_headers", False)),
        admin_token=_secret_or_none(raw.get("admin_token")),
    )


def _secret_or_none(value: Any) -> str | None:
    """A secret left as an unexpanded `${VAR}` reference is unset, not a literal."""
    if not value:
        return None
    text = str(value)
    if re.fullmatch(r"\$\{?[A-Za-z_][A-Za-z0-9_]*\}?", text):
        return None
    return text


def _tuplify(d: dict[str, Any], key: str) -> dict[str, Any]:
    if key in d:
        values = d[key] if isinstance(d[key], list | tuple) else [d[key]]
        d = {**d, key: tuple(int(v) if isinstance(v, int) else str(v) for v in values)}
    return d


def load(path: str | os.PathLike[str] | None = None) -> GatewayConfig:
    p = Path(path or os.environ.get("FAILSAFE_CONFIG") or DEFAULT_CONFIG_PATH)
    with p.open() as fh:
        raw = yaml.safe_load(fh) or {}
    return from_dict(raw)
