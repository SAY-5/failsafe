"""Configuration model for the gateway, loaded from a YAML file."""

from __future__ import annotations

import os
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
    port: int = 80
    scheme: str = "http"
    health_path: str = "/health"

    def __post_init__(self) -> None:
        if not self.replicas and not self.dns:
            raise ValueError(f"upstream {self.name!r} needs either replicas or dns")


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

    def __post_init__(self) -> None:
        if not self.prefix.startswith("/"):
            raise ValueError(f"route prefix must start with '/': {self.prefix!r}")
        if self.timeout_seconds <= 0 or self.connect_timeout_seconds <= 0:
            raise ValueError("route timeouts must be > 0")


@dataclass(frozen=True)
class GatewayConfig:
    routes: tuple[RouteConfig, ...]
    upstreams: dict[str, UpstreamConfig]
    health_check: HealthCheckConfig = field(default_factory=HealthCheckConfig)
    trust_proxy_headers: bool = False

    def __post_init__(self) -> None:
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
        upstreams[name] = UpstreamConfig(
            name=name,
            replicas=tuple(u.get("replicas") or ()),
            dns=u.get("dns"),
            port=int(u.get("port", 80)),
            scheme=u.get("scheme", "http"),
            health_path=u.get("health_path", "/health"),
        )

    routes: list[RouteConfig] = []
    for r in raw.get("routes") or []:
        rl = r.get("rate_limit", {})
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
            )
        )

    hc = raw.get("health_check") or {}
    return GatewayConfig(
        routes=tuple(routes),
        upstreams=upstreams,
        health_check=HealthCheckConfig(**hc),
        trust_proxy_headers=bool(raw.get("trust_proxy_headers", False)),
    )


def _tuplify(d: dict[str, Any], key: str) -> dict[str, Any]:
    if key in d:
        d = {**d, key: tuple(int(s) for s in d[key])}
    return d


def load(path: str | os.PathLike[str] | None = None) -> GatewayConfig:
    p = Path(path or os.environ.get("FAILSAFE_CONFIG") or DEFAULT_CONFIG_PATH)
    with p.open() as fh:
        raw = yaml.safe_load(fh) or {}
    return from_dict(raw)
