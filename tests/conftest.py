from __future__ import annotations

import httpx
import pytest
from prometheus_client.parser import text_string_to_metric_families

from failsafe import metrics
from failsafe.app import Gateway, create_app
from failsafe.config import from_dict
from tests.fake_upstream import FakeUpstream


def counter_value(name: str, **labels: str) -> float:
    """Sum a counter over all series matching the given label subset."""
    text = metrics.render()[0].decode()
    total = 0.0
    for family in text_string_to_metric_families(text):
        for sample in family.samples:
            if sample.name != name:
                continue
            if all(sample.labels.get(k) == v for k, v in labels.items()):
                total += sample.value
    return total


def gateway_config(
    replicas: list[str],
    *,
    rate_limit: dict | None = None,
    retry: dict | None = None,
    breaker: dict | None = None,
    concurrency: dict | None = None,
    timeout: float = 0.5,
    interval: float = 0.05,
) -> dict:
    route = {
        "prefix": "/orders",
        "upstream": "orders",
        "timeout_seconds": timeout,
        "rate_limit": rate_limit,
        "retry": retry or {"max_attempts": 4, "base_delay_ms": 1, "max_delay_ms": 5},
        "breaker": breaker
        or {"consecutive_failures": 3, "min_requests": 5, "open_seconds": 0.5, "half_open_max": 1},
        "concurrency": concurrency,
    }
    return {
        "health_check": {"interval_seconds": interval, "timeout_seconds": 0.3},
        "upstreams": {"orders": {"replicas": replicas, "health_path": "/health"}},
        "routes": [route],
    }


class Harness:
    def __init__(self, upstreams: list[FakeUpstream], gateway: Gateway, client: httpx.AsyncClient):
        self.upstreams = upstreams
        self.gateway = gateway
        self.client = client

    @property
    def pool(self):
        return self.gateway.pools["orders"]


@pytest.fixture
async def harness_factory():
    created: list[Harness] = []

    async def make(n: int = 3, **cfg_kw) -> Harness:
        ups = [await FakeUpstream(f"u{i}").start() for i in range(n)]
        config = from_dict(gateway_config([u.url for u in ups], **cfg_kw))
        gw = Gateway(config)
        app = create_app(gateway=gw)
        await gw.startup()
        client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://gateway.test"
        )
        h = Harness(ups, gw, client)
        created.append(h)
        return h

    yield make
    for h in created:
        await h.client.aclose()
        await h.gateway.shutdown()
        for u in h.upstreams:
            await u.stop()
