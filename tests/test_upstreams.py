import asyncio

import pytest

from failsafe.breaker import State
from failsafe.config import BreakerConfig, HealthCheckConfig, UpstreamConfig
from failsafe.upstreams import HealthChecker, UpstreamPool
from tests.fake_upstream import FakeUpstream


class FakeClock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


def make_pool(n: int = 3, **hc):
    cfg = UpstreamConfig("svc", replicas=tuple(f"http://r{i}:80" for i in range(n)))
    pool = UpstreamPool(
        cfg,
        BreakerConfig(consecutive_failures=2, open_seconds=1.0),
        HealthCheckConfig(**hc),
        clock=FakeClock(),
    )
    for r in pool.replicas:
        pool._set_health(r, True)
    return pool


def test_round_robin_over_healthy_replicas():
    pool = make_pool(3)
    picks = [pool.pick().label for _ in range(6)]
    assert picks == ["r0:80", "r1:80", "r2:80"] * 2


def test_pick_skips_excluded_and_unhealthy():
    pool = make_pool(3)
    r0, r1, r2 = pool.replicas
    pool.observe_check(r1, False)
    assert not r1.healthy
    assert pool.pick(exclude={r0.url}) is r2
    assert pool.pick(exclude={r0.url, r2.url}) is None


def test_connection_failure_marks_unhealthy_immediately():
    pool = make_pool(2)
    r0, r1 = pool.replicas
    pool.report_failure(r0, connection_failed=True)
    assert not r0.healthy
    assert pool.healthy_count() == 1
    assert pool.pick() is r1
    assert pool.pick() is r1


def test_breaker_open_removes_replica_from_rotation():
    pool = make_pool(2)
    r0, r1 = pool.replicas
    pool.report_failure(r0, connection_failed=False)
    pool.report_failure(r0, connection_failed=False)
    assert r0.breaker.state is State.OPEN
    assert r0.healthy  # breaker, not health, took it out
    assert pool.available_count() == 1
    assert {pool.pick().url for _ in range(4)} == {r1.url}
    pool.clock.t += 1.0
    picked = {pool.pick().url for _ in range(4)}
    assert r0.url in picked  # half-open probe admitted again


def test_health_thresholds_hysteresis():
    pool = make_pool(1, unhealthy_threshold=2, healthy_threshold=3)
    (r,) = pool.replicas
    pool.observe_check(r, False)
    assert r.healthy
    pool.observe_check(r, False)
    assert not r.healthy
    pool.observe_check(r, True)
    pool.observe_check(r, True)
    assert not r.healthy
    pool.observe_check(r, True)
    assert r.healthy


async def test_health_checker_detects_dead_and_recovered_replica():
    live = await FakeUpstream("live").start()
    dead = await FakeUpstream("dead").start()
    cfg = UpstreamConfig("svc", replicas=(live.url, dead.url))
    hc = HealthCheckConfig(interval_seconds=0.05, timeout_seconds=0.5)
    pool = UpstreamPool(cfg, BreakerConfig(), hc)
    assert pool.healthy_count() == 0  # unknown until the first probe

    await dead.stop()
    async with HealthChecker({"svc": pool}, hc):
        assert pool.healthy_count() == 1
        assert pool.get(live.url).healthy
        assert not pool.get(dead.url).healthy
        assert live.health_hits >= 1

        dead = FakeUpstream("dead")
        dead.port = pool.get(cfg.replicas[1]).url.rsplit(":", 1)[1]
        dead.port = int(dead.port)
        await dead.start()
        for _ in range(40):
            if pool.get(dead.url).healthy:
                break
            await asyncio.sleep(0.05)
        assert pool.get(dead.url).healthy

        live.health_ok = False
        for _ in range(40):
            if not pool.get(live.url).healthy:
                break
            await asyncio.sleep(0.05)
        assert not pool.get(live.url).healthy
    await live.stop()
    await dead.stop()


async def test_dns_discovery_reconciles_replicas(monkeypatch):
    pool = UpstreamPool(UpstreamConfig("svc", dns="svc.headless", port=9000), BreakerConfig())
    addrs = [("10.0.0.1",), ("10.0.0.2",)]

    async def fake_getaddrinfo(host, port, **kw):
        import socket

        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (a[0], port)) for a in addrs]

    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "getaddrinfo", fake_getaddrinfo)
    await pool.refresh_dns()
    assert sorted(r.url for r in pool.replicas) == [
        "http://10.0.0.1:9000",
        "http://10.0.0.2:9000",
    ]
    assert all(r.healthy for r in pool.replicas)
    addrs = [("10.0.0.2",), ("10.0.0.3",)]
    await pool.refresh_dns()
    assert sorted(r.url for r in pool.replicas) == [
        "http://10.0.0.2:9000",
        "http://10.0.0.3:9000",
    ]


async def test_dns_failure_keeps_existing_replicas(monkeypatch):
    pool = UpstreamPool(
        UpstreamConfig("svc", dns="svc.headless", port=9000, replicas=("http://10.0.0.9:9000",)),
        BreakerConfig(),
    )

    async def boom(*a, **k):
        raise OSError("no dns")

    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", boom)
    await pool.refresh_dns()
    assert [r.url for r in pool.replicas] == ["http://10.0.0.9:9000"]


def test_pool_requires_replicas_or_dns():
    with pytest.raises(ValueError):
        UpstreamConfig("svc")
