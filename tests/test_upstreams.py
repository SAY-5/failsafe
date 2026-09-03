import asyncio
import json

import httpx
import pytest

from failsafe.breaker import State
from failsafe.config import BreakerConfig, HealthCheckConfig, OutlierConfig, UpstreamConfig
from failsafe.upstreams import HealthChecker, KubernetesEndpoints, UpstreamPool
from tests.conftest import counter_value
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


def _slice(ips_ready: dict[str, bool]) -> dict:
    return {
        "items": [
            {
                "ports": [{"name": "http", "port": 9000}],
                "endpoints": [
                    {"addresses": [ip], "conditions": {"ready": ready}}
                    for ip, ready in ips_ready.items()
                ],
            }
        ]
    }


async def test_kubernetes_endpointslice_discovery_tracks_ready_pods():
    state = {"doc": _slice({"10.1.0.1": True, "10.1.0.2": True, "10.1.0.3": False})}
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=state["doc"])

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://api")
    pool = UpstreamPool(
        UpstreamConfig("svc", kubernetes_service="upstream", kubernetes_namespace="ns", port=9000),
        BreakerConfig(),
    )
    pool.k8s = KubernetesEndpoints("upstream", "ns", client=client, token="tok")
    await pool.refresh()
    assert sorted(r.url for r in pool.replicas) == ["http://10.1.0.1:9000", "http://10.1.0.2:9000"]
    assert seen[0].headers["authorization"] == "Bearer tok"
    assert "kubernetes.io/service-name%3Dupstream" in str(seen[0].url)
    assert "/namespaces/ns/endpointslices" in str(seen[0].url)

    state["doc"] = _slice({"10.1.0.2": True, "10.1.0.3": True})
    await pool.refresh()
    assert sorted(r.url for r in pool.replicas) == ["http://10.1.0.2:9000", "http://10.1.0.3:9000"]

    state["doc"] = json.loads('{"items": []}')
    await pool.refresh()
    assert pool.replicas == []
    await pool.aclose()


async def test_kubernetes_api_failure_keeps_replicas():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, json={"message": "forbidden"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://api")
    pool = UpstreamPool(
        UpstreamConfig("svc", kubernetes_service="upstream", replicas=("http://10.9.9.9:9000",)),
        BreakerConfig(),
    )
    pool.k8s = KubernetesEndpoints("upstream", "ns", client=client, token="tok")
    await pool.refresh()
    assert [r.url for r in pool.replicas] == ["http://10.9.9.9:9000"]
    await pool.aclose()


OUTLIER = OutlierConfig(
    window=50,
    min_requests=10,
    base_ejection_seconds=5.0,
    max_ejection_seconds=12.0,
    min_latency_seconds=0.05,
)


def make_outlier_pool(n: int = 3, outlier: OutlierConfig = OUTLIER):
    cfg = UpstreamConfig("svc", replicas=tuple(f"http://o{i}:80" for i in range(n)))
    pool = UpstreamPool(
        cfg,
        BreakerConfig(consecutive_failures=1000, min_requests=1000),
        HealthCheckConfig(),
        None,
        outlier,
        clock=FakeClock(),
    )
    for r in pool.replicas:
        pool._set_health(r, True)
    return pool


def feed(pool, replica, n: int, *, error_every: int = 0, latency: float = 0.01) -> None:
    for i in range(n):
        if error_every and i % error_every == 0:
            pool.report_failure(replica, connection_failed=False, latency=latency)
        else:
            pool.report_success(replica, latency)


def test_error_rate_outlier_is_ejected_then_readmitted_after_cooldown():
    pool = make_outlier_pool(3)
    r0, r1, r2 = pool.replicas
    feed(pool, r0, 20, error_every=2)
    feed(pool, r1, 20)
    feed(pool, r2, 20)
    before = counter_value("failsafe_outlier_ejections_total", upstream=r0.label, reason="errors")
    assert pool.detect_outliers() == [r0]
    assert r0.ejected and not r0.available and r0.healthy
    assert (
        counter_value("failsafe_outlier_ejections_total", upstream=r0.label, reason="errors")
        == before + 1
    )
    assert {pool.pick().url for _ in range(6)} == {r1.url, r2.url}
    assert pool.available_count() == 2
    pool.clock.t += 4.9
    assert pool.detect_outliers() == [] and r0.ejected
    pool.clock.t += 0.1
    pool.detect_outliers()
    assert not r0.ejected and r0.available
    assert r0.url in {pool.pick().url for _ in range(3)}


def test_latency_outlier_is_ejected_but_small_absolute_latency_is_not():
    pool = make_outlier_pool(3)
    r0, r1, r2 = pool.replicas
    feed(pool, r0, 20, latency=0.03)  # 6x its peers, but under the 50 ms floor
    feed(pool, r1, 20, latency=0.005)
    feed(pool, r2, 20, latency=0.005)
    assert pool.detect_outliers() == []
    feed(pool, r0, 50, latency=0.3)
    assert pool.detect_outliers() == [r0]
    assert (
        counter_value("failsafe_outlier_ejections_total", upstream=r0.label, reason="latency") >= 1
    )


def test_service_wide_failure_ejects_nobody():
    pool = make_outlier_pool(3)
    for r in pool.replicas:
        feed(pool, r, 20, error_every=2)
    assert pool.detect_outliers() == []
    assert pool.available_count() == 3


def test_needs_peers_with_enough_samples():
    pool = make_outlier_pool(1)
    feed(pool, pool.replicas[0], 20, error_every=1)
    assert pool.detect_outliers() == []
    pool = make_outlier_pool(2)
    r0, r1 = pool.replicas
    feed(pool, r0, 20, error_every=1)
    feed(pool, r1, 5)  # below min_requests: r0 has no peer to be compared with
    assert pool.detect_outliers() == []
    feed(pool, r1, 5)
    assert pool.detect_outliers() == [r0]


def test_ejection_budget_keeps_most_of_the_pool_in_rotation():
    pool = make_outlier_pool(3)
    r0, r1, r2 = pool.replicas
    feed(pool, r0, 20, error_every=2)
    feed(pool, r1, 20, error_every=5)  # 20%: would be ejected on its own against r2
    feed(pool, r2, 20)
    assert pool.detect_outliers() == [r0]  # worst first, then the 50% budget is spent
    assert not r1.ejected
    assert pool.detect_outliers() == []


def test_repeated_ejections_escalate_the_cooldown_up_to_the_cap():
    pool = make_outlier_pool(2)
    r0, r1 = pool.replicas
    feed(pool, r1, 20)
    for expected in (5.0, 10.0, 12.0):
        feed(pool, r0, 20, error_every=1)
        assert pool.detect_outliers() == [r0]
        assert r0.ejected_until - pool.clock.t == pytest.approx(expected)
        pool.clock.t = r0.ejected_until
        pool.detect_outliers()
        assert not r0.ejected
        feed(pool, r1, 20)


def test_manual_eject_and_readmit():
    pool = make_pool(2)
    r0, r1 = pool.replicas
    pool.eject(r0, 60.0)
    assert r0.ejected and pool.pick() is r1 and pool.pick() is r1
    pool.readmit(r0)
    assert not r0.ejected and r0.available
    assert r0.url in {pool.pick().url for _ in range(2)}


def test_prefer_picks_the_subset_first_and_falls_back_to_the_rest():
    pool = make_pool(3)
    r0, r1, r2 = pool.replicas
    canary = pool.resolve(["r2:80"])
    assert canary == {r2.url}
    assert [pool.pick(prefer=canary) for _ in range(3)] == [r2, r2, r2]
    assert {pool.pick(prefer=pool.resolve([r0.url, r1.url])).url for _ in range(4)} == {
        r0.url,
        r1.url,
    }
    pool.observe_check(r2, False)
    assert pool.pick(prefer=canary) in (r0, r1)
    assert pool.pick(prefer=canary, exclude={r0.url, r1.url}) is None
