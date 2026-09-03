import asyncio
import random

from failsafe.breaker import State
from tests.conftest import counter_value


async def test_proxies_to_upstreams_round_robin(harness_factory):
    h = await harness_factory(3)
    served = set()
    for _ in range(6):
        r = await h.client.get("/orders/1?x=1")
        assert r.status_code == 200
        body = r.json()
        assert body["path"] == "/orders/1?x=1"
        served.add(body["served_by"])
        assert r.headers["x-failsafe-upstream"].startswith("127.0.0.1:")
    assert served == {"u0", "u1", "u2"}


async def test_unknown_path_is_404(harness_factory):
    h = await harness_factory(1)
    r = await h.client.get("/nope")
    assert r.status_code == 404


async def test_health_and_readiness_endpoints(harness_factory):
    h = await harness_factory(2)
    assert (await h.client.get("/healthz")).status_code == 200
    r = await h.client.get("/readyz")
    assert r.status_code == 200
    assert r.json()["upstreams"]["orders"] == {"healthy": 2, "total": 2}

    for u in h.upstreams:
        u.health_ok = False
    for _ in range(40):
        if not h.gateway.ready():
            break
        await asyncio.sleep(0.05)
    r = await h.client.get("/readyz")
    assert r.status_code == 503
    assert r.json()["ready"] is False


async def test_rate_limit_returns_429_with_retry_after(harness_factory):
    h = await harness_factory(1, rate_limit={"capacity": 3, "refill_per_second": 1})
    codes = [
        (await h.client.get("/orders/1", headers={"X-API-Key": "k1"})).status_code for _ in range(5)
    ]
    assert codes == [200, 200, 200, 429, 429]
    r = await h.client.get("/orders/1", headers={"X-API-Key": "k1"})
    assert r.status_code == 429
    assert r.headers["Retry-After"] == "1"
    assert r.headers["X-RateLimit-Limit"] == "3"
    # A different key has its own bucket.
    assert (await h.client.get("/orders/1", headers={"X-API-Key": "k2"})).status_code == 200
    assert counter_value("failsafe_rate_limited_total", route="/orders") >= 3


async def test_non_idempotent_post_is_not_retried_but_keyed_post_is(harness_factory):
    h = await harness_factory(2, retry={"max_attempts": 3, "base_delay_ms": 0, "max_delay_ms": 0})
    bad, good = h.upstreams
    bad.mode = "status"
    bad.status_code = 503
    h.pool._rr = 0  # next pick is `bad`

    before = counter_value("failsafe_retries_total", route="/orders")
    r = await h.client.post("/orders", content=b"{}")
    assert r.status_code == 503
    assert bad.served == 1 and good.served == 0
    assert counter_value("failsafe_retries_total", route="/orders") == before

    h.pool._rr = 0
    r = await h.client.post("/orders", content=b"{}", headers={"Idempotency-Key": "abc"})
    assert r.status_code == 200
    assert r.json()["served_by"] == "u1"
    assert counter_value("failsafe_retries_total", route="/orders", reason="status") == before + 1


async def test_breaker_opens_and_fast_fails_then_recovers(harness_factory):
    h = await harness_factory(
        1,
        retry={"max_attempts": 1},
        breaker={
            "consecutive_failures": 2,
            "min_requests": 10,
            "open_seconds": 0.3,
            "half_open_max": 1,
        },
    )
    (u,) = h.upstreams
    u.mode = "status"
    assert (await h.client.get("/orders/1")).status_code == 500
    assert (await h.client.get("/orders/1")).status_code == 500
    replica = h.pool.replicas[0]
    assert replica.breaker.state is State.OPEN
    served = u.served
    r = await h.client.get("/orders/1")
    assert r.status_code == 503  # fast fail, upstream never called
    assert u.served == served
    assert r.json()["error"] == "upstream unavailable"

    u.mode = "ok"
    await asyncio.sleep(0.35)
    assert (await h.client.get("/orders/1")).status_code == 200
    assert replica.breaker.state is State.CLOSED
    assert (
        counter_value(
            "failsafe_breaker_transitions_total", from_state="half_open", to_state="closed"
        )
        >= 1
    )


async def test_timeout_fails_over_to_next_replica(harness_factory):
    h = await harness_factory(2, timeout=0.2)
    slow = h.upstreams[0]
    slow.mode = "timeout"
    h.pool._rr = 0
    before = counter_value("failsafe_failovers_total")
    r = await h.client.get("/orders/9")
    assert r.status_code == 200
    assert r.json()["served_by"] == "u1"
    assert counter_value("failsafe_failovers_total") == before + 1
    assert counter_value("failsafe_retries_total", route="/orders", reason="timeout") >= 1


async def test_all_replicas_down_returns_503_and_counts_client_failure(harness_factory):
    h = await harness_factory(2)
    for u in h.upstreams:
        await u.stop()
    before = counter_value("failsafe_client_failed_requests_total", route="/orders")
    r = await h.client.get("/orders/1")
    assert r.status_code in (502, 503)
    assert counter_value("failsafe_client_failed_requests_total", route="/orders") == before + 1
    assert h.pool.healthy_count() == 0


async def test_metrics_endpoint_exposes_expected_series(harness_factory):
    h = await harness_factory(1)
    await h.client.get("/orders/1")
    text = (await h.client.get("/metrics")).text
    for name in [
        "failsafe_requests_total",
        "failsafe_request_latency_seconds_bucket",
        "failsafe_rate_limited_total",
        "failsafe_breaker_state",
        "failsafe_breaker_transitions_total",
        "failsafe_retries_total",
        "failsafe_failovers_total",
        "failsafe_upstream_healthy",
        "failsafe_client_failed_requests_total",
    ]:
        assert name in text, name
    assert 'failsafe_requests_total{route="/orders",status="200"' in text
    assert "failsafe_upstream_healthy{upstream=" in text


async def test_failover_under_load_has_zero_client_failures(harness_factory):
    """1200 requests at concurrency 100 while one replica is killed and another hangs."""
    h = await harness_factory(
        3, timeout=0.3, breaker={"consecutive_failures": 3, "open_seconds": 1}
    )
    u0, u1, u2 = h.upstreams
    total = 1200
    sem = asyncio.Semaphore(100)
    statuses: list[int] = []
    rng = random.Random(1)

    async def one(i: int) -> None:
        async with sem:
            if i % 3 == 0:
                r = await h.client.post(
                    "/orders", content=b'{"sku":"a"}', headers={"Idempotency-Key": f"k{i}"}
                )
            else:
                r = await h.client.get(f"/orders/{i}")
            statuses.append(r.status_code)

    async def chaos() -> None:
        await asyncio.sleep(0.1)
        await u1.stop()  # hard kill: refuses connections, aborts in-flight ones
        await asyncio.sleep(0.1)
        u2.mode = "timeout"  # keeps accepting, never answers
        await asyncio.sleep(0.6)
        u2.mode = "ok"
        await asyncio.sleep(0.2)
        # Bring u1 back on the same port so the health checker rediscovers it.
        u1b = h.upstreams[1] = type(u1)("u1")
        u1b.port = u1.port
        await u1b.start()

    failed_before = counter_value("failsafe_client_failed_requests_total", route="/orders")
    failover_before = counter_value("failsafe_failovers_total")
    order = list(range(total))
    rng.shuffle(order)
    await asyncio.gather(chaos(), *(one(i) for i in order))

    assert len(statuses) == total
    assert all(s == 200 for s in statuses), sorted({s for s in statuses if s != 200})
    assert counter_value("failsafe_client_failed_requests_total", route="/orders") == failed_before
    assert counter_value("failsafe_failovers_total") > failover_before
    assert u0.served > 0
    # u1 was killed early, so the survivors carried the load.
    assert u0.served + u2.served + u1.served + h.upstreams[1].served >= total
