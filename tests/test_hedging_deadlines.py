import asyncio
import time

from failsafe.breaker import State
from tests.conftest import counter_value


async def test_hedge_cuts_tail_latency_when_the_first_replica_is_slow(harness_factory):
    h = await harness_factory(2, timeout=1.0, hedge={"after_ms": 30})
    slow = h.upstreams[0]
    slow.delay_seconds = 0.4
    h.pool._rr = 0  # first pick is the slow replica
    hedges_before = counter_value("failsafe_hedges_total", route="/orders")
    wins_before = counter_value("failsafe_hedge_wins_total", route="/orders")

    started = time.perf_counter()
    r = await h.client.get("/orders/1")
    elapsed = time.perf_counter() - started
    assert r.status_code == 200
    assert r.json()["served_by"] == "u1"
    assert elapsed < 0.3, elapsed
    assert counter_value("failsafe_hedges_total", route="/orders") == hedges_before + 1
    assert counter_value("failsafe_hedge_wins_total", route="/orders") == wins_before + 1

    # The abandoned attempt freed its slot and left no failure on the breaker.
    slow_replica = h.pool.get(slow.url)
    for _ in range(20):
        if slow_replica.limiter is None or slow_replica.limiter.inflight == 0:
            break
        await asyncio.sleep(0.01)
    assert slow_replica.breaker.state is State.CLOSED
    assert slow_replica.breaker.failure_rate == 0.0


async def test_adaptive_hedge_delay_follows_observed_p95(harness_factory):
    h = await harness_factory(2, timeout=1.0, hedge={"percentile": 95, "min_samples": 10})
    slow = h.upstreams[0]
    hedges_before = counter_value("failsafe_hedges_total", route="/orders")
    for _ in range(20):
        assert (await h.client.get("/orders/1")).status_code == 200
    assert counter_value("failsafe_hedges_total", route="/orders") == hedges_before  # all fast
    tracker = h.gateway.forwarder.tracker(h.gateway.config.routes[0])
    assert len(tracker) == 20 and tracker.percentile(95) < 0.1

    slow.delay_seconds = 0.5
    h.pool._rr = 0
    started = time.perf_counter()
    r = await h.client.get("/orders/2")
    assert r.status_code == 200 and r.json()["served_by"] == "u1"
    assert time.perf_counter() - started < 0.3
    assert counter_value("failsafe_hedges_total", route="/orders") == hedges_before + 1
    assert 'failsafe_hedge_delay_seconds{route="/orders"}' in (await h.client.get("/metrics")).text


async def test_non_idempotent_post_is_never_hedged(harness_factory):
    h = await harness_factory(2, timeout=1.0, hedge={"after_ms": 10})
    slow = h.upstreams[0]
    slow.delay_seconds = 0.2
    h.pool._rr = 0
    hedges_before = counter_value("failsafe_hedges_total", route="/orders")
    r = await h.client.post("/orders", content=b"{}")
    assert r.status_code == 200 and r.json()["served_by"] == "u0"
    assert counter_value("failsafe_hedges_total", route="/orders") == hedges_before
    h.pool._rr = 0
    r = await h.client.post("/orders", content=b"{}", headers={"Idempotency-Key": "k"})
    assert r.status_code == 200 and r.json()["served_by"] == "u1"  # keyed POST may hedge
    assert counter_value("failsafe_hedges_total", route="/orders") == hedges_before + 1


async def test_hedge_waits_for_the_first_attempt_when_no_other_replica_is_free(harness_factory):
    h = await harness_factory(1, timeout=1.0, hedge={"after_ms": 10})
    (u,) = h.upstreams
    u.delay_seconds = 0.1
    hedges_before = counter_value("failsafe_hedges_total", route="/orders")
    r = await h.client.get("/orders/1")
    assert r.status_code == 200 and u.served == 1
    assert counter_value("failsafe_hedges_total", route="/orders") == hedges_before


async def test_deadline_stops_retries_and_returns_504(harness_factory):
    h = await harness_factory(2, timeout=1.0)
    for u in h.upstreams:
        u.mode = "timeout"
    exceeded_before = counter_value("failsafe_deadline_exceeded_total", route="/orders")
    started = time.perf_counter()
    r = await h.client.get("/orders/1", headers={"X-Request-Timeout": "0.15"})
    elapsed = time.perf_counter() - started
    assert r.status_code == 504
    assert "deadline exceeded" in r.json()["detail"]
    assert 0.1 < elapsed < 0.5, elapsed  # the attempt timeout was clamped to the budget
    assert sum(u.served for u in h.upstreams) == 1  # no retry once the budget is gone
    assert counter_value("failsafe_deadline_exceeded_total", route="/orders") == exceeded_before + 1


async def test_past_deadline_is_refused_without_calling_upstream(harness_factory):
    h = await harness_factory(1)
    (u,) = h.upstreams
    r = await h.client.get("/orders/1", headers={"X-Request-Deadline": str(time.time() - 1)})
    assert r.status_code == 504 and u.served == 0
    r = await h.client.get("/orders/1", headers={"X-Request-Deadline": "soon"})
    assert r.status_code == 400 and r.json()["error"] == "bad deadline"
    r = await h.client.get("/orders/1", headers={"X-Request-Timeout": "-1"})
    assert r.status_code == 400


async def test_remaining_budget_is_propagated_to_the_upstream(harness_factory):
    h = await harness_factory(1, deadline=5.0)
    (u,) = h.upstreams
    r = await h.client.get("/orders/1")
    body = r.json()
    assert 4.5 < float(body["timeout"]) <= 5.0  # route default budget
    assert abs(float(body["deadline"]) - (time.time() + 5.0)) < 0.5

    r = await h.client.get("/orders/1", headers={"X-Request-Timeout": "2"})
    body = r.json()
    assert 1.5 < float(body["timeout"]) <= 2.0  # the tighter client budget wins
    assert u.served == 2
