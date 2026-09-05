import asyncio

from tests.conftest import counter_value


async def test_canary_weight_holds_and_stable_traffic_avoids_the_canary(harness_factory):
    h = await harness_factory(3, canary={"index": 2, "weight": 0.25})
    u2 = h.upstreams[2]
    before = counter_value("failsafe_canary_requests_total", route="/orders", canary="true")
    hits = 0
    for i in range(400):
        r = await h.client.get(f"/orders/{i}")
        assert r.status_code == 200
        if r.headers.get("x-failsafe-canary") == "1":
            hits += 1
            assert r.json()["served_by"] == "u2"
        else:
            assert r.json()["served_by"] != "u2"
    assert 0.17 < hits / 400 < 0.33, hits
    assert u2.served == hits
    assert (
        counter_value("failsafe_canary_requests_total", route="/orders", canary="true")
        == before + hits
    )


async def test_canary_header_forces_either_side(harness_factory):
    h = await harness_factory(3, canary={"index": 2, "weight": 0.0, "header": "X-Canary"})
    for _ in range(5):
        r = await h.client.get("/orders/1", headers={"X-Canary": "1"})
        assert r.json()["served_by"] == "u2" and r.headers["x-failsafe-canary"] == "1"
        r = await h.client.get("/orders/1", headers={"X-Canary": "false"})
        assert r.json()["served_by"] != "u2" and "x-failsafe-canary" not in r.headers
        r = await h.client.get("/orders/1")
        assert r.json()["served_by"] != "u2"  # weight 0 without the header


async def test_canary_traffic_falls_back_to_stable_replicas_when_the_canary_dies(harness_factory):
    h = await harness_factory(3, canary={"index": 2, "weight": 1.0})
    u2 = h.upstreams[2]
    assert (await h.client.get("/orders/1")).json()["served_by"] == "u2"
    await u2.stop()
    failed_before = counter_value("failsafe_client_failed_requests_total", route="/orders")
    for _ in range(10):
        r = await h.client.get("/orders/1")
        assert r.status_code == 200 and r.json()["served_by"] in ("u0", "u1")
        assert "x-failsafe-canary" not in r.headers
    assert counter_value("failsafe_client_failed_requests_total", route="/orders") == failed_before


async def test_flaky_replica_is_ejected_and_readmitted(harness_factory):
    h = await harness_factory(
        3,
        breaker={"consecutive_failures": 100, "min_requests": 100},
        outlier={
            "window": 50,
            "min_requests": 10,
            "base_ejection_seconds": 0.3,
            "max_ejection_seconds": 1.0,
        },
    )
    u0 = h.upstreams[0]
    u0.mode = "flaky"  # every other answer is a 500, retried on a sibling
    replica = h.pool.get(u0.url)
    for i in range(60):
        assert (await h.client.get(f"/orders/{i}")).status_code == 200
    for _ in range(40):
        if replica.ejected:
            break
        await asyncio.sleep(0.05)
    assert replica.ejected and not replica.available and replica.healthy
    assert (
        counter_value("failsafe_outlier_ejections_total", upstream=replica.label, reason="errors")
        >= 1
    )
    text = (await h.client.get("/metrics")).text
    assert f'failsafe_upstream_ejected{{upstream="{replica.label}"}} 1.0' in text

    served = u0.served
    for i in range(30):
        assert (await h.client.get(f"/orders/{i}")).status_code == 200
    assert u0.served == served  # no traffic while ejected

    u0.mode = "ok"
    for _ in range(40):
        if not replica.ejected:
            break
        await asyncio.sleep(0.05)
    assert not replica.ejected and replica.available
    for i in range(30):
        assert (await h.client.get(f"/orders/{i}")).status_code == 200
    assert u0.served > served
