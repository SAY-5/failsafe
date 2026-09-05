import asyncio

from failsafe.breaker import State
from tests.conftest import counter_value

TOKEN = "correct-horse-battery-staple"
AUTH = {"Authorization": f"Bearer {TOKEN}"}


async def test_admin_api_is_absent_without_a_token(harness_factory):
    h = await harness_factory(1)
    assert (await h.client.get("/admin/upstreams", headers=AUTH)).status_code == 404
    assert (await h.client.post("/admin/drain", headers=AUTH)).status_code == 404
    assert (await h.client.get("/orders/1")).status_code == 200


async def test_admin_api_requires_the_bearer_token(harness_factory):
    h = await harness_factory(2, admin_token=TOKEN)
    r = await h.client.get("/admin/upstreams")
    assert r.status_code == 401 and r.headers["www-authenticate"] == "Bearer"
    assert (
        await h.client.get("/admin/upstreams", headers={"Authorization": "Bearer nope"})
    ).status_code == 401
    assert (
        await h.client.get("/admin/upstreams", headers={"Authorization": TOKEN})
    ).status_code == 401
    r = await h.client.get("/admin/upstreams", headers=AUTH)
    assert r.status_code == 200
    body = r.json()
    assert body["draining"] is False
    orders = body["upstreams"]["orders"]
    assert orders["total"] == 2 and orders["available"] == 2
    keys = {"label", "url", "healthy", "available", "draining", "breaker", "ejected", "error_rate"}
    assert keys <= set(orders["replicas"][0])


async def test_manual_eject_and_readmit(harness_factory):
    h = await harness_factory(2, admin_token=TOKEN)
    u0 = h.upstreams[0]
    label = h.pool.get(u0.url).label
    base = f"/admin/upstreams/orders/replicas/{label}"
    r = await h.client.post(f"{base}/eject", params={"seconds": 60}, headers=AUTH)
    assert r.status_code == 200 and r.json()["ejected"] is True
    assert 55 < r.json()["ejected_for_seconds"] <= 60
    for i in range(6):
        assert (await h.client.get(f"/orders/{i}")).json()["served_by"] == "u1"
    assert counter_value("failsafe_outlier_ejections_total", upstream=label, reason="manual") >= 1
    r = await h.client.post(f"{base}/readmit", headers=AUTH)
    assert r.status_code == 200 and r.json()["ejected"] is False
    served = u0.served
    for i in range(4):
        assert (await h.client.get(f"/orders/{i}")).status_code == 200
    assert u0.served > served
    assert (
        await h.client.post(f"{base}/eject", params={"seconds": 0}, headers=AUTH)
    ).status_code == 422
    assert (await h.client.post(f"{base}/explode", headers=AUTH)).status_code == 404
    assert (
        await h.client.post("/admin/upstreams/orders/replicas/nope:1/eject", headers=AUTH)
    ).status_code == 404
    assert (
        await h.client.post(f"/admin/upstreams/nope/replicas/{label}/eject", headers=AUTH)
    ).status_code == 404


async def test_drain_and_undrain_a_replica(harness_factory):
    h = await harness_factory(2, admin_token=TOKEN)
    u0 = h.upstreams[0]
    replica = h.pool.get(u0.url)
    base = f"/admin/upstreams/orders/replicas/{replica.label}"
    r = await h.client.post(f"{base}/drain", headers=AUTH)
    assert r.status_code == 200 and r.json()["draining"] is True and r.json()["available"] is False
    served = u0.served
    for i in range(6):
        assert (await h.client.get(f"/orders/{i}")).json()["served_by"] == "u1"
    assert u0.served == served
    await asyncio.sleep(0.15)  # health checks keep running while drained
    assert replica.healthy and (await h.client.get("/readyz")).status_code == 200
    text = (await h.client.get("/metrics")).text
    assert f'failsafe_upstream_draining{{upstream="{replica.label}"}} 1.0' in text
    r = await h.client.post(f"{base}/undrain", headers=AUTH)
    assert r.json()["draining"] is False and r.json()["available"] is True
    for i in range(4):
        assert (await h.client.get(f"/orders/{i}")).status_code == 200
    assert u0.served > served


async def test_reset_breaker_closes_an_open_circuit(harness_factory):
    h = await harness_factory(
        2, admin_token=TOKEN, breaker={"consecutive_failures": 2, "open_seconds": 30}
    )
    u0 = h.upstreams[0]
    u0.mode = "status"
    replica = h.pool.get(u0.url)
    for i in range(6):
        assert (await h.client.get(f"/orders/{i}")).status_code == 200  # retried on u1
    assert replica.breaker.state is State.OPEN
    u0.mode = "ok"
    r = await h.client.post(
        f"/admin/upstreams/orders/replicas/{replica.label}/reset-breaker", headers=AUTH
    )
    assert r.status_code == 200 and r.json()["breaker"] == "closed"
    assert replica.breaker.state is State.CLOSED
    served = u0.served
    for i in range(4):
        assert (await h.client.get(f"/orders/{i}")).status_code == 200
    assert u0.served > served
    assert counter_value("failsafe_admin_actions_total", action="reset-breaker") >= 1


async def test_gateway_drain_withdraws_readiness_but_keeps_serving(harness_factory):
    h = await harness_factory(1, admin_token=TOKEN)
    assert (await h.client.get("/readyz")).status_code == 200
    r = await h.client.post("/admin/drain", headers=AUTH)
    assert r.status_code == 200 and r.json() == {"draining": True}
    r = await h.client.get("/readyz")
    assert r.status_code == 503 and r.json()["draining"] is True
    assert (await h.client.get("/orders/1")).status_code == 200
    assert (await h.client.get("/admin/upstreams", headers=AUTH)).json()["draining"] is True
    assert (await h.client.post("/admin/undrain", headers=AUTH)).json() == {"draining": False}
    assert (await h.client.get("/readyz")).status_code == 200
