import httpx
import pytest

from example_upstream.app import create_app


@pytest.fixture
async def client():
    app = create_app("test-instance")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://u") as c:
        yield c


async def test_health_and_served_by_header(client):
    r = await client.get("/health")
    assert r.status_code == 200
    r = await client.get("/orders/7")
    assert r.status_code == 200
    assert r.headers["x-served-by"] == "test-instance"
    assert r.json()["order"]["id"] == 7


async def test_create_is_idempotent_with_key(client):
    a = await client.post("/orders", json={"sku": "x"}, headers={"Idempotency-Key": "k"})
    b = await client.post("/orders", json={"sku": "x"}, headers={"Idempotency-Key": "k"})
    assert a.status_code == 201 and b.status_code == 201
    assert a.json()["order"] == b.json()["order"]
    assert b.json()["replayed"] is True
    c = await client.post("/orders", json={"sku": "x"})
    assert c.json()["order"]["id"] != a.json()["order"]["id"]
    assert (await client.delete(f"/orders/{c.json()['order']['id']}")).status_code == 204


async def test_runtime_failure_injection(client):
    r = await client.post("/admin/mode", json={"fail_rate": 1.0, "slow_ms": 0, "healthy": False})
    assert r.status_code == 200
    assert (await client.get("/orders/1")).status_code == 500
    assert (await client.get("/health")).status_code == 503
    await client.post("/admin/mode", json={"fail_rate": 0.0})
    assert (await client.get("/orders/1")).status_code == 200
    assert (await client.get("/health")).status_code == 200
