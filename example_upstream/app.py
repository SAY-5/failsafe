"""Orders service with env and runtime controlled failure injection.

Environment:
  UPSTREAM_NAME       instance label (defaults to the hostname)
  UPSTREAM_FAIL_RATE  probability in [0, 1] of answering 500 on /orders*
  UPSTREAM_SLOW_MS    artificial latency added to every /orders* request
  UPSTREAM_HEALTHY    "0" makes /health report 503 (readiness fails)
"""

from __future__ import annotations

import asyncio
import os
import random
import socket
from itertools import count
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Request, Response
from pydantic import BaseModel, Field


class Mode(BaseModel):
    fail_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    slow_ms: int = Field(default=0, ge=0)
    healthy: bool = True


class OrderIn(BaseModel):
    sku: str
    qty: int = Field(default=1, ge=1)


def create_app(name: str | None = None) -> FastAPI:
    app = FastAPI(title="orders", docs_url=None, redoc_url=None)
    instance = name or os.environ.get("UPSTREAM_NAME") or socket.gethostname()
    mode = Mode(
        fail_rate=float(os.environ.get("UPSTREAM_FAIL_RATE", "0")),
        slow_ms=int(os.environ.get("UPSTREAM_SLOW_MS", "0")),
        healthy=os.environ.get("UPSTREAM_HEALTHY", "1") != "0",
    )
    orders: dict[int, dict[str, Any]] = {}
    by_key: dict[str, dict[str, Any]] = {}
    ids = count(1)
    rng = random.Random()
    app.state.mode = mode

    @app.middleware("http")
    async def inject(request: Request, call_next):
        if request.url.path.startswith("/orders"):
            if mode.slow_ms:
                await asyncio.sleep(mode.slow_ms / 1000)
            if mode.fail_rate and rng.random() < mode.fail_rate:
                return Response(
                    '{"error":"injected failure"}',
                    status_code=500,
                    media_type="application/json",
                    headers={"X-Served-By": instance},
                )
        response = await call_next(request)
        response.headers["X-Served-By"] = instance
        return response

    @app.get("/health")
    async def health() -> Response:
        if not mode.healthy:
            raise HTTPException(status_code=503, detail="draining")
        return Response('{"status":"ok"}', media_type="application/json")

    @app.get("/orders")
    async def list_orders() -> dict[str, Any]:
        return {"instance": instance, "orders": list(orders.values())}

    @app.get("/orders/{order_id}")
    async def get_order(order_id: int) -> dict[str, Any]:
        order = orders.get(order_id) or {"id": order_id, "sku": "synthetic", "qty": 1}
        return {"instance": instance, "order": order}

    @app.post("/orders", status_code=201)
    async def create_order(
        body: OrderIn, idempotency_key: str | None = Header(default=None)
    ) -> dict[str, Any]:
        if idempotency_key and idempotency_key in by_key:
            return {"instance": instance, "order": by_key[idempotency_key], "replayed": True}
        order = {"id": next(ids), **body.model_dump()}
        orders[order["id"]] = order
        if idempotency_key:
            by_key[idempotency_key] = order
        return {"instance": instance, "order": order, "replayed": False}

    @app.delete("/orders/{order_id}", status_code=204)
    async def delete_order(order_id: int) -> Response:
        orders.pop(order_id, None)
        return Response(status_code=204)

    @app.get("/admin/mode")
    async def get_mode() -> Mode:
        return mode

    @app.post("/admin/mode")
    async def set_mode(new: Mode) -> Mode:
        mode.fail_rate, mode.slow_ms, mode.healthy = new.fail_rate, new.slow_ms, new.healthy
        return mode

    return app


app = create_app()
