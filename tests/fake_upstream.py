"""A minimal HTTP/1.1 upstream with switchable failure modes, used by the tests.

Modes:
  ok       respond 200 with a JSON echo
  status   respond with `status_code`
  reset    accept the connection and close it without a response
  timeout  accept, read the request, then hang for `hang_seconds`
`delay_seconds` adds latency before every successful answer (a slow replica).
Calling `stop()` closes the listening socket so new connections are refused,
which is what a killed pod looks like from the gateway.
"""

from __future__ import annotations

import asyncio
import json
import socket


class FakeUpstream:
    def __init__(self, name: str = "fake", *, healthy: bool = True) -> None:
        self.name = name
        self.mode = "ok"
        self.status_code = 500
        self.hang_seconds = 5.0
        self.delay_seconds = 0.0
        self.health_ok = healthy
        self.served = 0
        self.health_hits = 0
        self._server: asyncio.AbstractServer | None = None
        self._writers: set[asyncio.StreamWriter] = set()
        self.port = 0

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    async def start(self) -> FakeUpstream:
        sock = socket.socket()
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", self.port))
        self.port = sock.getsockname()[1]
        self._server = await asyncio.start_server(self._handle, sock=sock, backlog=2048)
        return self

    async def stop(self) -> None:
        """Stop accepting and abort every open connection, like a SIGKILL would."""
        for w in list(self._writers):
            w.transport.abort()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._writers.add(writer)
        try:
            while True:
                head = await reader.readuntil(b"\r\n\r\n")
                lines = head.decode().split("\r\n")
                method, path, _ = lines[0].split(" ", 2)
                headers = {}
                for line in lines[1:]:
                    if ":" in line:
                        k, v = line.split(":", 1)
                        headers[k.strip().lower()] = v.strip()
                body = b""
                if "content-length" in headers:
                    body = await reader.readexactly(int(headers["content-length"]))

                if path.startswith("/health"):
                    self.health_hits += 1
                    status = 200 if self.health_ok else 503
                    payload = {"status": "ok" if self.health_ok else "draining"}
                    self._write(writer, status, payload)
                    await writer.drain()
                    continue

                self.served += 1
                if self.mode == "reset":
                    return
                if self.mode == "timeout":
                    await asyncio.sleep(self.hang_seconds)
                    return
                if self.delay_seconds:
                    await asyncio.sleep(self.delay_seconds)
                status = self.status_code if self.mode == "status" else 200
                self._write(
                    writer,
                    status,
                    {
                        "served_by": self.name,
                        "method": method,
                        "path": path,
                        "body": body.decode(errors="replace"),
                        "idempotency_key": headers.get("idempotency-key"),
                    },
                )
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError, asyncio.CancelledError):
            pass
        finally:
            self._writers.discard(writer)
            writer.close()

    @staticmethod
    def _write(writer: asyncio.StreamWriter, status: int, payload: dict) -> None:
        data = json.dumps(payload).encode()
        reason = {200: "OK", 500: "Internal Server Error", 503: "Service Unavailable"}.get(
            status, "Status"
        )
        writer.write(
            f"HTTP/1.1 {status} {reason}\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(data)}\r\n"
            f"X-Served-By: fake\r\n\r\n".encode()
            + data
        )
