"""Token-bucket rate limiting keyed by client identity."""

from __future__ import annotations

import math
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

Clock = Callable[[], float]


@dataclass
class Decision:
    allowed: bool
    remaining: float
    retry_after: float  # seconds until at least one token is available (0 if allowed)


class TokenBucket:
    """A single token bucket with continuous refill.

    Tokens refill at `refill_rate` per second up to `capacity`. Time comes from an
    injectable monotonic clock so refill math is deterministic in tests. Safe to use
    from multiple threads and from a single asyncio loop (the lock is never held
    across an await).
    """

    __slots__ = ("_lock", "_tokens", "_updated", "capacity", "clock", "refill_rate")

    def __init__(self, capacity: int, refill_rate: float, clock: Clock = time.monotonic) -> None:
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        if refill_rate <= 0:
            raise ValueError("refill_rate must be > 0")
        self.capacity = float(capacity)
        self.refill_rate = float(refill_rate)
        self.clock = clock
        self._tokens = self.capacity
        self._updated = clock()
        self._lock = threading.Lock()

    def _refill(self, now: float) -> None:
        elapsed = now - self._updated
        if elapsed > 0:
            self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_rate)
            self._updated = now

    @property
    def tokens(self) -> float:
        with self._lock:
            self._refill(self.clock())
            return self._tokens

    def try_acquire(self, cost: float = 1.0) -> Decision:
        if cost <= 0:
            raise ValueError("cost must be > 0")
        with self._lock:
            now = self.clock()
            self._refill(now)
            if self._tokens >= cost:
                self._tokens -= cost
                return Decision(True, self._tokens, 0.0)
            deficit = cost - self._tokens
            return Decision(False, self._tokens, deficit / self.refill_rate)


class RateLimiter:
    """Per-key token buckets sharing one policy.

    Idle buckets are evicted lazily so an unbounded stream of distinct keys
    (for example spoofed client IPs) cannot grow memory without limit.
    """

    def __init__(
        self,
        capacity: int,
        refill_rate: float,
        *,
        clock: Clock = time.monotonic,
        max_keys: int = 100_000,
        idle_seconds: float = 300.0,
    ) -> None:
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.clock = clock
        self.max_keys = max_keys
        self.idle_seconds = idle_seconds
        self._buckets: dict[str, TokenBucket] = {}
        self._last_seen: dict[str, float] = {}
        self._lock = threading.Lock()

    def bucket(self, key: str) -> TokenBucket:
        with self._lock:
            b = self._buckets.get(key)
            now = self.clock()
            if b is None:
                if len(self._buckets) >= self.max_keys:
                    self._evict(now)
                b = TokenBucket(self.capacity, self.refill_rate, self.clock)
                self._buckets[key] = b
            self._last_seen[key] = now
            return b

    def _evict(self, now: float) -> None:
        stale = [k for k, t in self._last_seen.items() if now - t > self.idle_seconds]
        if not stale:
            # Nothing idle: drop the least recently used quarter to stay bounded.
            ordered = sorted(self._last_seen, key=self._last_seen.__getitem__)
            stale = ordered[: max(1, len(ordered) // 4)]
        for k in stale:
            self._buckets.pop(k, None)
            self._last_seen.pop(k, None)

    def check(self, key: str, cost: float = 1.0) -> Decision:
        return self.bucket(key).try_acquire(cost)

    def __len__(self) -> int:
        return len(self._buckets)


def retry_after_header(seconds: float) -> str:
    """Retry-After must be an integer number of seconds; round up so clients never retry early."""
    return str(max(1, math.ceil(seconds)))
