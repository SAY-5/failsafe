"""Adaptive concurrency limit per upstream replica.

The limit is adjusted with additive increase / multiplicative decrease driven by
observed round-trip time, in the spirit of TCP Vegas: a replica that keeps
answering within its no-load latency earns one more in-flight slot per window
of calls; a latency spike or a dropped call cuts the limit by `backoff_ratio`.
Requests beyond the limit are shed at the gateway instead of queueing on a
replica that is already saturated.
"""

from __future__ import annotations

import math
import threading
from collections.abc import Callable

Hook = Callable[["AdaptiveLimiter"], None]


class AdaptiveLimiter:
    """Bounds the number of in-flight calls to one replica and tunes that bound.

    * `acquire()` admits a call while `inflight < limit` and counts it.
    * `release(rtt, dropped=...)` frees the slot and feeds the sample back.
        - `dropped` (timeout, reset, 5xx) cuts the limit immediately:
          `limit = floor(limit * backoff_ratio)`.
        - Successful samples are judged once per `window` samples on their
          average, so jitter on a single call is not a spike: if the average
          exceeds `no_load_rtt * rtt_tolerance` the limit is cut the same way;
          otherwise, when the window ran at least half the limit in flight,
          the limit grows by one.
    * `no_load_rtt` is the smallest RTT seen since the last probe; every
      `probe_interval` samples it is reset to the current sample so a service
      that became permanently slower is not punished forever.
    The limit stays within [min_limit, max_limit]. Safe under threads and under
    a single asyncio loop; the lock is never held across an await.
    """

    def __init__(
        self,
        name: str = "",
        *,
        initial: int = 20,
        min_limit: int = 2,
        max_limit: int = 1000,
        backoff_ratio: float = 0.9,
        rtt_tolerance: float = 2.0,
        window: int = 10,
        probe_interval: int = 200,
        on_update: Hook | None = None,
    ) -> None:
        if min_limit < 1 or max_limit < min_limit or not min_limit <= initial <= max_limit:
            raise ValueError("limits must satisfy 1 <= min_limit <= initial <= max_limit")
        if not 0 < backoff_ratio < 1:
            raise ValueError("backoff_ratio must be in (0, 1)")
        if rtt_tolerance <= 1:
            raise ValueError("rtt_tolerance must be > 1")
        if window < 1 or probe_interval < 1:
            raise ValueError("window and probe_interval must be >= 1")
        self.name = name
        self.min_limit = min_limit
        self.max_limit = max_limit
        self.backoff_ratio = backoff_ratio
        self.rtt_tolerance = rtt_tolerance
        self.window = window
        self.probe_interval = probe_interval
        self.on_update = on_update

        self._limit = float(initial)
        self._inflight = 0
        self._no_load_rtt: float | None = None
        self._samples = 0
        self._win_n = 0
        self._win_rtt = 0.0
        self._win_load = 0
        self._lock = threading.Lock()

    # ---- inspection -------------------------------------------------------

    @property
    def limit(self) -> int:
        return int(self._limit)

    @property
    def inflight(self) -> int:
        return self._inflight

    @property
    def no_load_rtt(self) -> float | None:
        return self._no_load_rtt

    def has_capacity(self) -> bool:
        return self._inflight < int(self._limit)

    def retry_after(self) -> float:
        """Seconds a shed client should wait: one no-load RTT, at least a tenth of a second."""
        return max(0.1, self._no_load_rtt or 0.0)

    # ---- decisions --------------------------------------------------------

    def acquire(self) -> bool:
        with self._lock:
            if self._inflight >= int(self._limit):
                return False
            self._inflight += 1
        self._notify()
        return True

    def release(self, rtt: float | None = None, *, dropped: bool = False) -> None:
        """Free a slot. `rtt=None` (connection never established) leaves the limit alone."""
        with self._lock:
            was_inflight = self._inflight
            self._inflight = max(0, self._inflight - 1)
            if dropped:
                self._decrease()
            elif rtt is not None:
                self._sample(rtt, was_inflight)
        self._notify()

    # ---- internals (lock held) --------------------------------------------

    def _sample(self, rtt: float, inflight: int) -> None:
        self._samples += 1
        probe = self._no_load_rtt is None or self._samples % self.probe_interval == 0
        self._no_load_rtt = rtt if probe else min(self._no_load_rtt, rtt)
        self._win_n += 1
        self._win_rtt += rtt
        self._win_load += inflight
        if self._win_n < self.window:
            return
        avg_rtt = self._win_rtt / self._win_n
        avg_load = self._win_load / self._win_n
        self._reset_window()
        if not probe and avg_rtt > self._no_load_rtt * self.rtt_tolerance:
            self._decrease()
        elif avg_load * 2 >= int(self._limit):
            self._limit = min(float(self.max_limit), self._limit + 1.0)

    def _decrease(self) -> None:
        self._reset_window()
        self._limit = max(float(self.min_limit), math.floor(self._limit * self.backoff_ratio))

    def _reset_window(self) -> None:
        self._win_n = 0
        self._win_rtt = 0.0
        self._win_load = 0

    def _notify(self) -> None:
        if self.on_update is not None:
            self.on_update(self)
