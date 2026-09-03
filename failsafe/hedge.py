"""Hedged requests: latency tracking per route and the hedge delay derived from it."""

from __future__ import annotations

from collections import deque


class LatencyTracker:
    """Sliding window of attempt latencies with a cached percentile.

    The percentile is recomputed lazily, at most once per `window // 50`
    samples, so a request never pays for a sort on the hot path more than
    occasionally.
    """

    def __init__(self, window: int = 1000) -> None:
        if window < 1:
            raise ValueError("window must be >= 1")
        self.window = window
        self._samples: deque[float] = deque(maxlen=window)
        self._sorted: list[float] | None = None
        self._since_sort = 0
        self._refresh_every = max(1, window // 50)

    def record(self, latency: float) -> None:
        self._samples.append(latency)
        self._since_sort += 1
        if self._since_sort >= self._refresh_every:
            self._sorted = None

    def percentile(self, p: float) -> float | None:
        """Nearest-rank percentile of the window, or None when empty."""
        if not self._samples:
            return None
        if self._sorted is None:
            self._sorted = sorted(self._samples)
            self._since_sort = 0
        idx = min(len(self._sorted) - 1, max(0, round(p / 100 * (len(self._sorted) - 1))))
        return self._sorted[idx]

    def __len__(self) -> int:
        return len(self._samples)


def hedge_delay(
    tracker: LatencyTracker,
    *,
    after_ms: float | None,
    percentile: float,
    min_samples: int,
    attempt_timeout: float,
) -> float | None:
    """Seconds to wait before firing a hedge, or None when no hedge should be fired.

    A fixed `after_ms` always applies. Otherwise the delay is the route's
    observed latency percentile once `min_samples` have been seen. A delay that
    would not fire before the attempt times out is pointless and disables the hedge.
    """
    if after_ms is not None:
        delay = after_ms / 1000.0
    else:
        if len(tracker) < min_samples:
            return None
        p = tracker.percentile(percentile)
        if p is None:
            return None
        delay = p
    return delay if delay < attempt_timeout else None
