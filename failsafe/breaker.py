"""Circuit breaker with closed / open / half-open states."""

from __future__ import annotations

import enum
import threading
import time
from collections import deque
from collections.abc import Callable

Clock = Callable[[], float]
TransitionHook = Callable[["CircuitBreaker", "State", "State"], None]


class State(enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Tracks outcomes for one upstream and decides whether calls may proceed.

    Opening conditions (either one):
      * `consecutive_failures` failures in a row, or
      * within the last `window` outcomes, at least `min_requests` were recorded and
        the failure ratio is >= `failure_ratio`.

    While OPEN every `allow()` is refused until `open_seconds` has elapsed, after
    which the breaker moves to HALF_OPEN and admits at most `half_open_max`
    probe calls. A probe failure reopens the breaker; once `half_open_max`
    probes all succeed the breaker closes and the window is reset.
    """

    def __init__(
        self,
        name: str = "",
        *,
        window: int = 20,
        failure_ratio: float = 0.5,
        min_requests: int = 5,
        consecutive_failures: int = 5,
        open_seconds: float = 5.0,
        half_open_max: int = 2,
        clock: Clock = time.monotonic,
        on_transition: TransitionHook | None = None,
    ) -> None:
        if window < 1 or min_requests < 1 or half_open_max < 1 or consecutive_failures < 1:
            raise ValueError("window, min_requests, half_open_max, consecutive_failures must be >= 1")
        if not 0 < failure_ratio <= 1:
            raise ValueError("failure_ratio must be in (0, 1]")
        self.name = name
        self.window = window
        self.failure_ratio = failure_ratio
        self.min_requests = min_requests
        self.consecutive_failures = consecutive_failures
        self.open_seconds = open_seconds
        self.half_open_max = half_open_max
        self.clock = clock
        self.on_transition = on_transition

        self._state = State.CLOSED
        self._outcomes: deque[bool] = deque(maxlen=window)  # True == failure
        self._consecutive = 0
        self._opened_at = 0.0
        self._probes_in_flight = 0
        self._probes_succeeded = 0
        self._lock = threading.Lock()

    # ---- inspection -------------------------------------------------------

    @property
    def state(self) -> State:
        with self._lock:
            self._maybe_half_open(self.clock())
            return self._state

    @property
    def failure_rate(self) -> float:
        with self._lock:
            if not self._outcomes:
                return 0.0
            return sum(self._outcomes) / len(self._outcomes)

    def time_until_probe(self) -> float:
        with self._lock:
            if self._state is not State.OPEN:
                return 0.0
            return max(0.0, self._opened_at + self.open_seconds - self.clock())

    # ---- decisions --------------------------------------------------------

    def allow(self) -> bool:
        """Return True if a call may proceed. Half-open probes are counted here."""
        with self._lock:
            now = self.clock()
            self._maybe_half_open(now)
            if self._state is State.CLOSED:
                return True
            if self._state is State.OPEN:
                return False
            if self._probes_in_flight + self._probes_succeeded < self.half_open_max:
                self._probes_in_flight += 1
                return True
            return False

    def record_success(self) -> None:
        with self._lock:
            self._consecutive = 0
            if self._state is State.HALF_OPEN:
                self._probes_in_flight = max(0, self._probes_in_flight - 1)
                self._probes_succeeded += 1
                if self._probes_succeeded >= self.half_open_max:
                    self._transition(State.CLOSED)
                return
            self._outcomes.append(False)

    def record_failure(self) -> None:
        with self._lock:
            now = self.clock()
            self._consecutive += 1
            if self._state is State.HALF_OPEN:
                self._open(now)
                return
            if self._state is State.OPEN:
                return
            self._outcomes.append(True)
            if self._consecutive >= self.consecutive_failures:
                self._open(now)
                return
            n = len(self._outcomes)
            if n >= self.min_requests and sum(self._outcomes) / n >= self.failure_ratio:
                self._open(now)

    def reset(self) -> None:
        with self._lock:
            self._transition(State.CLOSED)

    # ---- internals (lock held) --------------------------------------------

    def _maybe_half_open(self, now: float) -> None:
        if self._state is State.OPEN and now - self._opened_at >= self.open_seconds:
            self._transition(State.HALF_OPEN)

    def _open(self, now: float) -> None:
        self._opened_at = now
        self._transition(State.OPEN)

    def _transition(self, new: State) -> None:
        old = self._state
        if new is State.CLOSED:
            self._outcomes.clear()
            self._consecutive = 0
        if new is not State.HALF_OPEN or old is not State.HALF_OPEN:
            self._probes_in_flight = 0
            self._probes_succeeded = 0
        self._state = new
        if old is not new and self.on_transition is not None:
            self.on_transition(self, old, new)
