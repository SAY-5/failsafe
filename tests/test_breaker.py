import pytest

from failsafe.breaker import CircuitBreaker, State


class FakeClock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


def make(**kw):
    clock = FakeClock()
    transitions = []
    cb = CircuitBreaker(
        "u1",
        clock=clock,
        on_transition=lambda _cb, old, new: transitions.append((old, new)),
        **kw,
    )
    return cb, clock, transitions


def test_starts_closed_and_allows():
    cb, _, _ = make()
    assert cb.state is State.CLOSED
    assert cb.allow()


def test_opens_on_consecutive_failures_and_fast_fails():
    cb, _, transitions = make(consecutive_failures=3, min_requests=100)
    for _ in range(2):
        cb.record_failure()
    assert cb.state is State.CLOSED
    cb.record_failure()
    assert cb.state is State.OPEN
    assert not cb.allow()
    assert transitions == [(State.CLOSED, State.OPEN)]


def test_opens_on_failure_ratio_only_after_min_requests():
    cb, _, _ = make(window=10, failure_ratio=0.5, min_requests=6, consecutive_failures=100)
    cb.record_failure()
    cb.record_failure()
    cb.record_failure()  # 3/3 failures but below min_requests
    assert cb.state is State.CLOSED
    cb.record_success()
    cb.record_success()
    assert cb.state is State.CLOSED  # 3/5
    cb.record_failure()  # 4/6 = 0.67 >= 0.5
    assert cb.state is State.OPEN


def test_sliding_window_forgets_old_failures():
    cb, _, _ = make(window=4, failure_ratio=0.75, min_requests=4, consecutive_failures=100)
    cb.record_failure()
    cb.record_failure()
    cb.record_failure()
    for _ in range(4):
        cb.record_success()
    cb.record_failure()  # window is [S,S,S,F]
    assert cb.state is State.CLOSED
    assert cb.failure_rate == pytest.approx(0.25)


def test_open_to_half_open_to_closed_cycle():
    cb, clock, transitions = make(consecutive_failures=2, open_seconds=5.0, half_open_max=2)
    cb.record_failure()
    cb.record_failure()
    assert cb.state is State.OPEN
    clock.advance(4.9)
    assert not cb.allow()
    assert cb.time_until_probe() == pytest.approx(0.1)
    clock.advance(0.1)
    assert cb.state is State.HALF_OPEN
    assert cb.allow()
    assert cb.allow()
    assert not cb.allow()  # only half_open_max probes admitted
    cb.record_success()
    assert cb.state is State.HALF_OPEN  # one probe is not enough
    cb.record_success()
    assert cb.state is State.CLOSED
    assert cb.allow()
    assert transitions == [
        (State.CLOSED, State.OPEN),
        (State.OPEN, State.HALF_OPEN),
        (State.HALF_OPEN, State.CLOSED),
    ]


def test_half_open_probe_failure_reopens_and_restarts_timer():
    cb, clock, transitions = make(consecutive_failures=1, open_seconds=2.0, half_open_max=1)
    cb.record_failure()
    clock.advance(2.0)
    assert cb.allow()
    cb.record_failure()
    assert cb.state is State.OPEN
    clock.advance(1.9)
    assert not cb.allow()
    clock.advance(0.1)
    assert cb.allow()
    assert transitions[-2:] == [(State.HALF_OPEN, State.OPEN), (State.OPEN, State.HALF_OPEN)]


def test_closing_resets_window_and_consecutive_count():
    cb, clock, _ = make(consecutive_failures=2, open_seconds=1.0, half_open_max=1)
    cb.record_failure()
    cb.record_failure()
    clock.advance(1.0)
    assert cb.allow()
    cb.record_success()
    assert cb.state is State.CLOSED
    cb.record_failure()
    assert cb.state is State.CLOSED  # needs two fresh consecutive failures again
    assert cb.failure_rate == pytest.approx(1.0)


def test_failures_while_open_do_not_extend_or_leak():
    cb, clock, transitions = make(consecutive_failures=1, open_seconds=1.0)
    cb.record_failure()
    clock.advance(0.5)
    cb.record_failure()  # late result from an in-flight call
    clock.advance(0.5)
    assert cb.state is State.HALF_OPEN
    assert len(transitions) == 2


def test_invalid_parameters():
    with pytest.raises(ValueError):
        CircuitBreaker(window=0)
    with pytest.raises(ValueError):
        CircuitBreaker(failure_ratio=0)
    with pytest.raises(ValueError):
        CircuitBreaker(failure_ratio=1.5)
