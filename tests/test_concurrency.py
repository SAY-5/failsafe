import pytest

from failsafe.concurrency import AdaptiveLimiter


def make(**kw):
    updates = []
    lim = AdaptiveLimiter("u1", on_update=updates.append, **kw)
    return lim, updates


def at_load(lim: AdaptiveLimiter, rtt: float, calls: int) -> None:
    """Keep the limiter full while `calls` calls complete with the given RTT."""
    while lim.acquire():
        pass
    for _ in range(calls):
        lim.release(rtt)
        lim.acquire()
    while lim.inflight:
        lim.release(None)


def test_acquire_refuses_at_limit_and_release_frees():
    lim, updates = make(initial=2, min_limit=1)
    assert lim.acquire() and lim.acquire()
    assert not lim.acquire()
    assert lim.inflight == 2 and not lim.has_capacity()
    lim.release(0.01)
    assert lim.has_capacity() and lim.acquire()
    assert len(updates) == 4


def test_limit_grows_by_one_per_window_of_fast_calls_at_load():
    lim, _ = make(initial=4, min_limit=1, max_limit=10)
    at_load(lim, 0.01, 4)
    assert lim.limit == 5  # 4 fast calls at full load: +1/4 each
    at_load(lim, 0.01, 200)
    assert lim.limit == 10  # capped at max_limit


def test_limit_does_not_grow_while_idle():
    lim, _ = make(initial=10, min_limit=1)
    for _ in range(50):
        assert lim.acquire()
        lim.release(0.01)  # one call in flight against a limit of 10
    assert lim.limit == 10


def test_limit_shrinks_on_latency_spike():
    lim, _ = make(initial=20, min_limit=1, rtt_tolerance=2.0, backoff_ratio=0.5)
    at_load(lim, 0.010, 20)
    assert lim.no_load_rtt == pytest.approx(0.010)
    assert lim.limit == 21
    lim.acquire()
    lim.release(0.019)  # under 2x: still fine
    assert lim.limit == 21
    lim.acquire()
    lim.release(0.030)  # 3x the no-load RTT
    assert lim.limit == 10


def test_limit_shrinks_on_dropped_call_and_respects_min():
    lim, _ = make(initial=4, min_limit=3, backoff_ratio=0.5)
    lim.acquire()
    lim.release(None, dropped=True)
    assert lim.limit == 3
    lim.acquire()
    lim.release(None, dropped=True)
    assert lim.limit == 3


def test_connect_failure_release_leaves_limit_alone():
    lim, _ = make(initial=4, min_limit=1)
    lim.acquire()
    lim.release(None)
    assert lim.limit == 4 and lim.inflight == 0 and lim.no_load_rtt is None


def test_probe_interval_lets_no_load_rtt_rise():
    lim, _ = make(initial=4, min_limit=1, probe_interval=5, rtt_tolerance=2.0)
    for _ in range(4):
        lim.acquire()
        lim.release(0.010)
    assert lim.no_load_rtt == pytest.approx(0.010)
    lim.acquire()
    lim.release(0.050)  # fifth sample is a probe: becomes the new baseline
    assert lim.no_load_rtt == pytest.approx(0.050)
    assert lim.limit == 4  # a probe sample never counts as a spike


def test_retry_after_tracks_no_load_rtt():
    lim, _ = make(initial=2, min_limit=1)
    assert lim.retry_after() == pytest.approx(0.1)
    lim.acquire()
    lim.release(0.4)
    assert lim.retry_after() == pytest.approx(0.4)


def test_rejects_bad_parameters():
    with pytest.raises(ValueError):
        AdaptiveLimiter(initial=0, min_limit=1)
    with pytest.raises(ValueError):
        AdaptiveLimiter(initial=5, min_limit=1, max_limit=4)
    with pytest.raises(ValueError):
        AdaptiveLimiter(backoff_ratio=1.0)
    with pytest.raises(ValueError):
        AdaptiveLimiter(rtt_tolerance=0.5)
