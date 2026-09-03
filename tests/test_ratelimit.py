import threading

import pytest

from failsafe.ratelimit import RateLimiter, TokenBucket, retry_after_header


class FakeClock:
    def __init__(self, t: float = 0.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


def test_bucket_starts_full_and_drains_to_zero():
    clock = FakeClock()
    b = TokenBucket(capacity=3, refill_rate=1.0, clock=clock)
    assert [b.try_acquire().allowed for _ in range(3)] == [True, True, True]
    d = b.try_acquire()
    assert not d.allowed
    assert d.remaining == 0
    assert d.retry_after == pytest.approx(1.0)


def test_refill_is_exact_and_capped_at_capacity():
    clock = FakeClock()
    b = TokenBucket(capacity=10, refill_rate=4.0, clock=clock)
    for _ in range(10):
        assert b.try_acquire().allowed
    assert b.tokens == 0
    clock.advance(0.5)  # 2 tokens
    assert b.tokens == pytest.approx(2.0)
    assert b.try_acquire().allowed
    assert b.try_acquire().allowed
    assert not b.try_acquire().allowed
    clock.advance(100)  # far beyond capacity
    assert b.tokens == pytest.approx(10.0)


def test_retry_after_reflects_deficit():
    clock = FakeClock()
    b = TokenBucket(capacity=1, refill_rate=0.5, clock=clock)  # one token every 2s
    assert b.try_acquire().allowed
    d = b.try_acquire()
    assert d.retry_after == pytest.approx(2.0)
    clock.advance(1.5)
    d = b.try_acquire()
    assert not d.allowed
    assert d.retry_after == pytest.approx(0.5)
    clock.advance(0.5)
    assert b.try_acquire().allowed


def test_fractional_cost_and_partial_refill():
    clock = FakeClock()
    b = TokenBucket(capacity=2, refill_rate=10.0, clock=clock)
    assert b.try_acquire(1.5).allowed
    assert b.try_acquire(0.5).allowed
    assert not b.try_acquire(0.5).allowed
    clock.advance(0.05)  # 0.5 token
    assert b.try_acquire(0.5).allowed


def test_invalid_parameters_rejected():
    with pytest.raises(ValueError):
        TokenBucket(0, 1.0)
    with pytest.raises(ValueError):
        TokenBucket(1, 0.0)
    with pytest.raises(ValueError):
        TokenBucket(1, 1.0).try_acquire(0)


def test_bucket_is_thread_safe_under_contention():
    b = TokenBucket(capacity=1000, refill_rate=0.0001)
    allowed = []
    lock = threading.Lock()

    def worker():
        n = sum(1 for _ in range(500) if b.try_acquire().allowed)
        with lock:
            allowed.append(n)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sum(allowed) == 1000


def test_limiter_isolates_keys():
    clock = FakeClock()
    rl = RateLimiter(capacity=2, refill_rate=1.0, clock=clock)
    assert rl.check("a").allowed and rl.check("a").allowed
    assert not rl.check("a").allowed
    assert rl.check("b").allowed
    assert len(rl) == 2


def test_limiter_evicts_idle_keys_when_full():
    clock = FakeClock()
    rl = RateLimiter(capacity=1, refill_rate=1.0, clock=clock, max_keys=4, idle_seconds=10)
    for k in "abcd":
        rl.check(k)
    clock.advance(11)
    rl.check("e")
    assert len(rl) == 1  # a-d were idle and got evicted before inserting e


def test_limiter_evicts_lru_quarter_when_nothing_idle():
    clock = FakeClock()
    rl = RateLimiter(capacity=1, refill_rate=1.0, clock=clock, max_keys=4, idle_seconds=10)
    for k in "abcd":
        rl.check(k)
        clock.advance(1)
    rl.check("e")
    assert len(rl) == 4
    assert "a" not in rl._buckets


def test_retry_after_header_rounds_up_to_whole_seconds():
    assert retry_after_header(0.01) == "1"
    assert retry_after_header(1.0) == "1"
    assert retry_after_header(1.2) == "2"
