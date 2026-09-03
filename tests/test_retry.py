import random

import pytest

from failsafe.retry import FailureKind, RetryPolicy, is_idempotent


@pytest.mark.parametrize("method", ["GET", "HEAD", "PUT", "DELETE", "OPTIONS", "get"])
def test_safe_methods_are_idempotent(method):
    assert is_idempotent(method)


def test_post_without_key_is_not_idempotent():
    assert not is_idempotent("POST")
    assert not is_idempotent("POST", {"content-type": "application/json"})
    assert not is_idempotent("PATCH", {"Idempotency-Key": ""})


def test_post_with_key_or_route_flag_is_idempotent():
    assert is_idempotent("POST", {"Idempotency-Key": "abc"})
    assert is_idempotent("post", {"IDEMPOTENCY-KEY": "abc"})
    assert is_idempotent("POST", None, idempotent_post=True)


def test_attempts_are_bounded():
    p = RetryPolicy(max_attempts=3)
    assert p.should_retry(1, FailureKind.STATUS, idempotent=True)
    assert p.should_retry(2, FailureKind.STATUS, idempotent=True)
    assert not p.should_retry(3, FailureKind.STATUS, idempotent=True)
    assert not RetryPolicy(max_attempts=1).should_retry(1, FailureKind.CONNECT, idempotent=True)


def test_non_idempotent_only_retries_connect_failures():
    p = RetryPolicy(max_attempts=5)
    assert p.should_retry(1, FailureKind.CONNECT, idempotent=False)
    assert not p.should_retry(1, FailureKind.TIMEOUT, idempotent=False)
    assert not p.should_retry(1, FailureKind.READ, idempotent=False)
    assert not p.should_retry(1, FailureKind.STATUS, idempotent=False)


def test_retryable_status_set():
    p = RetryPolicy(retry_on_status=frozenset({502, 503}))
    assert p.retryable_status(503)
    assert not p.retryable_status(500)
    assert not p.retryable_status(404)


def test_backoff_is_exponential_with_full_jitter_and_capped():
    p = RetryPolicy(base_delay=0.1, max_delay=0.5)
    rng = random.Random(42)
    for attempt, ceiling in [(1, 0.1), (2, 0.2), (3, 0.4), (4, 0.5), (10, 0.5)]:
        samples = [p.backoff(attempt, rng) for _ in range(200)]
        assert all(0.0 <= s <= ceiling for s in samples), (attempt, max(samples))
        assert max(samples) > ceiling * 0.8  # jitter spans the range, not a constant


def test_backoff_is_deterministic_for_seeded_rng():
    p = RetryPolicy(base_delay=0.05, max_delay=1.0)
    a = [p.backoff(i, random.Random(7)) for i in range(1, 5)]
    b = [p.backoff(i, random.Random(7)) for i in range(1, 5)]
    assert a == b


def test_invalid_policy():
    with pytest.raises(ValueError):
        RetryPolicy(max_attempts=0)
    with pytest.raises(ValueError):
        RetryPolicy(base_delay=1.0, max_delay=0.5)
    with pytest.raises(ValueError):
        RetryPolicy().backoff(0)
