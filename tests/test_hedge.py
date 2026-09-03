import pytest

from failsafe.hedge import LatencyTracker, hedge_delay


def test_percentile_is_nearest_rank_over_the_window():
    t = LatencyTracker(window=100)
    assert t.percentile(95) is None
    for i in range(1, 101):
        t.record(i / 1000)
    assert t.percentile(50) == pytest.approx(0.051)  # rank round(0.5 * 99) = 50
    assert t.percentile(95) == pytest.approx(0.095)
    assert t.percentile(99) == pytest.approx(0.099)
    for _ in range(100):
        t.record(0.5)  # window slides: only slow samples remain
    assert t.percentile(50) == pytest.approx(0.5)
    assert len(t) == 100


def test_percentile_cache_refreshes():
    t = LatencyTracker(window=50)  # refresh every sample
    t.record(0.01)
    assert t.percentile(95) == pytest.approx(0.01)
    t.record(0.09)
    assert t.percentile(95) == pytest.approx(0.09)


def test_fixed_delay_wins_and_must_beat_the_attempt_timeout():
    t = LatencyTracker()
    kw = {"percentile": 95, "min_samples": 20, "attempt_timeout": 1.0}
    assert hedge_delay(t, after_ms=25, **kw) == pytest.approx(0.025)
    assert hedge_delay(t, after_ms=1000, **kw) is None


def test_adaptive_delay_needs_min_samples_then_tracks_percentile():
    t = LatencyTracker(window=100)
    kw = {"after_ms": None, "percentile": 95, "min_samples": 10, "attempt_timeout": 1.0}
    for _ in range(9):
        t.record(0.01)
    assert hedge_delay(t, **kw) is None
    t.record(0.04)
    assert hedge_delay(t, **kw) == pytest.approx(0.04)
    for _ in range(50):
        t.record(2.0)
    assert hedge_delay(t, **kw) is None  # p95 beyond the timeout: hedging is off


def test_rejects_bad_window():
    with pytest.raises(ValueError):
        LatencyTracker(window=0)
