import pytest

from failsafe.config import from_dict, load


def base(**route):
    return {
        "upstreams": {"svc": {"replicas": ["http://a:1", "http://b:1"]}},
        "routes": [{"prefix": "/api", "upstream": "svc", **route}],
    }


def test_defaults_and_overrides():
    cfg = from_dict(base(timeout_seconds=1.5, connect_timeout_seconds=0.2, rate_limit=None))
    r = cfg.routes[0]
    assert r.timeout_seconds == 1.5
    assert r.connect_timeout_seconds == 0.2
    assert r.rate_limit is None
    assert r.retry.max_attempts == 3
    assert r.breaker.open_seconds == 5.0
    assert cfg.upstreams["svc"].replicas == ("http://a:1", "http://b:1")


def test_longest_prefix_match():
    cfg = from_dict(
        {
            "upstreams": {"svc": {"replicas": ["http://a:1"]}},
            "routes": [
                {"prefix": "/", "upstream": "svc"},
                {"prefix": "/api", "upstream": "svc"},
                {"prefix": "/api/v2", "upstream": "svc"},
            ],
        }
    )
    assert cfg.match("/api/v2/x").prefix == "/api/v2"
    assert cfg.match("/api/v22").prefix == "/api"
    assert cfg.match("/apiary").prefix == "/"
    assert cfg.match("/api").prefix == "/api"


def test_unknown_upstream_and_bad_values_rejected():
    with pytest.raises(ValueError, match="unknown upstream"):
        from_dict({"upstreams": {}, "routes": [{"prefix": "/x", "upstream": "nope"}]})
    with pytest.raises(ValueError):
        from_dict(base(timeout_seconds=0))
    with pytest.raises(ValueError):
        from_dict(base(prefix="nope"))
    with pytest.raises(ValueError):
        from_dict(base(retry={"max_attempts": 0}))
    with pytest.raises(ValueError):
        from_dict(base(breaker={"failure_ratio": 2}))


def test_concurrency_is_optional_and_validated():
    assert from_dict(base()).routes[0].concurrency is None
    cfg = from_dict(base(concurrency={"initial": 5, "min_limit": 1, "max_limit": 50}))
    c = cfg.routes[0].concurrency
    assert (c.initial, c.min_limit, c.max_limit) == (5, 1, 50)
    assert c.backoff_ratio == 0.9 and c.rtt_tolerance == 2.0
    with pytest.raises(ValueError):
        from_dict(base(concurrency={"initial": 0}))
    with pytest.raises(ValueError):
        from_dict(base(concurrency={"min_limit": 10, "max_limit": 5}))
    with pytest.raises(ValueError):
        from_dict(base(concurrency={"backoff_ratio": 1.0}))
    with pytest.raises(ValueError):
        from_dict(base(concurrency={"rtt_tolerance": 1.0}))


def test_env_expansion_and_dns_upstream(monkeypatch):
    monkeypatch.setenv("SVC_HOST", "svc.ns.svc.cluster.local")
    cfg = from_dict(
        {
            "upstreams": {"svc": {"dns": "${SVC_HOST}", "port": 9000}},
            "routes": [{"prefix": "/x", "upstream": "svc"}],
        }
    )
    assert cfg.upstreams["svc"].dns == "svc.ns.svc.cluster.local"
    assert cfg.upstreams["svc"].port == 9000


def test_bundled_routes_file_loads():
    cfg = load()
    assert cfg.routes[0].prefix == "/orders"
    assert len(cfg.upstreams["orders"].replicas) == 3
