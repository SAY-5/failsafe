# Contributing

```bash
make setup     # create .venv with uv and install dev dependencies
make lint      # ruff check + format check
make test      # pytest
make chaos     # compose chaos run (needs docker)
make k8s-chaos # kind chaos run (needs docker, kind, kubectl)
```

Guidelines:

* Keep modules single-purpose; `ratelimit`, `breaker`, `retry` and
  `upstreams` must stay importable without FastAPI.
* Every behaviour change needs a test. Time-dependent code takes an injectable
  clock so tests never sleep to observe refill or breaker timers.
* New metrics go in `failsafe/metrics.py` and get a row in the README table.
* Commit messages follow `type: summary` (feat, fix, docs, test, build, ci,
  deploy, chore).
* Run `make lint test` before pushing; CI also runs both chaos suites.
