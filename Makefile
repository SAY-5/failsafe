PY ?= .venv/bin/python
UV ?= uv

.PHONY: setup lint fmt test image chaos demo k8s-chaos clean

setup:
	$(UV) venv --clear --python 3.12 .venv
	$(UV) pip install --python .venv/bin/python -e ".[dev]"

lint:
	.venv/bin/ruff check .
	.venv/bin/ruff format --check .

fmt:
	.venv/bin/ruff format .
	.venv/bin/ruff check --fix .

test:
	.venv/bin/pytest -q

image:
	docker buildx build --load -t failsafe:dev .

chaos:
	PY=$(PY) scripts/compose-chaos.sh

demo: chaos

k8s-chaos:
	scripts/k8s-chaos.sh

clean:
	docker compose -f deploy/docker-compose.yml down -t 2 || true
