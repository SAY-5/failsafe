# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:0.4.30 /uv /bin/uv
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
COPY pyproject.toml README.md LICENSE ./
COPY failsafe ./failsafe
COPY example_upstream ./example_upstream
COPY chaos ./chaos
RUN uv venv /app/.venv && uv pip install --python /app/.venv/bin/python --no-cache .

FROM python:3.12-slim AS runtime
RUN groupadd --system --gid 10001 failsafe \
 && useradd --system --uid 10001 --gid failsafe --home /app --shell /usr/sbin/nologin failsafe
WORKDIR /app
COPY --from=builder --chown=failsafe:failsafe /app/.venv /app/.venv
COPY --from=builder --chown=failsafe:failsafe /app/failsafe /app/failsafe
COPY --from=builder --chown=failsafe:failsafe /app/example_upstream /app/example_upstream
COPY --from=builder --chown=failsafe:failsafe /app/chaos /app/chaos
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
USER failsafe
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=2s --retries=3 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=1).status == 200 else 1)"
ENTRYPOINT ["python", "-m", "failsafe"]
