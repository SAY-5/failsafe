"""Load generator that counts client-visible outcomes while replicas are being killed.

Example:
    python -m chaos.run --target http://localhost:8080 --rps 150 --duration 45 \
        --metrics-url http://localhost:8080/metrics --kill-log chaos/out/kills.log
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import httpx
from prometheus_client.parser import text_string_to_metric_families

COUNTERS = {
    "failsafe_requests_total": ("status",),
    "failsafe_retries_total": ("reason",),
    "failsafe_failovers_total": (),
    "failsafe_breaker_transitions_total": ("to_state",),
    "failsafe_rate_limited_total": (),
    "failsafe_client_failed_requests_total": (),
}


@dataclass
class Outcomes:
    total: int = 0
    ok: int = 0
    rate_limited: int = 0
    failed: int = 0
    exceptions: int = 0
    latencies_ms: list[float] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)


async def scrape(client: httpx.AsyncClient, urls: list[str]) -> dict[str, float]:
    """Sum selected counters (grouped by a few labels) across every gateway instance."""
    totals: dict[str, float] = {}
    for url in urls:
        try:
            text = (await client.get(url, timeout=5)).text
        except httpx.HTTPError as exc:
            print(f"warning: could not scrape {url}: {exc}", file=sys.stderr)
            continue
        for family in text_string_to_metric_families(text):
            for s in family.samples:
                if s.name not in COUNTERS:
                    continue
                key = s.name
                for label in COUNTERS[s.name]:
                    key += f"{{{label}={s.labels.get(label, '')}}}"
                totals[key] = totals.get(key, 0.0) + s.value
    return totals


def delta(before: dict[str, float], after: dict[str, float]) -> dict[str, float]:
    return {k: after.get(k, 0.0) - before.get(k, 0.0) for k in after.keys() | before.keys()}


def pct(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    idx = min(len(values) - 1, max(0, round(p / 100 * (len(values) - 1))))
    return values[idx]


async def worker(
    client: httpx.AsyncClient, i: int, target: str, api_key: str, out: Outcomes, timeout: float
) -> None:
    started = time.perf_counter()
    status = 0
    try:
        if i % 10 < 3:
            r = await client.post(
                f"{target}/orders",
                json={"sku": f"sku-{i % 50}", "qty": 1},
                headers={"X-API-Key": api_key, "Idempotency-Key": f"chaos-{i}"},
                timeout=timeout,
            )
        else:
            r = await client.get(
                f"{target}/orders/{i % 1000}", headers={"X-API-Key": api_key}, timeout=timeout
            )
        status = r.status_code
    except httpx.HTTPError as exc:
        out.exceptions += 1
        out.failed += 1
        out.failures.append(f"{type(exc).__name__}: {exc}"[:120])
    finally:
        out.total += 1
        out.latencies_ms.append((time.perf_counter() - started) * 1000)
    if status == 0:
        return
    if 200 <= status < 300:
        out.ok += 1
    elif status == 429:
        out.rate_limited += 1
    else:
        out.failed += 1
        out.failures.append(f"HTTP {status}")


async def generate(args: argparse.Namespace) -> tuple[Outcomes, dict[str, float]]:
    out = Outcomes()
    limits = httpx.Limits(max_connections=args.concurrency + 8, max_keepalive_connections=64)
    async with httpx.AsyncClient(limits=limits) as client:
        before = await scrape(client, args.metrics_url)
        sem = asyncio.Semaphore(args.concurrency)
        tasks: set[asyncio.Task[None]] = set()
        interval = 1.0 / args.rps
        deadline = time.perf_counter() + args.duration
        next_at = time.perf_counter()
        i = 0
        last_report = time.perf_counter()

        async def guarded(n: int) -> None:
            async with sem:
                await worker(client, n, args.target, args.api_key, out, args.timeout)

        while time.perf_counter() < deadline:
            now = time.perf_counter()
            if now < next_at:
                await asyncio.sleep(next_at - now)
            next_at += interval
            t = asyncio.create_task(guarded(i))
            tasks.add(t)
            t.add_done_callback(tasks.discard)
            i += 1
            if now - last_report >= 5:
                last_report = now
                print(
                    f"  t+{int(args.duration - (deadline - now)):>3}s sent={i} ok={out.ok} "
                    f"failed={out.failed} inflight={len(tasks)}",
                    flush=True,
                )
        if tasks:
            await asyncio.gather(*tasks)
        await asyncio.sleep(0.2)
        after = await scrape(client, args.metrics_url)
    return out, delta(before, after)


def read_kill_log(path: str | None) -> list[str]:
    if not path or not Path(path).exists():
        return []
    return [line.rstrip() for line in Path(path).read_text().splitlines() if line.strip()]


def _by_label(m: dict[str, float], prefix: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for k, v in m.items():
        if k.startswith(prefix) and v:
            out[k[len(prefix) : -1]] = int(v)
    return out


def summarize(args: argparse.Namespace, out: Outcomes, m: dict[str, float]) -> dict:
    lat = out.latencies_ms
    kills = read_kill_log(args.kill_log)
    n_kills = sum(1 for k in kills if "\tkill\t" in k)
    retries = _by_label(m, "failsafe_retries_total{reason=")
    transitions = _by_label(m, "failsafe_breaker_transitions_total{to_state=")
    statuses = _by_label(m, "failsafe_requests_total{status=")
    return {
        "target": args.target,
        "duration_seconds": args.duration,
        "target_rps": args.rps,
        "requests": out.total,
        "successes": out.ok,
        "rate_limited": out.rate_limited,
        "client_failed_requests": out.failed,
        "client_exceptions": out.exceptions,
        "achieved_rps": round(out.total / args.duration, 1) if args.duration else 0,
        "latency_ms": {
            "p50": round(pct(lat, 50), 1),
            "p95": round(pct(lat, 95), 1),
            "p99": round(pct(lat, 99), 1),
            "max": round(max(lat), 1) if lat else 0.0,
            "mean": round(statistics.fmean(lat), 1) if lat else 0.0,
        },
        "gateway": {
            "requests_by_status": statuses,
            "retries": retries,
            "retries_total": sum(retries.values()),
            "failovers": int(m.get("failsafe_failovers_total", 0)),
            "breaker_transitions": transitions,
            "breaker_transitions_total": sum(transitions.values()),
            "client_failed_counter": int(m.get("failsafe_client_failed_requests_total", 0)),
        },
        "kills": n_kills,
        "kill_timeline": kills,
        "failure_samples": out.failures[:10],
    }


def print_summary(s: dict) -> None:
    g = s["gateway"]
    lat = s["latency_ms"]
    line = "=" * 64
    print(f"\n{line}\nFailSafe chaos summary\n{line}")
    print(f"target                        {s['target']}")
    print(
        f"duration / target rps         {s['duration_seconds']}s / {s['target_rps']} rps "
        f"(achieved {s['achieved_rps']} rps)"
    )
    print(f"total requests                {s['requests']}")
    print(f"successful (2xx)              {s['successes']}")
    print(f"rate limited (429)            {s['rate_limited']}")
    print(
        f"client-visible failed         {s['client_failed_requests']}"
        f"   <- must be 0 (gateway counter: {g['client_failed_counter']})"
    )
    print(f"retries (gateway)             {g['retries_total']}  {g['retries']}")
    print(f"failovers (gateway)           {g['failovers']}")
    print(
        f"breaker transitions           {g['breaker_transitions_total']}  "
        f"{g['breaker_transitions']}"
    )
    print(
        f"latency ms p50 / p95 / p99    {lat['p50']} / {lat['p95']} / {lat['p99']}  "
        f"(max {lat['max']})"
    )
    print(f"kills                         {s['kills']}")
    if s["kill_timeline"]:
        print("kill timeline (UTC):")
        for entry in s["kill_timeline"]:
            print(f"  {entry}")
    if s["failure_samples"]:
        print("failure samples:")
        for f in s["failure_samples"]:
            print(f"  {f}")
    print(line)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="FailSafe chaos load generator")
    p.add_argument("--target", default="http://localhost:8080")
    p.add_argument("--rps", type=float, default=150)
    p.add_argument("--duration", type=float, default=45)
    p.add_argument("--concurrency", type=int, default=64)
    p.add_argument("--timeout", type=float, default=10.0, help="client-side timeout per request")
    p.add_argument("--api-key", default="chaos")
    p.add_argument("--metrics-url", action="append", default=[])
    p.add_argument("--kill-log")
    p.add_argument("--json", help="write the summary to this file")
    p.add_argument("--fail-on-errors", action="store_true", help="exit 1 if any request failed")
    args = p.parse_args(argv)
    if not args.metrics_url:
        args.metrics_url = [args.target.rstrip("/") + "/metrics"]
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    print(
        f"load: {args.rps} rps for {args.duration}s against {args.target} "
        f"(concurrency {args.concurrency})",
        flush=True,
    )
    out, m = asyncio.run(generate(args))
    summary = summarize(args, out, m)
    print_summary(summary)
    if args.json:
        Path(args.json).parent.mkdir(parents=True, exist_ok=True)
        Path(args.json).write_text(json.dumps(summary, indent=2))
    keys = ("requests", "successes", "client_failed_requests", "kills")
    print("CHAOS_RESULT " + json.dumps({k: summary[k] for k in keys}), flush=True)
    if args.fail_on_errors and summary["client_failed_requests"] > 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
