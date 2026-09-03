/** Counters mirroring the gateway's Prometheus metrics. Port of failsafe/metrics.py. */
import type { BreakerState } from "./breaker";
import type { FailureKind } from "./retry";

export interface Transition {
  at: number;
  upstream: string;
  from: BreakerState;
  to: BreakerState;
}

export class Metrics {
  requestsTotal = 0;
  successTotal = 0;
  rateLimitedTotal = 0;
  /** 5xx returned to a client after exhausting retries; must stay 0 in chaos. */
  clientFailedTotal = 0;
  retriesTotal = 0;
  retriesByKind: Record<FailureKind, number> = { connect: 0, timeout: 0, read: 0, status: 0 };
  failoversTotal = 0;
  breakerTransitions: Transition[] = [];
  breakerState = new Map<string, BreakerState>();
  upstreamHealthy = new Map<string, boolean>();
  inflight = 0;
  /** End-to-end latency samples in milliseconds, including retries. */
  latencies: number[] = [];

  recordRequest(status: number, latencyMs: number): void {
    this.requestsTotal += 1;
    if (status >= 200 && status < 300) this.successTotal += 1;
    if (status === 429) this.rateLimitedTotal += 1;
    if (status >= 500) this.clientFailedTotal += 1;
    this.latencies.push(latencyMs);
  }

  recordRetry(kind: FailureKind): void {
    this.retriesTotal += 1;
    this.retriesByKind[kind] += 1;
  }

  recordFailover(): void {
    this.failoversTotal += 1;
  }

  recordTransition(at: number, upstream: string, from: BreakerState, to: BreakerState): void {
    this.breakerTransitions.push({ at, upstream, from, to });
    this.breakerState.set(upstream, to);
  }

  snapshot(): MetricsSnapshot {
    return {
      requests: this.requestsTotal,
      success: this.successTotal,
      rateLimited: this.rateLimitedTotal,
      clientFailed: this.clientFailedTotal,
      retries: this.retriesTotal,
      retriesByKind: { ...this.retriesByKind },
      failovers: this.failoversTotal,
      breakerTransitions: this.breakerTransitions.length,
      inflight: this.inflight,
      ...percentiles(this.latencies),
    };
  }
}

export interface MetricsSnapshot {
  requests: number;
  success: number;
  rateLimited: number;
  clientFailed: number;
  retries: number;
  retriesByKind: Record<FailureKind, number>;
  failovers: number;
  breakerTransitions: number;
  inflight: number;
  p50: number;
  p95: number;
  p99: number;
  max: number;
}

export function percentiles(samples: readonly number[]): { p50: number; p95: number; p99: number; max: number } {
  if (samples.length === 0) return { p50: 0, p95: 0, p99: 0, max: 0 };
  const sorted = [...samples].sort((a, b) => a - b);
  const at = (q: number) => sorted[Math.min(sorted.length - 1, Math.floor(q * (sorted.length - 1)))];
  return { p50: at(0.5), p95: at(0.95), p99: at(0.99), max: sorted[sorted.length - 1] };
}
