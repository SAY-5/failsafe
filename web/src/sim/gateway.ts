/**
 * Forwarding of requests with retries, backoff and replica failover.
 * Port of failsafe/proxy.py, rewritten as an event-driven engine on a virtual clock:
 * an attempt is planned when it starts and resolved when the clock reaches its end,
 * so a kill that lands while an attempt is in flight turns it into a transport error.
 */
import { Metrics } from "./metrics";
import type { Clock, Prng } from "./prng";
import { RateLimiter, retryAfterHeader } from "./ratelimit";
import { RetryPolicy, isIdempotent, type FailureKind } from "./retry";
import { Replica, UpstreamPool } from "./upstreams";

export interface RouteConfig {
  /** Per attempt, seconds. */
  timeoutSeconds: number;
  connectTimeoutSeconds: number;
  idempotentPost: boolean;
}

export const DEFAULT_ROUTE: RouteConfig = {
  timeoutSeconds: 2,
  connectTimeoutSeconds: 0.5,
  idempotentPost: false,
};

export interface RequestSpec {
  id: number;
  method: string;
  path: string;
  headers?: Record<string, string>;
  clientKey: string;
}

export type AttemptOutcome = "success" | FailureKind | "refused";

export interface Attempt {
  n: number;
  replica: string;
  startedAt: number;
  endedAt: number;
  outcome: AttemptOutcome;
  status: number | null;
  /** Backoff slept after this attempt before the next one, seconds. */
  backoff: number;
  backoffCeiling: number;
  failedOverFrom: string | null;
}

export interface RequestResult {
  id: number;
  method: string;
  path: string;
  idempotent: boolean;
  status: number;
  /** End-to-end, including retries, milliseconds. */
  latencyMs: number;
  attempts: Attempt[];
  arrivedAt: number;
  completedAt: number;
  servedBy: string | null;
  retryAfter: string | null;
  detail: string | null;
}

interface InFlight {
  spec: RequestSpec;
  idempotent: boolean;
  arrivedAt: number;
  attempts: Attempt[];
  tried: Set<string>;
  prev: Replica | null;
  attempt: number;
  phase: "attempt" | "sleeping";
  replica: Replica | null;
  current: Attempt | null;
  planned: { outcome: AttemptOutcome; status: number | null };
  wakeAt: number;
}

/** Latency an upstream needs to answer one attempt, in seconds. */
export type LatencyModel = (rng: Prng) => number;

/** p50 around 3 ms with a modest tail, like the example upstream behind the gateway. */
export const defaultLatency: LatencyModel = (rng) => {
  const base = 0.0022 + rng.uniform(0, 0.0016);
  const tail = rng.next() < 0.08 ? rng.uniform(0.002, 0.005) : 0;
  return base + tail;
};

/** Time to learn that nobody is listening on a dead replica, in seconds. */
const CONNECT_REFUSED = 0.0003;
/** Time the connect handshake takes on a live replica, in seconds. */
const CONNECT_PHASE = 0.0004;

export interface GatewayOptions {
  route?: Partial<RouteConfig>;
  latency?: LatencyModel;
  limiter?: RateLimiter | null;
}

export class Gateway {
  readonly route: RouteConfig;
  readonly limiter: RateLimiter | null;
  readonly metrics: Metrics;
  private readonly latency: LatencyModel;
  private inflight: InFlight[] = [];
  onResult: ((r: RequestResult) => void) | undefined;

  constructor(
    readonly pool: UpstreamPool,
    readonly policy: RetryPolicy,
    private readonly rng: Prng,
    private readonly clock: Clock,
    opts: GatewayOptions = {},
  ) {
    this.route = { ...DEFAULT_ROUTE, ...opts.route };
    this.latency = opts.latency ?? defaultLatency;
    this.limiter = opts.limiter ?? null;
    this.metrics = new Metrics();
  }

  get inflightCount(): number {
    return this.inflight.length;
  }

  get inflightView(): readonly { id: number; replica: string | null; phase: string }[] {
    return this.inflight.map((f) => ({ id: f.spec.id, replica: f.replica?.label ?? null, phase: f.phase }));
  }

  /** Time of the next attempt end or backoff wake, or Infinity when idle. */
  nextEventAt(): number {
    let t = Infinity;
    for (const f of this.inflight) if (f.wakeAt < t) t = f.wakeAt;
    return t;
  }

  /** Accept one client request at the current virtual time. */
  submit(spec: RequestSpec): void {
    const now = this.clock();
    const method = spec.method.toUpperCase();
    const idempotent = isIdempotent(method, spec.headers, this.route.idempotentPost);
    if (this.limiter) {
      const d = this.limiter.check(spec.clientKey);
      if (!d.allowed) {
        this.finish(
          {
            spec,
            idempotent,
            arrivedAt: now,
            attempts: [],
            tried: new Set(),
            prev: null,
            attempt: 0,
            phase: "attempt",
            replica: null,
            current: null,
            planned: { outcome: "refused", status: 429 },
            wakeAt: now,
          },
          429,
          null,
          retryAfterHeader(d.retryAfter),
          "rate limited",
        );
        return;
      }
    }
    const f: InFlight = {
      spec,
      idempotent,
      arrivedAt: now,
      attempts: [],
      tried: new Set(),
      prev: null,
      attempt: 0,
      phase: "attempt",
      replica: null,
      current: null,
      planned: { outcome: "success", status: 200 },
      wakeAt: now,
    };
    this.metrics.inflight += 1;
    this.inflight.push(f);
    this.beginAttempt(f);
  }

  /** Resolve every attempt end and backoff wake that is due at the current time. */
  advance(): void {
    const now = this.clock();
    let progressed = true;
    while (progressed) {
      progressed = false;
      for (const f of [...this.inflight]) {
        if (f.wakeAt > now || !this.inflight.includes(f)) continue;
        progressed = true;
        if (f.phase === "sleeping") this.beginAttempt(f);
        else this.endAttempt(f, f.planned.outcome, f.planned.status);
      }
    }
  }

  /** SIGKILL a replica: attempts in flight to it fail right now. */
  kill(replica: Replica): void {
    const now = this.clock();
    replica.alive = false;
    replica.killedAt = now;
    for (const f of [...this.inflight]) {
      if (f.phase !== "attempt" || f.replica !== replica) continue;
      const kind: FailureKind = now - f.current!.startedAt < CONNECT_PHASE ? "connect" : "read";
      this.endAttempt(f, kind, null);
    }
  }

  restart(replica: Replica): void {
    replica.alive = true;
    replica.killedAt = null;
    replica.startedAt = this.clock();
  }

  private beginAttempt(f: InFlight): void {
    const now = this.clock();
    let replica = this.pool.pick(f.tried);
    if (replica === null && f.tried.size > 0) replica = this.pool.pick();
    if (replica === null) {
      this.metrics.inflight -= 1;
      this.finish(f, 503, f.prev, null, "no healthy upstream replica");
      return;
    }
    f.attempt += 1;
    f.tried.add(replica.label);
    f.phase = "attempt";
    f.replica = replica;
    let failedOverFrom: string | null = null;
    if (f.prev !== null && f.prev !== replica) {
      this.metrics.recordFailover();
      failedOverFrom = f.prev.label;
    }
    const plan = this.plan(replica);
    f.planned = { outcome: plan.outcome, status: plan.status };
    f.current = {
      n: f.attempt,
      replica: replica.label,
      startedAt: now,
      endedAt: now + plan.duration,
      outcome: plan.outcome,
      status: plan.status,
      backoff: 0,
      backoffCeiling: 0,
      failedOverFrom,
    };
    f.wakeAt = now + plan.duration;
  }

  private plan(replica: Replica): { outcome: AttemptOutcome; status: number | null; duration: number } {
    if (!replica.alive) return { outcome: "connect", status: null, duration: CONNECT_REFUSED };
    if (replica.hang) {
      return { outcome: "timeout", status: null, duration: this.route.timeoutSeconds };
    }
    const duration = CONNECT_PHASE + this.latency(this.rng);
    if (replica.failStatus !== null) return { outcome: "status", status: replica.failStatus, duration };
    return { outcome: "success", status: 200, duration };
  }

  private endAttempt(f: InFlight, outcome: AttemptOutcome, status: number | null): void {
    const now = this.clock();
    const replica = f.replica!;
    const a = f.current!;
    a.endedAt = now;
    a.outcome = outcome;
    a.status = status;
    f.attempts.push(a);
    f.current = null;

    if (outcome === "success") {
      this.pool.reportSuccess(replica);
      this.metrics.inflight -= 1;
      this.finish(f, status ?? 200, replica, null, null);
      return;
    }
    if (outcome === "status") {
      this.pool.reportFailure(replica, false);
      if (this.policy.shouldRetry(f.attempt, "status", f.idempotent)) {
        this.scheduleRetry(f, "status", a);
        return;
      }
      this.metrics.inflight -= 1;
      this.finish(f, status ?? 502, replica, null, `upstream answered ${status}`);
      return;
    }
    if (outcome === "refused") return;
    const kind: FailureKind = outcome;
    this.pool.reportFailure(replica, kind === "connect");
    if (this.policy.shouldRetry(f.attempt, kind, f.idempotent)) {
      this.scheduleRetry(f, kind, a);
      return;
    }
    this.metrics.inflight -= 1;
    this.finish(f, kind === "timeout" ? 504 : 502, replica, null, `${kind}: ${replica.label} did not answer`);
  }

  private scheduleRetry(f: InFlight, kind: FailureKind, last: Attempt): void {
    this.metrics.recordRetry(kind);
    const delay = this.policy.backoff(f.attempt, this.rng);
    last.backoff = delay;
    last.backoffCeiling = this.policy.ceiling(f.attempt);
    f.prev = f.replica;
    f.replica = null;
    f.phase = "sleeping";
    f.wakeAt = this.clock() + delay;
  }

  private finish(
    f: InFlight,
    status: number,
    replica: Replica | null,
    retryAfter: string | null,
    detail: string | null,
  ): void {
    const now = this.clock();
    const idx = this.inflight.indexOf(f);
    if (idx >= 0) this.inflight.splice(idx, 1);
    const latencyMs = (now - f.arrivedAt) * 1000;
    this.metrics.recordRequest(status, latencyMs);
    this.onResult?.({
      id: f.spec.id,
      method: f.spec.method.toUpperCase(),
      path: f.spec.path,
      idempotent: f.idempotent,
      status,
      latencyMs,
      attempts: f.attempts,
      arrivedAt: f.arrivedAt,
      completedAt: now,
      servedBy: status < 500 && status !== 429 ? (replica?.label ?? null) : null,
      retryAfter,
      detail,
    });
  }
}
