/**
 * The chaos run: a fixed request rate through the gateway while replicas are
 * SIGKILLed and restarted. Mirrors chaos/run.py + chaos/kill.sh on a virtual clock.
 */
import { Gateway, type LatencyModel, type RequestResult } from "./gateway";
import { percentiles } from "./metrics";
import { Prng, VirtualClock } from "./prng";
import { RateLimiter } from "./ratelimit";
import { RetryPolicy } from "./retry";
import { HealthChecker, UpstreamPool, type HealthCheckConfig, DEFAULT_HEALTH } from "./upstreams";
import type { BreakerOptions } from "./breaker";

export interface ChaosConfig {
  seed: number;
  durationSeconds: number;
  rps: number;
  replicas: number;
  /** Auto-chaos: seconds between kills, drawn uniformly from this range. */
  killIntervalMin: number;
  killIntervalMax: number;
  /** Seconds a killed replica stays down before it is restarted. */
  restartAfter: number;
  /** Share of traffic that is POST with an Idempotency-Key, like chaos/run.py. */
  postShare: number;
  rateLimit: { capacity: number; refillPerSecond: number } | null;
  retry: { maxAttempts: number; baseDelayMs: number; maxDelayMs: number; idempotentPost: boolean };
  breaker: BreakerOptions;
  health: HealthCheckConfig;
  latency?: LatencyModel;
}

/** routes.yaml defaults for the /orders route and the compose chaos target. */
export const DEFAULT_CHAOS: ChaosConfig = {
  seed: 20260903,
  durationSeconds: 45,
  rps: 150,
  replicas: 3,
  killIntervalMin: 8,
  killIntervalMax: 13,
  restartAfter: 3,
  postShare: 0.25,
  rateLimit: { capacity: 200, refillPerSecond: 400 },
  retry: { maxAttempts: 4, baseDelayMs: 10, maxDelayMs: 150, idempotentPost: false },
  breaker: { window: 20, failureRatio: 0.5, minRequests: 5, consecutiveFailures: 3, openSeconds: 3, halfOpenMax: 2 },
  health: DEFAULT_HEALTH,
};

export type TimelineKind = "kill" | "start" | "unhealthy" | "healthy" | "breaker";

export interface TimelineEvent {
  at: number;
  kind: TimelineKind;
  replica: string;
  note?: string;
}

export interface LatencyBucket {
  second: number;
  count: number;
  p50: number;
  p95: number;
  p99: number;
}

export class ChaosRun {
  readonly clock = new VirtualClock();
  readonly rng: Prng;
  readonly pool: UpstreamPool;
  readonly gateway: Gateway;
  readonly checker: HealthChecker;
  readonly timeline: TimelineEvent[] = [];
  readonly results: RequestResult[] = [];
  readonly buckets: LatencyBucket[] = [];
  /** Requests whose lifecycle included a retry, most recent last. */
  readonly retried: RequestResult[] = [];
  kills = 0;
  autoChaos = false;
  private nextArrival = 0;
  private nextKill = Infinity;
  private pendingRestarts: { at: number; label: string }[] = [];
  private nextId = 1;
  private bucketSamples: number[] = [];
  private bucketSecond = 0;

  constructor(readonly cfg: ChaosConfig = DEFAULT_CHAOS) {
    this.rng = new Prng(cfg.seed);
    this.pool = new UpstreamPool(
      Array.from({ length: cfg.replicas }, (_, i) => `upstream-${i + 1}`),
      this.clock.now,
      cfg.breaker,
      cfg.health,
      (name, from, to) => {
        this.gateway.metrics.recordTransition(this.clock.now(), name, from, to);
        this.timeline.push({ at: this.clock.now(), kind: "breaker", replica: name, note: `${from} -> ${to}` });
      },
    );
    this.pool.onHealth = (r, healthy, at) => {
      this.timeline.push({ at, kind: healthy ? "healthy" : "unhealthy", replica: r.label });
    };
    const policy = new RetryPolicy({
      maxAttempts: cfg.retry.maxAttempts,
      baseDelay: cfg.retry.baseDelayMs / 1000,
      maxDelay: cfg.retry.maxDelayMs / 1000,
    });
    const limiter = cfg.rateLimit
      ? new RateLimiter(cfg.rateLimit.capacity, cfg.rateLimit.refillPerSecond, this.clock.now)
      : null;
    this.gateway = new Gateway(this.pool, policy, this.rng, this.clock.now, {
      route: { idempotentPost: cfg.retry.idempotentPost },
      latency: cfg.latency,
      limiter,
    });
    this.gateway.onResult = (r) => this.onResult(r);
    this.checker = new HealthChecker(this.pool, this.clock.now);
    this.checker.start();
  }

  get now(): number {
    return this.clock.now();
  }

  get finished(): boolean {
    return this.now >= this.cfg.durationSeconds && this.gateway.inflightCount === 0;
  }

  get arrivalsDone(): boolean {
    return this.now >= this.cfg.durationSeconds;
  }

  get snapshot() {
    return this.gateway.metrics.snapshot();
  }

  startAutoChaos(): void {
    this.autoChaos = true;
    if (this.nextKill === Infinity) this.scheduleKill(2 + this.rng.uniform(0, 3));
  }

  stopAutoChaos(): void {
    this.autoChaos = false;
    this.nextKill = Infinity;
  }

  private scheduleKill(delay: number): void {
    this.nextKill = this.now + delay;
  }

  /** Kill a specific replica (or a random live one) right now, like kill.sh. */
  kill(label?: string): string | null {
    const live = this.pool.replicas.filter((r) => r.alive);
    if (live.length <= 1) return null;
    const target = label ? live.find((r) => r.label === label) : this.rng.pick(live);
    if (!target) return null;
    this.gateway.kill(target);
    this.kills += 1;
    this.timeline.push({ at: this.now, kind: "kill", replica: target.label });
    this.pendingRestarts.push({ at: this.now + this.cfg.restartAfter, label: target.label });
    return target.label;
  }

  private restart(label: string): void {
    const r = this.pool.get(label);
    if (!r || r.alive) return;
    this.gateway.restart(r);
    this.timeline.push({ at: this.now, kind: "start", replica: label });
  }

  private nextEventAt(): number {
    let t = this.gateway.nextEventAt();
    if (this.now < this.cfg.durationSeconds && this.nextArrival < t) t = this.nextArrival;
    if (this.nextKill < t) t = this.nextKill;
    for (const p of this.pendingRestarts) if (p.at < t) t = p.at;
    const nextSecond = Math.floor(this.now) + 1;
    if (nextSecond < t) t = nextSecond;
    return t;
  }

  /** Advance the virtual clock by `seconds`, resolving every event in order. */
  step(seconds: number): void {
    const target = this.now + seconds;
    for (;;) {
      const t = this.nextEventAt();
      if (t > target) break;
      this.clock.set(t);
      this.dispatch();
    }
    this.clock.set(target);
    this.dispatch();
  }

  /** Run to completion synchronously (used by the self-check). */
  runAll(): void {
    while (!this.finished) this.step(1);
  }

  private dispatch(): void {
    const now = this.now;
    if (Math.floor(now) !== this.bucketSecond) this.flushBucket(Math.floor(now));
    this.checker.tick();
    for (const p of [...this.pendingRestarts]) {
      if (p.at <= now) {
        this.restart(p.label);
        this.pendingRestarts = this.pendingRestarts.filter((x) => x !== p);
      }
    }
    if (this.autoChaos && now >= this.nextKill) {
      if (now < this.cfg.durationSeconds - this.cfg.restartAfter) this.kill();
      this.scheduleKill(this.rng.uniform(this.cfg.killIntervalMin, this.cfg.killIntervalMax));
    }
    while (this.nextArrival <= now && this.nextArrival < this.cfg.durationSeconds) {
      this.submitOne();
      this.nextArrival += 1 / this.cfg.rps;
    }
    this.gateway.advance();
  }

  private submitOne(): void {
    const id = this.nextId++;
    const isPost = this.rng.next() < this.cfg.postShare;
    this.gateway.submit({
      id,
      method: isPost ? "POST" : "GET",
      path: isPost ? "/orders" : `/orders/${1 + this.rng.int(500)}`,
      headers: isPost ? { "Idempotency-Key": `chaos-${id}` } : undefined,
      clientKey: "key:chaos",
    });
  }

  private onResult(r: RequestResult): void {
    this.results.push(r);
    this.bucketSamples.push(r.latencyMs);
    if (r.attempts.length > 1) {
      this.retried.push(r);
      if (this.retried.length > 40) this.retried.shift();
    }
  }

  private flushBucket(second: number): void {
    if (this.bucketSamples.length > 0) {
      const p = percentiles(this.bucketSamples);
      this.buckets.push({ second: this.bucketSecond, count: this.bucketSamples.length, ...p });
    }
    this.bucketSamples = [];
    this.bucketSecond = second;
  }
}
