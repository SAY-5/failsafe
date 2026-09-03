/** Circuit breaker with closed / open / half-open states. Port of failsafe/breaker.py. */
import type { Clock } from "./prng";

export type BreakerState = "closed" | "open" | "half_open";

export type TransitionHook = (breaker: CircuitBreaker, from: BreakerState, to: BreakerState) => void;

export interface BreakerOptions {
  window?: number;
  failureRatio?: number;
  minRequests?: number;
  consecutiveFailures?: number;
  openSeconds?: number;
  halfOpenMax?: number;
  onTransition?: TransitionHook;
}

export class CircuitBreaker {
  readonly window: number;
  readonly failureRatio: number;
  readonly minRequests: number;
  readonly consecutiveFailures: number;
  readonly openSeconds: number;
  readonly halfOpenMax: number;
  onTransition: TransitionHook | undefined;

  private current: BreakerState = "closed";
  /** Sliding window of outcomes, true == failure, bounded to `window`. */
  private outcomes: boolean[] = [];
  private consecutive = 0;
  private openedAt = 0;
  private probesInFlight = 0;
  private probesSucceeded = 0;

  constructor(
    readonly name: string,
    private readonly clock: Clock,
    opts: BreakerOptions = {},
  ) {
    this.window = opts.window ?? 20;
    this.failureRatio = opts.failureRatio ?? 0.5;
    this.minRequests = opts.minRequests ?? 5;
    this.consecutiveFailures = opts.consecutiveFailures ?? 5;
    this.openSeconds = opts.openSeconds ?? 5;
    this.halfOpenMax = opts.halfOpenMax ?? 2;
    this.onTransition = opts.onTransition;
    if (this.window < 1 || this.minRequests < 1 || this.halfOpenMax < 1 || this.consecutiveFailures < 1) {
      throw new Error("window, min_requests, half_open_max, consecutive_failures: all >= 1");
    }
    if (!(this.failureRatio > 0 && this.failureRatio <= 1)) {
      throw new Error("failure_ratio must be in (0, 1]");
    }
  }

  get state(): BreakerState {
    this.maybeHalfOpen(this.clock());
    return this.current;
  }

  get failureRate(): number {
    if (this.outcomes.length === 0) return 0;
    return this.outcomes.filter(Boolean).length / this.outcomes.length;
  }

  get windowSnapshot(): readonly boolean[] {
    return this.outcomes;
  }

  get consecutiveCount(): number {
    return this.consecutive;
  }

  get probes(): { inFlight: number; succeeded: number } {
    return { inFlight: this.probesInFlight, succeeded: this.probesSucceeded };
  }

  timeUntilProbe(): number {
    if (this.current !== "open") return 0;
    return Math.max(0, this.openedAt + this.openSeconds - this.clock());
  }

  /** True if a call may proceed. Half-open probes are counted here. */
  allow(): boolean {
    const now = this.clock();
    this.maybeHalfOpen(now);
    if (this.current === "closed") return true;
    if (this.current === "open") return false;
    if (this.probesInFlight + this.probesSucceeded < this.halfOpenMax) {
      this.probesInFlight += 1;
      return true;
    }
    return false;
  }

  recordSuccess(): void {
    this.consecutive = 0;
    if (this.current === "half_open") {
      this.probesInFlight = Math.max(0, this.probesInFlight - 1);
      this.probesSucceeded += 1;
      if (this.probesSucceeded >= this.halfOpenMax) this.transition("closed");
      return;
    }
    this.push(false);
  }

  recordFailure(): void {
    const now = this.clock();
    this.consecutive += 1;
    if (this.current === "half_open") {
      this.open(now);
      return;
    }
    if (this.current === "open") return;
    this.push(true);
    if (this.consecutive >= this.consecutiveFailures) {
      this.open(now);
      return;
    }
    const n = this.outcomes.length;
    const failures = this.outcomes.filter(Boolean).length;
    if (n >= this.minRequests && failures / n >= this.failureRatio) this.open(now);
  }

  reset(): void {
    this.transition("closed");
  }

  private push(failure: boolean): void {
    this.outcomes.push(failure);
    if (this.outcomes.length > this.window) this.outcomes.shift();
  }

  private maybeHalfOpen(now: number): void {
    if (this.current === "open" && now - this.openedAt >= this.openSeconds) this.transition("half_open");
  }

  private open(now: number): void {
    this.openedAt = now;
    this.transition("open");
  }

  private transition(next: BreakerState): void {
    const old = this.current;
    if (next === "closed") {
      this.outcomes = [];
      this.consecutive = 0;
    }
    if (next !== "half_open" || old !== "half_open") {
      this.probesInFlight = 0;
      this.probesSucceeded = 0;
    }
    this.current = next;
    if (old !== next && this.onTransition) this.onTransition(this, old, next);
  }
}
