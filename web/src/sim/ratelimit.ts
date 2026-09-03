/** Token-bucket rate limiting keyed by client identity. Port of failsafe/ratelimit.py. */
import type { Clock } from "./prng";

export interface Decision {
  allowed: boolean;
  remaining: number;
  /** Seconds until at least one token is available (0 when allowed). */
  retryAfter: number;
}

export class TokenBucket {
  readonly capacity: number;
  readonly refillRate: number;
  private tokensNow: number;
  private updated: number;

  constructor(
    capacity: number,
    refillRate: number,
    private readonly clock: Clock,
  ) {
    if (capacity < 1) throw new Error("capacity must be >= 1");
    if (refillRate <= 0) throw new Error("refill_rate must be > 0");
    this.capacity = capacity;
    this.refillRate = refillRate;
    this.tokensNow = capacity;
    this.updated = clock();
  }

  private refill(now: number): void {
    const elapsed = now - this.updated;
    if (elapsed > 0) {
      this.tokensNow = Math.min(this.capacity, this.tokensNow + elapsed * this.refillRate);
      this.updated = now;
    }
  }

  get tokens(): number {
    this.refill(this.clock());
    return this.tokensNow;
  }

  tryAcquire(cost = 1): Decision {
    if (cost <= 0) throw new Error("cost must be > 0");
    const now = this.clock();
    this.refill(now);
    if (this.tokensNow >= cost) {
      this.tokensNow -= cost;
      return { allowed: true, remaining: this.tokensNow, retryAfter: 0 };
    }
    const deficit = cost - this.tokensNow;
    return { allowed: false, remaining: this.tokensNow, retryAfter: deficit / this.refillRate };
  }
}

export class RateLimiter {
  private buckets = new Map<string, TokenBucket>();
  private lastSeen = new Map<string, number>();

  constructor(
    readonly capacity: number,
    readonly refillRate: number,
    private readonly clock: Clock,
    private readonly maxKeys = 100_000,
    private readonly idleSeconds = 300,
  ) {}

  bucket(key: string): TokenBucket {
    let b = this.buckets.get(key);
    const now = this.clock();
    if (b === undefined) {
      if (this.buckets.size >= this.maxKeys) this.evict(now);
      b = new TokenBucket(this.capacity, this.refillRate, this.clock);
      this.buckets.set(key, b);
    }
    this.lastSeen.set(key, now);
    return b;
  }

  private evict(now: number): void {
    let stale = [...this.lastSeen].filter(([, t]) => now - t > this.idleSeconds).map(([k]) => k);
    if (stale.length === 0) {
      const ordered = [...this.lastSeen].sort((a, b) => a[1] - b[1]).map(([k]) => k);
      stale = ordered.slice(0, Math.max(1, Math.floor(ordered.length / 4)));
    }
    for (const k of stale) {
      this.buckets.delete(k);
      this.lastSeen.delete(k);
    }
  }

  check(key: string, cost = 1): Decision {
    return this.bucket(key).tryAcquire(cost);
  }

  get size(): number {
    return this.buckets.size;
  }
}

/** Retry-After must be an integer number of seconds; round up so clients never retry early. */
export function retryAfterHeader(seconds: number): string {
  return String(Math.max(1, Math.ceil(seconds)));
}
