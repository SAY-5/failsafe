/** Deterministic PRNG (mulberry32) so every run is reproducible from a seed. */
export class Prng {
  private state: number;

  constructor(seed: number) {
    this.state = seed >>> 0;
  }

  /** Uniform float in [0, 1). */
  next(): number {
    this.state = (this.state + 0x6d2b79f5) >>> 0;
    let t = this.state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  }

  /** Uniform float in [lo, hi). Mirrors random.uniform. */
  uniform(lo: number, hi: number): number {
    return lo + (hi - lo) * this.next();
  }

  /** Integer in [0, n). */
  int(n: number): number {
    return Math.floor(this.next() * n);
  }

  pick<T>(items: readonly T[]): T {
    return items[this.int(items.length)];
  }
}

export type Clock = () => number;

/** Monotonic clock in seconds that only moves when the simulation advances it. */
export class VirtualClock {
  private t = 0;

  now: Clock = () => this.t;

  advance(seconds: number): void {
    if (seconds > 0) this.t += seconds;
  }

  set(seconds: number): void {
    if (seconds > this.t) this.t = seconds;
  }
}
