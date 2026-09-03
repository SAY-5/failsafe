/** Console self-check for the TypeScript port. Run with `npx tsx src/sim/selfcheck.ts`. */
import { CircuitBreaker } from "./breaker";
import { ChaosRun, DEFAULT_CHAOS } from "./chaos";
import { VirtualClock, Prng } from "./prng";
import { TokenBucket, retryAfterHeader } from "./ratelimit";
import { RetryPolicy, isIdempotent } from "./retry";

let failures = 0;
function check(name: string, ok: boolean, detail = ""): void {
  console.log(`${ok ? "ok  " : "FAIL"} ${name}${detail ? `  (${detail})` : ""}`);
  if (!ok) failures += 1;
}
const close = (a: number, b: number, eps = 1e-9) => Math.abs(a - b) <= eps;

export function runSelfCheck(): boolean {
  failures = 0;

  // ---- token bucket refill math -------------------------------------------
  {
    const clock = new VirtualClock();
    const b = new TokenBucket(10, 5, clock.now);
    for (let i = 0; i < 10; i++) b.tryAcquire();
    const denied = b.tryAcquire();
    check("bucket drains to zero and refuses", !denied.allowed && close(denied.remaining, 0));
    check("retry_after is deficit / rate", close(denied.retryAfter, 1 / 5));
    check("Retry-After header rounds up to >= 1s", retryAfterHeader(denied.retryAfter) === "1");
    clock.advance(1);
    check("one second refills rate tokens", close(b.tokens, 5));
    clock.advance(10);
    check("refill is capped at capacity", close(b.tokens, 10));
    const partial = new TokenBucket(3, 2, clock.now);
    partial.tryAcquire(3);
    clock.advance(0.25);
    check("continuous refill: 0.25s at 2/s gives 0.5 token", close(partial.tokens, 0.5));
    check("a 0.5 token bucket needs 0.25s more", close(partial.tryAcquire().retryAfter, 0.25));
  }

  // ---- breaker transitions -------------------------------------------------
  {
    const clock = new VirtualClock();
    const seen: string[] = [];
    const cb = new CircuitBreaker("u", clock.now, {
      consecutiveFailures: 3,
      openSeconds: 3,
      halfOpenMax: 2,
      onTransition: (_b, from, to) => seen.push(`${from}>${to}`),
    });
    cb.recordFailure();
    cb.recordFailure();
    check("two failures keep the breaker closed", cb.state === "closed");
    cb.recordFailure();
    check("third consecutive failure opens it", cb.state === "open");
    check("open refuses calls", !cb.allow());
    clock.advance(2.9);
    check("still open before open_seconds", cb.state === "open");
    clock.advance(0.1);
    check("moves to half-open after open_seconds", cb.state === "half_open");
    check("half-open admits half_open_max probes", cb.allow() && cb.allow() && !cb.allow());
    cb.recordSuccess();
    check("one probe success is not enough", cb.state === "half_open");
    cb.recordSuccess();
    check("all probes succeeding closes it", cb.state === "closed");
    check("window cleared on close", cb.failureRate === 0);
    for (let i = 0; i < 3; i++) cb.recordFailure();
    clock.advance(3);
    cb.allow();
    cb.recordFailure();
    check("a probe failure reopens the breaker", cb.state === "open");
    check(
      "transition sequence recorded",
      seen.join(" ") === "closed>open open>half_open half_open>closed closed>open open>half_open half_open>open",
      seen.join(" "),
    );
    const ratio = new CircuitBreaker("r", clock.now, { window: 10, minRequests: 5, failureRatio: 0.5, consecutiveFailures: 99 });
    ratio.recordSuccess();
    ratio.recordFailure();
    ratio.recordSuccess();
    ratio.recordFailure();
    check("ratio not applied under min_requests", ratio.state === "closed");
    ratio.recordFailure();
    check("failure ratio >= 0.5 over min_requests opens", ratio.state === "open");
  }

  // ---- retry policy --------------------------------------------------------
  {
    const p = new RetryPolicy({ maxAttempts: 4, baseDelay: 0.01, maxDelay: 0.15 });
    check("GET is idempotent", isIdempotent("GET"));
    check("POST is not idempotent by default", !isIdempotent("POST"));
    check("POST with Idempotency-Key is idempotent", isIdempotent("POST", { "Idempotency-Key": "abc" }));
    check("route idempotent_post makes POST retryable", isIdempotent("post", undefined, true));
    check("connect errors retry even for POST", p.shouldRetry(1, "connect", false));
    check("read errors do not retry for POST", !p.shouldRetry(1, "read", false));
    check("read errors retry for GET", p.shouldRetry(1, "read", true));
    check("attempts are bounded", !p.shouldRetry(4, "connect", true));
    check("backoff ceiling doubles then caps", close(p.ceiling(1), 0.01) && close(p.ceiling(3), 0.04) && close(p.ceiling(6), 0.15));
    const rng = new Prng(7);
    let inRange = true;
    for (let i = 0; i < 200; i++) {
      const d = p.backoff(2, rng);
      if (d < 0 || d > 0.02) inRange = false;
    }
    check("full jitter stays within [0, ceiling]", inRange);
  }

  // ---- 1000-request run with a replica killed mid-way ----------------------
  {
    const run = new ChaosRun({ ...DEFAULT_CHAOS, durationSeconds: 1000 / 150, rps: 150, seed: 42 });
    run.step(3);
    const killed = run.kill();
    run.runAll();
    const s = run.snapshot;
    check("1000 requests completed", s.requests === 1000, `requests=${s.requests}`);
    check("a replica was killed", killed !== null, `killed=${killed}`);
    check("zero client-visible failures", s.clientFailed === 0, `client_failed=${s.clientFailed}`);
    check("at least one failover", s.failovers > 0, `failovers=${s.failovers} retries=${JSON.stringify(s.retriesByKind)}`);
    check("all requests succeeded", s.success === 1000, `success=${s.success}`);
    const unhealthy = run.timeline.some((e) => e.kind === "unhealthy");
    const healthy = run.timeline.some((e) => e.kind === "healthy" && e.at > 3);
    check("replica marked unhealthy then restored", unhealthy && healthy);
    check("p50 latency in the low milliseconds", s.p50 > 1 && s.p50 < 6, `p50=${s.p50.toFixed(2)}ms p99=${s.p99.toFixed(2)}ms`);
  }

  // ---- full 45 s auto-chaos run ----------------------------------------------
  {
    const run = new ChaosRun({ ...DEFAULT_CHAOS, seed: 1 });
    run.startAutoChaos();
    run.runAll();
    const s = run.snapshot;
    check("45s run at 150 rps sends 6750 requests", s.requests === 6750, `requests=${s.requests}`);
    check("auto chaos killed replicas", run.kills >= 3, `kills=${run.kills}`);
    check("still zero client-visible failures", s.clientFailed === 0, `client_failed=${s.clientFailed}`);
    check("failovers happened", s.failovers > 0, `failovers=${s.failovers}`);
    check("no rate limiting at 150 rps under a 400/s bucket", s.rateLimited === 0);
  }

  // ---- non-idempotent POST during a kill is the one that can fail ------------
  {
    const run = new ChaosRun({ ...DEFAULT_CHAOS, durationSeconds: 2, seed: 3, postShare: 0 });
    run.step(1);
    run.kill();
    run.runAll();
    check("GET-only traffic survives a kill", run.snapshot.clientFailed === 0);
  }

  console.log(failures === 0 ? "\nself-check passed" : `\nself-check FAILED (${failures})`);
  return failures === 0;
}

const isMain = typeof process !== "undefined" && process.argv?.[1]?.endsWith("selfcheck.ts");
if (isMain) process.exit(runSelfCheck() ? 0 : 1);
