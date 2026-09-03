import { AnimatePresence, motion, useReducedMotion } from "framer-motion";
import { useCallback, useState } from "react";
import { Gateway, type RequestResult } from "../sim/gateway";
import { Prng, VirtualClock } from "../sim/prng";
import { RetryPolicy } from "../sim/retry";
import { HealthChecker, UpstreamPool } from "../sim/upstreams";

type Scenario = "kill" | "refused" | "status" | "hang";

const SCENARIOS: { id: Scenario; label: string; kind: string; blurb: string }[] = [
  { id: "kill", label: "Killed mid-flight", kind: "read", blurb: "upstream-1 is SIGKILLed while the first attempt is in flight; the connection drops mid-response." },
  { id: "refused", label: "Connection refused", kind: "connect", blurb: "upstream-1 is already dead but the health checker has not noticed yet; nothing reaches it." },
  { id: "status", label: "Upstream 503", kind: "status", blurb: "upstream-1 answers 503; the request was processed by the replica." },
  { id: "hang", label: "Replica hangs", kind: "timeout", blurb: "upstream-1 accepts the request and never answers; the per-attempt timeout fires." },
];

const OUTCOME_TEXT: Record<string, string> = {
  success: "200 OK",
  connect: "connection refused",
  read: "connection dropped mid-response",
  timeout: "read timeout after 2.0 s",
  status: "503 from upstream",
};

function runScenario(opts: { method: string; key: boolean; scenario: Scenario; maxAttempts: number; seed: number }): RequestResult {
  const clock = new VirtualClock();
  const rng = new Prng(opts.seed);
  const pool = new UpstreamPool(["upstream-1", "upstream-2", "upstream-3"], clock.now, {
    consecutiveFailures: 3,
    openSeconds: 3,
    halfOpenMax: 2,
  });
  const checker = new HealthChecker(pool, clock.now);
  checker.start();
  const policy = new RetryPolicy({ maxAttempts: opts.maxAttempts, baseDelay: 0.01, maxDelay: 0.15 });
  const gw = new Gateway(pool, policy, rng, clock.now);
  let result: RequestResult | null = null;
  gw.onResult = (r) => (result = r);
  const first = pool.get("upstream-1")!;
  if (opts.scenario === "refused") first.alive = false;
  if (opts.scenario === "status") first.failStatus = 503;
  if (opts.scenario === "hang") first.hang = true;
  gw.submit({
    id: 1,
    method: opts.method,
    path: opts.method === "POST" ? "/orders" : "/orders/42",
    headers: opts.key ? { "Idempotency-Key": "order-7f3a" } : undefined,
    clientKey: "key:demo",
  });
  if (opts.scenario === "kill") {
    clock.advance(0.0012);
    gw.advance();
    gw.kill(first);
  }
  let guard = 0;
  while (result === null && guard++ < 10_000) {
    const t = gw.nextEventAt();
    if (t === Infinity) break;
    clock.set(t);
    gw.advance();
  }
  return result!;
}

const ms = (s: number) => `${(s * 1000).toFixed(1)} ms`;

export function RetrySection() {
  const reduce = useReducedMotion();
  const [method, setMethod] = useState<"GET" | "POST">("GET");
  const [key, setKey] = useState(false);
  const [scenario, setScenario] = useState<Scenario>("kill");
  const [maxAttempts, setMaxAttempts] = useState(4);
  const [seed, setSeed] = useState(11);
  const [result, setResult] = useState<RequestResult | null>(null);

  const send = useCallback(() => {
    setResult(runScenario({ method, key, scenario, maxAttempts, seed }));
    setSeed((s) => s + 1);
  }, [method, key, scenario, maxAttempts, seed]);

  const idempotent = method === "GET" || key;
  const total = result ? result.latencyMs / 1000 : 0;
  const scale = (s: number) => `${Math.max(0.6, (s / Math.max(total, 0.001)) * 100)}%`;
  const sc = SCENARIOS.find((s) => s.id === scenario)!;

  return (
    <section className="section" id="retries" aria-labelledby="retries-title">
      <div className="wrap">
        <div className="section-head">
          <p className="section-index">03 / retries + failover</p>
          <h2 id="retries-title">A retry is only useful if it lands somewhere else.</h2>
          <p>
            Every attempt asks the pool for a replica it has not tried yet, skipping anything unhealthy or with an
            open breaker. Between attempts the gateway sleeps a full-jitter backoff,{" "}
            <code className="mono">uniform(0, min(max_delay, base * 2^(n-1)))</code>. Connection errors are always
            retried: no bytes reached the upstream. Anything after that is retried only when the request is
            idempotent: GET, HEAD, PUT, DELETE, OPTIONS, or a POST carrying an <code className="mono">Idempotency-Key</code>.
          </p>
        </div>

        <div className="retry-grid">
          <div className="glass retry-controls">
            <div className="control">
              <span className="label">method</span>
              <div className="seg" role="group" aria-label="HTTP method">
                {(["GET", "POST"] as const).map((m) => (
                  <button key={m} type="button" className={`seg-btn ${method === m ? "is-on" : ""}`} onClick={() => setMethod(m)} aria-pressed={method === m}>
                    {m}
                  </button>
                ))}
              </div>
            </div>
            <label className={`toggle ${method !== "POST" ? "is-disabled" : ""}`}>
              <input type="checkbox" checked={key} disabled={method !== "POST"} onChange={(e) => setKey(e.target.checked)} />
              <span className="toggle-track" aria-hidden="true" />
              <span>
                send <code className="mono">Idempotency-Key: order-7f3a</code>
              </span>
            </label>
            <div className="control">
              <span className="label">what goes wrong on upstream-1</span>
              <div className="scenario-list" role="group" aria-label="Failure scenario">
                {SCENARIOS.map((s) => (
                  <button
                    key={s.id}
                    type="button"
                    className={`scenario ${scenario === s.id ? "is-on" : ""}`}
                    onClick={() => setScenario(s.id)}
                    aria-pressed={scenario === s.id}
                  >
                    <b>{s.label}</b>
                    <span className="mono">{s.kind}</span>
                  </button>
                ))}
              </div>
              <p className="retry-blurb">{sc.blurb}</p>
            </div>
            <div className="control">
              <label htmlFor="attempts">
                max_attempts <output>{maxAttempts}</output>
              </label>
              <input id="attempts" type="range" min={1} max={4} value={maxAttempts} onChange={(e) => setMaxAttempts(Number(e.target.value))} />
            </div>
            <div className="retry-verdict mono">
              {idempotent ? (
                <span className="is-ok">idempotent: retried on connect, timeout, read and 5xx</span>
              ) : (
                <span className="is-warn">not idempotent: retried on connect errors only</span>
              )}
            </div>
            <button type="button" className="btn btn--crimson" onClick={send}>
              Send {method} {method === "POST" ? "/orders" : "/orders/42"}
            </button>
          </div>

          <div className="glass trace" aria-live="polite">
            <div className="trace-head mono">
              <span>request trace</span>
              {result && (
                <span className={result.status >= 500 ? "trace-final trace-final--fail" : "trace-final"}>
                  client saw {result.status} in {result.latencyMs.toFixed(1)} ms
                  {result.servedBy ? ` via ${result.servedBy}` : ""}
                </span>
              )}
            </div>
            {!result && <p className="trace-empty">Pick a method and a failure, then send a request to see every attempt, backoff and hop.</p>}
            <AnimatePresence mode="wait">
              {result && (
                <motion.ol
                  key={`${result.id}-${seed}`}
                  className="trace-list"
                  initial="hidden"
                  animate="show"
                  variants={{ show: { transition: { staggerChildren: reduce ? 0 : 0.14 } } }}
                >
                  {result.attempts.map((a, i) => {
                    const dur = a.endedAt - a.startedAt;
                    const ok = a.outcome === "success";
                    const last = i === result.attempts.length - 1;
                    return (
                      <motion.li
                        key={a.n}
                        className="trace-attempt"
                        variants={{ hidden: { opacity: 0, x: reduce ? 0 : -12 }, show: { opacity: 1, x: 0 } }}
                        transition={{ duration: 0.4, ease: [0.22, 1, 0.36, 1] }}
                      >
                        <div className="trace-row">
                          <span className="trace-n mono">#{a.n}</span>
                          <span className="trace-target">
                            {a.failedOverFrom && <span className="pill pill--steel">failover {a.failedOverFrom} to {a.replica}</span>}
                            {!a.failedOverFrom && <span className="pill">{a.replica}</span>}
                          </span>
                          <span className={`trace-outcome mono ${ok ? "is-ok" : "is-fail"}`}>{OUTCOME_TEXT[a.outcome] ?? a.outcome}</span>
                          <span className="trace-ms mono">{ms(dur)}</span>
                        </div>
                        <div className="trace-bar" aria-hidden="true">
                          <i className={ok ? "is-ok" : "is-fail"} style={{ width: scale(dur) }} />
                        </div>
                        {!ok && !last && (
                          <div className="trace-backoff">
                            <span className="mono">
                              sleep {ms(a.backoff)} <em>jitter window 0 to {ms(a.backoffCeiling)}</em>
                            </span>
                            <div className="trace-bar trace-bar--backoff" aria-hidden="true">
                              <i style={{ width: scale(a.backoff) }} />
                              <b style={{ width: scale(a.backoffCeiling) }} />
                            </div>
                          </div>
                        )}
                        {!ok && last && (
                          <p className="trace-stop mono">
                            {result.status >= 500
                              ? a.n >= maxAttempts
                                ? `attempts exhausted (${maxAttempts}), ${result.status} returned to the client`
                                : `${a.outcome} is not retried for a non-idempotent request: the upstream may have processed it. ${result.status} returned to the client.`
                              : ""}
                          </p>
                        )}
                      </motion.li>
                    );
                  })}
                  <motion.li
                    className={`trace-final-row ${result.status >= 500 ? "is-fail" : "is-ok"}`}
                    variants={{ hidden: { opacity: 0 }, show: { opacity: 1 } }}
                  >
                    <span className="mono">{result.status >= 500 ? "client-visible failure" : "client-visible failures: 0"}</span>
                    <span className="mono">{result.attempts.length} attempt{result.attempts.length === 1 ? "" : "s"}, {result.latencyMs.toFixed(1)} ms end to end</span>
                  </motion.li>
                </motion.ol>
              )}
            </AnimatePresence>
          </div>
        </div>
      </div>
    </section>
  );
}
