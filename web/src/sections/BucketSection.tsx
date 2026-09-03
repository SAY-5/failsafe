import { useReducedMotion } from "framer-motion";
import { useCallback, useMemo, useRef, useState } from "react";
import { useTicker } from "../hooks/useTicker";
import { VirtualClock } from "../sim/prng";
import { TokenBucket, retryAfterHeader } from "../sim/ratelimit";

interface LogLine {
  id: number;
  at: number;
  status: 200 | 429;
  remaining: number;
  retryAfter: number;
  header: string | null;
}

export function BucketSection() {
  const reduce = useReducedMotion();
  const clock = useMemo(() => new VirtualClock(), []);
  const [capacity, setCapacity] = useState(10);
  const [rate, setRate] = useState(3);
  const [autoRps, setAutoRps] = useState(0);
  const bucketRef = useRef<TokenBucket>(new TokenBucket(capacity, rate, clock.now));
  const [tokens, setTokens] = useState(capacity);
  const [log, setLog] = useState<LogLine[]>([]);
  const [totals, setTotals] = useState({ ok: 0, limited: 0 });
  const nextId = useRef(1);
  const autoAcc = useRef(0);
  const [flash, setFlash] = useState<"ok" | "limited" | null>(null);
  const flashTimer = useRef(0);

  const rebuild = useCallback(
    (cap: number, r: number) => {
      const old = bucketRef.current;
      const ratio = old.tokens / old.capacity;
      const b = new TokenBucket(cap, r, clock.now);
      b.tryAcquire(Math.max(0.0001, cap - ratio * cap) || 0.0001);
      bucketRef.current = b;
    },
    [clock],
  );

  const send = useCallback(() => {
    const d = bucketRef.current.tryAcquire();
    const line: LogLine = {
      id: nextId.current++,
      at: clock.now(),
      status: d.allowed ? 200 : 429,
      remaining: d.remaining,
      retryAfter: d.retryAfter,
      header: d.allowed ? null : retryAfterHeader(d.retryAfter),
    };
    setLog((l) => [line, ...l].slice(0, 9));
    setTotals((t) => (d.allowed ? { ...t, ok: t.ok + 1 } : { ...t, limited: t.limited + 1 }));
    setFlash(d.allowed ? "ok" : "limited");
    window.clearTimeout(flashTimer.current);
    flashTimer.current = window.setTimeout(() => setFlash(null), 260);
  }, [clock]);

  const burst = useCallback(() => {
    for (let i = 0; i < capacity + 4; i++) send();
  }, [capacity, send]);

  useTicker((dt) => {
    clock.advance(dt);
    if (autoRps > 0) {
      autoAcc.current += dt * autoRps;
      while (autoAcc.current >= 1) {
        autoAcc.current -= 1;
        send();
      }
    }
    setTokens(bucketRef.current.tokens);
  }, true);

  const level = Math.max(0, Math.min(1, tokens / capacity));
  const empty = tokens < 1;
  const deficit = empty ? (1 - tokens) / rate : 0;

  return (
    <section className="section" id="bucket" aria-labelledby="bucket-title">
      <div className="wrap">
        <div className="section-head">
          <p className="section-index">01 / token bucket</p>
          <h2 id="bucket-title">Exact refill math, no timers.</h2>
          <p>
            Each API key gets a bucket of <code className="mono">capacity</code> tokens refilled continuously at{" "}
            <code className="mono">refill_per_second</code>. Refill is computed lazily from a monotonic clock:{" "}
            <code className="mono">tokens = min(capacity, tokens + (now - last) * rate)</code>. A request costs one token.
            When the bucket is empty the gateway answers 429 with <code className="mono">Retry-After</code> set to the
            ceiling of <code className="mono">deficit / rate</code>, the earliest moment a retry can succeed.
          </p>
        </div>

        <div className="bucket-grid">
          <div className={`glass bucket-visual ${flash ? `bucket-visual--${flash}` : ""}`}>
            <svg viewBox="0 0 320 360" className="bucket-svg" role="img" aria-label={`Bucket holding ${tokens.toFixed(1)} of ${capacity} tokens`}>
              <defs>
                <linearGradient id="liquid" x1="0" x2="0" y1="0" y2="1">
                  <stop offset="0" stopColor={empty ? "#ff6b82" : "#dbe6f0"} />
                  <stop offset="1" stopColor={empty ? "#a60f2b" : "#7f96ad"} />
                </linearGradient>
                <clipPath id="bucket-clip">
                  <path d="M40 40 L280 40 L256 330 Q160 350 64 330 Z" />
                </clipPath>
              </defs>
              <path d="M40 40 L280 40 L256 330 Q160 350 64 330 Z" className="bucket-shell" />
              <g clipPath="url(#bucket-clip)">
                <rect
                  x="0"
                  y={40 + (1 - level) * 290}
                  width="320"
                  height="330"
                  fill="url(#liquid)"
                  className="bucket-liquid"
                  style={{ transition: reduce ? "none" : "y 120ms linear" }}
                />
                {!reduce && (
                  <path
                    d={`M-40 ${40 + (1 - level) * 290} q 40 -8 80 0 t 80 0 t 80 0 t 80 0 t 80 0 v 20 h -400 z`}
                    className="bucket-wave"
                    fill={empty ? "rgba(255,107,130,0.5)" : "rgba(232,238,245,0.45)"}
                  />
                )}
              </g>
              {Array.from({ length: 5 }, (_, i) => {
                const f = (i + 1) / 5;
                const y = 40 + (1 - f) * 290;
                return (
                  <g key={i}>
                    <line x1={30 + 4 * (1 - f)} x2={40 + 4 * (1 - f)} y1={y} y2={y} className="bucket-tick" />
                    <text x="24" y={y + 4} className="bucket-tick-label">{Math.round(f * capacity)}</text>
                  </g>
                );
              })}
              <ellipse cx="160" cy="40" rx="120" ry="14" className="bucket-rim" />
              <text x="160" y="196" className="bucket-count">{tokens.toFixed(1)}</text>
              <text x="160" y="222" className="bucket-count-label">tokens of {capacity}</text>
              <g className={`bucket-drip ${autoRps > 0 || !reduce ? "bucket-drip--on" : ""}`}>
                <circle cx="160" cy="6" r="4" />
              </g>
            </svg>
            <div className="bucket-status mono" aria-live="polite">
              {empty ? (
                <span className="bucket-status--limited">
                  429 Too Many Requests, Retry-After: {retryAfterHeader(deficit)} ({deficit.toFixed(2)} s to next token)
                </span>
              ) : (
                <span>200 OK, {Math.floor(tokens)} request{Math.floor(tokens) === 1 ? "" : "s"} admitted before the bucket runs dry</span>
              )}
            </div>
          </div>

          <div className="bucket-panel">
            <div className="glass bucket-controls">
              <div className="control">
                <label htmlFor="cap">
                  capacity <output>{capacity}</output>
                </label>
                <input
                  id="cap"
                  type="range"
                  min={1}
                  max={40}
                  value={capacity}
                  onChange={(e) => {
                    const v = Number(e.target.value);
                    setCapacity(v);
                    rebuild(v, rate);
                  }}
                />
              </div>
              <div className="control">
                <label htmlFor="rate">
                  refill per second <output>{rate}</output>
                </label>
                <input
                  id="rate"
                  type="range"
                  min={0.5}
                  max={20}
                  step={0.5}
                  value={rate}
                  onChange={(e) => {
                    const v = Number(e.target.value);
                    setRate(v);
                    rebuild(capacity, v);
                  }}
                />
              </div>
              <div className="control">
                <label htmlFor="auto">
                  synthetic client <output>{autoRps === 0 ? "off" : `${autoRps} rps`}</output>
                </label>
                <input id="auto" type="range" min={0} max={30} value={autoRps} onChange={(e) => setAutoRps(Number(e.target.value))} />
              </div>
              <div className="bucket-actions">
                <button type="button" className="btn" onClick={send}>
                  Send one request
                </button>
                <button type="button" className="btn btn--crimson" onClick={burst}>
                  Burst {capacity + 4}
                </button>
              </div>
              <dl className="bucket-totals">
                <div className="stat">
                  <dt className="label">admitted</dt>
                  <dd className="value steel">{totals.ok.toLocaleString("en-US")}</dd>
                </div>
                <div className="stat">
                  <dt className="label">429</dt>
                  <dd className="value crimson">{totals.limited.toLocaleString("en-US")}</dd>
                </div>
                <div className="stat">
                  <dt className="label">sustained max</dt>
                  <dd className="value">{rate} rps</dd>
                </div>
              </dl>
            </div>

            <ol className="glass bucket-log mono" aria-label="Recent rate-limit decisions">
              {log.length === 0 && <li className="bucket-log-empty">no requests yet</li>}
              {log.map((l) => (
                <li key={l.id} className={l.status === 429 ? "is-limited" : ""}>
                  <span className="bucket-log-status">{l.status}</span>
                  <span className="bucket-log-key">key:demo</span>
                  {l.status === 200 ? (
                    <span>remaining {l.remaining.toFixed(2)}</span>
                  ) : (
                    <span>
                      Retry-After: {l.header} <em>({l.retryAfter.toFixed(3)} s deficit)</em>
                    </span>
                  )}
                </li>
              ))}
            </ol>
          </div>
        </div>
      </div>
    </section>
  );
}
