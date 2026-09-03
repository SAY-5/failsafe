import { useReducedMotion } from "framer-motion";
import { useCallback, useMemo, useRef, useState } from "react";
import { useTicker } from "../hooks/useTicker";
import { CircuitBreaker, type BreakerState } from "../sim/breaker";
import { VirtualClock } from "../sim/prng";

const OPEN_SECONDS = 3;
const CONSECUTIVE = 3;
const WINDOW = 20;
const HALF_OPEN_MAX = 2;

interface Event {
  id: number;
  at: number;
  text: string;
  kind: "ok" | "fail" | "fast" | "transition";
}

const NODE = {
  closed: { x: 120, y: 90 },
  open: { x: 480, y: 90 },
  half_open: { x: 300, y: 250 },
} as const;

const LABEL: Record<BreakerState, string> = { closed: "CLOSED", open: "OPEN", half_open: "HALF-OPEN" };

export function BreakerSection() {
  const reduce = useReducedMotion();
  const clock = useMemo(() => new VirtualClock(), []);
  const [events, setEvents] = useState<Event[]>([]);
  const [edge, setEdge] = useState<string | null>(null);
  const nextId = useRef(1);
  const push = useCallback((text: string, kind: Event["kind"]) => {
    setEvents((e) => [{ id: nextId.current++, at: clock.now(), text, kind }, ...e].slice(0, 8));
  }, [clock]);
  const breaker = useMemo(
    () =>
      new CircuitBreaker("upstream-2", clock.now, {
        window: WINDOW,
        failureRatio: 0.5,
        minRequests: 5,
        consecutiveFailures: CONSECUTIVE,
        openSeconds: OPEN_SECONDS,
        halfOpenMax: HALF_OPEN_MAX,
        onTransition: (_b, from, to) => {
          setEdge(`${from}-${to}`);
          push(`breaker ${from} -> ${to}`, "transition");
        },
      }),
    [clock, push],
  );
  const [failing, setFailing] = useState(false);
  const [auto, setAuto] = useState(false);
  const [fastFails, setFastFails] = useState(0);
  const [counts, setCounts] = useState({ ok: 0, fail: 0 });
  const [, setFrame] = useState(0);
  const acc = useRef(0);

  const send = useCallback(() => {
    if (!breaker.allow()) {
      setFastFails((n) => n + 1);
      push("503 fast-fail, breaker refused the call in 0.02 ms", "fast");
      return;
    }
    if (failing) {
      breaker.recordFailure();
      setCounts((c) => ({ ...c, fail: c.fail + 1 }));
      push(breaker.state === "half_open" || breaker.state === "open" ? "probe failed: upstream 503" : "upstream 503 recorded", "fail");
    } else {
      breaker.recordSuccess();
      setCounts((c) => ({ ...c, ok: c.ok + 1 }));
      push(breaker.state === "half_open" ? "probe succeeded: 200" : "200 recorded", "ok");
    }
  }, [breaker, failing, push]);

  const burst = useCallback(() => {
    for (let i = 0; i < 3; i++) send();
  }, [send]);

  useTicker((dt) => {
    clock.advance(dt);
    if (auto) {
      acc.current += dt * 4;
      while (acc.current >= 1) {
        acc.current -= 1;
        send();
      }
    }
    setFrame((f) => f + 1);
  }, true);

  const state = breaker.state;
  const window = breaker.windowSnapshot;
  const untilProbe = breaker.timeUntilProbe();
  const probes = breaker.probes;

  const edgeClass = (id: string) => `sm-edge ${edge === id ? "sm-edge--live" : ""}`;

  return (
    <section className="section" id="breaker" aria-labelledby="breaker-title">
      <div className="wrap">
        <div className="section-head">
          <p className="section-index">02 / circuit breaker</p>
          <h2 id="breaker-title">One breaker per replica. Failing fast is a feature.</h2>
          <p>
            Closed: calls flow and outcomes land in a sliding window of the last {WINDOW} results. Open: entered
            after {CONSECUTIVE} consecutive failures or a 50 % failure ratio over at least 5 outcomes; every call is
            refused for {OPEN_SECONDS} s. Half-open: at most {HALF_OPEN_MAX} probes are admitted; one probe failure
            reopens the breaker, all probes succeeding closes it and clears the window.
          </p>
        </div>

        <div className="breaker-grid">
          <div className={`glass sm-panel sm-panel--${state}`}>
            <svg viewBox="0 0 600 330" className="sm-svg" role="img" aria-label={`Breaker state machine, current state ${LABEL[state]}`}>
              <defs>
                <marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
                  <path d="M0,0 L10,5 L0,10 z" fill="currentColor" />
                </marker>
              </defs>
              <path d="M186 74 Q300 20 414 74" className={edgeClass("closed-open")} markerEnd="url(#arrow)" />
              <text x="300" y="38" className="sm-edge-label">3 consecutive failures, or ratio &gt;= 0.5</text>

              <path d="M458 138 Q400 210 356 236" className={edgeClass("open-half_open")} markerEnd="url(#arrow)" />
              <text x="438" y="205" className="sm-edge-label sm-edge-label--right">after {OPEN_SECONDS} s</text>

              <path d="M244 236 Q200 210 142 138" className={edgeClass("half_open-closed")} markerEnd="url(#arrow)" />
              <text x="164" y="205" className="sm-edge-label sm-edge-label--left">{HALF_OPEN_MAX} probes ok</text>

              <path d="M348 214 Q420 150 468 128" className={edgeClass("half_open-open")} markerEnd="url(#arrow)" strokeDasharray="5 5" />
              <text x="430" y="160" className="sm-edge-label sm-edge-label--right">probe failed</text>

              {(Object.keys(NODE) as BreakerState[]).map((s) => {
                const n = NODE[s];
                const active = s === state;
                return (
                  <g key={s} className={`sm-node sm-node--${s} ${active ? "is-active" : ""}`}>
                    {active && !reduce && <circle cx={n.x} cy={n.y} r="58" className="sm-node-halo" />}
                    <circle cx={n.x} cy={n.y} r="46" />
                    <text x={n.x} y={n.y + 5} className="sm-node-label">{LABEL[s]}</text>
                  </g>
                );
              })}
              {state === "open" && (
                <g>
                  <rect x={NODE.open.x - 46} y={NODE.open.y + 56} width="92" height="6" rx="3" className="sm-timer-track" />
                  <rect
                    x={NODE.open.x - 46}
                    y={NODE.open.y + 56}
                    width={92 * (1 - untilProbe / OPEN_SECONDS)}
                    height="6"
                    rx="3"
                    className="sm-timer-fill"
                  />
                  <text x={NODE.open.x} y={NODE.open.y + 78} className="sm-timer-label">probe in {untilProbe.toFixed(1)} s</text>
                </g>
              )}
              {state === "half_open" && (
                <text x={NODE.half_open.x} y={NODE.half_open.y + 70} className="sm-timer-label">
                  probes {probes.succeeded} ok / {probes.inFlight} in flight / {HALF_OPEN_MAX} max
                </text>
              )}
            </svg>

            <div className="sm-window" aria-label="Sliding window of the last outcomes, newest on the right">
              <span className="sm-window-label mono">window</span>
              <div className="sm-window-dots">
                {Array.from({ length: WINDOW }, (_, i) => {
                  const idx = i - (WINDOW - window.length);
                  const v = idx >= 0 ? window[idx] : null;
                  return <i key={i} className={v === null ? "" : v ? "is-fail" : "is-ok"} aria-hidden="true" />;
                })}
              </div>
              <span className="mono sm-window-rate">
                {window.length} / {WINDOW}, failure rate {(breaker.failureRate * 100).toFixed(0)} %, consecutive {breaker.consecutiveCount}
              </span>
            </div>
          </div>

          <div className="breaker-side">
            <div className="glass breaker-controls">
              <div className="control">
                <span className="label">upstream-2 behaviour</span>
                <div className="seg" role="group" aria-label="Upstream behaviour">
                  <button type="button" className={`seg-btn ${!failing ? "is-on" : ""}`} onClick={() => setFailing(false)} aria-pressed={!failing}>
                    answers 200
                  </button>
                  <button type="button" className={`seg-btn seg-btn--crimson ${failing ? "is-on" : ""}`} onClick={() => setFailing(true)} aria-pressed={failing}>
                    answers 503
                  </button>
                </div>
              </div>
              <div className="bucket-actions">
                <button type="button" className="btn" onClick={send}>
                  Send request
                </button>
                <button type="button" className="btn btn--crimson" onClick={() => { setFailing(true); burst(); }}>
                  Inject 3 failures
                </button>
                <button type="button" className={`btn ${auto ? "btn--on" : ""}`} onClick={() => setAuto((a) => !a)} aria-pressed={auto}>
                  {auto ? "Stop" : "Start"} 4 rps client
                </button>
              </div>
              <dl className="breaker-stats">
                <div className="stat">
                  <dt className="label">state</dt>
                  <dd className={`value ${state === "open" ? "crimson" : state === "half_open" ? "" : "steel"}`}>{LABEL[state]}</dd>
                </div>
                <div className="stat">
                  <dt className="label">fast-fails while open</dt>
                  <dd className="value crimson">{fastFails}</dd>
                </div>
                <div className="stat">
                  <dt className="label">200 / 503 recorded</dt>
                  <dd className="value">
                    {counts.ok} / {counts.fail}
                  </dd>
                </div>
              </dl>
              <p className="breaker-hint">
                An open breaker removes only this replica from rotation. The route stays up on the others, and the
                gateway never waits on a replica it already knows is broken.
              </p>
            </div>
            <ol className="glass bucket-log mono breaker-log" aria-label="Breaker events">
              {events.length === 0 && <li className="bucket-log-empty">no calls yet</li>}
              {events.map((e) => (
                <li key={e.id} className={`ev-${e.kind}`}>
                  <span className="bucket-log-status">{e.kind === "transition" ? "->" : e.kind === "ok" ? "200" : "503"}</span>
                  <span className="bucket-log-key">t+{e.at.toFixed(1)}s</span>
                  <span>{e.text}</span>
                </li>
              ))}
            </ol>
          </div>
        </div>
      </div>
    </section>
  );
}
