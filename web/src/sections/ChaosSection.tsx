import { useCallback, useRef, useState } from "react";
import { Sparkline } from "../components/Sparkline";
import { useTicker } from "../hooks/useTicker";
import { ChaosRun, DEFAULT_CHAOS, type TimelineEvent } from "../sim/chaos";

const DURATION = DEFAULT_CHAOS.durationSeconds;

function fmtT(s: number): string {
  const m = Math.floor(s / 60);
  const sec = s - m * 60;
  return `${String(m).padStart(2, "0")}:${sec.toFixed(1).padStart(4, "0")}`;
}

const EVENT_LABEL: Record<TimelineEvent["kind"], string> = {
  kill: "kill",
  start: "start",
  unhealthy: "-> unhealthy",
  healthy: "-> healthy",
  breaker: "breaker",
};

export function ChaosSection() {
  const [seed, setSeed] = useState(DEFAULT_CHAOS.seed);
  const runRef = useRef<ChaosRun>(new ChaosRun({ ...DEFAULT_CHAOS, seed }));
  const [running, setRunning] = useState(false);
  const [auto, setAuto] = useState(true);
  const [speed, setSpeed] = useState(1);
  const [, setFrame] = useState(0);
  const frames = useRef(0);
  const [lastKill, setLastKill] = useState<string | null>(null);
  const flashTimer = useRef(0);

  const run = runRef.current;
  const done = run.finished;

  const reset = useCallback(() => {
    const next = seed + 1;
    setSeed(next);
    runRef.current = new ChaosRun({ ...DEFAULT_CHAOS, seed: next });
    if (auto) runRef.current.startAutoChaos();
    setRunning(false);
    setLastKill(null);
    setFrame((f) => f + 1);
  }, [seed, auto]);

  const start = useCallback(() => {
    if (run.finished) return;
    if (auto && !run.autoChaos) run.startAutoChaos();
    setRunning(true);
  }, [run, auto]);

  const toggleAuto = useCallback(() => {
    setAuto((a) => {
      const next = !a;
      if (next) run.startAutoChaos();
      else run.stopAutoChaos();
      return next;
    });
  }, [run]);

  const kill = useCallback((target?: string) => {
    const label = run.kill(target);
    if (label) {
      setLastKill(label);
      window.clearTimeout(flashTimer.current);
      flashTimer.current = window.setTimeout(() => setLastKill(null), 900);
      setFrame((f) => f + 1);
    }
  }, [run]);

  useTicker((dt) => {
    run.step(dt * speed);
    frames.current += 1;
    if (frames.current % 4 === 0) setFrame((f) => f + 1);
    if (run.finished) setRunning(false);
  }, running);

  const snap = run.snapshot;
  const elapsed = Math.min(DURATION, run.now);
  const buckets = run.buckets;
  const killMarks = run.timeline.filter((e) => e.kind === "kill").map((e) => ({ x: Math.floor(e.at), label: e.replica }));
  const achieved = elapsed > 0 ? snap.requests / elapsed : 0;
  const served = new Map<string, number>();
  for (const r of run.results) if (r.servedBy) served.set(r.servedBy, (served.get(r.servedBy) ?? 0) + 1);
  const retriesText = Object.entries(snap.retriesByKind)
    .filter(([, n]) => n > 0)
    .map(([k, n]) => `'${k}': ${n}`)
    .join(", ");

  return (
    <section className={`section chaos ${lastKill ? "chaos--flash" : ""}`} id="chaos" aria-labelledby="chaos-title">
      <div className="wrap">
        <div className="section-head">
          <p className="section-index">04 / chaos run</p>
          <h2 id="chaos-title">45 seconds at 150 rps. Kill whatever you like.</h2>
          <p>
            The same run the repo executes on every push, replayed through the browser port: a fixed request rate
            with GET and idempotent POST traffic, three upstream replicas, and a kill script that SIGKILLs a random
            replica every few seconds and restarts it {DEFAULT_CHAOS.restartAfter} s later. The run passes only if the
            client-visible failed counter is still zero at the end.
          </p>
        </div>

        <div className="glass chaos-console">
          <div className="chaos-toolbar">
            <div className="chaos-buttons">
              {!running ? (
                <button type="button" className="btn btn--crimson" onClick={start} disabled={done}>
                  {run.now === 0 ? "Start run" : done ? "Finished" : "Resume"}
                </button>
              ) : (
                <button type="button" className="btn" onClick={() => setRunning(false)}>
                  Pause
                </button>
              )}
              <button type="button" className="btn" onClick={() => kill()} disabled={done || run.arrivalsDone}>
                SIGKILL a replica
              </button>
              <button type="button" className={`btn ${auto ? "btn--on" : ""}`} onClick={toggleAuto} aria-pressed={auto}>
                auto-chaos {auto ? "on" : "off"}
              </button>
              <button type="button" className="btn btn--ghost" onClick={reset}>
                Reset (seed {seed + 1})
              </button>
            </div>
            <div className="seg" role="group" aria-label="Playback speed">
              {[1, 2, 4].map((s) => (
                <button key={s} type="button" className={`seg-btn ${speed === s ? "is-on" : ""}`} onClick={() => setSpeed(s)} aria-pressed={speed === s}>
                  {s}x
                </button>
              ))}
            </div>
          </div>

          <div className="chaos-progress" role="progressbar" aria-valuemin={0} aria-valuemax={DURATION} aria-valuenow={Math.round(elapsed)} aria-label="Run progress">
            <i style={{ width: `${(elapsed / DURATION) * 100}%` }} />
            {killMarks.map((k, i) => (
              <b key={i} style={{ left: `${(k.x / DURATION) * 100}%` }} title={`kill ${k.label}`} />
            ))}
            <span className="mono">
              {fmtT(elapsed)} / {fmtT(DURATION)} at {DEFAULT_CHAOS.rps} rps{elapsed > 0 ? ` (achieved ${achieved.toFixed(1)})` : ""}
            </span>
          </div>

          <div className="chaos-grid">
            <dl className="chaos-counters">
              <div className="stat">
                <dt className="label">total requests</dt>
                <dd className="value">{snap.requests.toLocaleString("en-US")}</dd>
              </div>
              <div className="stat">
                <dt className="label">successful (2xx)</dt>
                <dd className="value steel">{snap.success.toLocaleString("en-US")}</dd>
              </div>
              <div className="stat stat--pinned">
                <dt className="label">client-visible failed</dt>
                <dd className={`value ${snap.clientFailed === 0 ? "" : "crimson"}`}>{snap.clientFailed}</dd>
                <span className="stat-note mono">must be 0</span>
              </div>
              <div className="stat">
                <dt className="label">rate limited (429)</dt>
                <dd className="value">{snap.rateLimited}</dd>
              </div>
              <div className="stat">
                <dt className="label">retries</dt>
                <dd className="value">{snap.retries}</dd>
                <span className="stat-note mono">{retriesText ? `{${retriesText}}` : "{}"}</span>
              </div>
              <div className="stat">
                <dt className="label">failovers</dt>
                <dd className="value steel">{snap.failovers}</dd>
              </div>
              <div className="stat">
                <dt className="label">breaker transitions</dt>
                <dd className="value">{snap.breakerTransitions}</dd>
              </div>
              <div className="stat">
                <dt className="label">kills</dt>
                <dd className="value crimson">{run.kills}</dd>
              </div>
            </dl>

            <div className="chaos-replicas" aria-label="Replica status">
              {run.pool.replicas.map((r) => {
                const dead = !r.alive;
                const cls = dead ? "is-dead" : !r.healthy ? "is-warm" : r.breaker.state !== "closed" ? "is-warm" : "is-ok";
                return (
                  <div key={r.label} className={`replica ${cls} ${lastKill === r.label ? "is-flash" : ""}`}>
                    <div className="replica-head">
                      <i className="replica-dot" aria-hidden="true" />
                      <b className="display">{r.label}</b>
                      <button type="button" className="replica-kill" onClick={() => kill(r.label)} disabled={dead || done || run.arrivalsDone || run.pool.replicas.filter((x) => x.alive).length <= 1} aria-label={`SIGKILL ${r.label}`}>
                        kill
                      </button>
                    </div>
                    <dl className="replica-meta mono">
                      <div>
                        <dt>process</dt>
                        <dd>{dead ? `killed, restart in ${Math.max(0, (r.killedAt ?? 0) + DEFAULT_CHAOS.restartAfter - run.now).toFixed(1)} s` : "running"}</dd>
                      </div>
                      <div>
                        <dt>health</dt>
                        <dd>{r.healthy ? "healthy" : "unhealthy"}</dd>
                      </div>
                      <div>
                        <dt>breaker</dt>
                        <dd>{r.breaker.state.replace("_", "-")}</dd>
                      </div>
                      <div>
                        <dt>served</dt>
                        <dd>{(served.get(r.label) ?? 0).toLocaleString("en-US")}</dd>
                      </div>
                    </dl>
                  </div>
                );
              })}
            </div>
          </div>

          <div className="chaos-bottom">
            <div className="chaos-latency">
              <Sparkline
                title="Latency percentiles per second"
                maxPoints={DURATION}
                series={[
                  { name: "p99", values: buckets.map((b) => b.p99), color: "#ff6b82", width: 1.2 },
                  { name: "p95", values: buckets.map((b) => b.p95), color: "#ffc76b", width: 1.2 },
                  { name: "p50", values: buckets.map((b) => b.p50), color: "#c9d6e3", width: 2 },
                ]}
                markers={killMarks}
              />
              <p className="chaos-latency-total mono">
                whole run: p50 {snap.p50.toFixed(1)} / p95 {snap.p95.toFixed(1)} / p99 {snap.p99.toFixed(1)} ms (max {snap.max.toFixed(1)})
              </p>
            </div>

            <div className="chaos-timeline">
              <p className="trace-head mono">kill timeline</p>
              <ol className="timeline mono" aria-live="polite">
                {run.timeline.length === 0 && <li className="bucket-log-empty">nothing killed yet</li>}
                {[...run.timeline].reverse().slice(0, 14).map((e, i) => (
                  <li key={`${e.at}-${e.kind}-${i}`} className={`tl-${e.kind}`}>
                    <span>{fmtT(e.at)}</span>
                    <span>{EVENT_LABEL[e.kind]}</span>
                    <span>
                      {e.replica}
                      {e.note ? ` ${e.note}` : ""}
                    </span>
                  </li>
                ))}
              </ol>
            </div>
          </div>

          {done && (
            <pre className={`chaos-summary mono ${snap.clientFailed === 0 ? "is-pass" : "is-fail"}`} aria-live="polite">
{`================================================================
FailSafe chaos summary (browser port, seed ${seed})
================================================================
duration / target rps         ${DURATION.toFixed(1)}s / ${DEFAULT_CHAOS.rps.toFixed(1)} rps (achieved ${achieved.toFixed(1)} rps)
total requests                ${snap.requests}
successful (2xx)              ${snap.success}
rate limited (429)            ${snap.rateLimited}
client-visible failed         ${snap.clientFailed}   <- must be 0
retries (gateway)             ${snap.retries}  {${retriesText}}
failovers (gateway)           ${snap.failovers}
breaker transitions           ${snap.breakerTransitions}
latency ms p50 / p95 / p99    ${snap.p50.toFixed(1)} / ${snap.p95.toFixed(1)} / ${snap.p99.toFixed(1)}  (max ${snap.max.toFixed(1)})
kills                         ${run.kills}
================================================================
${snap.clientFailed === 0 ? `PASS: zero client-visible failures across ${run.kills} kills` : `FAIL: ${snap.clientFailed} client-visible failures`}`}
            </pre>
          )}
        </div>
      </div>
    </section>
  );
}
