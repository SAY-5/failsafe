import { useReducedMotion } from "framer-motion";
import { useEffect, useMemo, useRef, useState } from "react";
import { ChaosRun, DEFAULT_CHAOS } from "../sim/chaos";
import type { RequestResult } from "../sim/gateway";
import { useTicker } from "../hooks/useTicker";

const W = 900;
const H = 520;
const CLIENT = { x: 60, y: 232, w: 120, h: 56 };
const GATE = { x: 300, y: 190, w: 190, h: 140 };
const REP_X = 700;
const REP_W = 160;
const REP_H = 66;
const REP_Y = [86, 227, 368];

type Pt = { x: number; y: number };

function bezier(p0: Pt, p1: Pt, p2: Pt, p3: Pt, t: number): Pt {
  const u = 1 - t;
  return {
    x: u * u * u * p0.x + 3 * u * u * t * p1.x + 3 * u * t * t * p2.x + t * t * t * p3.x,
    y: u * u * u * p0.y + 3 * u * u * t * p1.y + 3 * u * t * t * p2.y + t * t * t * p3.y,
  };
}

function replicaPath(i: number): { d: string; at: (t: number) => Pt } {
  const p0 = { x: GATE.x + GATE.w, y: GATE.y + GATE.h / 2 };
  const p3 = { x: REP_X, y: REP_Y[i] + REP_H / 2 };
  const p1 = { x: p0.x + 90, y: p0.y };
  const p2 = { x: p3.x - 90, y: p3.y };
  return {
    d: `M${p0.x},${p0.y} C${p1.x},${p1.y} ${p2.x},${p2.y} ${p3.x},${p3.y}`,
    at: (t) => bezier(p0, p1, p2, p3, t),
  };
}

const clientPath = {
  at: (t: number): Pt => ({ x: CLIENT.x + CLIENT.w + (GATE.x - CLIENT.x - CLIENT.w) * t, y: CLIENT.y + CLIENT.h / 2 }),
};

interface Segment {
  at: (t: number) => Pt;
  reverse: boolean;
  color: string;
  duration: number;
  /** Flash crimson at the end of this segment (an attempt failed there). */
  burst?: boolean;
}

interface Journey {
  segments: Segment[];
  start: number;
  total: number;
}

const STEEL = "#c9d6e3";
const CRIMSON = "#ff3b5c";
const SEG = 0.34;

function journeyFor(r: RequestResult, paths: ReturnType<typeof replicaPath>[], now: number): Journey {
  const segs: Segment[] = [{ at: clientPath.at, reverse: false, color: STEEL, duration: SEG * 0.6 }];
  for (const a of r.attempts) {
    const i = Number(a.replica.split("-")[1]) - 1;
    const p = paths[i] ?? paths[0];
    const ok = a.outcome === "success";
    segs.push({ at: p.at, reverse: false, color: STEEL, duration: SEG, burst: !ok });
    segs.push({ at: p.at, reverse: true, color: ok ? STEEL : CRIMSON, duration: ok ? SEG : SEG * 0.7 });
  }
  segs.push({ at: clientPath.at, reverse: true, color: STEEL, duration: SEG * 0.6 });
  return { segments: segs, start: now, total: segs.reduce((s, x) => s + x.duration, 0) };
}

interface Props {
  className?: string;
}

/** Gateway fanning out to three replicas with live request particles and periodic kills. */
export function Fanout({ className }: Props) {
  const reduce = useReducedMotion();
  const run = useMemo(
    () =>
      new ChaosRun({
        ...DEFAULT_CHAOS,
        seed: 7,
        rps: 22,
        durationSeconds: 1e9,
        killIntervalMin: 6,
        killIntervalMax: 10,
        restartAfter: 3,
      }),
    [],
  );
  const paths = useMemo(() => [0, 1, 2].map(replicaPath), []);
  const canvas = useRef<HTMLCanvasElement>(null);
  const journeys = useRef<Journey[]>([]);
  const consumed = useRef(0);
  const elapsed = useRef(0);
  const [, setFrame] = useState(0);
  const frameCount = useRef(0);

  useEffect(() => {
    run.startAutoChaos();
  }, [run]);

  useTicker((dt) => {
    run.step(dt);
    elapsed.current += dt;
    const now = elapsed.current;
    while (consumed.current < run.results.length) {
      const r = run.results[consumed.current++];
      if (!reduce) journeys.current.push(journeyFor(r, paths, now));
    }
    if (journeys.current.length > 90) journeys.current.splice(0, journeys.current.length - 90);
    journeys.current = journeys.current.filter((j) => now - j.start < j.total + 0.4);
    draw(now);
    frameCount.current += 1;
    if (frameCount.current % 8 === 0) setFrame((f) => f + 1);
  }, true);

  function draw(now: number) {
    const c = canvas.current;
    if (!c) return;
    const ctx = c.getContext("2d");
    if (!ctx) return;
    const dpr = Math.min(2, window.devicePixelRatio || 1);
    if (c.width !== W * dpr) {
      c.width = W * dpr;
      c.height = H * dpr;
    }
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, W, H);
    for (const j of journeys.current) {
      let t = now - j.start;
      let seg: Segment | null = null;
      for (const s of j.segments) {
        if (t <= s.duration) {
          seg = s;
          break;
        }
        t -= s.duration;
      }
      if (!seg) continue;
      const f = t / seg.duration;
      const p = seg.at(seg.reverse ? 1 - f : f);
      const burst = seg.burst && f > 0.85;
      ctx.beginPath();
      ctx.fillStyle = burst ? CRIMSON : seg.color;
      ctx.shadowColor = burst ? CRIMSON : seg.color;
      ctx.shadowBlur = burst ? 22 : 10;
      ctx.arc(p.x, p.y, burst ? 5.5 : 3.2, 0, Math.PI * 2);
      ctx.fill();
    }
    ctx.shadowBlur = 0;
  }

  const reps = run.pool.replicas;
  const snap = run.snapshot;

  return (
    <div className={`fanout ${className ?? ""}`} aria-label="Live gateway diagram: requests flow from clients through the FailSafe gateway to three upstream replicas while replicas are killed and restarted">
      <svg viewBox={`0 0 ${W} ${H}`} className="fanout-svg" role="img" aria-hidden="true">
        <defs>
          <linearGradient id="gate-fill" x1="0" x2="1" y1="0" y2="1">
            <stop offset="0" stopColor="rgba(201,214,227,0.16)" />
            <stop offset="1" stopColor="rgba(201,214,227,0.04)" />
          </linearGradient>
          <filter id="soft" x="-20%" y="-20%" width="140%" height="140%">
            <feGaussianBlur stdDeviation="6" />
          </filter>
        </defs>

        <line x1={CLIENT.x + CLIENT.w} y1={CLIENT.y + CLIENT.h / 2} x2={GATE.x} y2={GATE.y + GATE.h / 2} className="fan-wire" />
        {paths.map((p, i) => {
          const r = reps[i];
          const dead = !r.alive;
          const off = !r.available;
          return <path key={i} d={p.d} className={`fan-wire ${dead ? "fan-wire--dead" : off ? "fan-wire--off" : ""}`} />;
        })}

        <g className="fan-node">
          <rect x={CLIENT.x} y={CLIENT.y} width={CLIENT.w} height={CLIENT.h} rx="14" />
          <text x={CLIENT.x + CLIENT.w / 2} y={CLIENT.y + 24} className="fan-title">clients</text>
          <text x={CLIENT.x + CLIENT.w / 2} y={CLIENT.y + 43} className="fan-sub">{snap.requests.toLocaleString("en-US")} req</text>
        </g>

        <g className="fan-node fan-node--gate">
          <rect x={GATE.x} y={GATE.y} width={GATE.w} height={GATE.h} rx="20" fill="url(#gate-fill)" />
          <text x={GATE.x + GATE.w / 2} y={GATE.y + 34} className="fan-title fan-title--big">FailSafe</text>
          <text x={GATE.x + GATE.w / 2} y={GATE.y + 62} className="fan-sub">token bucket</text>
          <text x={GATE.x + GATE.w / 2} y={GATE.y + 82} className="fan-sub">breaker per replica</text>
          <text x={GATE.x + GATE.w / 2} y={GATE.y + 102} className="fan-sub">retry + failover</text>
          <text x={GATE.x + GATE.w / 2} y={GATE.y + 126} className="fan-sub fan-sub--crimson">client-visible failed: {snap.clientFailed}</text>
        </g>

        {reps.map((r, i) => {
          const y = REP_Y[i];
          const dead = !r.alive;
          const state = dead ? "killed" : !r.healthy ? "restarting" : r.breaker.state !== "closed" ? r.breaker.state.replace("_", "-") : "healthy";
          return (
            <g key={r.label} className={`fan-node fan-rep fan-rep--${dead ? "dead" : r.healthy ? "ok" : "warm"}`}>
              {dead && <rect x={REP_X - 6} y={y - 6} width={REP_W + 12} height={REP_H + 12} rx="20" className="fan-rep-glow" filter="url(#soft)" />}
              <rect x={REP_X} y={y} width={REP_W} height={REP_H} rx="16" />
              <circle cx={REP_X + 22} cy={y + REP_H / 2} r="6" className="fan-dot" />
              <text x={REP_X + 40} y={y + 28} className="fan-title fan-title--left">{r.label}</text>
              <text x={REP_X + 40} y={y + 48} className="fan-sub fan-sub--left">{state}</text>
              {dead && <text x={REP_X + REP_W - 14} y={y + 28} className="fan-kill">SIGKILL</text>}
            </g>
          );
        })}

        <text x={W - 20} y={H - 16} className="fan-foot">kills {run.kills}  failovers {snap.failovers}  retries {snap.retries}</text>
      </svg>
      <canvas ref={canvas} className="fanout-canvas" aria-hidden="true" />
    </div>
  );
}
