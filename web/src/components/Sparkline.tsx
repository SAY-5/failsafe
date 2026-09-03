interface Series {
  name: string;
  values: number[];
  color: string;
  width?: number;
}

interface Props {
  series: Series[];
  /** Vertical markers (x index) drawn as crimson ticks, e.g. kills. */
  markers?: { x: number; label: string }[];
  height?: number;
  maxPoints: number;
  unit?: string;
  title: string;
}

/** Multi-series SVG sparkline with a shared log-free linear scale and kill markers. */
export function Sparkline({ series, markers = [], height = 120, maxPoints, unit = "ms", title }: Props) {
  const w = 600;
  const h = height;
  const pad = 6;
  const all = series.flatMap((s) => s.values);
  const max = Math.max(8, ...all) * 1.1;
  const x = (i: number) => pad + (i / Math.max(1, maxPoints - 1)) * (w - pad * 2);
  const y = (v: number) => h - pad - (Math.min(v, max) / max) * (h - pad * 2);

  const path = (vals: number[]) =>
    vals.length === 0 ? "" : vals.map((v, i) => `${i === 0 ? "M" : "L"}${x(i).toFixed(1)},${y(v).toFixed(1)}`).join(" ");

  const last = series.map((s) => s.values[s.values.length - 1] ?? 0);
  const desc = series.map((s, i) => `${s.name} ${last[i].toFixed(1)} ${unit}`).join(", ");

  return (
    <figure className="spark" aria-label={title}>
      <svg viewBox={`0 0 ${w} ${h}`} role="img" aria-label={`${title}: ${desc}`} preserveAspectRatio="none">
        <defs>
          <linearGradient id="spark-fill" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0" stopColor="rgba(201,214,227,0.22)" />
            <stop offset="1" stopColor="rgba(201,214,227,0)" />
          </linearGradient>
        </defs>
        {[0.25, 0.5, 0.75].map((f) => (
          <line key={f} x1={pad} x2={w - pad} y1={y(max * f)} y2={y(max * f)} stroke="rgba(201,214,227,0.08)" strokeDasharray="3 5" />
        ))}
        {markers.map((m, i) => (
          <g key={i}>
            <line x1={x(m.x)} x2={x(m.x)} y1={pad} y2={h - pad} stroke="#ff3b5c" strokeWidth={1.2} strokeOpacity={0.8} />
            <circle cx={x(m.x)} cy={pad + 3} r={3} fill="#ff3b5c" />
          </g>
        ))}
        {series[0] && series[0].values.length > 1 && (
          <path d={`${path(series[0].values)} L${x(series[0].values.length - 1).toFixed(1)},${h - pad} L${pad},${h - pad} Z`} fill="url(#spark-fill)" />
        )}
        {series.map((s) => (
          <path key={s.name} d={path(s.values)} fill="none" stroke={s.color} strokeWidth={s.width ?? 1.6} strokeLinejoin="round" strokeLinecap="round" />
        ))}
      </svg>
      <figcaption className="spark-legend mono">
        {series.map((s, i) => (
          <span key={s.name}>
            <i style={{ background: s.color }} aria-hidden="true" />
            {s.name} <b>{last[i].toFixed(1)}</b> {unit}
          </span>
        ))}
        <span className="spark-max">scale 0 to {max.toFixed(0)} {unit}</span>
      </figcaption>
    </figure>
  );
}
