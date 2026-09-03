import { useCallback, useEffect, useRef, useState } from "react";

/**
 * requestAnimationFrame loop that hands the callback wall-clock seconds since the
 * previous frame (capped so a background tab does not fast-forward the sim).
 */
export function useTicker(onFrame: (dtSeconds: number) => void, running: boolean): void {
  const cb = useRef(onFrame);
  cb.current = onFrame;
  useEffect(() => {
    if (!running) return;
    let raf = 0;
    let last = performance.now();
    const loop = (t: number) => {
      const dt = Math.min(0.1, (t - last) / 1000);
      last = t;
      cb.current(dt);
      raf = requestAnimationFrame(loop);
    };
    raf = requestAnimationFrame(loop);
    return () => cancelAnimationFrame(raf);
  }, [running]);
}

/** Re-render at most every `ms` while `running`, for panels that read a mutable sim. */
export function useRerender(ms: number, running: boolean): [number, () => void] {
  const [n, setN] = useState(0);
  const bump = useCallback(() => setN((x) => x + 1), []);
  useEffect(() => {
    if (!running) return;
    const id = window.setInterval(bump, ms);
    return () => window.clearInterval(id);
  }, [ms, running, bump]);
  return [n, bump];
}
