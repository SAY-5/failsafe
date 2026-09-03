import { animate, useInView, useReducedMotion } from "framer-motion";
import { useEffect, useRef, useState } from "react";

interface Props {
  value: number;
  duration?: number;
  format?: (n: number) => string;
  className?: string;
  /** Start counting only when scrolled into view. */
  inView?: boolean;
}

const defaultFormat = (n: number) => Math.round(n).toLocaleString("en-US");

/** Counts from 0 to `value` once, then tracks later changes without re-animating. */
export function AnimatedNumber({ value, duration = 1.6, format = defaultFormat, className, inView = true }: Props) {
  const ref = useRef<HTMLSpanElement>(null);
  const seen = useInView(ref, { once: true, margin: "-10% 0px" });
  const reduce = useReducedMotion();
  const [shown, setShown] = useState(0);
  const played = useRef(false);

  useEffect(() => {
    if (played.current) {
      setShown(value);
      return;
    }
    if (inView && !seen) return;
    played.current = true;
    if (reduce) {
      setShown(value);
      return;
    }
    const controls = animate(0, value, {
      duration,
      ease: [0.22, 1, 0.36, 1],
      onUpdate: (v) => setShown(v),
    });
    return () => controls.stop();
  }, [value, seen, inView, reduce, duration]);

  return (
    <span ref={ref} className={className}>
      {format(shown)}
    </span>
  );
}
