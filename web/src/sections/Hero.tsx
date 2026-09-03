import { motion, useReducedMotion } from "framer-motion";
import { AnimatedNumber } from "../components/AnimatedNumber";
import { Fanout } from "../components/Fanout";

const REPO = "https://github.com/SAY-5/failsafe";

export function Hero() {
  const reduce = useReducedMotion();
  const rise = (delay: number) => ({
    initial: reduce ? false : { opacity: 0, y: 22 },
    animate: { opacity: 1, y: 0 },
    transition: { duration: 0.9, delay, ease: [0.22, 1, 0.36, 1] },
  });

  return (
    <header className="hero" id="top">
      <div className="wrap hero-grid">
        <div className="hero-copy">
          <motion.p className="section-index" {...rise(0)}>
            resilient API gateway
          </motion.p>
          <motion.h1 className="hero-title display" {...rise(0.08)}>
            Kill a pod.
            <br />
            <span className="hero-title-muted">Nobody notices.</span>
          </motion.h1>
          <motion.p className="hero-lede" {...rise(0.18)}>
            FailSafe sits in front of a set of upstream replicas and keeps client requests succeeding while
            those replicas are rate limited, timing out, crashing or being killed outright. Token buckets,
            circuit breakers, retries with jittered backoff and replica failover, each one ported to
            TypeScript and running live on this page.
          </motion.p>
          <motion.dl className="hero-stats" {...rise(0.28)} aria-label="Results from the recorded chaos run">
            <div className="stat">
              <dt className="label">requests</dt>
              <dd className="value">
                <AnimatedNumber value={6751} />
              </dd>
            </div>
            <div className="stat">
              <dt className="label">client-visible failed</dt>
              <dd className="value crimson">
                <AnimatedNumber value={0} />
              </dd>
            </div>
            <div className="stat">
              <dt className="label">containers killed</dt>
              <dd className="value">
                <AnimatedNumber value={4} />
              </dd>
            </div>
            <div className="stat">
              <dt className="label">p50 latency</dt>
              <dd className="value steel">
                <AnimatedNumber value={3.2} format={(n) => `${n.toFixed(1)} ms`} />
              </dd>
            </div>
          </motion.dl>
          <motion.div className="hero-actions" {...rise(0.38)}>
            <a className="btn btn--crimson" href="#chaos">
              Run the chaos
            </a>
            <a className="btn btn--ghost" href={REPO} target="_blank" rel="noreferrer">
              Read the source
            </a>
          </motion.div>
          <motion.p className="hero-note mono" {...rise(0.46)}>
            45 s at 150 rps through the real gateway, one upstream container SIGKILLed every few seconds.
          </motion.p>
        </div>
        <motion.div
          className="hero-visual glass"
          initial={reduce ? false : { opacity: 0, scale: 0.97 }}
          animate={{ opacity: 1, scale: 1 }}
          transition={{ duration: 1.1, delay: 0.2, ease: [0.22, 1, 0.36, 1] }}
        >
          <Fanout />
          <p className="hero-visual-cap mono">live: synthetic traffic through the browser port, one replica killed every 6 to 10 s</p>
        </motion.div>
      </div>
    </header>
  );
}
