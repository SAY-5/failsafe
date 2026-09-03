const REPO = "https://github.com/SAY-5/failsafe";

export function Footer() {
  return (
    <footer className="footer">
      <div className="wrap footer-grid">
        <div>
          <p className="footer-brand display">FailSafe</p>
          <p className="footer-text">
            This page is a browser port of the real gateway. The production system is Python 3.12 (FastAPI, httpx,
            uvicorn) packaged with Docker, deployed to Kubernetes with liveness and readiness probes and an EndpointSlice
            based replica discovery, and observed through Prometheus and a provisioned Grafana dashboard. The token
            bucket, breaker, retry policy, replica pool and health checker here follow the Python modules line for line;
            the traffic and the replica processes are synthetic and run on a seeded virtual clock.
          </p>
        </div>
        <ul className="footer-links mono">
          <li>
            <a href={REPO} target="_blank" rel="noreferrer">
              github.com/SAY-5/failsafe
            </a>
          </li>
          <li>
            <a href={`${REPO}/blob/main/ARCHITECTURE.md`} target="_blank" rel="noreferrer">
              ARCHITECTURE.md
            </a>
          </li>
          <li>
            <a href={`${REPO}/tree/main/failsafe`} target="_blank" rel="noreferrer">
              failsafe/ (gateway package)
            </a>
          </li>
          <li>
            <a href={`${REPO}/tree/main/web/src/sim`} target="_blank" rel="noreferrer">
              web/src/sim (this port)
            </a>
          </li>
          <li>
            <span>
              console: <code className="mono">failsafeSelfCheck()</code>
            </span>
          </li>
        </ul>
      </div>
      <p className="wrap footer-fine mono">MIT licensed. Numbers in the hero come from the recorded compose run in the README; everything below it is computed live.</p>
    </footer>
  );
}
