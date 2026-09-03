const LINKS = [
  ["#bucket", "Bucket"],
  ["#breaker", "Breaker"],
  ["#retries", "Retries"],
  ["#chaos", "Chaos"],
] as const;

export function Nav() {
  return (
    <nav className="nav" aria-label="Sections">
      <div className="wrap nav-row">
        <a href="#top" className="nav-brand display">
          <span className="nav-mark" aria-hidden="true" />
          FailSafe
        </a>
        <ul className="nav-links">
          {LINKS.map(([href, label]) => (
            <li key={href}>
              <a href={href}>{label}</a>
            </li>
          ))}
          <li>
            <a href="https://github.com/SAY-5/failsafe" target="_blank" rel="noreferrer">
              GitHub
            </a>
          </li>
        </ul>
      </div>
    </nav>
  );
}
