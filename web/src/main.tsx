import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
import "./styles/theme.css";
import "./styles/sections.css";
import { runSelfCheck } from "./sim/selfcheck";

declare global {
  interface Window {
    failsafeSelfCheck: () => boolean;
  }
}
window.failsafeSelfCheck = runSelfCheck;
if (import.meta.env.DEV) runSelfCheck();

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
