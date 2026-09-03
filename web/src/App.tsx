import { Nav } from "./components/Nav";
import { Hero } from "./sections/Hero";

export default function App() {
  return (
    <>
      <a className="sr-only" href="#bucket">
        Skip to the interactive sections
      </a>
      <Nav />
      <Hero />
      <main id="main" />
    </>
  );
}
