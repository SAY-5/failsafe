import { Nav } from "./components/Nav";
import { BreakerSection } from "./sections/BreakerSection";
import { BucketSection } from "./sections/BucketSection";
import { ChaosSection } from "./sections/ChaosSection";
import { Footer } from "./sections/Footer";
import { Hero } from "./sections/Hero";
import { RetrySection } from "./sections/RetrySection";

export default function App() {
  return (
    <>
      <a className="sr-only" href="#bucket">
        Skip to the interactive sections
      </a>
      <Nav />
      <Hero />
      <main id="main">
        <BucketSection />
        <BreakerSection />
        <RetrySection />
        <ChaosSection />
      </main>
      <Footer />
    </>
  );
}
