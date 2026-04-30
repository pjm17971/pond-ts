/**
 * perf-rolling-sample.mjs
 *
 * Measures the per-event overhead of `rolling.sample(sequence)` on top
 * of a baseline `LiveRollingAggregation`.
 *
 * Analytical complexity:
 *   - #check: O(1) per source event (one Math.floor, one comparison)
 *   - Emission (rare): O(C) where C = column count (rolling.value() snapshot)
 *   - Overall: O(N) for N source events
 *
 * Sampler is data-driven: it observes the rolling's 'event' stream and
 * fires only when bucket index advances. The expected hot path is one
 * Math.floor + one int comparison per source event.
 */

import { performance } from 'node:perf_hooks';
import { LiveSeries, Sequence } from '../dist/index.js';

const schema = Object.freeze([
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number' },
]);

const ROWS_30K = Array.from({ length: 30_000 }, (_, t) => [t * 1_000, t % 100]);

function median(values) {
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0
    ? (sorted[mid - 1] + sorted[mid]) / 2
    : sorted[mid];
}

function bench(label, fn, repeats = 6) {
  fn(); // warmup
  const samples = [];
  for (let i = 0; i < repeats; i++) {
    const t0 = performance.now();
    fn();
    samples.push(performance.now() - t0);
  }
  const med = median(samples);
  console.log(`  ${label}: ${med.toFixed(1)} ms`);
  return med;
}

// ── Baseline: rolling only ───────────────────────────────────────
console.log('\nBaseline — 30 000 events, 1 Hz, 60 s rolling, no sampler:');
const baseMs = bench('rolling only', () => {
  const live = new LiveSeries({ name: 'bench', schema });
  const rolling = live.rolling('60s', { value: 'avg' });
  live.pushMany(ROWS_30K);
  rolling.dispose();
});

// ── Scenario 1: 30 s sampler (~1 000 emissions) ──────────────────
console.log('\nScenario 1 — same stream + .sample(30s) (~1 000 emissions):');
const s1Ms = bench('rolling + sample(30s)', () => {
  const live = new LiveSeries({ name: 'bench', schema });
  const rolling = live.rolling('60s', { value: 'avg' });
  const sample = rolling.sample(Sequence.every('30s'));
  live.pushMany(ROWS_30K);
  sample.dispose();
  rolling.dispose();
});

// ── Scenario 2: 1 s sampler (~30 000 emissions) ──────────────────
console.log(
  '\nScenario 2 — same stream + .sample(1s) (~30 000 emissions, every event emits):',
);
const s2Ms = bench('rolling + sample(1s)', () => {
  const live = new LiveSeries({ name: 'bench', schema });
  const rolling = live.rolling('60s', { value: 'avg' });
  const sample = rolling.sample(Sequence.every('1s'));
  live.pushMany(ROWS_30K);
  sample.dispose();
  rolling.dispose();
});

// ── Overhead summary ─────────────────────────────────────────────
console.log('\nOverhead vs baseline (30 000 events):');
console.log(
  `  sample(30s): +${(s1Ms - baseMs).toFixed(1)} ms (+${(((s1Ms - baseMs) / baseMs) * 100).toFixed(0)}%)`,
);
console.log(
  `  sample(1s):  +${(s2Ms - baseMs).toFixed(1)} ms (+${(((s2Ms - baseMs) / baseMs) * 100).toFixed(0)}%)`,
);
console.log(
  `\nPer-event overhead of #check (scenario 2, ~30 000 checks + emissions):`,
);
console.log(`  ~${(((s2Ms - baseMs) / 30_000) * 1e6).toFixed(0)} ns/event`);
console.log('\nDone.');
