/**
 * perf-sequence-rolling.mjs
 *
 * Measures per-event overhead of LiveSequenceRollingAggregation.
 *
 * Analytical complexity:
 *   - #check: O(1) per source event (one Math.floor, one comparison)
 *   - Emission (rare): O(C) where C = column count (rolling.value() snapshot)
 *   - Overall: O(N) for N source events; amortised O(C/I) allocations where
 *     I = number of events per interval
 *
 * We measure the added cost of the sequence rolling on top of the baseline
 * rolling aggregation by running the same event stream with and without it.
 */

import { performance } from 'node:perf_hooks';
import { LiveSeries, LiveRollingAggregation, Sequence } from '../dist/index.js';

const schema = Object.freeze([
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number' },
]);

// Pre-build rows outside the timed section so allocation doesn't skew results
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

// ── Baseline: rolling only, no sequence ─────────────────────────
console.log(
  '\nBaseline — 30 000 events, 1 Hz, 60 s rolling, no sequence rolling:',
);
const baseMs = bench('rolling only', () => {
  const live = new LiveSeries({ name: 'bench', schema });
  const rolling = new LiveRollingAggregation(live, '60s', { value: 'avg' });
  live.pushMany(ROWS_30K);
  rolling.dispose();
});

// ── Scenario 1: 30 s interval (~1 000 emissions) ─────────────────
console.log(
  '\nScenario 1 — same stream + sequence rolling, 30 s interval (~1 000 emissions):',
);
const s1Ms = bench('live.rolling(Sequence.every(30s), 60s, ...)', () => {
  const live = new LiveSeries({ name: 'bench', schema });
  const seq = live.rolling(Sequence.every('30s'), '60s', { value: 'avg' });
  live.pushMany(ROWS_30K);
  seq.dispose();
});

// ── Scenario 2: 1 s interval (~30 000 emissions — every event emits) ──
console.log(
  '\nScenario 2 — same stream + sequence rolling, 1 s interval (~30 000 emissions):',
);
const s2Ms = bench('live.rolling(Sequence.every(1s), 60s, ...)', () => {
  const live = new LiveSeries({ name: 'bench', schema });
  const seq = live.rolling(Sequence.every('1s'), '60s', { value: 'avg' });
  live.pushMany(ROWS_30K);
  seq.dispose();
});

// ── Overhead summary ─────────────────────────────────────────────
console.log('\nOverhead vs baseline (30 000 events):');
console.log(
  `  sequence(30s): +${(s1Ms - baseMs).toFixed(1)} ms (+${(((s1Ms - baseMs) / baseMs) * 100).toFixed(0)}%)`,
);
console.log(
  `  sequence(1s):  +${(s2Ms - baseMs).toFixed(1)} ms (+${(((s2Ms - baseMs) / baseMs) * 100).toFixed(0)}%) — every event emits`,
);
console.log(
  `\nPer-event overhead of #check (scenario 2, ~30 000 checks + emissions):`,
);
console.log(`  ~${(((s2Ms - baseMs) / 30_000) * 1e6).toFixed(0)} ns/event`);
console.log('\nDone.');
