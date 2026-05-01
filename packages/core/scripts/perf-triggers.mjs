/**
 * perf-triggers.mjs
 *
 * Measures per-event overhead of trigger-based emission.
 *
 * Two scenarios cover the v0.12.0-experimental triggers surface:
 *
 * 1. **Non-partitioned clock-triggered rolling** — replaces
 *    v0.11.8's `LiveSequenceRollingAggregation`. Standard webapp
 *    telemetry shape: 1Hz to ~10Hz events, sample to 30s.
 *
 * 2. **Synchronised partitioned rolling** — the gRPC dashboard's
 *    M3.5 use case. ~5Hz to ~10Hz per partition, P=100 hosts,
 *    sampled at 200ms cadence.
 *
 * Bench data is shaped to match realistic event-vs-trigger
 * cadences (events arrive much faster than trigger fires); each
 * tick collects multiple events, each emission is amortised over
 * a tick's worth of inputs.
 *
 * Analytical complexity:
 *   - Per source event: O(1) state update (rolling deque add+evict)
 *     + O(1) bucket-index check.
 *   - Per emission: O(P · C) where P is known partition count and
 *     C is reducer column count.
 */

import { performance } from 'node:perf_hooks';
import { LiveSeries, Sequence, Trigger } from '../dist/index.js';

const flatSchema = Object.freeze([
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number' },
]);

const partitionedSchema = Object.freeze([
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'number' },
  { name: 'host', kind: 'string' },
]);

// 30 000 events at 10 ms apart = 300 s of data time. With a 30 s
// clock trigger, that's 10 boundary crossings (10 emissions); with
// a 1 s trigger, 300 emissions. Per-event work dominates.
const FLAT_ROWS_30K = Array.from({ length: 30_000 }, (_, t) => [
  t * 10,
  t % 100,
]);

// 30 000 events × 100 hosts at 10 ms apart per global event (round-
// robin assignment to hosts). Total span: 300 s of data time. With
// a 200 ms clock trigger that's 1 500 ticks × 100 partitions =
// 150 000 emissions.
const HOSTS = Array.from({ length: 100 }, (_, i) => `host-${i}`);
const PARTITIONED_ROWS_30K = Array.from({ length: 30_000 }, (_, t) => [
  t * 10,
  t % 100,
  HOSTS[t % HOSTS.length],
]);

function median(values) {
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0
    ? (sorted[mid - 1] + sorted[mid]) / 2
    : sorted[mid];
}

function bench(label, fn, repeats = 6) {
  fn();
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

// ── Non-partitioned ─────────────────────────────────────────────

console.log(
  '\nNon-partitioned: 30 000 events @ 10 ms apart (300 s of data time), 60 s rolling window.',
);

const flatEvent = bench('Trigger.event() — emits per source event', () => {
  const live = new LiveSeries({ name: 'b', schema: flatSchema });
  const r = live.rolling('60s', { value: 'avg' });
  live.pushMany(FLAT_ROWS_30K);
  r.dispose();
});

const flatClock30s = bench('Trigger.clock(30s) — ~10 emissions', () => {
  const live = new LiveSeries({ name: 'b', schema: flatSchema });
  const r = live.rolling(
    '60s',
    { value: 'avg' },
    { trigger: Trigger.clock(Sequence.every('30s')) },
  );
  live.pushMany(FLAT_ROWS_30K);
  r.dispose();
});

const flatClock1s = bench('Trigger.clock(1s) — ~300 emissions', () => {
  const live = new LiveSeries({ name: 'b', schema: flatSchema });
  const r = live.rolling(
    '60s',
    { value: 'avg' },
    { trigger: Trigger.clock(Sequence.every('1s')) },
  );
  live.pushMany(FLAT_ROWS_30K);
  r.dispose();
});

console.log('\nOverhead vs Trigger.event() (non-partitioned, 30 000 events):');
console.log(
  `  clock(30s): ${(flatClock30s - flatEvent).toFixed(1)} ms (${(((flatClock30s - flatEvent) / flatEvent) * 100).toFixed(0)}%)`,
);
console.log(
  `  clock(1s):  ${(flatClock1s - flatEvent).toFixed(1)} ms (${(((flatClock1s - flatEvent) / flatEvent) * 100).toFixed(0)}%)`,
);

// ── Synchronised partitioned ────────────────────────────────────

console.log(
  '\nSynchronised partitioned: 30 000 events × 100 hosts @ 10 ms global cadence, 60 s window.',
);

const partEvent = bench(
  'partitionBy + rolling — per-partition (no trigger)',
  () => {
    const live = new LiveSeries({ name: 'b', schema: partitionedSchema });
    live.partitionBy('host').rolling('60s', { cpu: 'avg' }).toMap();
    live.pushMany(PARTITIONED_ROWS_30K);
  },
);

const partClock1s = bench(
  'partitionBy + rolling, Trigger.clock(1s) — ~300 ticks × 100 hosts = 30 000 emissions',
  () => {
    const live = new LiveSeries({ name: 'b', schema: partitionedSchema });
    live
      .partitionBy('host')
      .rolling(
        '60s',
        { cpu: 'avg' },
        { trigger: Trigger.clock(Sequence.every('1s')) },
      );
    live.pushMany(PARTITIONED_ROWS_30K);
  },
);

const partClock200ms = bench(
  'partitionBy + rolling, Trigger.clock(200ms) — ~1500 ticks × 100 hosts = 150 000 emissions',
  () => {
    const live = new LiveSeries({ name: 'b', schema: partitionedSchema });
    live
      .partitionBy('host')
      .rolling(
        '60s',
        { cpu: 'avg' },
        { trigger: Trigger.clock(Sequence.every('200ms')) },
      );
    live.pushMany(PARTITIONED_ROWS_30K);
  },
);

console.log(
  '\nOverhead vs no-trigger per-partition baseline (partitioned, 30 000 events × 100 hosts):',
);
console.log(
  `  clock(1s):    ${(partClock1s - partEvent).toFixed(1)} ms (${(((partClock1s - partEvent) / partEvent) * 100).toFixed(0)}%) — 30 000 emissions, ~${(((partClock1s - partEvent) / 30_000) * 1e6).toFixed(0)} ns/emission`,
);
console.log(
  `  clock(200ms): ${(partClock200ms - partEvent).toFixed(1)} ms (${(((partClock200ms - partEvent) / partEvent) * 100).toFixed(0)}%) — 150 000 emissions, ~${(((partClock200ms - partEvent) / 150_000) * 1e6).toFixed(0)} ns/emission`,
);
console.log('\nDone.');
