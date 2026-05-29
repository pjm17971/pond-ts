// Benchmark for Phase 4.7 Step 7 — LiveSeries columnar ring buffer.
//
// Measures the streaming ingest pattern (pushMany + retention) that
// the ring backing targets, snapshot cost, and — the headline — heap
// retention for a full bounded window. The row backing keeps N live
// `Event` objects (tenured → major GC pressure); the ring backing
// keeps typed-array columns and lets pushed events become nursery
// garbage.
//
// Run with GC instrumentation:
//
//     npm run build --workspace=pond-ts
//     node --expose-gc scripts/perf-live-series.mjs            # timing
//     node --expose-gc scripts/perf-live-series.mjs --heap=strict
//     node --expose-gc scripts/perf-live-series.mjs --heap=reorder
//
// Heap is measured in process isolation (one backing per process) so
// the two backings' retained-heap numbers can't contaminate each
// other. The strict run is ring-backed; reorder is Event[]-backed.
//
// `strict` / `drop` ordering use the ring backing; `reorder` uses the
// Event[] backing. Compare the two columns (same workload) to read the
// win. To compare against the pre-Step-7 baseline, revert the
// RingLiveStorage selection in live-series.ts, rebuild, and re-run.
//
// Note on eviction: pushMany evicts once per batch (amortized
// splice for the array backing). Single-event push() into a large
// window is O(window) per evict on the array backing and O(1) on the
// ring — a real asymptotic difference, but pathologically slow to
// bench on the array side, so this script uses pushMany batches.

import { performance } from 'node:perf_hooks';
import { LiveSeries } from '../dist/index.js';

const schema = Object.freeze([
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number' },
  { name: 'load', kind: 'number' },
]);

function median(values) {
  const sorted = [...values].sort((a, b) => a - b);
  const m = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[m - 1] + sorted[m]) / 2 : sorted[m];
}

function bench(label, fn, repeats = 8) {
  for (let i = 0; i < 2; i += 1) fn();
  const samples = [];
  for (let i = 0; i < repeats; i += 1) {
    const start = performance.now();
    fn();
    samples.push(performance.now() - start);
  }
  return {
    label,
    medianMs: Number(median(samples).toFixed(3)),
    minMs: Number(Math.min(...samples).toFixed(3)),
    maxMs: Number(Math.max(...samples).toFixed(3)),
  };
}

function heapMB() {
  if (globalThis.gc) {
    globalThis.gc();
    globalThis.gc();
  }
  return Number((process.memoryUsage().heapUsed / 1024 / 1024).toFixed(1));
}

// Heap measurement runs in process isolation: `--heap=<ordering>`
// builds ONE backing, measures retained heap, prints one record, and
// exits. Run it once per ordering as separate processes so the two
// measurements can't contaminate each other (shared input arrays,
// the other backing's series staying reachable, etc.).
const heapMode = process.argv
  .find((a) => a.startsWith('--heap='))
  ?.slice('--heap='.length);

if (heapMode) {
  const W = 200_000;
  // Build inputs, then settle the heap so `baseline` excludes the
  // transient input array churn.
  const rows = Array.from({ length: W }, (_, i) => [
    1_000 + i,
    i % 100,
    (i % 7) + 1,
  ]);
  const baseline = heapMB();
  const live = new LiveSeries({
    name: 's',
    schema,
    ordering: heapMode,
    retention: { maxEvents: W },
  });
  live.pushMany(rows);
  const retained = heapMB() - baseline;
  if (live.length !== W) throw new Error('unexpected window length');
  console.log(
    JSON.stringify({
      heap: {
        ordering: heapMode,
        backing: heapMode === 'reorder' ? 'Event[]' : 'ring',
        windowSize: W,
        retainedHeapMB: Number(retained.toFixed(1)),
        length: live.length,
      },
    }),
  );
  process.exit(0);
}

const results = [];

/* ── Throughput: pushMany batches (gRPC wire-batch shape) ─────────── */
// 300k rows in 1k batches through a 50k-row window. Eviction is
// amortized per batch on both backings; this measures the per-event
// decompose-into-ring vs Event-array-push cost plus batch eviction.

{
  const total = 300_000;
  const batchSize = 1_000;
  const batches = [];
  for (let b = 0; b < total / batchSize; b += 1) {
    batches.push(
      Array.from({ length: batchSize }, (_, i) => {
        const idx = b * batchSize + i;
        return [1_000 + idx, idx % 100, (idx % 7) + 1];
      }),
    );
  }
  for (const ordering of ['strict', 'reorder']) {
    results.push(
      bench(
        `pushMany ${total} (1k batches, window 50k) / ordering=${ordering}`,
        () => {
          const live = new LiveSeries({
            name: 's',
            schema,
            ordering,
            retention: { maxEvents: 50_000 },
          });
          for (const batch of batches) live.pushMany(batch);
        },
      ),
    );
  }
}

/* ── Snapshot cost: toTimeSeries over a full window ───────────────── */

{
  const W = 50_000;
  const rows = Array.from({ length: W }, (_, i) => [
    1_000 + i,
    i % 100,
    (i % 7) + 1,
  ]);
  for (const ordering of ['strict', 'reorder']) {
    const live = new LiveSeries({
      name: 's',
      schema,
      ordering,
      retention: { maxEvents: W },
    });
    live.pushMany(rows);
    results.push(
      bench(`toTimeSeries (window=${W}) / ordering=${ordering}`, () => {
        live.toTimeSeries();
      }),
    );
  }
}

console.log(JSON.stringify({ timing: results }, null, 2));

if (!globalThis.gc) {
  console.error(
    '\n[warn] run with `node --expose-gc` for meaningful heap numbers',
  );
}
