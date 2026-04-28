// Throughput scaling test for LivePartitionedSeries with rolling
// aggregation. Sweeps a matrix of (P partitions × N events/sec/partition)
// to find the limits of single-threaded sustained ingest.
//
// Each cell builds:
//
//   live → partitionBy('host') → rolling('1m', { value: 'avg', host: 'last' })
//        → collect()
//
// and pushes a fixed simulated duration's worth of globally-ordered events,
// measuring wall-clock ingest time + memory delta.
//
// Run from packages/core:
//   npm run build && node --expose-gc scripts/perf-live-partitioned-scale.mjs
//
// `--expose-gc` is optional but improves the memory baselines.

import { performance } from 'node:perf_hooks';
import { LiveSeries } from '../dist/index.js';

const schema = Object.freeze([
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number', required: false },
  { name: 'host', kind: 'string', required: false },
]);

function memoryMB() {
  if (global.gc) global.gc();
  return Math.round(process.memoryUsage().heapUsed / 1024 / 1024);
}

/**
 * Generate globally time-ordered events for P partitions × N events/sec
 * across `simSec` simulated seconds. Each "tick" emits one event per
 * partition; ticks are spaced 1000/N ms apart. The result is
 * monotonically non-decreasing on time (LiveSeries strict-ordering safe).
 */
function generateEvents(P, N, simSec) {
  const total = P * N * simSec;
  const events = new Array(total);
  let idx = 0;
  const intervalMs = 1000 / N;
  for (let s = 0; s < simSec; s++) {
    for (let n = 0; n < N; n++) {
      const t = Math.round(s * 1000 + n * intervalMs);
      for (let p = 0; p < P; p++) {
        events[idx] = [t, Math.sin(idx * 0.01), `host-${p}`];
        idx += 1;
      }
    }
  }
  return events;
}

function runCell(P, N, simSec) {
  const events = generateEvents(P, N, simSec);
  const totalEvents = events.length;

  // Baseline memory after generation (events array is already on the heap)
  const memBefore = memoryMB();

  // Build pipeline
  const live = new LiveSeries({ name: 'cpu', schema });
  const partitioned = live.partitionBy('host');
  const unified = partitioned
    .rolling('1m', { value: 'avg', host: 'last' })
    .collect();
  void unified;

  // Measure ingestion
  const t0 = performance.now();
  for (let i = 0; i < events.length; i++) live.push(events[i]);
  const t1 = performance.now();

  const memAfter = memoryMB();
  const wallMs = t1 - t0;
  const throughput = (totalEvents / wallMs) * 1000;

  // Cleanup so memory doesn't accumulate across cells
  partitioned.dispose();

  return {
    P,
    N,
    sec: simSec,
    totalEvents,
    wallMs: Number(wallMs.toFixed(0)),
    eventsPerSec: Number(throughput.toFixed(0)),
    memDeltaMB: memAfter - memBefore,
  };
}

console.log('=== Matrix sweep (10s simulated, partial rolling-window state) ===');
const SIM_SEC = 10;
const Ps = [1, 10, 100, 1000];
const Ns = [1, 10, 100];
const results = [];
for (const P of Ps) {
  for (const N of Ns) {
    const r = runCell(P, N, SIM_SEC);
    results.push(r);
    console.log(JSON.stringify(r));
  }
}

console.log('\n=== Saturation cell (full 1m rolling state at the upper-right corner) ===');
console.log('(P=100 × N=100 × 60s — 600k events, ~6k events per rolling window per partition)');
const saturation = runCell(100, 100, 60);
console.log(JSON.stringify(saturation));

console.log('\n=== JSON summary ===');
console.log(JSON.stringify({ matrix: results, saturation }, null, 2));
