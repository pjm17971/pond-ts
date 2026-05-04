/**
 * Targets the `samples` reducer's rolling-state allocation behaviour.
 *
 * Motivated by gRPC experiment V7 vs V6 numbers — V7's all-pond
 * pipeline (using `samples()`) regressed throughput ~19% at the
 * ceiling regime (1k partitions × 1k events/s) and ran +17% heap
 * vs V6's hybrid pond-rolling + manual-deque pattern. The leading
 * suspect was per-event 1-element ScalarValue[] allocations in
 * `rollingState().add()`.
 *
 * Heap pressure is the real symptom — allocation count compounds
 * under sustained kHz × N-partition load. We measure both heap
 * delta and wall time. Run with `node --expose-gc` for the heap
 * numbers to be meaningful.
 *
 * Scenarios:
 *   1. Scalar source, partitioned rolling, samples — main hot path.
 *   2. Comparison reducer (avg) on the same shape — context anchor.
 *   3. Array source flatten path — verifies array-kind branch is
 *      unchanged after the scalar fast-path is added.
 */
import { performance } from 'node:perf_hooks';
import { LiveSeries } from '../dist/index.js';
import { samples } from '../dist/reducers/samples.js';

const scalarSchema = Object.freeze([
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'number' },
  { name: 'host', kind: 'string' },
]);

const arraySchema = Object.freeze([
  { name: 'time', kind: 'time' },
  { name: 'tags', kind: 'array' },
  { name: 'host', kind: 'string' },
]);

function median(values) {
  const sorted = [...values].sort((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0
    ? (sorted[middle - 1] + sorted[middle]) / 2
    : sorted[middle];
}

function tryGc() {
  if (typeof globalThis.gc === 'function') globalThis.gc();
}

function benchmark(label, fn, repeats = 5) {
  for (let run = 0; run < 2; run += 1) fn();
  tryGc();
  const wall = [];
  const heap = [];
  for (let run = 0; run < repeats; run += 1) {
    tryGc();
    const heapBefore = process.memoryUsage().heapUsed;
    const start = performance.now();
    fn();
    const end = performance.now();
    const heapAfter = process.memoryUsage().heapUsed;
    wall.push(end - start);
    heap.push((heapAfter - heapBefore) / (1024 * 1024));
  }
  return {
    label,
    medianMs: Number(median(wall).toFixed(2)),
    minMs: Number(Math.min(...wall).toFixed(2)),
    maxMs: Number(Math.max(...wall).toFixed(2)),
    medianHeapMb: Number(median(heap).toFixed(2)),
    minHeapMb: Number(Math.min(...heap).toFixed(2)),
    maxHeapMb: Number(Math.max(...heap).toFixed(2)),
  };
}

const results = [];

// Scenario 0 — focused micro-bench of `samples.rollingState()`. No
// LiveSeries / partition routing in the hot path; just add+remove
// cycles. This is where the allocation optimization lives, so it
// shows the per-call cost difference cleanly. Mirrors the steady-
// state of a rolling window: every add is followed by a remove
// once the window is full.
{
  const N = 5_000_000;
  results.push(
    benchmark(
      `rollingState micro — ${N} scalar add+remove cycles`,
      () => {
        const state = samples.rollingState();
        // Prime the window
        const W = 1_000;
        for (let i = 0; i < W; i++) state.add(i, i);
        // Steady-state: add / remove pairs
        for (let i = W; i < N; i++) {
          state.add(i, i);
          state.remove(i - W, i - W);
        }
        // Final snapshot to ensure result is observed
        const out = state.snapshot();
        if (out.length !== W) {
          throw new Error(`unexpected window size: ${out.length}`);
        }
      },
      5,
    ),
  );
}

// Scenario 1a — scalar samples, moderate scale (N=100k × P=100 hosts)
{
  const N = 100_000;
  const hosts = 100;
  results.push(
    benchmark(
      `samples (scalar) — ${N} events × ${hosts} hosts, rolling 1m`,
      () => {
        const live = new LiveSeries({ name: 'cpu', schema: scalarSchema });
        const _r = live
          .partitionBy('host')
          .rolling('1m', { vals: { from: 'cpu', using: 'samples' } });
        void _r;
        for (let i = 0; i < N; i++) {
          live.push([i, i % 100, `host-${i % hosts}`]);
        }
      },
    ),
  );
}

// Scenario 1b — scalar samples, high cardinality (matches V7 ceiling)
{
  const N = 100_000;
  const hosts = 1_000;
  results.push(
    benchmark(
      `samples (scalar) — ${N} events × ${hosts} hosts, rolling 1m`,
      () => {
        const live = new LiveSeries({ name: 'cpu', schema: scalarSchema });
        const _r = live
          .partitionBy('host')
          .rolling('1m', { vals: { from: 'cpu', using: 'samples' } });
        void _r;
        for (let i = 0; i < N; i++) {
          live.push([i, i % 100, `host-${i % hosts}`]);
        }
      },
      3,
    ),
  );
}

// Scenario 1c — scalar samples, narrower window (more eviction churn).
// Window slid every event = add+remove per push; tests `remove` path too.
{
  const N = 100_000;
  const hosts = 100;
  results.push(
    benchmark(
      `samples (scalar) — ${N} events × ${hosts} hosts, rolling 5s (high churn)`,
      () => {
        const live = new LiveSeries({ name: 'cpu', schema: scalarSchema });
        const _r = live
          .partitionBy('host')
          .rolling('5s', { vals: { from: 'cpu', using: 'samples' } });
        void _r;
        for (let i = 0; i < N; i++) {
          live.push([i, i % 100, `host-${i % hosts}`]);
        }
      },
      3,
    ),
  );
}

// Scenario 2 — context anchor: avg reducer on same shape.
// Difference between (1a) and (2) = samples-specific overhead.
{
  const N = 100_000;
  const hosts = 100;
  results.push(
    benchmark(`avg (anchor) — ${N} events × ${hosts} hosts, rolling 1m`, () => {
      const live = new LiveSeries({ name: 'cpu', schema: scalarSchema });
      const _r = live
        .partitionBy('host')
        .rolling('1m', { mean: { from: 'cpu', using: 'avg' } });
      void _r;
      for (let i = 0; i < N; i++) {
        live.push([i, i % 100, `host-${i % hosts}`]);
      }
    }),
  );
}

// Scenario 3 — array source flatten path. Verifies the array branch
// doesn't regress when the scalar fast-path is added.
{
  const N = 50_000;
  const hosts = 50;
  const tags = ['a', 'b', 'c'];
  results.push(
    benchmark(
      `samples (array) — ${N} events × ${hosts} hosts, rolling 1m`,
      () => {
        const live = new LiveSeries({ name: 'cpu', schema: arraySchema });
        const _r = live
          .partitionBy('host')
          .rolling('1m', { all: { from: 'tags', using: 'samples' } });
        void _r;
        for (let i = 0; i < N; i++) {
          live.push([i, [tags[i % 3], tags[(i + 1) % 3]], `host-${i % hosts}`]);
        }
      },
      3,
    ),
  );
}

console.log(JSON.stringify(results, null, 2));
