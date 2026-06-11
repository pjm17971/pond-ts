// Perf check — mapColumns() (per-cell column value transform).
//
// mapColumns is the column-scoped counterpart of the event-based map(): it
// applies (value) => newValue per column, reading the columns directly
// (col.read(i) → fn → rebuild) with NO per-row Event. Before mapColumns, the
// only way to transform a column's values was the event-based map(schema,
// e => newEvent) — materialize every event, build a new Event per row, then
// re-columnarize. That event round-trip is the cost mapColumns removes.
//
// Complexity (T mapped columns, N rows, C columns):
//   Old (event map): materialize N events O(N·C) + N new Event + re-
//     columnarize O(N·C). Event-touching.
//   New: per mapped column one O(N) col.read scan + fn + O(N) rebuild;
//     untouched columns + key shared by reference. O(T·N), zero events.
//
// Honest framing (the cumulative/fill lesson): the realistic comparison is the
// PIPELINE build → mapColumns → read. `build` is shared, reported separately.
// The new pipeline ≈ build + one column scan per mapped column; the `old`
// proxy materializes events + transforms per-event but SKIPS the re-
// columnarize the event map() also forces, so it UNDER-states the old cost.
//
//   npm run build --workspace=pond-ts
//   node --expose-gc scripts/perf-map-columnar.mjs

import { performance } from 'node:perf_hooks';
import { TimeSeries } from '../dist/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'a', kind: 'number' },
  { name: 'b', kind: 'number' },
  { name: 'c', kind: 'number' },
  { name: 'd', kind: 'number' },
];

function median(values) {
  const s = [...values].sort((x, y) => x - y);
  const m = Math.floor(s.length / 2);
  return s.length % 2 === 0 ? (s[m - 1] + s[m]) / 2 : s[m];
}

function bench(fn, repeats = 7) {
  for (let i = 0; i < 2; i += 1) fn();
  const samples = [];
  for (let i = 0; i < repeats; i += 1) {
    const t0 = performance.now();
    fn();
    samples.push(performance.now() - t0);
  }
  return Number(median(samples).toFixed(3));
}

function makeRows(n) {
  const rows = new Array(n);
  for (let i = 0; i < n; i += 1) rows[i] = [1000 + i, i % 97, i % 13, i, -i];
  return rows;
}

// Old per-event transform of one column (no rebuild) — the map() workaround.
function oldMapProxy(series) {
  let acc = 0;
  for (const e of series.events) {
    const v = e.get('a');
    acc += typeof v === 'number' ? v * 2 + 1 : 0;
  }
  return acc;
}

const CELLS = [100_000, 1_000_000];
const double = (x) => x * 2 + 1;

const out = [];
for (const n of CELLS) {
  const rows = makeRows(n);

  const buildMs = bench(() => new TimeSeries({ name: 's', schema, rows }));

  // NEW pipeline: build → mapColumns (single col) → read.
  const mapNewMs = bench(() => {
    const s = new TimeSeries({ name: 's', schema, rows });
    return s.mapColumns({ a: double }).column('a').sum();
  });

  // OLD proxy: build → materialize events → per-event transform → read.
  const mapOldMs = bench(() => {
    const s = new TimeSeries({ name: 's', schema, rows });
    return oldMapProxy(s);
  });

  // multi-column — confirms the win holds as mapped-column count grows.
  const multiNewMs = bench(() => {
    const s = new TimeSeries({ name: 's', schema, rows });
    return s
      .mapColumns({ a: double, b: double, c: double, d: double })
      .column('a')
      .sum();
  });

  out.push({
    rows: n,
    buildMs,
    mapNewMs,
    mapOldMs,
    mapSpeedup: Number((mapOldMs / mapNewMs).toFixed(1)),
    multiColumnNewMs: multiNewMs,
  });
}

console.log(JSON.stringify({ mapColumnar: out }, null, 2));
console.log(
  '\nbuild is shared. mapNew ≈ build + one column scan per mapped column;\n' +
    'mapOld ≈ build + the event-materialization tax the event map() forced\n' +
    '(and the proxy still skips the re-columnarize, so the real win is larger).',
);
if (!globalThis.gc)
  console.error('\n[note] run with --expose-gc for stable GC timing');
