// §A collect-output prize — sizing measurement (gather vs sigma).
//
// The question before committing the collect-output arc (re-open #175's
// coalescing → chunk collect() → windowColumns → wire baseline): how much of
// the dashboard's per-tick baseline memo is the GATHER (building typed arrays
// off the Event[]-backed collected series — the part a columnar collect()
// output would make zero-copy) vs the SIGMA arithmetic (avg ± σ·sd
// element-wise per partition — unchanged regardless of backing)?
//
// Amdahl: columnar collect zero-copies the gather; sigma is the floor. If
// gather dominates, the prize is real and worth re-opening #175. If sigma
// dominates, it isn't — and the narrower raw-line increment 2 is the better
// move.
//
// Models the collected baseline faithfully: an Event[]-backed LiveSeries
// (what collect() produces) of shape {time, host, cpu, avg, sd, n}, windowed,
// then the increment-1 gather (shipped 0.19.0: view.partitionBy('host')
// .toMap(g => g.column(...).toFloat64Array())) and the sigma band loop.
//
//   npm run build --workspace=pond-ts
//   node --expose-gc scripts/perf-baseline-memo-split.mjs

import { performance } from 'node:perf_hooks';
import { LiveSeries } from '../dist/index.js';

const schema = Object.freeze([
  { name: 'time', kind: 'time' },
  { name: 'host', kind: 'string' },
  { name: 'cpu', kind: 'number' },
  { name: 'avg', kind: 'number' },
  { name: 'sd', kind: 'number' },
  { name: 'n', kind: 'number' },
]);

function median(values) {
  const s = [...values].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 === 0 ? (s[m - 1] + s[m]) / 2 : s[m];
}

function bench(fn, repeats = 7) {
  for (let i = 0; i < 3; i += 1) fn();
  const samples = [];
  for (let i = 0; i < repeats; i += 1) {
    const t0 = performance.now();
    fn();
    samples.push(performance.now() - t0);
  }
  return Number(median(samples).toFixed(3));
}

// An Event[]-backed collected-baseline view: hosts × events, interleaved by
// host the way collect()'s fan-in delivers them.
function makeView(hosts, events) {
  const live = new LiveSeries({
    name: 'baseline',
    schema,
    ordering: 'strict',
    retention: { maxEvents: events },
    __backing: 'array', // what collect() produces
  });
  const BATCH = 1_000;
  let rows = new Array(BATCH);
  let k = 0;
  for (let i = 0; i < events; i += 1) {
    const v = i % 97;
    // time, host, cpu, avg, sd, n
    rows[k++] = [1000 + i, `host-${i % hosts}`, v, v + 0.5, (i % 13) / 7, 30];
    if (k === BATCH) {
      live.pushMany(rows);
      k = 0;
    }
  }
  if (k > 0) live.pushMany(rows.slice(0, k));
  return live.window(events);
}

// GATHER — what columnar collect() output + a structural window would make
// zero-copy. Increment-1 surface (0.19.0): per-partition typed-array extract.
function gather(view) {
  return view.partitionBy('host').toMap((g) => ({
    xs: g.keyColumn().begin,
    cpu: g.column('cpu').toFloat64Array(),
    avg: g.column('avg').toFloat64Array(),
    sd: g.column('sd').toFloat64Array(),
  }));
}

// SIGMA — the band arithmetic, unchanged regardless of backing. Element-wise
// avg ± σ·sd over each partition's gathered arrays (NaN through the warm-up
// gate, as the recipe does).
function sigma(gathered, sig) {
  const out = new Map();
  for (const [host, cols] of gathered) {
    const { avg, sd } = cols;
    const len = avg.length;
    const upper = new Float64Array(len);
    const lower = new Float64Array(len);
    for (let i = 0; i < len; i += 1) {
      const a = avg[i];
      const s = sd[i];
      if (Number.isNaN(a) || Number.isNaN(s)) {
        upper[i] = lower[i] = NaN;
      } else {
        upper[i] = a + sig * s;
        lower[i] = a - sig * s;
      }
    }
    out.set(host, { upper, lower });
  }
  return out;
}

const CELLS = [
  { hosts: 8, events: 12_000 },
  { hosts: 32, events: 48_000 },
  { hosts: 64, events: 96_000 },
];

const rows = [];
for (const { hosts, events } of CELLS) {
  const view = makeView(hosts, events);
  // Pre-gather once for the isolated sigma measurement.
  const pre = gather(view);
  const gatherMs = bench(() => gather(view));
  const sigmaMs = bench(() => sigma(pre, 3));
  const total = gatherMs + sigmaMs;
  rows.push({
    hosts,
    events,
    gatherMs,
    sigmaMs,
    gatherPct: Number(((100 * gatherMs) / total).toFixed(0)),
  });
}

console.log(JSON.stringify({ baselineMemoSplit: rows }, null, 2));
console.log(
  '\ngatherPct = share of (gather+sigma) that a columnar collect() output\n' +
    'could drive toward zero-copy. High → the prize is worth re-opening #175;\n' +
    'low → sigma is the floor and the arc is not worth it.',
);
if (!globalThis.gc)
  console.error('\n[note] run with --expose-gc for stable GC timing');
