/**
 * Benchmark: pivotByGroup vs the groupBy + rename + joinMany workaround.
 *
 * Run: node bench/pivot-by-group.cjs
 *
 * pivotByGroup is a long-to-wide reshape; the only meaningful baseline
 * is the manual composition (groupBy → rename → joinMany) it replaces.
 * pondjs has no pivot equivalent, so this benchmark is purely
 * intra-package.
 */

/* eslint-disable @typescript-eslint/no-var-requires */

function timeIt(label, fn, warmup = 3, iterations = 15) {
  for (let i = 0; i < warmup; i++) fn();

  const times = [];
  for (let i = 0; i < iterations; i++) {
    const start = process.hrtime.bigint();
    fn();
    const end = process.hrtime.bigint();
    times.push(Number(end - start) / 1e6); // ms
  }
  times.sort((a, b) => a - b);
  return {
    median: times[Math.floor(times.length / 2)],
    p95: times[Math.floor(times.length * 0.95)],
    min: times[0],
    max: times[times.length - 1],
  };
}

function generateLongRows(timestamps, groups) {
  const rows = [];
  for (let t = 0; t < timestamps; t++) {
    const ts = t * 60_000;
    for (let g = 0; g < groups; g++) {
      // sin wave keyed off (t, g) so values look real
      const value = Math.sin(t * 0.01 + g * 0.5) * 50 + 50;
      rows.push([ts, value, `host-${g}`]);
    }
  }
  return rows;
}

const SCHEMA = [
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'number' },
  { name: 'host', kind: 'string' },
];

function makeSeries(TimeSeries, rows) {
  return new TimeSeries({ name: 'bench', schema: SCHEMA, rows });
}

function manualWorkaround(TimeSeries, series) {
  const groups = series.groupBy('host', (g, host) =>
    g.select('cpu').rename({ cpu: `${host}_cpu` }),
  );
  return TimeSeries.joinMany([...groups.values()], { type: 'outer' });
}

function naiveJSPivot(rows) {
  const groupSet = new Set();
  const byTs = new Map();
  for (const [ts, cpu, host] of rows) {
    groupSet.add(host);
    let bucket = byTs.get(ts);
    if (!bucket) {
      bucket = {};
      byTs.set(ts, bucket);
    }
    bucket[`${host}_cpu`] = cpu;
  }
  const groups = [...groupSet].sort();
  const sortedTs = [...byTs.keys()].sort((a, b) => a - b);
  const out = new Array(sortedTs.length);
  for (let i = 0; i < sortedTs.length; i++) {
    const ts = sortedTs[i];
    const bucket = byTs.get(ts);
    const row = { ts };
    for (const g of groups) {
      row[`${g}_cpu`] = bucket[`${g}_cpu`];
    }
    out[i] = row;
  }
  return out;
}

const SIZES = [
  { timestamps: 200, groups: 5, label: 'small (1k events, 5 groups)' },
  { timestamps: 1000, groups: 10, label: 'medium (10k events, 10 groups)' },
  { timestamps: 1000, groups: 50, label: 'large (50k events, 50 groups)' },
  { timestamps: 2000, groups: 100, label: 'xl (200k events, 100 groups)' },
];

async function run() {
  const { TimeSeries } = await import('../dist/index.js');

  console.log('pivotByGroup benchmark');
  console.log('='.repeat(75));
  console.log();
  console.log('Each row reports median ms over 15 iterations after 3 warmups.');
  console.log(
    'Manual = series.groupBy(host, g => g.rename(...)) + TimeSeries.joinMany(...).',
  );
  console.log('Naive JS = hand-rolled wide-row builder, no TimeSeries output.');
  console.log();

  const header = `${'Size'.padEnd(38)} ${'pivotByGroup'.padStart(13)} ${'Manual'.padStart(11)} ${'Naive JS'.padStart(11)} ${'speedup'.padStart(9)}`;
  console.log(header);
  console.log('-'.repeat(header.length));

  for (const { timestamps, groups, label } of SIZES) {
    const rows = generateLongRows(timestamps, groups);
    const series = makeSeries(TimeSeries, rows);

    const pivot = timeIt(label, () => series.pivotByGroup('host', 'cpu'));
    const manual = timeIt(label, () => manualWorkaround(TimeSeries, series));
    const naive = timeIt(label, () => naiveJSPivot(rows));

    const speedup = (manual.median / pivot.median).toFixed(1) + 'x';
    console.log(
      `${label.padEnd(38)} ${pivot.median.toFixed(2).padStart(11)}ms ${manual.median.toFixed(2).padStart(9)}ms ${naive.median.toFixed(2).padStart(9)}ms ${speedup.padStart(9)}`,
    );
  }

  console.log();
  console.log('— with aggregator (avg) on dense (no actual duplicates):');
  console.log('-'.repeat(header.length));

  for (const { timestamps, groups, label } of SIZES) {
    const rows = generateLongRows(timestamps, groups);
    const series = makeSeries(TimeSeries, rows);

    const pivot = timeIt(label, () =>
      series.pivotByGroup('host', 'cpu', { aggregate: 'avg' }),
    );
    console.log(
      `${label.padEnd(38)} ${pivot.median.toFixed(2).padStart(11)}ms`,
    );
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
