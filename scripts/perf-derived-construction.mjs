import { performance } from 'node:perf_hooks';
import { TimeSeries } from '../dist/index.js';

const schema = Object.freeze([
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'number' },
  { name: 'requests', kind: 'number' },
  { name: 'host', kind: 'string' },
  { name: 'active', kind: 'boolean' },
]);

function makeSeries(length) {
  return new TimeSeries({
    name: 'cpu',
    schema,
    rows: Array.from({ length }, (_, index) => [
      index * 10,
      index % 100,
      index * 2,
      `host-${index % 7}`,
      index % 3 !== 0,
    ]),
  });
}

function median(values) {
  const sorted = [...values].sort((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0
    ? (sorted[middle - 1] + sorted[middle]) / 2
    : sorted[middle];
}

function derive(series) {
  const selected = series.filter((event) => event.get('active')).select(
    'cpu',
    'requests',
    'host',
  );
  const renamed = selected.rename({ host: 'server' });
  const collapsed = renamed.collapse(
    ['cpu', 'requests'],
    'score',
    ({ cpu, requests }) => (cpu ?? 0) + (requests ?? 0),
    { append: true },
  );

  return collapsed.map(
    Object.freeze([
      { name: 'time', kind: 'time' },
      { name: 'score', kind: 'number' },
      { name: 'server', kind: 'string' },
    ]),
    (event) => event.select('score', 'server'),
  );
}

function benchmark(length, repeats = 5) {
  const series = makeSeries(length);

  derive(series);

  const samples = [];
  for (let run = 0; run < repeats; run += 1) {
    const start = performance.now();
    const derived = derive(series);
    const end = performance.now();

    if (derived.length === 0) {
      throw new Error(`unexpected empty derived series for ${length}`);
    }

    samples.push(end - start);
  }

  return {
    length,
    medianMs: Number(median(samples).toFixed(2)),
    minMs: Number(Math.min(...samples).toFixed(2)),
    maxMs: Number(Math.max(...samples).toFixed(2)),
  };
}

const scales = [1_000, 2_000, 4_000, 8_000];
const results = scales.map((scale) => benchmark(scale));

console.log(JSON.stringify(results, null, 2));
