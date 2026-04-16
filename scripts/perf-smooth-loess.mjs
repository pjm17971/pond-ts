import { performance } from 'node:perf_hooks';
import { TimeSeries } from '../dist/index.js';

const schema = Object.freeze([
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number', required: false },
  { name: 'status', kind: 'string' },
]);

function makeSeries(length) {
  return new TimeSeries({
    name: 'cpu',
    schema,
    rows: Array.from({ length }, (_, index) => [
      index * 10,
      index % 11 === 0 ? undefined : index + (index % 7) * 0.25,
      `s${index % 5}`,
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

function benchmark(length, repeats = 3) {
  const series = makeSeries(length);

  series.smooth('value', 'loess', { span: 0.25, output: 'valueLoess' });

  const samples = [];
  for (let run = 0; run < repeats; run += 1) {
    const start = performance.now();
    const smoothed = series.smooth('value', 'loess', {
      span: 0.25,
      output: 'valueLoess',
    });
    const end = performance.now();

    if (smoothed.length !== length) {
      throw new Error(`unexpected smoothed length for ${length}: ${smoothed.length}`);
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

const scales = [200, 400, 800, 1_600];
const results = scales.map((scale) => benchmark(scale));

console.log(JSON.stringify(results, null, 2));
