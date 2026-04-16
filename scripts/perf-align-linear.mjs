import { performance } from 'node:perf_hooks';
import { Sequence, TimeRange, TimeSeries } from '../dist/index.js';

const schema = Object.freeze([
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number' },
  { name: 'status', kind: 'string' },
]);

function makeSeries(length) {
  return new TimeSeries({
    name: 'cpu',
    schema,
    rows: Array.from({ length }, (_, index) => [
      index * 10,
      index,
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

function benchmark(length, repeats = 5) {
  const series = makeSeries(length);
  const range = new TimeRange({ start: 0, end: (length - 1) * 10 });
  const sequence = Sequence.every(5);

  series.align(sequence, { method: 'linear', range });

  const samples = [];
  for (let run = 0; run < repeats; run += 1) {
    const start = performance.now();
    const aligned = series.align(sequence, { method: 'linear', range });
    const end = performance.now();

    if (aligned.length !== Math.floor(((length - 1) * 10) / 5) + 1) {
      throw new Error(`unexpected aligned length for ${length}: ${aligned.length}`);
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

const scales = [250, 500, 1_000, 2_000, 4_000];
const results = scales.map((scale) => benchmark(scale));

console.log(JSON.stringify(results, null, 2));
