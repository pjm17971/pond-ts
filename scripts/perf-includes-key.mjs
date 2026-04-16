import { performance } from 'node:perf_hooks';
import { Time, TimeSeries } from '../dist/index.js';

const schema = Object.freeze([
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number' },
]);

function makeSeries(length) {
  return new TimeSeries({
    name: 'cpu',
    schema,
    rows: Array.from({ length }, (_, index) => [index * 10, index]),
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
  const probes = Array.from({ length }, (_, index) =>
    index % 2 === 0 ? new Time(index * 10) : new Time(index * 10 + 1),
  );

  for (const key of probes) {
    series.includesKey(key);
  }

  const samples = [];
  for (let run = 0; run < repeats; run += 1) {
    const start = performance.now();
    let matches = 0;
    for (const key of probes) {
      if (series.includesKey(key)) {
        matches += 1;
      }
    }
    const end = performance.now();
    if (matches !== Math.ceil(length / 2)) {
      throw new Error(`unexpected match count for ${length}: ${matches}`);
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

const scales = [500, 1_000, 2_000, 4_000, 8_000];
const results = scales.map((scale) => benchmark(scale));

console.log(JSON.stringify(results, null, 2));
