import { performance } from 'node:perf_hooks';
import { TimeSeries } from '../dist/index.js';

const schema = Object.freeze([
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number', required: false },
  { name: 'host', kind: 'string', required: false },
]);

function makeSeries(length, hostCount) {
  return new TimeSeries({
    name: 'cpu',
    schema,
    rows: Array.from({ length }, (_, index) => [
      index * 1_000,
      Math.sin(index * 0.01),
      `host-${index % hostCount}`,
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

function benchmark(label, fn, repeats = 5) {
  for (let run = 0; run < 2; run += 1) fn();
  const samples = [];
  for (let run = 0; run < repeats; run += 1) {
    const start = performance.now();
    fn();
    const end = performance.now();
    samples.push(end - start);
  }
  return {
    label,
    medianMs: Number(median(samples).toFixed(2)),
    minMs: Number(Math.min(...samples).toFixed(2)),
    maxMs: Number(Math.max(...samples).toFixed(2)),
  };
}

const results = [];

// Scenario 1: Validation overhead at construction time
{
  const N = 100_000;
  const hosts = 10;
  const ts = makeSeries(N, hosts);
  const HOSTS = Array.from({ length: hosts }, (_, i) => `host-${i}`);
  results.push(
    benchmark(`${N} events × ${hosts} hosts → partitionBy('host') (no groups)`, () =>
      ts.partitionBy('host'),
    ),
  );
  results.push(
    benchmark(
      `${N} events × ${hosts} hosts → partitionBy('host', { groups })`,
      () => ts.partitionBy('host', { groups: HOSTS }),
    ),
  );
}

// Scenario 2: Full chain with declared groups (chained ops use _trusted)
{
  const N = 100_000;
  const hosts = 10;
  const ts = makeSeries(N, hosts);
  const HOSTS = Array.from({ length: hosts }, (_, i) => `host-${i}`);
  results.push(
    benchmark(
      `${N} events × ${hosts} hosts → partitionBy + 4-step chain + collect (no groups)`,
      () =>
        ts
          .partitionBy('host')
          .dedupe({ keep: 'last' })
          .fill({ value: 'linear' })
          .diff('value')
          .rate('value')
          .collect(),
      3,
    ),
  );
  results.push(
    benchmark(
      `${N} events × ${hosts} hosts → partitionBy(groups) + same chain + collect`,
      () =>
        ts
          .partitionBy('host', { groups: HOSTS })
          .dedupe({ keep: 'last' })
          .fill({ value: 'linear' })
          .diff('value')
          .rate('value')
          .collect(),
      3,
    ),
  );
}

// Scenario 3: toMap with declared groups
{
  const N = 100_000;
  const hosts = 10;
  const ts = makeSeries(N, hosts);
  const HOSTS = Array.from({ length: hosts }, (_, i) => `host-${i}`);
  results.push(
    benchmark(
      `${N} events × ${hosts} hosts → partitionBy(groups).toMap()`,
      () => ts.partitionBy('host', { groups: HOSTS }).toMap(),
    ),
  );
  results.push(
    benchmark(
      `${N} events × ${hosts} hosts → partitionBy(groups).toMap(g => g.toPoints())`,
      () => ts.partitionBy('host', { groups: HOSTS }).toMap((g) => g.toPoints()),
    ),
  );
}

console.log(JSON.stringify(results, null, 2));
