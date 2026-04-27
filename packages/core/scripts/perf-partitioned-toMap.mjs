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

// Scenario 1: Many partitions, each with many events
{
  const N = 100_000;
  const hosts = 10;
  const ts = makeSeries(N, hosts);
  results.push(
    benchmark(`${N} events × ${hosts} hosts → toMap()`, () =>
      ts.partitionBy('host').toMap(),
    ),
  );
  results.push(
    benchmark(
      `${N} events × ${hosts} hosts → toMap(g => g.toPoints())`,
      () => ts.partitionBy('host').toMap((g) => g.toPoints()),
    ),
  );
  results.push(
    benchmark(
      `${N} events × ${hosts} hosts → .collect().groupBy(host, toPoints)`,
      () =>
        ts
          .partitionBy('host')
          .collect()
          .groupBy('host', (g) => g.toPoints()),
    ),
  );
}

// Scenario 2: Few partitions (single-host dashboard)
{
  const N = 100_000;
  const hosts = 1;
  const ts = makeSeries(N, hosts);
  results.push(
    benchmark(`${N} events × ${hosts} host → toMap()`, () =>
      ts.partitionBy('host').toMap(),
    ),
  );
}

// Scenario 3: Many partitions, each tiny (high partition cardinality)
{
  const N = 100_000;
  const hosts = 1_000;
  const ts = makeSeries(N, hosts);
  results.push(
    benchmark(`${N} events × ${hosts} hosts → toMap()`, () =>
      ts.partitionBy('host').toMap(),
    ),
  );
}

console.log(JSON.stringify(results, null, 2));
