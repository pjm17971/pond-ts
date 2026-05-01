import { performance } from 'node:perf_hooks';
import { Sequence, TimeRange, TimeSeries } from '../dist/index.js';

const schema = Object.freeze([
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number', required: false },
  { name: 'host', kind: 'string', required: false },
]);

function makeSeries(length, spacingMs) {
  return new TimeSeries({
    name: 'cpu',
    schema,
    rows: Array.from({ length }, (_, index) => [
      index * spacingMs,
      Math.sin(index * 0.01),
      `host-${index % 4}`,
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
  // Warm-up
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

// Scenario 1: ~10 events per bucket (typical downsampling)
{
  const length = 100_000;
  const series = makeSeries(length, 100);
  const range = new TimeRange({ start: 0, end: (length - 1) * 100 });
  const seq = Sequence.every('1s');
  results.push(
    benchmark(
      `${length} events / 100ms spacing → 1s bucket (select=last)`,
      () => series.materialize(seq, { select: 'last', range }),
    ),
  );
  results.push(
    benchmark(
      `${length} events / 100ms spacing → 1s bucket (select=first)`,
      () => series.materialize(seq, { select: 'first', range }),
    ),
  );
  results.push(
    benchmark(
      `${length} events / 100ms spacing → 1s bucket (select=nearest)`,
      () => series.materialize(seq, { select: 'nearest', range }),
    ),
  );
}

// Scenario 2: ~1 event per bucket (matching grid)
{
  const length = 100_000;
  const series = makeSeries(length, 1_000);
  const range = new TimeRange({ start: 0, end: (length - 1) * 1_000 });
  const seq = Sequence.every('1s');
  results.push(
    benchmark(`${length} events / 1s spacing → 1s bucket (select=last)`, () =>
      series.materialize(seq, { select: 'last', range }),
    ),
  );
}

// Scenario 3: Sparse source projected onto dense grid
{
  const length = 1_000;
  const series = makeSeries(length, 60_000);
  const range = new TimeRange({ start: 0, end: length * 60_000 });
  const seq = Sequence.every('1s');
  results.push(
    benchmark(
      `${length} events / 1m spacing → 1s grid (~60k empty buckets)`,
      () => series.materialize(seq, { range }),
    ),
  );
}

// Scenario 4: Partitioned variant (the v0.10 chain)
{
  const length = 100_000;
  const series = makeSeries(length, 100);
  const range = new TimeRange({ start: 0, end: (length - 1) * 100 });
  const seq = Sequence.every('1s');
  results.push(
    benchmark(
      `${length} events / 4 hosts → partitionBy + materialize + collect`,
      () => series.partitionBy('host').materialize(seq, { range }).collect(),
      3,
    ),
  );
  results.push(
    benchmark(
      `${length} events / 4 hosts → full v0.10 pipeline (dedupe + materialize + fill)`,
      () =>
        series
          .partitionBy('host')
          .dedupe({ keep: 'last' })
          .materialize(seq, { range })
          .fill({ value: 'linear' }, { maxGap: '5s' })
          .collect(),
      3,
    ),
  );
}

console.log(JSON.stringify(results, null, 2));
