import { performance } from 'node:perf_hooks';
import { LiveSeries } from '../dist/index.js';

const schema = Object.freeze([
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number', required: false },
  { name: 'host', kind: 'string', required: false },
]);

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

// Scenario 1: Routing overhead — push events through a partitioned view
// vs through bare LiveSeries.
{
  const N = 100_000;
  const hosts = 10;

  results.push(
    benchmark(
      `${N} events × ${hosts} hosts → bare LiveSeries.push (no partition)`,
      () => {
        const live = new LiveSeries({ name: 'cpu', schema });
        for (let i = 0; i < N; i++) {
          live.push([i, Math.sin(i * 0.01), `host-${i % hosts}`]);
        }
      },
    ),
  );

  results.push(
    benchmark(
      `${N} events × ${hosts} hosts → partitionBy('host') routing`,
      () => {
        const live = new LiveSeries({ name: 'cpu', schema });
        const _p = live.partitionBy('host');
        void _p;
        for (let i = 0; i < N; i++) {
          live.push([i, Math.sin(i * 0.01), `host-${i % hosts}`]);
        }
      },
    ),
  );
}

// Scenario 2: collect() — materializing a unified buffer
{
  const N = 100_000;
  const hosts = 10;

  results.push(
    benchmark(
      `${N} events × ${hosts} hosts → partitionBy + collect (subscribe overhead)`,
      () => {
        const live = new LiveSeries({ name: 'cpu', schema });
        const partitioned = live.partitionBy('host');
        const _u = partitioned.collect();
        void _u;
        for (let i = 0; i < N; i++) {
          live.push([i, Math.sin(i * 0.01), `host-${i % hosts}`]);
        }
      },
      3,
    ),
  );
}

// Scenario 3: apply() with fill — operator chain per partition
{
  const N = 100_000;
  const hosts = 10;

  results.push(
    benchmark(
      `${N} events × ${hosts} hosts → partitionBy + apply(fill)`,
      () => {
        const live = new LiveSeries({ name: 'cpu', schema });
        const _u = live
          .partitionBy('host')
          .apply((sub) => sub.fill({ value: 'hold' }));
        void _u;
        for (let i = 0; i < N; i++) {
          live.push([i, Math.sin(i * 0.01), `host-${i % hosts}`]);
        }
      },
      3,
    ),
  );
}

// Scenario 4: High partition cardinality
{
  const N = 100_000;
  const hosts = 1_000;

  results.push(
    benchmark(
      `${N} events × ${hosts} hosts → partitionBy('host') routing`,
      () => {
        const live = new LiveSeries({ name: 'cpu', schema });
        const _p = live.partitionBy('host');
        void _p;
        for (let i = 0; i < N; i++) {
          live.push([i, Math.sin(i * 0.01), `host-${i % hosts}`]);
        }
      },
      3,
    ),
  );
}

console.log(JSON.stringify(results, null, 2));
