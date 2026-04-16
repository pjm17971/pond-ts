# Aggregate Performance Study

This note tracks the performance work on bucketed `TimeSeries.aggregate(...)`.

## Scope

- benchmark the current implementation at multiple input sizes
- preserve functionality with dedicated correctness tests
- hand off a narrow optimization to an implementation worker
- re-run measurements after the fix

## Benchmark setup

- script: `scripts/perf-aggregate.mjs`
- target operation: `series.aggregate(Sequence.every(60_000), { value: 'avg', load: 'sum' }, { range })`
- dataset: sorted point events with numeric payload columns
- scales: `1_000`, `2_000`, `4_000`, `8_000`, `16_000`
- metric: median wall-clock time across repeated runs

## Baseline observations

Benchmark output before optimization:

| Events | Median ms | Min ms | Max ms |
|--------|-----------|--------|--------|
| 1,000 | 0.51 | 0.46 | 0.60 |
| 2,000 | 1.59 | 1.52 | 1.80 |
| 4,000 | 5.84 | 5.58 | 6.69 |
| 8,000 | 23.63 | 23.38 | 24.27 |
| 16,000 | 91.15 | 90.58 | 96.53 |

Observations:

- Doubling the input size increases runtime by about 3.1x to 4.0x across the measured range.
- The `4,000 -> 8,000 -> 16,000` progression is especially close to quadratic behavior.
- This is consistent with the current `buckets.map(... this.events.filter(...))` implementation described in [AUDIT.md](AUDIT.md).
- Functionality is covered by existing aggregate tests in [test/TimeSeries.test.ts](test/TimeSeries.test.ts) plus the targeted reducer regression in [test/TimeSeries.aggregate-perf.test.ts](test/TimeSeries.aggregate-perf.test.ts).

## Post-fix observations

Benchmark output after optimization:

| Events | Median ms | Min ms | Max ms |
|--------|-----------|--------|--------|
| 1,000 | 0.20 | 0.16 | 0.39 |
| 2,000 | 0.31 | 0.27 | 0.38 |
| 4,000 | 0.46 | 0.44 | 0.78 |
| 8,000 | 0.27 | 0.25 | 0.61 |
| 16,000 | 0.53 | 0.48 | 0.56 |

Approximate speedup vs baseline median:

| Events | Baseline ms | Post-fix ms | Speedup |
|--------|-------------|-------------|---------|
| 1,000 | 0.51 | 0.20 | ~2.6x |
| 2,000 | 1.59 | 0.31 | ~5.1x |
| 4,000 | 5.84 | 0.46 | ~12.7x |
| 8,000 | 23.63 | 0.27 | ~87.5x |
| 16,000 | 91.15 | 0.53 | ~172.0x |

Observations:

- The point-series aggregate path no longer shows the clear near-quadratic growth from baseline.
- The largest measured case (`16,000` events) dropped from `91.15 ms` to `0.53 ms`.
- Smaller post-fix numbers are now low enough that some benchmark noise is visible, but the overall scaling shape is still dramatically flatter than baseline.
- The optimized path is specialized to time-keyed point events; interval-like overlap aggregation remains on the original path, preserving its existing semantics.
- Final verification passed with:
  - `npm run build`
  - `npm test`
  - targeted correctness tests in [test/TimeSeries.aggregate-perf.test.ts](test/TimeSeries.aggregate-perf.test.ts)
