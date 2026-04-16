# Smooth Performance Study

This note tracks the performance work on `TimeSeries.smooth(...)`, scoped first
to the `movingAverage` path.

## Scope

- benchmark the current `smooth('movingAverage')` implementation at multiple input sizes
- preserve functionality with dedicated correctness tests
- hand off a narrow optimization to an implementation worker
- re-run measurements after the fix

## Benchmark setup

- script: `scripts/perf-smooth-moving-average.mjs`
- target operation: `series.smooth('value', 'movingAverage', { window: 60_000, alignment: 'trailing' })`
- dataset: sorted point events with numeric payload columns
- scales: `250`, `500`, `1_000`, `2_000`, `4_000`
- metric: median wall-clock time across repeated runs

## Baseline observations

Benchmark output before optimization:

| Events | Median ms | Min ms | Max ms |
|--------|-----------|--------|--------|
| 250 | 1.85 | 1.74 | 2.32 |
| 500 | 3.43 | 3.21 | 4.49 |
| 1,000 | 7.08 | 6.98 | 7.59 |
| 2,000 | 17.40 | 16.83 | 18.22 |
| 4,000 | 47.01 | 46.74 | 47.89 |

Observations:

- The runtime curve is clearly superlinear, with the larger scales growing faster than a simple linear trend.
- The `2,000 -> 4,000` jump is about `2.7x`, which is consistent with the current full-scan-per-anchor implementation being a real hotspot at scale.
- Functionality is covered by the existing smoothing tests in [test/TimeSeries.test.ts](test/TimeSeries.test.ts) plus the targeted moving-average regression cases in [test/TimeSeries.smooth-perf.test.ts](test/TimeSeries.smooth-perf.test.ts).

## Post-fix observations

Benchmark output after optimization:

| Events | Median ms | Min ms | Max ms |
|--------|-----------|--------|--------|
| 250 | 0.38 | 0.34 | 0.63 |
| 500 | 0.66 | 0.42 | 0.96 |
| 1,000 | 1.02 | 0.77 | 1.29 |
| 2,000 | 1.67 | 1.54 | 1.91 |
| 4,000 | 3.08 | 2.56 | 5.51 |

Approximate speedup vs baseline median:

| Events | Baseline ms | Post-fix ms | Speedup |
|--------|-------------|-------------|---------|
| 250 | 1.85 | 0.38 | ~4.9x |
| 500 | 3.43 | 0.66 | ~5.2x |
| 1,000 | 7.08 | 1.02 | ~6.9x |
| 2,000 | 17.40 | 1.67 | ~10.4x |
| 4,000 | 47.01 | 3.08 | ~15.3x |

Observations:

- The moving-average path now scales much more smoothly with input size and no longer shows the sharp superlinear growth from baseline.
- The largest measured case (`4,000` events) dropped from `47.01 ms` to `3.08 ms`.
- The optimization preserves trailing, leading, and centered alignment behavior while still ignoring `undefined` values in the average.
- Final verification passed with:
  - `npm run build`
  - `npm test`
  - `npx prettier --check test/TimeSeries.smooth-perf.test.ts`
  - targeted correctness tests in [test/TimeSeries.smooth-perf.test.ts](test/TimeSeries.smooth-perf.test.ts)
