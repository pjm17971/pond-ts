# Rolling Performance Study

This note tracks the performance work on event-driven `TimeSeries.rolling(...)`.

## Scope

- benchmark the current implementation at multiple input sizes
- preserve functionality with dedicated correctness tests
- hand off a narrow optimization to an implementation worker
- re-run measurements after the fix

## Benchmark setup

- script: `scripts/perf-rolling.mjs`
- target operation: `series.rolling(60_000, { value: 'avg', load: 'sum' })`
- dataset: sorted point events with numeric payload columns
- scales: `250`, `500`, `1_000`, `2_000`, `4_000`
- metric: median wall-clock time across repeated runs

## Baseline observations

Benchmark output before optimization:

| Events | Median ms | Min ms | Max ms |
|--------|-----------|--------|--------|
| 250 | 2.45 | 2.28 | 3.50 |
| 500 | 7.91 | 7.67 | 8.71 |
| 1,000 | 25.29 | 25.08 | 25.50 |
| 2,000 | 92.50 | 91.02 | 93.31 |
| 4,000 | 366.25 | 362.28 | 371.10 |

Observations:

- Doubling the input size increases runtime by roughly 3.2x to 3.9x across the measured range.
- The `2,000 -> 4,000` jump is especially close to a 4x increase, which is consistent with the current O(N²) event-driven implementation in [AUDIT.md](AUDIT.md).
- Functionality is covered by the existing `rolling()` tests in [test/TimeSeries.test.ts](test/TimeSeries.test.ts) plus the targeted regression comparison in [test/TimeSeries.rolling-perf.test.ts](test/TimeSeries.rolling-perf.test.ts).

## Post-fix observations

Benchmark output after optimization:

| Events | Median ms | Min ms | Max ms |
|--------|-----------|--------|--------|
| 250 | 0.23 | 0.21 | 0.48 |
| 500 | 0.44 | 0.32 | 0.70 |
| 1,000 | 0.59 | 0.51 | 0.84 |
| 2,000 | 1.23 | 0.92 | 2.06 |
| 4,000 | 2.01 | 1.87 | 2.69 |

Approximate speedup vs baseline median:

| Events | Baseline ms | Post-fix ms | Speedup |
|--------|-------------|-------------|---------|
| 250 | 2.45 | 0.23 | ~10.7x |
| 500 | 7.91 | 0.44 | ~18.0x |
| 1,000 | 25.29 | 0.59 | ~42.9x |
| 2,000 | 92.50 | 1.23 | ~75.2x |
| 4,000 | 366.25 | 2.01 | ~182.2x |

Observations:

- The scaling shape changed from roughly quadratic to close to linear over the tested range.
- Doubling the input size now increases runtime by about 1.7x to 2.1x instead of roughly 3.2x to 3.9x.
- The largest measured case (`4,000` events) dropped from `366.25 ms` to `2.01 ms`.
- Review found one semantic regression in the first worker attempt (`first` behaved like `last`); that was sent back and fixed before acceptance.
- Final verification passed with:
  - `npm run build`
  - `npm test`
  - targeted correctness tests in [test/TimeSeries.rolling-perf.test.ts](test/TimeSeries.rolling-perf.test.ts)
