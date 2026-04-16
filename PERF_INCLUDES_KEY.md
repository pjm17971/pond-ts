# includesKey Performance Study

This note tracks the performance work on `TimeSeries.includesKey(...)`.

## Scope

- benchmark the current implementation at multiple input sizes
- preserve exact-key functionality with dedicated regression tests
- hand off a narrow optimization to an implementation worker
- re-run measurements after the fix

## Benchmark setup

- script: `scripts/perf-includes-key.mjs`
- target operation: repeated `series.includesKey(key)` lookups over a mix of hits and misses
- dataset: sorted time-keyed events
- scales: `500`, `1_000`, `2_000`, `4_000`, `8_000`
- metric: median wall-clock time across repeated runs

## Baseline observations

- `500`: `4.10 ms`
- `1,000`: `16.25 ms`
- `2,000`: `63.66 ms`
- `4,000`: `253.59 ms`
- `8,000`: `1015.50 ms`

The original benchmark shape at larger sizes (`20,000` and `40,000`) was
impractical for interactive iteration, which is itself a useful signal: the
current implementation scales quadratically when used for repeated lookups over
a sorted series.

## Post-fix observations

- `500`: `0.29 ms`
- `1,000`: `0.20 ms`
- `2,000`: `0.32 ms`
- `4,000`: `0.68 ms`
- `8,000`: `1.24 ms`

The optimized implementation is effectively flat at these sizes, with the
largest measured case dropping from `1015.50 ms` to `1.24 ms` (about `819x`
faster). The growth pattern now looks consistent with repeated `O(log n)`
lookups rather than repeated full-array scans.
