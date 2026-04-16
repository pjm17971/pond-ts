# LOESS Performance Study

This note tracks the performance work on `TimeSeries.smooth(..., "loess", ...)`.

## Scope

- benchmark LOESS smoothing at multiple input sizes
- preserve output-column behavior and handling of undefined source values with
  dedicated regression coverage
- hand off a narrow optimization to an implementation worker
- re-run measurements after the fix

## Benchmark setup

- script: `scripts/perf-smooth-loess.mjs`
- target operation: `series.smooth("value", "loess", { span: 0.25, output: "valueLoess" })`
- dataset: sorted time-keyed point events with one optional numeric column and
  one string column
- scales: `200`, `400`, `800`, `1,600`
- metric: median wall-clock time across repeated runs

## Baseline observations

- `200`: `4.64 ms`
- `400`: `16.56 ms`
- `800`: `63.82 ms`
- `1,600`: `253.79 ms`

The current branch grows close to quadratically over the measured range. The
main cost centers are rebuilding the defined-point set and sorting distances for
every output anchor in the public `smooth("loess")` path.

## Post-fix observations

- `200`: `0.90 ms`
- `400`: `2.70 ms`
- `800`: `9.07 ms`
- `1,600`: `34.05 ms`

The optimized branch is still span-dependent work per output sample, but the
constant factor is much better. At `1,600` source events, the measured median
dropped from `253.79 ms` to `34.05 ms` (about `7.5x` faster), which matches the
expected gain from precomputing defined points once and avoiding a full
distance-sort for every output anchor.
