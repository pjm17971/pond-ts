# alignLinearAt Performance Study

This note tracks the performance work on the linear alignment path used by
`TimeSeries.align(..., { method: "linear" })`.

## Scope

- benchmark linear alignment at multiple input sizes
- preserve exact-sample, interpolated, and right-edge fallback behavior with
  dedicated regression coverage
- hand off a narrow optimization to an implementation worker
- re-run measurements after the fix

## Benchmark setup

- script: `scripts/perf-align-linear.mjs`
- target operation: `series.align(sequence, { method: "linear", range })`
- dataset: sorted time-keyed point events with one numeric and one string column
- scales: `250`, `500`, `1,000`, `2,000`, `4,000`
- metric: median wall-clock time across repeated runs

## Baseline observations

- `250`: `2.75 ms`
- `500`: `8.24 ms`
- `1,000`: `30.84 ms`
- `2,000`: `125.20 ms`
- `4,000`: `491.26 ms`

The current path grows roughly quadratically at these sizes. The main suspect is
the repeated exact-match scan in `#alignLinearAt(...)`, which is exercised once
per sampled interval during dense linear alignment.

## Post-fix observations

- `250`: `0.55 ms`
- `500`: `0.62 ms`
- `1,000`: `0.73 ms`
- `2,000`: `1.32 ms`
- `4,000`: `3.67 ms`

The linear alignment path is now much flatter. At `4,000` source events, the
measured median dropped from `491.26 ms` to `3.67 ms` (about `134x` faster),
which is consistent with replacing repeated exact-match scans with a single
forward cursor over the already-ordered event sequence.
