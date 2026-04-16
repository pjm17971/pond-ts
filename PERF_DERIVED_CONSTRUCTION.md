# Derived Construction Performance Study

This note tracks performance work on event-based derived transforms that
currently round-trip through `rows` and validation even when they already hold
trusted normalized events.

## Scope

- benchmark a representative chain of event-based derived transforms
- preserve observable behavior for filter/select/rename/collapse/map output
- hand off a narrow implementation to an independent worker
- re-run measurements after the fix

## Benchmark setup

- script: `scripts/perf-derived-construction.mjs`
- target operation: a chained `filter -> select -> rename -> collapse -> map`
  derivation over a sorted point series
- scales: `1,000`, `2,000`, `4,000`, `8,000`
- metric: median wall-clock time across repeated runs

## Baseline observations

- `1,000`: `2.61 ms`
- `2,000`: `3.67 ms`
- `4,000`: `7.21 ms`
- `8,000`: `14.15 ms`

The baseline is already roughly linear, which suggests this optimization is
about constant-factor overhead rather than asymptotic behavior. The likely win
is removing repeated `events -> rows -> validate -> events` work from hot
derivation chains.

## Post-fix observations

- `1,000`: `1.18 ms`
- `2,000`: `1.54 ms`
- `4,000`: `3.07 ms`
- `8,000`: `5.55 ms`

The optimized path remains roughly linear, but with a meaningfully lower
constant factor. At `8,000` events, the measured median dropped from `14.15 ms`
to `5.55 ms` (about `2.5x` faster), which matches the expected gain from
skipping the internal `events -> rows -> validate -> events` round-trip in
order-preserving derived transforms.
