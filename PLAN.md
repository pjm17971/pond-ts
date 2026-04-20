# Plan

## Current state

The library now has a solid typed core:
- `Time`, `TimeRange`, `Interval`
- `Event`
- `Sequence`
- `TimeSeries`
- typed construction and validation
- event-level transforms
- series-level selection, lookup, trimming, and alignment

Recent structural cleanup:
- primitives are split into separate source files
- runtime tests are split by primitive plus `TimeSeries`
- `TimeSeries` tests are now more integration-focused

## Key gaps

### 1. Aggregation
Missing:
- `aggregate(sequence, ...)`
- built-in reducers such as:
  - `sum`
  - `avg`
  - `min`
  - `max`
  - `count`
  - `first`
  - `last`
- custom reducer path over sequence buckets

### 2. Merge / join over aligned series
`align(...)` exists, but there is no series merge/join layer yet.
This is the next obvious use case for `Sequence`.

### 3. Rolling windows
Missing:
- event-driven rolling windows
- sequence-driven rolling windows
- reuse of built-in aggregators for rolling operations

### 4. Alignment improvements
Missing or incomplete:
- per-column alignment policies
  - example: numeric columns `linear`, categorical columns `hold`
- explicit edge fill policy
- richer control over alignment gaps
- non-time-keyed interpolation is intentionally unsupported for now

### 5. Sequence improvements
Missing:
- calendar-aware sequence generation
- richer generation helpers
- clearer anchor/grid APIs

### 6. Type-test structure
Runtime tests are now split well.
Type tests are still monolithic and should eventually mirror the runtime layout.

## Semantics to preserve

### Half-open bucketing
For sequence-based bucketing and alignment, interval membership is half-open:
- `[begin, end)`

Example:
- times: `10`, `15`, `20`
- bucket: `[10, 20)`
- included: `10`, `15`
- excluded: `20`

### Alignment sample position
- default: `begin`
- optional: `center`
- `end` is intentionally not a target mode

### Temporal selection vocabulary
Keep these distinct:
- `within(...)` = fully contained
- `overlapping(...)` = intersects, no key modification
- `trim(...)` = intersects and clips key extents

## Recommended next steps

### Near term
1. Add built-in `aggregate(sequence, ...)`
2. Add merge/join for aligned series
3. Add rolling windows on top of that

Completed:
- ✅ Validate explicit sequence interval lists (`BoundedSequence([...])`)

### Aggregation design
Recommended shape:
- `TimeSeries.aggregate(sequence, aggregations)`
- built-in reducer names first
- custom reducer support later if needed

Target output:
- interval-keyed `TimeSeries`
- one event per sequence bucket

### Merge design
Recommended flow:
1. align series to a shared `Sequence`
2. merge/join aligned series

This keeps alignment separate from combination logic.

### Rolling design
Recommended order:
1. event-driven rolling first
2. sequence-driven rolling second

Window semantics should also use half-open intervals.

## Real-time / live support

This is a real gap and should likely be handled in a separate layer rather than by bloating `TimeSeries`.

### Recommended direction
Keep:
- `TimeSeries` as immutable analytical data

Add:
- `LiveSeries<S>` or similar ingestion-oriented structure

### Why separate it
Real-time needs differ from batch analysis:
- append-heavy workloads
- retention policies
- possible late/out-of-order events
- efficient snapshots
- later: incremental rolling and aggregation

### Minimal first pass for live support
A reasonable first API would be:
- `push(row)`
- `pushEvent(event)`
- `first()`
- `last()`
- `length`
- `toTimeSeries()`

And retention options such as:
- `maxEvents`
- `maxRangeMs`

### Future live concerns
Later, this layer may need:
- ordering policy
- lateness policy
- dedupe policy
- subscriptions
- incremental aggregate views

## Design principles
- Keep `TimeSeries` immutable
- Keep alignment separate from aggregation
- Keep aggregation separate from rolling
- Keep selection (`within`, `overlapping`) separate from clipping (`trim`)
- Use `Sequence` as the grid abstraction for alignment and bucket aggregation
- Use separate live/incremental structures for real-time support
