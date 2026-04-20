# Plan

This document is the single source of truth for what has shipped, what is next,
and the design decisions behind each phase. Update it whenever meaningful work
lands so a lost session does not erase the current state of the project.

---

## Current baseline

What already exists today:

- typed `TimeSeries` construction and JSON ingest/export
- `Time`, `TimeRange`, and `Interval` temporal keys
- immutable `Event` values
- temporal selection and slicing
- alignment, aggregation, joins, rolling windows, and smoothing
- calendar-aware `Sequence` and `BoundedSequence`
- npm packaging and automated release flow
- a Docusaurus docs site plus generated API reference

What is still not stable enough to build on aggressively:

- edge-case coverage in several analytical paths is still lighter than it
  should be
- a settled plan for live/stateful composition is still ahead of us

---

## Completed work

### Phase 0: Core performance (done)

All five critical O(N^2) hot paths have been optimized:

| Method                     | Was             | Now                   | Speedup (largest test)  |
| -------------------------- | --------------- | --------------------- | ----------------------- |
| `aggregate()`              | O(N x B)        | O(N + B)              | **172x** at 16k events  |
| `rolling()` (event-driven) | O(N^2)          | O(N) sliding window   | **182x** at 4k events   |
| `smooth('movingAverage')`  | O(N^2)          | O(N) sliding deque    | **15x** at 4k events    |
| `smooth('loess')`          | O(N^2 log N)    | precomputed neighbors | **7.5x** at 1.6k events |
| `includesKey()`            | O(N)            | O(log N) bisect       | **819x** at 8k events   |
| `#alignLinearAt()`         | O(N) + O(log N) | forward cursor        | **134x** at 4k events   |

Landed in commits `05a7af3` and `60b2f07`. Each change has dedicated regression
tests and a benchmark script.

Internal pre-validated constructor path now skips the
`events -> toRows() -> validateAndNormalize() -> events` round-trip for
order-preserving derived transforms (`filter`, `select`, `rename`, `collapse`,
`map`, etc.). Landed in commit `2ef6265`. A chained `filter -> select -> rename
-> collapse -> map` derivation is **2.5x** faster at 8k events.

### Phase 1 progress: Batch hardening (in progress)

- [x] `toJSON()` round-trips with `fromJSON(...)`
- [x] `toRows()` and `toObjects()` explicit normalized export helpers
- [x] both array-row and object-row JSON shapes supported
- [x] docs cover both ingest and export
- [x] custom aggregate reducers and named aggregate outputs
- [x] edge-case tests for empty series, single-event series, empty aggregation
      buckets, rolling alignment edge cases, and half-open interval semantics
- [x] test and document custom reducers for `rolling()` (type plumbing already
      accepted `CustomAggregateReducer`; added edge-case tests and docs)

---

## Phase 1: Batch hardening (in progress)

Goal: make the existing batch surface trustworthy enough to extend.

Remaining scope: none — all items complete. Phase 1 is ready for the decision
gate: is the batch layer complete and trustworthy enough to be the foundation?

Definition of done:

- [x] custom reducer typing and runtime behavior are documented and covered
- [x] edge-case coverage exists for every current analytical primitive

### Remaining performance items (lower priority, address incrementally)

From the original audit, not yet addressed:

- `Time`/`Interval` temporal comparisons still allocate a throwaway `TimeRange`
  per call
- `Event` constructor still does `Object.freeze({ ...data })` +
  `Object.freeze(this)` — measurable overhead at scale
- `rows` getter still materializes N frozen arrays on every access — should
  cache lazily or become a method
- `aggregateValues` still filters the values array twice — one pass suffices
- `compareEventKeys` still uses `localeCompare` for tiebreaking on fixed
  strings — plain `<` is ~10x faster
- `joinMany` still does repeated pairwise joins — an N-way sorted merge would
  be one pass
- `parseDurationInput` is duplicated in `TimeSeries.ts` and `Sequence.ts`

---

## Phase 2: Batch expansion

Status: in progress.

Goal: fill the most obvious product gaps in the batch analytics story.

Completed:

- [x] `reduce` — collapse a series to a scalar or record (whole-series aggregation)
- [x] `groupBy` — partition by column value, optional transform callback
- [x] `diff` / `rate` — per-event differences and per-second rates of change

Scope:

- `fill` / `fillNull` — explicit gap-filling for sparse or nullable data

Dropped:

- `resample` — everything it would do is already covered by `aggregate()`
  (downsample) and `align()` (upsample); adding it as pure sugar doesn't earn
  its API surface

Nice-to-have in the same wave:

- per-column alignment policies

Hold for later unless a concrete user need appears:

- `pivot` / `unpivot`

### Design notes

**`groupBy`**: returns `Map<string, TimeSeries<S>>` keyed by group values,
preserving full typing on inner series. Optional transform callback avoids
materializing intermediate maps:

```ts
const perHost = series.groupBy('host');
const perHostRolling = series.groupBy('host', (group) =>
  group.rolling('5m', { cpu: 'avg' }),
);
```

**`reduce`**: collapses an entire series to a scalar or record, using the same
reducer specs as `aggregate` but without a time-bucketing sequence. Where
`aggregate` always produces a new `TimeSeries`, `reduce` produces a plain value.
Supports both built-in and custom reducers, same as `aggregate`:

```ts
// single column
const avg = series.reduce('cpu', 'avg'); // => number

// multi-column
const summary = series.reduce({
  cpu: 'avg',
  requests: 'p95',
});
// => { cpu: number, requests: number }

// custom reducer
const weighted = series.reduce(
  'cpu',
  (values) => values.reduce((a, b) => a + b, 0) / values.length,
);

// per-group reduction
const perHost = series.groupBy('host', (g) =>
  g.reduce({ cpu: 'avg', requests: 'p95' }),
);
// => Map<string, { cpu: number, requests: number }>
```

**`diff` / `rate`**: operate on one or more named numeric columns. Non-specified
columns pass through unchanged. First event gets `undefined` in affected columns
by default; `{ drop: true }` removes it instead. `rate` divides by time gap in
seconds. Options object is always the last argument, after column names.

```ts
// single column
const deltas = series.diff('requests');
const perSec = series.rate('requests');

// multi-column
const deltas = series.diff('requests', 'cpu');
const perSec = series.rate('requests', 'cpu');

// drop first event instead of undefined
const deltas = series.diff('requests', { drop: true });
```

**`fill`**: per-column strategies (`hold`, `linear`, `zero`, `null`, or
literal). Optional `limit` caps consecutive fills:

```ts
const filled = series.fill({ cpu: 'linear', host: 'hold', limit: 3 });
```

**Per-column alignment**: extend `align()` to accept a per-column map. Default
(`'hold'`) applies to any column not in the map:

```ts
const aligned = series.align(Sequence.every('1m'), {
  method: { cpu: 'linear', host: 'hold' },
});
```

Definition of done:

- each method has both API docs and worked examples
- type flow is preserved through all new methods
- batch examples cover realistic host/service metrics workflows

---

## Phase 3: Live core

Status: not started.

Goal: introduce a minimal but principled live layer without collapsing the
immutable `TimeSeries` model.

Scope:

- `LiveSeries<S>` — mutable, append-optimized buffer sharing the same schema
  type as `TimeSeries`
- push/append APIs
- retention policies (`maxEvents`, `maxAge`, `maxBytes`)
- immutable snapshot via `toTimeSeries()`
- ordering modes (`strict`, `drop`, `reorder`) and late-arrival policy
- subscriptions (`event`, `batch`, `evict`) — synchronous, inline with push

Non-goals for this phase:

- live aggregation, rolling, or smoothing
- React hooks

### Design notes

**Retention** runs on every push. No background timers — the caller controls the
event loop. Data is the clock.

**Ordering**: three modes — `strict` (default, throws on out-of-order),
`drop` (silently discards late events), `reorder` (inserts in sorted position
within a grace window).

**Subscriptions** are synchronous and fire inline with `push`. Async fanout is
the caller's responsibility.

Definition of done:

- `LiveSeries` can ingest ordered data reliably
- retention and snapshot semantics are clearly documented
- subscriptions are predictable and synchronous
- the API is small enough to change if the composition model reveals flaws

---

## Phase 4: Live composition

Status: not started.

Goal: validate the live composition model before building UI integrations on top
of it.

### Stateless transforms: views, not copies

`filter`, `map`, `select`, `rename`, and `collapse` return lightweight derived
views that apply the transform lazily. No separate buffer — reads walk the
source and apply the predicate. Views compose by stacking.

```ts
const pipeline = live
  .filter((e) => e.get('host') === 'api-1')
  .select('cpu', 'requests');
// materializes lazily on .toTimeSeries()
```

Views also expose `.on('event', fn)` so stateful transforms can subscribe to
filtered streams.

### Stateful transforms: accumulators

If a transform needs memory between events, it becomes its own live object.

**`LiveAggregation`**: maintains closed buckets (finalized), open bucket
(partial), and watermark. A bucket closes when an event arrives past the
boundary. `.closed()` returns finalized results; `.snapshot()` includes the
open bucket's partial value.

**`LiveRolling`**: each push produces a new rolling output event. Maintains a
deque of recent events and evicts those outside the window.

**`LiveSmooth`**: EMA needs only previous smoothed value. Moving average and
LOESS use a sliding deque.

| Transform                                       | Live behavior                          | Owns a buffer? |
| ----------------------------------------------- | -------------------------------------- | -------------- |
| `filter`, `map`, `select`, `rename`, `collapse` | Lazy view over source                  | No             |
| `aggregate`                                     | Accumulator per bucket + closed output | Yes            |
| `rolling`                                       | Sliding deque + output per event       | Yes            |
| `smooth`                                        | Running state + output per event       | Yes            |

### Composition

Stateless and stateful compose naturally: filter before aggregate gates which
events reach the accumulator. Multiple consumers fan out from one source with
shared buffer but separate accumulators.

**Windowed snapshots**: `live.window('5m')` returns a lightweight view backed by
the same buffer, materialized on `.toTimeSeries()`. Window boundary is relative
to latest event timestamp, not wall-clock.

Definition of done:

- stateless and stateful transforms compose cleanly
- filtered/live aggregation pipelines are demonstrated in examples
- snapshot vs closed/finalized semantics are explicit where relevant

---

## Phase 5: React integration

Status: not started.

Goal: make Pond useful in frontend apps without forcing a framework-y runtime
model into the core package.

Entry point: `pond-ts/react`

### Hooks

- `useLiveSeries` — creates and owns a `LiveSeries` for component lifetime;
  returns a stable `live` ref and a throttled `TimeSeries` snapshot
- `useTimeSeries` — thin wrapper around `fromJSON` for static/fetched data
- `useSnapshot` — converts any live stateful transform into a throttled
  `TimeSeries` for rendering
- `useWindow` — derived windowed view that updates as the live series grows
- `useDerived` — applies a batch transform to a snapshot, recomputing when the
  source changes

**Render throttling** is critical. Raw data can arrive at hundreds of events per
second. The `throttle` option caps how often the snapshot is recomputed.
Stateless transforms are cheap enough to build inline during render; stateful
transforms must be created once via `useMemo` on the `live` ref.

Requirements before starting:

- live composition semantics from phases 3 and 4 should already feel stable

Definition of done:

- live data can flow from WebSocket-like sources into throttled React renders
- hooks have examples that mirror likely product use
- the docs explain when to use lazy views vs memoized derived data

---

## Phase 6: Ecosystem and adapters

Status: not started.

Goal: make Pond easier to adopt in real products before committing to a full
first-party charting system.

Scope:

- `pond-ts/node` — Node stream adapters (`Readable`/`Writable`); views and live
  aggregations also expose `.toReadable()`
- `pond-ts/adapters` — bridge helpers such as `toRecharts`, `toObservablePlot`
- improved docs and examples for integrating with existing chart libraries

Later, only after the previous phases are stable:

- `@pond-ts/charts` — first-party chart components built directly on the
  `pond-ts` data model, successor to `react-timeseries-charts`

### Package structure

```
pond-ts              -> core batch library (current)
pond-ts/live         -> LiveSeries, subscriptions, retention, live transforms
pond-ts/node         -> Node stream adapters (Readable/Writable)
pond-ts/react        -> hooks (useLiveSeries, useSnapshot, etc.)
pond-ts/adapters     -> bridge adapters for third-party chart libs (interim)
@pond-ts/charts      -> first-party chart components (separate package, later)
```

Browser-safe by default. Node-specific APIs go behind a separate entry point.

Definition of done:

- Node-specific APIs stay out of the browser-safe default entry point
- adapters solve common "how do I graph this?" questions in the docs
- a chart package remains an intentional future decision, not implied scope creep

---

## Recommended release grouping

| Release band | Focus                                                        |
| ------------ | ------------------------------------------------------------ |
| `0.1.x`      | Performance fixes, hardening, serialization, custom reducers |
| `0.2.x`      | `groupBy`, `reduce`, `diff`/`rate`, `fill`                   |
| `0.3.x`      | `LiveSeries` core and subscriptions                          |
| `0.4.x`      | Live views and live stateful transforms                      |
| `0.5.x`      | React hooks                                                  |
| `0.6.x`      | Node adapters and third-party chart adapters                 |

---

## Decision gates

Before moving from one major phase to the next, answer the relevant question:

- After Phase 1: is the batch layer complete and trustworthy enough to be the
  foundation?
- After Phase 3: is the `LiveSeries` shape correct, or are we still learning?
- After Phase 4: do live/stateful composition rules feel simple enough for
  users?
- After Phase 5: do common frontend use cases work without ad hoc glue?

If the answer is no, stay in the phase and tighten the model before expanding.

---

## Design principles

These hold across all new work:

- **`TimeSeries` stays immutable.** Live mutation belongs in `LiveSeries`.
- **Schema types flow through every operation.** New methods must produce typed
  output schemas. If a method can't be typed, it shouldn't ship.
- **Half-open `[begin, end)` bucketing.** All sequence-based operations use this
  convention.
- **Alignment is separate from aggregation.** `resample` composes them; it
  doesn't merge them.
- **Stateless transforms are views, stateful transforms own buffers.** If an
  operation needs memory between events, it gets its own object. If it doesn't,
  it's a lazy lens over the source.
- **Data is the clock.** Bucket close, watermark advance, and window eviction
  are all driven by event timestamps, not wall-clock timers.
- **No background timers or implicit scheduling.** The caller owns the event
  loop. The library is a data structure, not a framework.
- **Browser-safe by default.** Node-specific APIs go behind a separate entry
  point.

## Semantics to preserve

### Half-open bucketing

For sequence-based bucketing and alignment, interval membership is half-open:
`[begin, end)`. Example: times `10`, `15`, `20` in bucket `[10, 20)` includes
`10` and `15`, excludes `20`.

### Alignment sample position

- default: `begin`
- optional: `center`
- `end` is intentionally not a target mode

### Temporal selection vocabulary

Keep these distinct:

- `within(...)` = fully contained
- `overlapping(...)` = intersects, no key modification
- `trim(...)` = intersects and clips key extents

---

## Cross-cutting work

These happen throughout the phases rather than being deferred:

- keep this document current whenever a meaningful implementation milestone lands
- keep the docs site aligned with shipped behavior
- add end-to-end examples whenever a major capability lands
- keep API reference generation working in CI
- expand tests alongside every new public API
- prefer benchmark-backed changes for performance-sensitive core refactors
