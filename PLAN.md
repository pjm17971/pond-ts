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

Status: complete.

Goal: fill the most obvious product gaps in the batch analytics story.

Completed:

- [x] `reduce` — collapse a series to a scalar or record (whole-series aggregation)
- [x] `groupBy` — partition by column value, optional transform callback
- [x] `diff` / `rate` — per-event differences and per-second rates of change
- [x] `fill` — per-column gap-filling strategies (hold, linear, zero, literal)

Scope: none — all items complete.

Dropped:

- `fillNull` — `fill()` covers all use cases; a separate method doesn't earn its
  API surface

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

**`fill`**: replaces `undefined` values using per-column strategies. Strategies
and options are separate arguments. Strategy names: `hold` (forward fill),
`linear` (time-interpolated), `zero`. Non-string values in the mapping are
literal fill values. `limit` caps consecutive fills per column.

```ts
// single strategy for all columns
series.fill('hold');
series.fill('hold', { limit: 3 });

// per-column strategies
series.fill({ cpu: 'linear', host: 'hold' });

// literal fill values
series.fill({ cpu: 0, host: 'unknown' });
```

`linear` requires known values on both sides of a gap; leading and trailing
undefined runs are left unfilled.

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

## Phase 2.5: Columnar primitives

Status: complete.

Goal: fill the remaining analytical gaps that pandas users expect, without
exposing a general "access neighboring events" API. Each operation is a named
columnar primitive that the library implements internally by walking the event
array — the user describes what they want, not how to iterate.

Completed:

- [x] `pctChange` — percentage change relative to previous value
- [x] `cumulative` — running accumulation (sum, max, min, count, custom)
- [x] `shift` — lag/lead column values by N events
- [x] `bfill` strategy for `fill()` — backward fill (propagate next known value backward)
- [x] built-in aggregator parity with original pondjs: `median`, `stdev`,
      `percentile` (`p50`, `p95`, `p99`, etc.), `difference`, `keep`

### Design notes

**`pctChange`**: same shape as `diff`/`rate`. Computes `(curr - prev) / prev`
for named numeric columns. First event gets `undefined` (no previous value).
Purely value-relative — time gap doesn't matter.

```ts
const pct = series.pctChange('requests');
const pct = series.pctChange(['cpu', 'mem']);
const pct = series.pctChange('requests', { drop: true });
```

For period-over-period comparison (today vs yesterday, current vs one hour ago),
the idiomatic approach is `shiftKeys` + `join` rather than a single-series
`pctChange` — that's a separate composition pattern, not a primitive.

**`cumulative`**: takes a mapping of column names to accumulation functions.
Returns a series of the same length with running values. Supported built-ins:
`sum`, `max`, `min`, `count`. Custom accumulators via function.

```ts
const running = series.cumulative({ requests: 'sum' });
const peaks = series.cumulative({ cpu: 'max' });
const mixed = series.cumulative({
  requests: 'sum',
  cpu: 'max',
  errors: 'min',
});
```

Non-accumulated columns pass through unchanged. Unlike `rolling` (fixed window),
`cumulative` grows from the first event — every event sees all prior values.

**`shift`**: moves column values forward (lag) or backward (lead) by N events.
Vacated positions get `undefined`. Useful for "compare to N ticks ago" on
regular-grid data, or as a building block for custom derived metrics.

```ts
const lagged = series.shift('value', 1); // lag by 1
const lead = series.shift('value', -1); // lead by 1
const lagged = series.shift(['cpu', 'mem'], 2);
```

For time-based shifting (e.g. "value 1 hour ago" on irregular data), the
pattern is to `align` to a regular grid first, then `shift` by the
corresponding number of events. A dedicated `shiftKeys(duration)` that offsets
event timestamps (for join-based period comparison) may come later if the
pattern proves common enough.

**`bfill` for `fill()`**: adds a `'bfill'` strategy to the existing `fill()`
method — the mirror of `'hold'` (forward fill). Walks the event array backward,
propagating the next known value into preceding `undefined` gaps. Supports
`limit` to cap consecutive fills, same as other strategies. Works in per-column
mode too:

```ts
series.fill('bfill');
series.fill('bfill', { limit: 3 });
series.fill({ cpu: 'linear', host: 'bfill' });
```

Trailing `undefined` runs (no future value to propagate) are left unfilled,
mirroring how `'hold'` leaves leading runs unfilled.

**Aggregator parity**: the original pondjs shipped 12 built-in reducers. We have
7 (`sum`, `avg`, `min`, `max`, `count`, `first`, `last`). The five missing ones:

- **`median`** — middle value of the sorted bucket. Same as `percentile(50)` but
  earns its own name for readability.
- **`stdev`** — population standard deviation of bucket values.
- **`percentile`** — q-th percentile. Expressed as `'p50'`, `'p95'`, `'p99'`,
  etc. in reducer specs. Linear interpolation between adjacent ranks by default.
- **`difference`** — range within a bucket (`max - min`). Useful for spread /
  volatility measures.
- **`keep`** — returns the value if all bucket values are identical, `undefined`
  otherwise. Useful for preserving constant columns (e.g. `host`) through
  aggregation.

These extend the existing `AggregateFunction` union and work everywhere reducers
are accepted: `aggregate()`, `reduce()`, `rolling()`, and `collapse()`.

```ts
series.aggregate(Sequence.every('10m'), {
  latency: 'p95',
  cpu: 'median',
  host: 'keep',
});

series.reduce({ latency: 'stdev', spread: 'difference' });
```

Definition of done:

- each method follows the `diff`/`rate` pattern (columns + options)
- type flow is preserved — affected columns become optional number
- tests cover empty series, single event, leading/trailing gaps, and
  composition with groupBy
- all 12 original pondjs reducers are available as built-in names
- `percentile` patterns (`p50`, `p95`, `p99`) parse correctly in reducer specs

---

## Phase 3: Live core

Status: complete.

Goal: introduce a minimal but principled live layer without collapsing the
immutable `TimeSeries` model.

Scope:

- [x] `LiveSeries<S>` — mutable, append-optimized buffer sharing the same schema
      type as `TimeSeries`
- [x] push/append APIs
- [x] retention policies (`maxEvents`, `maxAge`, `maxBytes`)
- [x] immutable snapshot via `toTimeSeries()`
- [x] ordering modes (`strict`, `drop`, `reorder`) and late-arrival policy
- [x] subscriptions (`event`, `batch`, `evict`) — synchronous, inline with push
- [x] docs page for LiveSeries

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

**Subscription ordering**: within a single `push()` call, listeners fire in
this order: `event` (once per event, inline with insertion) → retention runs →
`batch` (once with all added events) → `evict` (if retention removed events).

**`reorder` mode**: without a `graceWindow`, any out-of-order event is inserted
in sorted position via binary search. With a `graceWindow`, events older than
the window relative to the latest timestamp throw. This gives callers control
over how much disorder they'll tolerate.

**`toTimeSeries()` snapshot**: reconstructs rows from the internal event array
and passes them through the standard `TimeSeries` constructor. This re-validates
events redundantly but keeps the two classes fully decoupled. Snapshot is not a
hot path — if profiling proves otherwise, a trusted constructor bridge can be
added later.

**Byte estimation** for `maxBytes`: rough per-event estimate (64 bytes base +
8 per number + 2×length per string + 4 per boolean). Intentionally approximate —
the goal is order-of-magnitude memory capping, not precise measurement.

Definition of done:

- [x] `LiveSeries` can ingest ordered data reliably
- [x] retention and snapshot semantics are clearly documented
- [x] subscriptions are predictable and synchronous
- [x] the API is small enough to change if the composition model reveals flaws

---

## Phase 4: Live composition

Status: complete.

Goal: validate the live composition model before building UI integrations on top
of it.

Completed:

- [x] `LiveAggregation` — incremental bucketed aggregation over a `LiveSeries`
- [x] `LiveRollingAggregation` — sliding-window reduction (time-based or count-based) over
      a `LiveSeries`
- [x] `LiveSource<S>` interface — common contract for LiveSeries and LiveView
- [x] `LiveView<S>` — derived view with `filter()`, `map()`, `select()`,
      `window()`, composable with all live transforms via `LiveSource`
- [x] `LiveAggregation` and `LiveRollingAggregation` accept any `LiveSource<S>`, not just
      `LiveSeries<S>`
- [x] `LiveAggregation` and `LiveRollingAggregation` satisfy `LiveSource` for chaining
      (`name`, `schema`, `length`, `at()`, `on('event')`)
- [x] Grace period for `LiveAggregation` — delays bucket closing so
      out-of-order events within the window accumulate into their correct bucket

Remaining:

- [x] per-event views: `diff`, `rate`, `pctChange` (stateless, prev→curr)
- [x] carry-forward views: `fill`, `cumulative` (small state per column)
- [x] docs page for live transforms

### Batch → Live applicability

Not every batch `TimeSeries` method needs a live equivalent. The live layer is
about ingestion and incremental computation — when you need the full analytical
toolkit, snapshot to `TimeSeries` and use the batch API.

| Batch method      | Live?    | Notes                                                    |
| ----------------- | -------- | -------------------------------------------------------- |
| `filter(pred)`    | **done** | LiveView                                                 |
| `map(fn)`         | **done** | LiveView                                                 |
| `select(...cols)` | **done** | LiveView, schema-narrowing                               |
| `aggregate()`     | **done** | LiveAggregation (bucketed)                               |
| `diff(...cols)`   | **done** | stateless view, needs previous event                     |
| `rate(...cols)`   | **done** | stateless view, delta / time gap                         |
| `pctChange()`     | **done** | stateless view, (curr-prev)/prev                         |
| `fill(strategy)`  | **done** | carry-forward state per column (hold, zero, literal)     |
| `cumulative()`    | **done** | carry-forward state per column (sum, max, min)           |
| `rename(mapping)` | skip     | achievable with `map()`                                  |
| `collapse()`      | skip     | achievable with `map()`                                  |
| `rolling()`       | covered  | `LiveRollingAggregation` as chainable source (see below) |
| `smooth()`        | covered  | EMA is a closure in `map()`; MA is rolling avg           |
| `shift(col, n)`   | maybe    | needs lookback buffer, niche for live                    |
| `align()`         | no       | resampling assumes complete data                         |
| `join()`          | no       | two-stream join is a different primitive                 |
| `groupBy()`       | no       | partitioning is a source-level concern                   |
| `within/trim`     | no       | temporal selection — snapshot then slice                 |
| `reduce()`        | no       | whole-series → scalar — that's `LiveRollingAggregation`  |

### Chainable stateful transforms

`LiveAggregation` emits closed buckets. `LiveRollingAggregation` emits per-event aggregate
values. Both should implement `LiveSource<S>` so their output can feed further
views:

```ts
live
  .filter((e) => e.get('host') === 'api-1')
  .aggregate(Sequence.every('1m'), { value: 'avg' })
  .filter((e) => (e.get('value') as number) > threshold)
  .on('event', alertBucket);
```

For `LiveAggregation`, the output events are interval-keyed (closed buckets).
For `LiveRollingAggregation`, each source event produces a new time-keyed output event with
the current sliding-window aggregate. This makes LiveRollingAggregation-as-source the live
equivalent of `rolling()` — no separate class needed.

Similarly, `LiveSmooth` is not needed as a dedicated class: EMA is a stateful
closure inside `map()`, and moving average is `LiveRollingAggregation`-as-source with
`'avg'`.

### Views

`filter`, `map`, `select`, and `window` return `LiveView` — a derived view
that subscribes to its source's event stream and forwards processed events.

**Stateless views** (`filter`, `map`, `select`) apply a per-event transform.
**Bounded views** (`window`) add eviction to keep the buffer within a time or
count limit.

Planned per-event views (`diff`, `rate`, `pctChange`) carry one value per
column from the previous event. Planned carry-forward views (`fill`,
`cumulative`) carry state that accumulates across events. Both fit the LiveView
model — the `process` function closes over the state.

### Accumulators

**`LiveAggregation`**: maintains pending buckets (accumulating), a watermark
(highest timestamp seen), and an optional grace period. A bucket closes when
its `end <= watermark - grace`. With zero grace (default), buckets close
immediately on boundary crossing — matching the behavior before grace was
added. With grace > 0, multiple buckets can be pending simultaneously, and
late events within the grace window route to their correct bucket instead of
being lost.

`.closed()` returns only finalized buckets; `.snapshot()` includes all
pending buckets as provisional results. As a `LiveSource`, `at(index)` and
`length` expose the closed-bucket event buffer; `on('event', fn)` fires
when a bucket finalizes.

```ts
new LiveAggregation(
  source,
  Sequence.every('1m'),
  { value: 'avg' },
  { grace: '5s' },
);
```

**`LiveRollingAggregation`**: maintains a sliding-window reduction. Supports both
time-based windows (`'5m'`) and count-based windows (`100`). Uses
`RollingReducerState` from the reducer registry for incremental add/remove.
As a `LiveSource`, each source event produces an output event containing the
current aggregate value at that point. The output buffer grows with each
source event (downstream consumers can use `.window()` to bound it).
`on('event', fn)` fires per source event with the new aggregate.

| Transform                | Live behavior                          | Owns a buffer? | Chainable? |
| ------------------------ | -------------------------------------- | -------------- | ---------- |
| `filter/map/select`      | Per-event transform                    | Yes (view)     | Yes        |
| `window`                 | Bounded view with eviction             | Yes (view)     | Yes        |
| `diff/rate/pctChange`    | Per-event with prev-event state        | Yes (view)     | Yes        |
| `fill/cumulative`        | Per-event with carry-forward state     | Yes (view)     | Yes        |
| `LiveAggregation`        | Accumulator per bucket + closed stream | Yes            | Yes        |
| `LiveRollingAggregation` | Sliding window + per-event output      | Yes            | Yes        |

### LiveSource interface and LiveView

`LiveSource<S>` is the common interface that all live objects expose for
downstream consumers: `name`, `schema`, `length`, `at(index)`, and
`on('event', fn)`. Both `LiveSeries` and `LiveView` satisfy it, so
`LiveAggregation` and `LiveRollingAggregation` accept any `LiveSource<S>`.

`LiveView<S>` wraps a source with a `process: (event) => event | undefined`
function. If `process` returns `undefined`, the event is filtered out. This
unifies filter (predicate → event or undefined) and map (transform → always
returns event) in one class.

Views maintain their own buffer of processed events for O(1) `at()` and
`length`. Views mirror evictions from their source: when a retention-capped
`LiveSeries` evicts old events, downstream views (filter, map, etc.) remove
corresponding events automatically. This prevents unbounded growth on
filtered/mapped views of a retention-capped source. Detection uses the
`EMITS_EVICT` symbol to safely identify sources that fire `'evict'` events
(avoids duck-typing `on('evict')` which breaks on `LiveAggregation`).

**`select`** narrows the schema. The output `LiveView` has a different schema
type from the input. The constructor accepts an optional output schema for this
case; filter/map omit it (schema is inherited).

**`window`** bounds the view by time or event count. Uses an eviction function
that runs after each event is added. Time-based windows evict events whose
timestamp is below `latest - duration`. Count-based windows keep the last N
events. Unlike retention on `LiveSeries`, window is a query over the data, not a
memory policy — you can keep a large source buffer but view a narrow window.

Views compose by stacking:

```ts
live.filter(pred).select('cpu', 'mem').window('5m').aggregate(seq, mapping);
```

Each view subscribes to its source's `'event'` stream and forwards processed
events to its own subscribers.

### Composition

Views, accumulators, and further views compose naturally:

```ts
live
  .filter(pred)
  .select('cpu', 'mem')
  .window('5m')
  .aggregate(Sequence.every('1m'), { cpu: 'avg' })
  .filter((e) => (e.get('cpu') as number) > threshold);
```

Multiple consumers fan out from one source with shared buffer but separate
state.

**Windowed snapshots**: `live.window('5m')` returns a view backed by the same
source, materialized on `.toTimeSeries()`. Window boundary is relative to
latest event timestamp, not wall-clock.

### Dropped from scope

- **`LiveRolling`**: covered by `LiveRollingAggregation` implementing `LiveSource` — the
  per-event output stream IS the rolling output.
- **`LiveSmooth`**: EMA is a stateful closure in `map()`. Moving average is
  `LiveRollingAggregation`-as-source with `'avg'`. LOESS is too expensive for per-event
  streaming.
- **`rename`/`collapse` views**: achievable with `map()`. Don't earn dedicated
  API surface in the live layer.

Definition of done:

- [x] stateful transforms use existing reducer infrastructure incrementally
- [x] stateless and stateful transforms compose cleanly
- [x] stateful transforms satisfy `LiveSource` for pipeline chaining
- [x] filtered/live aggregation pipelines are demonstrated in examples
- [x] snapshot vs closed/finalized semantics are explicit where relevant

---

## Phase 5: React integration

Status: in progress. Monorepo restructure complete — `@pond-ts/react` package
at `packages/react/`. Hooks shipped at v0.4.2; usability fixes in progress.

Goal: make Pond useful in frontend apps without forcing a framework-y runtime
model into the core package.

Entry point: `@pond-ts/react` (separate workspace package)

### Hooks

- [x] `useLiveSeries` — creates and owns a `LiveSeries` for component lifetime;
      returns a stable `live` ref and a throttled `TimeSeries` snapshot
- [x] `useTimeSeries` — memoized `TimeSeries.fromJSON(...)` for static/fetched
      data; re-parses only when key changes
- [x] `useSnapshot` — converts any `LiveSource` into a throttled `TimeSeries`
      snapshot for rendering; works with `LiveSeries`, `LiveView`,
      `LiveAggregation`, and `LiveRollingAggregation`
- [x] `useWindow` — derived windowed view that updates as the source grows;
      disposes the view on cleanup
- [x] `useDerived` — applies a batch transform to a snapshot, recomputing when
      the input changes
- [x] `takeSnapshot` — utility: build a `TimeSeries` from any `LiveSource`

### Usability fixes (from external testing)

- [x] `Time.toDate()` — added missing convenience method
- [x] `useWindow` StrictMode fix — view created in `useEffect`, not `useMemo`
- [x] `TimeSeries[Symbol.iterator]` and `toArray()` — ergonomic iteration
- [x] `useSnapshot` accepts `SnapshotSource<S>` structural type — avoids casts
      when passing `LiveAggregation` or `LiveRollingAggregation`
- [x] `LiveView` eviction mirroring — filtered/mapped views now mirror source
      evictions (uses `EMITS_EVICT` symbol to safely detect evict-capable sources)
- [x] `LiveAggregation<S, Out>` and `LiveRollingAggregation<S, Out>` — output
      schema type parameter enables `event.get('col')` to narrow through
      aggregation chains (e.g. `agg.at(0)?.get('cpu')` returns `number | undefined`
      instead of `ScalarValue | undefined`)
- [x] Schema-transform types already exported: `AggregateSchema`, `RollingSchema`,
      `DiffSchema`, `SmoothSchema`, `SmoothAppendSchema`, `SelectSchema`,
      `RenameSchema`, `CollapseSchema`

**Render throttling** is critical. Raw data can arrive at hundreds of events per
second. The `throttle` option caps how often the snapshot is recomputed.
Stateless transforms are cheap enough to build inline during render; stateful
transforms must be created once via `useMemo` on the `live` ref.

Requirements before starting:

- live composition semantics from phases 3 and 4 should already feel stable

Definition of done:

- [x] live data can flow from WebSocket-like sources into throttled React renders
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

Monorepo with npm workspaces (`packages/*`):

```
pond-ts              -> packages/core — batch + live library
@pond-ts/react       -> packages/react — React hooks
@pond-ts/charts      -> future — first-party chart components
```

Subpath entry points within `pond-ts`:

```
pond-ts              -> core batch library
pond-ts/live         -> LiveSeries, subscriptions, retention, live transforms
pond-ts/node         -> Node stream adapters (Readable/Writable, future)
pond-ts/adapters     -> bridge adapters for third-party chart libs (future)
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
| `0.2.5`      | `pctChange`, `cumulative`, `shift`                           |
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
- **Transforms are views or accumulators.** If an operation needs only per-event
  or carry-forward state, it's a `LiveView`. If it needs a growing buffer
  (buckets, sliding window), it's an accumulator. Both implement `LiveSource`
  for chaining.
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
