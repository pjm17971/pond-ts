# Future API Development

This document covers four areas: performance fixes to the existing core,
extending the batch analytics story, adding live/streaming primitives for Node,
and making the library useful inside React applications.

---

## 1. Performance — fix before building on top

The existing batch implementation has algorithmic issues that should be resolved
before adding new surface area. These are documented in full in
[AUDIT.md](AUDIT.md). The highest-priority items are summarized here.

### 1.1 O(N²) hot paths → O(N) or O(N + B)

Five methods scan the entire event array inside an inner loop:

| Method | Current | Target |
|--------|---------|--------|
| `aggregate()` | O(N × B) per bucket | O(N + B) single pass or bisect |
| `rolling()` (event-driven) | O(N²) per event | O(N) sliding two-pointer |
| `rolling()` (sequence-driven) | O(N × B) per bucket | O(N + B) |
| `smooth('movingAverage')` | O(N²) per event | O(N) sliding deque |
| `smooth('loess')` | O(N² log N) sort per event | O(N²) incremental neighbor set |

Events are sorted by time. Every one of these can exploit that invariant with
either a two-pointer sweep or a bisect-per-bucket approach.

### 1.2 Eliminate re-validation on derived series

Every transform (`filter`, `select`, `rename`, `map`, `collapse`, `slice`,
`within`, `before`, `after`, `trim`, `overlapping`, `containedBy`, `asTime`,
`asTimeRange`, `asInterval`) creates a `new TimeSeries(...)` which re-validates
every cell and re-checks sorted order.

Fix: add an internal constructor path that accepts pre-validated events directly.
This also eliminates the `events → toRows() → validateAndNormalize() → events`
round-trip that decomposes and recomposes the same data.

### 1.3 Use bisect where linear scans exist

- `includesKey()` does O(N) linear scan — should use the existing `bisect()`
  for O(log N).
- `#alignLinearAt()` does a `find()` (O(N)) before falling back to
  `atOrBefore`/`atOrAfter` (O(log N)) — should bisect once and check for exact
  match at that index.

### 1.4 Reduce allocation in tight loops

- `Time.overlaps/contains/isBefore/isAfter` each allocate a throwaway
  `TimeRange`. For a point, these are direct arithmetic comparisons. Same issue
  on `Interval` — it has `start`/`endMs` fields and doesn't need the
  intermediate `TimeRange`.
- `Event` constructor does `Object.freeze({ ...data })` + `Object.freeze(this)`.
  With private fields, the instance is already effectively immutable. The freeze
  adds overhead when constructing thousands of events on every derived series.
- The `rows` getter materializes N frozen arrays on every access. Should cache
  lazily or become a method.

### 1.5 Minor fixes

- `aggregateValues` filters the values array twice (once for defined, once for
  numeric) — one pass suffices.
- `compareEventKeys` uses `localeCompare` for tiebreaking on fixed strings —
  plain `<` is ~10× faster.
- `joinMany` does repeated pairwise joins, each creating a new `TimeSeries`.
  An N-way sorted merge would be one pass.
- `parseDurationInput` is duplicated in `TimeSeries.ts` and `Sequence.ts` —
  should be a shared utility.

---

## 2. Batch API extensions

The current batch surface is already strong: typed construction, temporal
selection, alignment, aggregation, joins, rolling windows, and smoothing are all
implemented and tested. The extensions below build on that foundation.

### 2.1 Custom reducers

The built-in aggregate functions (`sum`, `avg`, `min`, `max`, `count`, `first`,
`last`) cover the common cases. The next step is a custom-reducer path so users
can plug their own logic into `aggregate()` and `rolling()`.

```ts
const p95 = series.aggregate(Sequence.every('5m'), {
  latency: (events) => quantile(events.map(e => e.get('latency')), 0.95),
  host: 'last',
});
```

The type signature would accept `AggregateFunction | ((events: Event[]) => ScalarValue)`
per column. The return schema maps each column to its declared output type.

### 2.2 `groupBy`

Group a series by one or more categorical columns and apply per-group transforms:

```ts
const perHost = series.groupBy('host');
// Map<string, TimeSeries<S>>

const perHostRolling = series.groupBy('host', (group) =>
  group.rolling('5m', { cpu: 'avg' })
);
// Map<string, TimeSeries<RollingSchema<S, ...>>>
```

`groupBy` is the main missing piece for multi-entity analytics. It should return
a `Map` keyed by the group values, preserving full typing on the inner series.
An optional transform callback avoids materialising intermediate maps.

### 2.3 `resample` (convenience over align + aggregate)

Many workflows follow the pattern: align to a grid, then aggregate. A `resample`
method would compose these internally:

```ts
const hourly = series.resample(Sequence.hourly(), {
  cpu: 'avg',
  requests: 'sum',
  host: 'last',
});
```

This is purely syntactic sugar. The implementation delegates to `align` then
`aggregate`, but the single call is much more discoverable for new users.

### 2.4 `diff` / `rate`

Compute per-event differences or rates of change across a numeric column:

```ts
const deltas = series.diff('requests');          // absolute difference
const perSecond = series.rate('requests');        // diff / dt
```

Both return a new series with the same key type. The first event is either
dropped or filled with `null` (configurable). `rate` divides by the time gap
between consecutive events in seconds.

### 2.5 `fill` / `fillNull`

Explicit gap-filling for sparse or nullable data:

```ts
const filled = series.fill({ cpu: 'linear', host: 'hold' });
const capped = series.fill({ cpu: 'linear', host: 'hold', limit: 3 });
```

Per-column strategies: `hold` (forward-fill), `linear` (interpolate between
neighbors), `zero`, `null`, or a literal value. An optional `limit` caps the
number of consecutive fills.

### 2.6 Per-column alignment policies

Currently `align()` takes a single method for all columns. Extending it to
accept a per-column map would cover the common case of numeric interpolation
alongside categorical hold:

```ts
const aligned = series.align(Sequence.every('1m'), {
  method: { cpu: 'linear', host: 'hold' },
});
```

The default (`'hold'`) applies to any column not in the map.

### 2.7 `pivot` / `unpivot`

For wide-to-long and long-to-wide reshaping:

```ts
// wide -> long: one row per (time, metric_name, metric_value)
const long = wide.unpivot(['cpu', 'memory'], {
  nameColumn: 'metric',
  valueColumn: 'value',
});

// long -> wide: one column per distinct metric name
const wide = long.pivot('metric', 'value');
```

These are common in dashboard and charting workflows where the shape of the data
needs to change depending on the visualization.

### 2.8 `toJSON` / serialisation

The ingest path (`fromJSON`) exists but there is no symmetric export. A
`toJSON()` method should produce the same shape that `fromJSON` accepts, enabling
round-trip serialisation:

```ts
const json = series.toJSON();
// { name, schema, rows: [...] }

const restored = TimeSeries.fromJSON(json);
```

`toRows()` and `toObjects()` would be useful lower-level variants:

```ts
series.toRows();     // array of tuple rows
series.toObjects();  // array of { time, cpu, host, ... } objects
```

### 2.9 Hardening and edge cases

Before adding more surface area, the existing API benefits from coverage of:

- Empty series (zero events) through all operations
- Single-event series through rolling and smoothing
- Aggregation with empty buckets (zero events in a window)
- `min` and `max` reducers (implemented but not tested at the series level)
- Leading rolling alignment (type exists, no runtime test)
- Half-open `[begin, end)` semantics validated in an isolated test

---

## 3. Live / streaming layer

The current `TimeSeries` is immutable and batch-oriented. A separate mutable
structure handles the append-heavy, retention-bounded world of live data.

The central design question is how live data composes with transforms like
`.map()`, `.filter()`, and `.aggregate()`. The answer depends on whether the
transform is **stateless** (can evaluate each event independently) or
**stateful** (needs memory between events). This distinction drives the entire
live API.

### 3.1 `LiveSeries<S>`

A mutable, append-optimised buffer that shares the same schema type as
`TimeSeries`:

```ts
import { LiveSeries } from 'pond-ts';

const live = new LiveSeries({
  name: 'cpu',
  schema,
  retention: { maxEvents: 10_000 },
});

live.push([new Date(), 0.72, 'api-1', true]);
live.pushEvent(event);

live.length;       // current buffer size
live.first();      // oldest retained event
live.last();       // newest event

const snapshot = live.toTimeSeries();  // immutable snapshot
```

**Retention policies** (pick one or combine):

| Policy | Meaning |
|--------|---------|
| `maxEvents: N` | Evict oldest beyond N |
| `maxAge: '1h'` | Evict events older than duration |
| `maxBytes: N` | Approximate memory cap (stretch) |

Retention runs on every push. No background timers — the caller controls the
event loop.

### 3.2 Ordering and late arrival

By default, `LiveSeries` assumes events arrive in order. Options for relaxing
this:

```ts
const live = new LiveSeries({
  schema,
  name: 'cpu',
  ordering: 'reorder',       // buffer and sort within a grace window
  lateArrivalGrace: '10s',   // how far back a late event can land
});
```

Three modes:
- `'strict'` (default): throws on out-of-order push
- `'drop'`: silently discards late events
- `'reorder'`: inserts in sorted position within the grace window

### 3.3 Subscriptions

Event-driven notifications for downstream consumers:

```ts
live.on('event', (event) => { /* per-event */ });
live.on('batch', (events) => { /* periodic batch */ });
live.on('evict', (events) => { /* retention evictions */ });

// typed EventEmitter, or a simple callback registration
```

Subscriptions are synchronous and fire inline with `push`. Async fanout is the
caller's responsibility. This keeps the library out of the scheduler business.

### 3.4 Stateless transforms: views, not copies

`map`, `filter`, `select`, `rename`, and `collapse` do not need their own
buffer. Each incoming event can be evaluated independently. Rather than producing
a second `LiveSeries` with its own retention bookkeeping, these return a
lightweight **derived view** that applies the transform lazily:

```ts
// no separate buffer — reads walk the source and apply the predicate
const highCpu = live.filter(e => e.get('cpu') > 0.8);

// same — transform is applied per-event on read
const renamed = live.select('cpu', 'host').rename({ cpu: 'usage' });

// snapshot materialises the view into an immutable TimeSeries
const snapshot = highCpu.toTimeSeries();
```

The view holds a reference to the source `LiveSeries` plus the transform
function. When you call `.toTimeSeries()`, `.last()`, `.length`, etc., it walks
the source buffer and applies the transform. No duplication, no sync problem.

Views compose by stacking:

```ts
const pipeline = live
  .filter(e => e.get('host') === 'api-1')
  .select('cpu', 'requests');

// internally: source → filter → select
// materialises lazily on .toTimeSeries()
```

This keeps the caller in control — you pay for the transform when you read, not
on every push. In a React context, that means the transform runs once per
throttled render, not once per incoming event.

The full list of stateless transforms that return views:

| Method | Behavior |
|--------|----------|
| `filter(predicate)` | Skips non-matching events on read |
| `map(schema, mapper)` | Transforms each event on read |
| `select(...keys)` | Projects columns on read |
| `rename(mapping)` | Renames columns on read |
| `collapse(keys, output, fn)` | Collapses columns on read |

Views also expose `.on('event', fn)` — the callback fires on each source push
that passes the view's predicates. This lets stateful transforms subscribe to
filtered streams (see 3.5 below).

### 3.5 Stateful transforms: accumulators

Aggregation, rolling, and smoothing need memory between events. These return
dedicated live objects with their own internal state.

#### Live aggregation

```ts
const buckets = live.aggregate(Sequence.every('5m'), {
  cpu: 'avg',
  requests: 'sum',
  host: 'last',
});
```

`buckets` is a `LiveAggregation` — not a `LiveSeries`. It maintains three
pieces of internal state:

```
┌─────────────────────────────────────────────────┐
│  closed buckets                                 │
│  (finalized, immutable, interval-keyed events)  │
├─────────────────────────────────────────────────┤
│  open bucket                                    │
│  (partial accumulator, still receiving events)  │
├─────────────────────────────────────────────────┤
│  watermark                                      │
│  (latest event timestamp seen)                  │
└─────────────────────────────────────────────────┘
```

A bucket closes when an event arrives whose timestamp is past the bucket
boundary. No timers — the data is the clock.

```ts
// closed buckets only — fully accurate
buckets.closed();          // TimeSeries<AggregateSchema<...>>

// closed + open bucket's partial result — useful for live dashboards
buckets.snapshot();        // TimeSeries<AggregateSchema<...>>

// the open bucket's current partial value
buckets.current();         // Event<...> | undefined

// fires when a bucket finalises
buckets.on('close', (event) => { /* ... */ });
```

The distinction between `.closed()` and `.snapshot()` matters for dashboards:
you almost always want to show the in-progress bucket too, even though its value
will change.

#### Live rolling

Rolling is structurally similar but the window is event-anchored rather than
grid-anchored:

```ts
const rolling = live.rolling('5m', {
  cpu: 'avg',
  requests: 'sum',
});
```

Each push produces a new rolling output event (the aggregate of all source
events within the trailing window of the new event). The implementation
maintains a deque of recent events and evicts those outside the window.

```ts
rolling.last();            // most recent rolling result
rolling.snapshot();        // all rolling results as TimeSeries
rolling.on('event', (e) => { /* new rolling value */ });
```

#### Live smoothing

EMA is a natural fit for streaming — it only needs the previous smoothed value:

```ts
const smoothed = live.smooth('cpu', 'ema', { alpha: 0.3 });

smoothed.last();           // most recent smoothed event
smoothed.snapshot();       // full smoothed history as TimeSeries
```

Moving average and LOESS are backed by a sliding window deque, like rolling.

#### Classification of transforms

| Transform | Live behavior | Owns a buffer? |
|-----------|---------------|----------------|
| `filter`, `map`, `select`, `rename`, `collapse` | Lazy view over source | No |
| `aggregate` | Accumulator per bucket + closed output | Yes |
| `rolling` | Sliding deque + output per event | Yes |
| `smooth` | Running state (EMA alpha, etc.) + output per event | Yes |

Stateless transforms are free to chain and don't allocate. Stateful transforms
create a new live object with its own lifecycle. If the operation needs memory
between events, it gets its own object. If it doesn't, it's just a lens over the
source.

### 3.6 Composing stateless and stateful transforms

The two categories compose naturally because stateless views forward
subscriptions.

**Filter before aggregate** — only count events matching a predicate:

```ts
const errors = live
  .filter(e => e.get('status') >= 500)
  .aggregate(Sequence.every('1m'), { status: 'count' });
```

`.filter()` returns a view. When `.aggregate()` is called on a view, it
subscribes to the source `LiveSeries` pushes but applies the filter predicate
before feeding events into the accumulator. The filter doesn't buffer anything —
it just gates which events reach the aggregation.

The push path:

```
live.push(event)
  → filter predicate: status >= 500?
    → yes: feed to aggregate accumulator
    → no:  skip
```

**Aggregate then transform** — reshape the output of aggregation:

```ts
const buckets = live.aggregate(Sequence.every('5m'), {
  cpu: 'avg',
  requests: 'sum',
});

// batch transforms on the finalized output
const labeled = buckets.closed().rename({ cpu: 'avgCpu' });
```

`.closed()` returns an immutable `TimeSeries`, and the full batch API applies.

For the live case where you want the rename to apply on every snapshot:

```ts
const derived = buckets.derive(s => s.rename({ cpu: 'avgCpu' }));
// derived.snapshot() applies the batch transform on each read
```

**Multiple consumers from one source:**

```ts
const api1 = live.filter(e => e.get('host') === 'api-1');
const api2 = live.filter(e => e.get('host') === 'api-2');

const api1Buckets = api1.aggregate(Sequence.every('1m'), { cpu: 'avg' });
const api2Buckets = api2.aggregate(Sequence.every('1m'), { cpu: 'avg' });
```

Each push fans out to both filter views, which independently gate their
downstream aggregators. The source buffer is shared; the accumulators are
separate.

### 3.7 Windowed snapshots

Take analytical snapshots without copying the entire buffer:

```ts
const last5m = live.window('5m');           // DerivedView (time-bounded)
const snapshot = live.window('5m').toTimeSeries();  // immutable
```

`window()` returns a lightweight view backed by the same buffer. It only
materialises events on iteration or `toTimeSeries()`. The window boundary
is relative to the latest event timestamp, not wall-clock time.

Windows compose with other views:

```ts
const recentHighCpu = live
  .window('10m')
  .filter(e => e.get('cpu') > 0.8);

recentHighCpu.toTimeSeries();  // last 10 min, cpu > 0.8 only
```

### 3.8 Node stream integration

A `Readable` adapter for piping into Node's stream ecosystem:

```ts
import { Readable } from 'node:stream';

const readable = live.toReadable();        // objectMode Readable
readable.pipe(transform).pipe(destination);
```

And a `Writable` adapter for ingesting from upstream:

```ts
import { pipeline } from 'node:stream/promises';

const writable = live.toWritable();        // objectMode Writable
await pipeline(source, writable);
```

Views and live aggregations also expose `.toReadable()`:

```ts
// only errors flow into the Node stream
const errorStream = live
  .filter(e => e.get('status') >= 500)
  .toReadable();

// closed buckets emit as they finalize
const bucketStream = live
  .aggregate(Sequence.every('1m'), { requests: 'sum' })
  .toReadable();   // emits one event per closed bucket
```

These should be in a separate entry point (`pond-ts/node`) so the browser bundle
doesn't pull in `node:stream`.

---

## 4. React integration

React apps need to subscribe to changing data and render efficiently. The
integration layer lives in `pond-ts/react` and exports hooks that bridge the
live composition model (section 3) with React's render cycle.

The core challenge is that live data pushes happen outside React's control
(WebSocket callbacks, timers, server-sent events), while React wants to batch
state updates and control when re-renders happen. The hooks below handle that
boundary.

### 4.1 `useLiveSeries`

Creates and owns a `LiveSeries` for the lifetime of the component:

```tsx
import { useLiveSeries } from 'pond-ts/react';

function CpuChart() {
  const { series, live } = useLiveSeries({
    name: 'cpu',
    schema,
    retention: { maxEvents: 500 },
    throttle: 100,  // ms — re-render at most every 100ms
  });

  useEffect(() => {
    const ws = new WebSocket('/metrics');
    ws.onmessage = (msg) => live.push(JSON.parse(msg.data));
    return () => ws.close();
  }, [live]);

  // `series` is a TimeSeries snapshot, updated on the throttle interval
  return <Chart data={series} />;
}
```

`live` is a stable ref (same `LiveSeries` instance across renders). `series` is
a `TimeSeries` snapshot that updates at the throttle rate. Internally the hook
subscribes to pushes, coalesces them via `requestAnimationFrame` or a timer, and
calls `live.toTimeSeries()` once per render cycle.

**Render throttling** is critical. Raw sensor data can arrive at hundreds of
events per second — re-rendering on every push will kill frame rate. The
`throttle` option (default: `0`, meaning every animation frame) caps how often
the snapshot is recomputed.

### 4.2 `useTimeSeries`

For static / fetched data that doesn't change after load:

```tsx
function Dashboard() {
  const series = useTimeSeries(rawJson, { parse: { timeZone: 'UTC' } });

  const hourly = useMemo(
    () => series?.aggregate(Sequence.hourly(), { cpu: 'avg' }),
    [series]
  );

  return hourly ? <Table data={hourly} /> : <Loading />;
}
```

A thin wrapper around `TimeSeries.fromJSON` that memoises the result and avoids
reconstructing the series on every render. When `rawJson` changes (new fetch),
the series is rebuilt.

### 4.3 Stateless views in React

Stateless views (section 3.4) are cheap to create, so they can be built inline
during render without `useMemo`:

```tsx
function HostChart({ hostname }: { hostname: string }) {
  const { series, live } = useLiveSeries({ name: 'cpu', schema, retention: { maxEvents: 1000 } });

  // this is fine — filter is a lazy view, not a buffer copy
  const filtered = series.filter(e => e.get('host') === hostname);

  return <Chart data={filtered} />;
}
```

However, if the view involves expensive iteration (large buffer + complex
predicate), pull it into `useMemo`:

```tsx
const filtered = useMemo(
  () => series.filter(e => expensiveCheck(e)),
  [series]
);
```

Because `series` is a new `TimeSeries` snapshot on each throttled update, the
memo recomputes at the throttle rate — not on every render.

### 4.4 Stateful transforms in React

Stateful transforms (aggregation, rolling, smoothing) must be created once and
persist across renders. Use `useMemo` on the `live` ref:

```tsx
function BucketChart() {
  const { live } = useLiveSeries({ name: 'cpu', schema, retention: { maxEvents: 5000 } });

  // one LiveAggregation for the component's lifetime
  const buckets = useMemo(
    () => live.aggregate(Sequence.every('1m'), { cpu: 'avg', requests: 'sum' }),
    [live],
  );

  // snapshot includes the open bucket's partial value
  const chartData = useSnapshot(buckets, { throttle: 200 });

  return <Chart data={chartData} />;
}
```

`useMemo` on `live` ensures the aggregation accumulator is created once. The
`useSnapshot` hook (see 4.5) handles the throttled re-snapshot.

**Composing filter + aggregate in React:**

```tsx
function ErrorRateChart() {
  const { live } = useLiveSeries({ name: 'http', schema, retention: { maxEvents: 10_000 } });

  // filter view (stateless) → aggregate (stateful)
  const errorBuckets = useMemo(
    () => live
      .filter(e => e.get('status') >= 500)
      .aggregate(Sequence.every('1m'), { status: 'count' }),
    [live],
  );

  const chartData = useSnapshot(errorBuckets, { throttle: 500 });

  return <Chart data={chartData} />;
}
```

The filter view doesn't allocate a buffer. The aggregate subscribes to the
source pushes, applies the filter predicate inline, and feeds matching events
into its accumulators.

### 4.5 `useSnapshot`

Converts any live stateful transform into a throttled `TimeSeries` for rendering:

```tsx
const chartData = useSnapshot(buckets, { throttle: 200 });
// TimeSeries<AggregateSchema<...>>, updated every 200ms
```

Works with `LiveAggregation`, `LiveRolling`, `LiveSmooth`, or plain
`LiveSeries`. Calls `.snapshot()` (or `.toTimeSeries()` for `LiveSeries`) on
the throttle interval. Returns `undefined` before the first snapshot is ready.

### 4.6 `useWindow`

Derived windowed view that updates as the live series grows:

```tsx
const last5m = useWindow(live, '5m', { throttle: 200 });
// TimeSeries<S> — trailing 5 minutes, updated every 200ms
```

Internally calls `live.window('5m').toTimeSeries()` on the throttle interval.

### 4.7 `useDerived`

Applies a batch transform to a snapshot, recomputing when the source changes:

```tsx
const smoothed = useDerived(
  chartData,
  (s) => s.smooth('cpu', 'ema', { alpha: 0.3 }),
);
```

This is `useMemo` with the right dependency — but making it explicit avoids
users accidentally recomputing expensive transforms on every render. The
transform function receives an immutable `TimeSeries` and returns one.

### 4.8 Full composition example

Putting it all together — a dashboard with filtered live aggregation, rolling
averages, and smoothing:

```tsx
function MetricsDashboard() {
  const { live } = useLiveSeries({
    name: 'metrics',
    schema,
    retention: { maxEvents: 10_000 },
    throttle: 100,
  });

  // stateful: one accumulator, persists across renders
  const minuteBuckets = useMemo(
    () => live.aggregate(Sequence.every('1m'), {
      cpu: 'avg',
      requests: 'sum',
    }),
    [live],
  );

  // stateful: rolling 5m average
  const rollingCpu = useMemo(
    () => live.rolling('5m', { cpu: 'avg' }),
    [live],
  );

  // throttled snapshots for rendering
  const bucketData = useSnapshot(minuteBuckets, { throttle: 200 });
  const rollingData = useSnapshot(rollingCpu, { throttle: 200 });

  // stateless batch transform on the snapshot (recomputes when snapshot changes)
  const trend = useDerived(
    rollingData,
    (s) => s.smooth('cpu', 'ema', { alpha: 0.3, output: 'cpuTrend' }),
  );

  useEffect(() => {
    const ws = new WebSocket('/metrics');
    ws.onmessage = (msg) => live.push(JSON.parse(msg.data));
    return () => ws.close();
  }, [live]);

  return (
    <>
      <BucketChart data={bucketData} />
      <TrendChart data={trend} />
    </>
  );
}
```

The data flow:

```
WebSocket → live.push()
              ├─→ minuteBuckets (accumulates into 1m buckets)
              │     └─→ useSnapshot (200ms) → bucketData → <BucketChart>
              └─→ rollingCpu (sliding 5m deque)
                    └─→ useSnapshot (200ms) → rollingData
                          └─→ useDerived(smooth) → trend → <TrendChart>
```

Each arrow is a subscription. Stateless steps (smooth via `useDerived`) run on
the snapshot, not on every push. Stateful steps (aggregate, rolling) run on
every push but only snapshot on the throttle interval.

### 4.9 `@pond-ts/charts`

`pond-ts` is the successor to `pondjs`, which powered `react-timeseries-charts`.
The long-term goal is a first-party charting package (`@pond-ts/charts`) that
replaces that library with components built directly on the `pond-ts` data model.

This is a separate package, not an entry point in `pond-ts` itself. It depends
on `pond-ts` and `pond-ts/react` as peer dependencies.

The rough shape:

```tsx
import { TimeSeriesChart, LineChart, BarChart, Axis } from '@pond-ts/charts';

function Dashboard({ series }: { series: TimeSeries<S> }) {
  return (
    <TimeSeriesChart timeRange={series.timeRange()}>
      <Axis column="cpu" position="left" />
      <LineChart series={series} columns={['cpu']} />
      <BarChart series={buckets} columns={['requests']} />
    </TimeSeriesChart>
  );
}
```

Key differences from `react-timeseries-charts`:

- **Built on the new type system.** Chart components accept `TimeSeries<S>` and
  can validate column names at the type level — no runtime "column not found"
  errors.
- **Live-aware.** Charts accept either a `TimeSeries` (static) or a
  `useSnapshot` result (live). The `TimeSeriesChart` component handles the
  scrolling time window automatically when given a live source.
- **Composable with the hook layer.** The hooks from `pond-ts/react` feed
  directly into chart components without intermediate conversion.

This is far enough out that the API should be designed after the live layer and
React hooks are stable. Placeholder adapters (`toRecharts`, `toObservablePlot`,
etc.) can bridge the gap in the meantime for users who need charting before
`@pond-ts/charts` ships:

```ts
import { toRecharts } from 'pond-ts/adapters';

const data = toRecharts(series, { x: 'time', y: ['cpu', 'memory'] });
```

---

## 5. Package structure

As live and React layers are added, the package should split into entry points
rather than separate npm packages (at least initially):

```
pond-ts              → core batch library (current)
pond-ts/live         → LiveSeries, subscriptions, retention, live transforms
pond-ts/node         → Node stream adapters (Readable/Writable)
pond-ts/react        → hooks (useLiveSeries, useSnapshot, etc.)
pond-ts/adapters     → bridge adapters for third-party chart libs (interim)
@pond-ts/charts      → first-party chart components (separate package, later)
```

`pond-ts` and its sub-paths are a single npm install. `@pond-ts/charts` is a
separate package with its own release cycle, depending on `pond-ts` and
`pond-ts/react` as peer dependencies.

The `exports` map in `pond-ts`'s `package.json`:

```json
{
  "exports": {
    ".": { "types": "./dist/index.d.ts", "import": "./dist/index.js" },
    "./live": { "types": "./dist/live/index.d.ts", "import": "./dist/live/index.js" },
    "./node": { "types": "./dist/node/index.d.ts", "import": "./dist/node/index.js" },
    "./react": { "types": "./dist/react/index.d.ts", "import": "./dist/react/index.js" },
    "./adapters": { "types": "./dist/adapters/index.d.ts", "import": "./dist/adapters/index.js" }
  }
}
```

---

## 6. Recommended sequencing

| Phase | Scope | Depends on |
|-------|-------|------------|
| **0** | **Performance**: O(N²) → O(N) for aggregate/rolling/smooth, internal constructor path, bisect fixes, allocation reduction (see [AUDIT.md](AUDIT.md)) | — |
| **1** | Edge-case hardening, `toJSON`, custom reducers | Phase 0 |
| **2** | `LiveSeries` basics: push, retention, `toTimeSeries` | Phase 1 |
| **3** | `groupBy`, `resample`, `diff`/`rate`, `fill` | Phase 0 (parallel with 2) |
| **4** | `LiveSeries` subscriptions, ordering policies | Phase 2 |
| **5** | Stateless derived views: `filter`, `map`, `select` on `LiveSeries` | Phase 4 |
| **6** | Stateful live transforms: `LiveAggregation`, `LiveRolling`, `LiveSmooth` | Phase 5 |
| **7** | React hooks: `useLiveSeries`, `useSnapshot`, `useTimeSeries` | Phase 6 |
| **8** | React composition: `useWindow`, `useDerived`, full pipeline example | Phase 7 |
| **9** | Node stream adapters (`.toReadable()` on views and aggregations) | Phase 6 |
| **10** | Bridge adapters (`toRecharts`, etc.), `pivot`/`unpivot`, per-column alignment | Phase 3 |
| **11** | `@pond-ts/charts` — first-party chart components | Phase 8 |

**Phase 0 is the priority.** The O(N²) methods are the operations users call on
their largest series, and the re-validation overhead is a multiplier on every
transform. Fixing these before adding new surface area means all new features
benefit from the faster core.

Phases 1 and 3 can run in parallel with the live layer work. Stateless views
(phase 5) are simple and unblock everything else — they should land before
stateful transforms. The React hooks depend on the live composition model being
stable. Bridge adapters can land at any point to cover charting needs before
`@pond-ts/charts`. The first-party chart package comes last — it should be
designed after the live + React layers are proven.

---

## 7. Design principles to preserve

These should hold across all new work:

- **`TimeSeries` stays immutable.** Live mutation belongs in `LiveSeries`.
- **Schema types flow through every operation.** New methods must produce typed output schemas. If a method can't be typed, it shouldn't ship.
- **Half-open `[begin, end)` bucketing.** All sequence-based operations use this convention.
- **Alignment is separate from aggregation.** `resample` composes them; it doesn't merge them.
- **Stateless transforms are views, stateful transforms own buffers.** If an operation needs memory between events, it gets its own object. If it doesn't, it's a lazy lens over the source. This is the fundamental composition rule for the live layer.
- **Data is the clock.** Bucket close, watermark advance, and window eviction are all driven by event timestamps, not wall-clock timers. The library never schedules its own work.
- **No background timers or implicit scheduling.** The caller owns the event loop. The library is a data structure, not a framework.
- **Browser-safe by default.** Node-specific APIs go behind a separate entry point.
