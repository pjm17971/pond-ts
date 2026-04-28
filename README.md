# pond-ts - A modern typescript timeseries library

TypeScript-first time series primitives built around typed events, typed schemas, and explicit temporal keys.

**pond-ts** is the successor to [pondjs](https://github.com/esnet/pond), rewritten from scratch in TypeScript with a focus on performance, type safety, and composable live streaming.

- typed `TimeSeries` construction and immutable `Event` objects
- `Time`, `TimeRange`, and `Interval` temporal keys
- alignment, aggregation, joins, rolling windows, and smoothing
- `LiveSeries` with push-based ingestion, retention policies, and subscriptions
- `LiveView`, `LiveAggregation`, and `LiveRollingAggregation` for streaming composition
- timezone-aware calendar sequences and ingest helpers

The package is intended to work in modern Node and frontend projects.

## Performance

pond-ts is **7.6x faster** than pondjs on average across all comparable operations,
with no regressions. The advantage grows with data size.

| Category          | Speedup (N=16k) | Notes                                         |
| ----------------- | --------------- | --------------------------------------------- |
| **Aggregation**   | 25–32x          | O(N+B) bucketing vs O(N×B) Pipeline           |
| **Alignment**     | 32x             | Forward cursor vs repeated binary search      |
| **Rate/diff**     | 18x             | Direct array walk vs Pipeline materialization |
| **Fill**          | 10–11x          | Single-pass vs Pipeline per strategy          |
| **Transforms**    | 3–16x           | Pre-validated constructor skips re-validation |
| **Construction**  | 7x              | Plain objects vs ImmutableJS wrapping         |
| **Statistics**    | 7–9x            | Direct computation vs ImmutableJS iteration   |
| **Serialization** | 4x              | Simpler internal representation               |
| **Event access**  | 23x             | Array indexing vs ImmutableJS `get()`         |

See the [full benchmark results](website/docs/reference/benchmarks.mdx) for detailed numbers.
Run locally: `npm run build && node packages/core/bench/vs-pondjs.cjs`

## Install

```sh
npm install pond-ts
```

## Build

The repo toolchain should work on Node 18, but use `nvm` to verify against newer stable Node releases when needed.

```sh
npm run build
```

## Format

```sh
npm run format
```

## Test

```sh
npm test
```

## Verify

```sh
npm run verify
```

## Docs site

The documentation website lives in [`website/`](./website) and is built with Docusaurus.

## Examples

- **[pond-ts-dashboard](https://github.com/pjm17971/pond-ts-dashboard)** —
  a working React dashboard that streams synthetic per-host CPU/request
  metrics, computes per-host rolling baselines, flags anomalies against
  ±σ bands, and renders everything as live line and bar charts (~600
  lines of TypeScript). The repo's README is also published as a docs-
  site guide: [Building a dashboard with pond-ts](website/docs/how-to-guides/dashboard-guide.mdx).

## License

MIT

## Core model

The key types are:

- `Time`: a point in time
- `TimeRange`: an unlabeled interval
- `Interval`: a labeled interval

An `Event` is a key plus typed data.

A `TimeSeries` is an ordered immutable collection of events sharing one schema.

## Quick start

```ts
import { TimeSeries } from 'pond-ts';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'number' },
  { name: 'host', kind: 'string' },
  { name: 'healthy', kind: 'boolean' },
] as const;

const series = new TimeSeries({
  name: 'cpu',
  schema,
  rows: [
    [new Date('2025-01-01T00:00:00.000Z'), 0.42, 'api-1', true],
    [new Date('2025-01-01T00:01:00.000Z'), 0.51, 'api-2', true],
  ],
});

const event = series.at(1);
if (!event) {
  throw new Error('missing event');
}

event.key();
event.timeRange();
event.get('cpu');
event.data().host;
```

## Worked example

This is the kind of flow `pond-ts` is built for: start with typed events, then derive aligned, aggregated, and smoothed analytical views without mutating the original series.

```ts
import { Sequence, TimeSeries } from 'pond-ts';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'number' },
  { name: 'requests', kind: 'number' },
  { name: 'host', kind: 'string' },
] as const;

const cpu = TimeSeries.fromJSON({
  name: 'cpu',
  schema,
  rows: [
    ['2025-01-01T00:00:00Z', 0.31, 120, 'api-1'],
    ['2025-01-01T00:01:00Z', 0.44, 135, 'api-1'],
    ['2025-01-01T00:02:00Z', 0.52, 141, 'api-1'],
    ['2025-01-01T00:03:00Z', 0.48, 128, 'api-1'],
    ['2025-01-01T00:04:00Z', 0.63, 166, 'api-1'],
  ],
});

const perMinute = cpu.align(Sequence.every('1m'), {
  method: 'hold',
});

const fiveMinute = cpu.aggregate(Sequence.every('5m'), {
  cpu: 'avg',
  requests: 'sum',
  host: 'last',
});

const rolling = cpu.rolling('3m', {
  cpu: 'avg',
  requests: 'sum',
});

const smoothed = cpu.smooth('cpu', 'ema', {
  alpha: 0.35,
  output: 'cpuTrend',
});

console.log(perMinute.first()?.key().asString());
console.log(fiveMinute.first()?.data());
console.log(rolling.last()?.data());
console.log(smoothed.last()?.get('cpuTrend'));
```

From one typed source series, you can derive:

- aligned interval views for dashboards and joins
- bucketed aggregates for reporting
- rolling metrics for short-term behavior
- smoothed trends for visualization or alerting

All of those remain fully typed and immutable.

## JSON ingest

Use `TimeSeries.fromJSON(...)` for external data and ambiguous local timestamps.

```ts
import { TimeSeries } from 'pond-ts';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number' },
  { name: 'status', kind: 'string', required: false },
] as const;

const series = TimeSeries.fromJSON({
  name: 'cpu',
  schema,
  rows: [
    ['2025-01-01T09:00', 0.42, 'ok'],
    ['2025-01-01T10:00', 0.51, null],
  ],
  parse: { timeZone: 'Europe/Madrid' },
});
```

Export back into the same JSON-friendly shape:

```ts
const rows = series.toJSON();
const objectRows = series.toJSON({ rowFormat: 'object' });
```

For normalized in-memory export helpers:

```ts
const normalizedRows = series.toRows();
const normalizedObjects = series.toObjects();
```

## Event and series transforms

Event-level transforms:

- `get(...)`
- `set(...)`
- `merge(...)`
- `select(...)`
- `rename(...)`
- `collapse(...)`
- `asTime(...)`
- `asTimeRange()`
- `asInterval(...)`

Series-level transforms:

- `map(...)`
- `select(...)`
- `rename(...)`
- `collapse(...)`
- `asTime(...)`
- `asTimeRange()`
- `asInterval(...)`

Example:

```ts
const renamed = series.rename({ cpu: 'usage' });
const selected = renamed.select('usage', 'healthy');
```

## Temporal selection

`TimeSeries` includes both positional and temporal selection methods:

- `slice(...)`
- `filter(...)`
- `find(...)`
- `first()`
- `last()`
- `before(...)`
- `after(...)`
- `within(...)`
- `overlapping(...)`
- `containedBy(...)`
- `trim(...)`
- `includesKey(...)`
- `bisect(...)`
- `atOrBefore(...)`
- `atOrAfter(...)`

Vocabulary is intentionally distinct:

- `within(...)`: fully contained
- `overlapping(...)`: intersects without clipping
- `trim(...)`: intersects and clips event extents

## Sequences

Use `Sequence` for unbounded grids and `BoundedSequence` for explicit finite interval lists.

Fixed-step sequences:

```ts
import { Sequence } from 'pond-ts';

const minuteGrid = Sequence.every('1m');
const hourlyGrid = Sequence.hourly();
```

Calendar-aware sequences:

```ts
const localDays = Sequence.calendar('day', {
  timeZone: 'America/New_York',
});
```

Explicit bounded sequences:

```ts
import { BoundedSequence, Interval } from 'pond-ts';

const buckets = new BoundedSequence([
  new Interval({ value: 'a', start: 0, end: 10 }),
  new Interval({ value: 'b', start: 20, end: 30 }),
]);
```

## Alignment and aggregation

Align onto a sequence:

```ts
const aligned = series.align(Sequence.every('1m'), {
  method: 'hold',
});
```

Aggregate into buckets:

```ts
const aggregated = series.aggregate(Sequence.every('5m'), {
  cpu: 'avg',
  host: 'last',
});
```

Built-in aggregations:

- `sum`
- `avg`
- `min`
- `max`
- `count`
- `first`
- `last`

## Joins

Join two aligned or bucketed series:

```ts
const joined = left.join(right, { type: 'outer' });
```

Supported join types:

- `outer`
- `left`
- `right`
- `inner`

Join many:

```ts
const wide = TimeSeries.joinMany([cpu, memory, errors], {
  type: 'outer',
});
```

Conflict handling:

- default: `onConflict: "error"`
- optional prefixing:

```ts
const joined = left.join(right, {
  onConflict: 'prefix',
  prefixes: ['left', 'right'] as const,
});
```

## Rolling windows

Event-driven rolling:

```ts
const rolled = series.rolling('5m', {
  cpu: 'avg',
  host: 'last',
});
```

Sequence-driven rolling:

```ts
const rolledOnGrid = series.rolling(Sequence.every('1m'), '5m', { cpu: 'avg' });
```

Rolling alignment options:

- `trailing`
- `centered`
- `leading`

## Smoothing

Smoothing targets one numeric column at a time.

Replace the source column:

```ts
const smoothed = series.smooth('cpu', 'ema', { alpha: 0.2 });
```

Append the smoothed output:

```ts
const smoothed = series.smooth('cpu', 'movingAverage', {
  window: '5m',
  alignment: 'centered',
  output: 'cpuAvg',
});
```

Supported smoothing methods:

- `ema`
- `movingAverage`
- `loess`

For interval-like keys, smoothing uses the key midpoint as the internal anchor.

## Calendar-aware helpers

Primitive helpers normalize local calendar inputs into absolute time:

```ts
import { Interval, Time, TimeRange } from 'pond-ts';

const time = Time.parse('2025-01-01T09:00', { timeZone: 'Europe/Madrid' });
const day = TimeRange.fromDate('2025-01-01', { timeZone: 'UTC' });
const month = Interval.fromCalendar('month', '2025-01', {
  timeZone: 'UTC',
  value: '2025-01',
});
```

## Current scope

The library provides both batch analytics (`TimeSeries`) and live streaming
(`LiveSeries`, `LiveView`, `LiveAggregation`, `LiveRollingAggregation`).

- type-safe construction with schema types that flow through every operation
- temporal modeling with `Time`, `TimeRange`, and `Interval` keys
- composable batch analytics (aggregate, align, join, rolling, smooth, fill, diff, rate, groupBy)
- push-based live ingestion with retention policies and subscriptions
- live composition: filter, map, select, window, diff, rate, fill, cumulative, aggregate, rolling

## License

MIT
