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

<details>
<summary>Full benchmark results (54 operations, 3 data sizes)</summary>

```
Operation                       N      pondjs (ms)  pond-ts (ms) Speedup
new TimeSeries()                1000   0.94         0.23         4.1x
new TimeSeries()                4000   2.91         0.53         5.5x
new TimeSeries()                16000  14.36        2.17         6.6x
aggregate(10s, avg)             1000   1.67         0.19         8.9x
aggregate(1m, sum)              1000   0.91         0.08         11.0x
aggregate(10s, avg+max+min)     1000   1.86         0.20         9.1x
aggregate(10s, avg)             4000   4.89         0.20         24.9x
aggregate(1m, sum)              4000   3.86         0.12         31.2x
aggregate(10s, avg+max+min)     4000   7.09         0.34         21.2x
aggregate(10s, avg)             16000  20.35        0.83         24.7x
aggregate(1m, sum)              16000  15.39        0.49         31.5x
aggregate(10s, avg+max+min)     16000  32.41        1.33         24.5x
rate(value)                     1000   1.06         0.27         4.0x
rate(value)                     4000   5.26         0.35         15.1x
rate(value)                     16000  23.81        1.35         17.7x
fill(hold/pad)                  1000   0.67         0.29         2.3x
fill(zero)                      1000   0.66         0.30         2.2x
fill(linear)                    1000   0.65         0.31         2.1x
fill(hold/pad)                  4000   3.02         0.81         3.7x
fill(zero)                      4000   3.05         0.29         10.4x
fill(linear)                    4000   3.09         0.29         10.5x
fill(hold/pad)                  16000  12.55        1.20         10.5x
fill(zero)                      16000  12.27        1.19         10.3x
fill(linear)                    16000  12.66        1.18         10.7x
select(value)                   1000   0.75         0.14         5.3x
map(x*2)                        1000   0.73         0.44         1.6x
collapse(a+b+c, sum)            1000   1.17         0.27         4.3x
rename(value→measurement)       1000   0.82         0.20         4.2x
select(value)                   4000   3.68         0.26         14.2x
map(x*2)                        4000   3.43         1.02         3.4x
collapse(a+b+c, sum)            4000   5.09         0.83         6.1x
rename(value→measurement)       4000   4.28         0.45         9.6x
select(value)                   16000  16.07        1.01         15.9x
map(x*2)                        16000  15.39        4.79         3.2x
collapse(a+b+c, sum)            16000  23.40        3.63         6.4x
rename(value→measurement)       16000  21.43        1.67         12.8x
align(5s, linear)               1000   1.10         0.13         8.8x
align(5s, linear)               4000   4.19         0.17         24.7x
align(10s, linear)              16000  14.13        0.44         32.3x
at(i).get() full scan           1000   0.23         0.08         2.9x
at(i).get() full scan           4000   0.59         0.17         3.5x
at(i).get() full scan           16000  2.55         0.11         22.9x
toJSON()                        1000   0.41         0.14         3.0x
toJSON()                        4000   1.27         0.28         4.5x
toJSON()                        16000  5.70         1.31         4.4x
map → select                    1000   1.52         0.35         4.3x
map → select                    4000   7.27         1.23         5.9x
map → select                    16000  32.25        6.31         5.1x
median(value)                   1000   0.29         0.07         4.3x
stdev(value)                    1000   0.23         0.07         3.5x
median(value)                   4000   1.65         0.25         6.5x
stdev(value)                    4000   1.07         0.14         7.9x
median(value)                   16000  9.70         1.31         7.4x
stdev(value)                    16000  5.22         0.57         9.1x
```

Run locally: `npm run build && node bench/vs-pondjs.cjs`

</details>

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
