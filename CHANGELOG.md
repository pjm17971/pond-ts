# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
`pond-ts` and `@pond-ts/react` release together under a single `v*` tag, so this
file covers both packages. Pre-1.0: minor bumps may include new features and
type-level changes; patch bumps are strictly additive.

[Unreleased]: https://github.com/pjm17971/pond-ts/compare/v0.8.2...HEAD

## [Unreleased]

## [0.8.2] — 2026-04-26

Strictly additive over v0.8.1. Closes friction surfaced by two
independent agent runs against a realistic CSV-cleaning task —
specifically, the missing fan-in primitive that forces callers out
of the typed contract when reassembling per-host transformed
subseries.

### Added

- **`TimeSeries.merge([s1, s2, ...])`** — concatenates the events of
  N same-schema `TimeSeries` instances, re-sorted by key. The
  row-append / vertical-stack counterpart to `joinMany` (which
  column-merges by key). Closes the round-trip after
  `groupBy(col, fn)` + per-group transforms without forcing callers
  to unwrap events back to row tuples.

  ```ts
  const filledByHost = series.groupBy('host', (g) =>
    g.fill({ cpu: 'linear' }, { limit: 2 }),
  );
  const merged = TimeSeries.merge([...filledByHost.values()]);
  // back to one TimeSeries<S>; events from all hosts re-sorted.
  ```

  Schemas must match column-by-column on `name` and `kind`; throws
  upfront on mismatch. Same-key events from different inputs are
  both kept (row-append, not key-dedupe).

- **`TimeSeries.fromEvents(events, { schema, name })`** — builds a
  typed series from a flat `Event[]` array. Sorts by key. Companion
  to `merge` for the case where you have raw events rather than a
  list of series.

- **`TimeRange.toJSON()`** returns `{ start: number, end: number }`,
  the same shape `JsonTimeRangeInput` accepts, so
  `new TimeRange(range.toJSON())` round-trips. Implicitly invoked by
  `JSON.stringify(range)`.

- **`TimeRange.toString()`** returns ISO-8601 `start/end` format
  (e.g. `2025-01-15T09:00:00.000Z/2025-01-15T10:00:00.000Z`) for
  debug logs and human-readable display.

### Known limitation

Two type-level fixes flagged by the agents are tracked but deferred
to a future variance refactor:

- `toJSON()` returns `TimeSeriesJsonInput<SeriesSchema>` (loose),
  not `TimeSeriesJsonInput<S>`. Cast the result at the call site
  if you need the narrow schema preserved.
- `RowForSchema` doesn't honor `required: false`. Use `fromJSON`
  with `null` cells instead of the row-array constructor with
  `undefined`.

Both are real but blocked by class-wide invariance through method
overloads. See PLAN.md "Known type-level limitation" for the full
story.

## [0.8.1] — 2026-04-26

Strictly additive over v0.8.0 — typed overload narrows result types when
opted in via `groups`; untyped form is unchanged. Plus a docs reorg.

### Added

- **`pivotByGroup` typed overload** — pass `{ groups: [...] as const }`
  and the output schema becomes literal-typed, so downstream
  `baseline` / `rolling` / `toPoints` calls narrow without `as never`
  casts. Eliminates the dashboard friction reported on v0.8.0.

  ```ts
  const HOSTS = ['api-1', 'api-2'] as const;
  const wide = long.pivotByGroup('host', 'cpu', { groups: HOSTS });
  // wide.schema is now literal-typed:
  //   [time, { name: 'api-1_cpu', kind: 'number', required: false },
  //          { name: 'api-2_cpu', kind: 'number', required: false }]
  wide.baseline('api-1_cpu', { window: '1m', sigma: 2 }); // no cast
  ```

  Behavior in the typed path: declaration order (not alphabetical),
  declared-but-empty groups still emit columns, runtime values not
  in the declared set throw upfront. Untyped form (no `groups`)
  keeps existing alphabetical / dynamic-discovery / loose-output
  behavior.

### Changed

- **Docs site reorganized.** `Transforms` → **TimeSeries**;
  `Live` → **LiveSeries**; new **Advanced** section for charting and
  array columns. Concepts moves to `Start here`. New **Reshaping**
  page splits `pivotByGroup` / `groupBy` / `join` / `joinMany` from
  Aggregation, plus a new **Queries** page covering `at` / `first` /
  `timeRange` / `includesKey` / `intersection` / iterators / output
  forms — everything that interrogates a series rather than
  transforming it. JSON ingest renamed to **Ingest** and slotted as
  the first page under TimeSeries.

## [0.8.0] — 2026-04-25

### Added

- **`TimeSeries.pivotByGroup(groupCol, valueCol, options?)`** — long-to-wide
  reshape on a categorical column. Each distinct value of `groupCol` becomes
  its own column in the output schema named `${group}_${value}`, holding the
  value column at that timestamp. Rows sharing a timestamp collapse into one
  output row; missing `(timestamp, group)` cells are `undefined`.

  ```ts
  // Long: { ts, cpu, host } per row
  // Wide: { ts, "api-1_cpu", "api-2_cpu", ... } per row
  long.pivotByGroup('host', 'cpu').toPoints();
  // Drops straight into <Line dataKey="api-1_cpu" /> etc.
  ```

  Duplicate `(timestamp, group)` pairs throw by default; opt-in
  `{ aggregate: 'avg' | 'sum' | 'first' | 'last' | 'min' | 'max' | 'median' | 'p95' | ... }`
  to combine. The aggregator's output kind must match the value column's
  kind — `count`, `unique`, `topN` and other kind-changing reducers are
  rejected upfront with a clear error. Output schema is dynamic so the
  return type is `TimeSeries<SeriesSchema>` (loosely typed). Time-keyed
  input required.

  Use `pivotByGroup` for the per-group dashboard case ("one source, many
  producers, one chart line per producer"). Use `groupBy + joinMany` when
  each group spawns multiple derived columns (e.g. per-host baseline →
  cpu/avg/upper/lower per host). At 200k events × 100 groups, runs in
  ~43 ms — at parity with hand-rolled JS that skips `TimeSeries`
  construction entirely.

### Changed

- Charting docs lead with `series.join(other, ...).toPoints()` for
  cross-source overlays. The manual `mergeWideRows` recipe is demoted to
  "non-`TimeSeries` inputs". A new "Per-group wide rows" section covers
  `pivotByGroup` end-to-end with Recharts.

### Notes

- **Live counterpart deferred.** No `LiveSeries.pivotByGroup` /
  `LiveSeries.merge` / `LiveSeries.join` yet — see PLAN.md "Known scope
  gap: live merge / join". Snapshot-then-batch is the workaround:
  `useSnapshot` per source + `useMemo` running a batch `pivotByGroup` or
  `join`.

## [0.7.0] — 2026-04-25

### Changed (breaking)

- **`TimeSeries.toPoints()` returns wide rows** instead of single-column
  `{ ts, value }[]`. Every event becomes one row with `ts` plus every
  value column from the schema as a top-level key:

  ```ts
  // Before:                       // After:
  series.toPoints('cpu');
  series.toPoints();
  // [{ ts, value }, ...]          // [{ ts, cpu, host, ... }, ...]
  ```

  This aligns pond-ts's multi-column nature with what every chart
  library actually wants (Recharts, Observable Plot, visx all consume
  wide rows directly). Band charts, multi-series overlays, and
  `<Area>` ranged-`dataKey` patterns become a single `toPoints()`
  call instead of a manual merge.

  **Migration:** for the common single-column case, compose with
  `select`:

  ```ts
  const cpuPoints = series.select('cpu').toPoints();
  // [{ ts, cpu }, ...]
  ```

  Then read the column by name (`row.cpu`) instead of the old
  `.value`. Wide form keeps every event — the old narrow form
  dropped events whose column was `undefined`; the new form preserves
  them so chart libraries can render gaps via `connectNulls={false}`.

  **Watch out for `value`-named columns.** If your schema has a value
  column literally named `value`, the new wide rows will have a
  `value` key that looks identical to the old narrow shape — but it's
  the column-named-`value`, not the narrow-form `value`. Audit any
  `row.value` reads after upgrading; the safe migration is
  `row.<schema-column-name>`.

- **`TimeSeries.fromPoints()` accepts wide-row points** with a schema
  of any number of value columns. Schema's first column must still be
  `kind: 'time'`.

  ```ts
  TimeSeries.fromPoints(
    [{ ts: 0, cpu: 0.3, host: 'api-1' }, ...],
    {
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'cpu', kind: 'number' },
        { name: 'host', kind: 'string' },
      ] as const,
    },
  );
  ```

  Previously restricted to exactly two columns with `{ ts, value }`
  rows; that form is gone.

## [0.6.0] — 2026-04-25

### Added

- **`'end'` sample option** for `align()` and `Sequence.bounded()`. Joins
  `'begin'` and `'center'` as a third anchor inside each grid step.
  Useful for end-of-period readings (close-of-day, last value before
  bucket close). Inclusion semantics are left-exclusive
  (`sample ∈ (range.begin, range.end]`) so an end-sample at exactly
  `range.begin()` doesn't pull in an interval that sits entirely
  before the range.

### Type-surface change

- `AlignSample` and `SequenceSample` literal unions widen from
  `'begin' | 'center'` to `'begin' | 'center' | 'end'`. Pattern-matching
  consumers that exhaustively `switch` on the old two-value union
  silently miss the new arm — minor bump rather than a patch per this
  project's "patch bumps are strictly additive" rule. Update any
  `switch (sample)` blocks to handle `'end'` (or add a `default`).

## [0.5.11] — 2026-04-24

### Fixed

- **`LiveSeries` rejects `graceWindow > retention.maxAge` at construction.**
  A late event accepted within grace but older than `maxAge` would be evicted
  immediately by retention — the grace contract would be meaningless. The
  guard only fires when both options are set explicitly; default behavior is
  unchanged. `LiveAggregation` bucket closure (which inherits grace from the
  source) still behaves as before.

### Changed

- Docs: clarified `graceWindow`'s scope in the `LiveSeriesOptions`
  docstring. Enforced at ingest and honored by `LiveAggregation` bucket
  closure; `rolling()` / `window()` live views do not re-flow late events
  through historical windows. Matches the actual pipeline behavior; full
  late-event propagation through live transforms is explicitly out of
  scope (see Akidau's Streaming 102 for the larger story).

## [0.5.10] — 2026-04-24

### Fixed

- **`baseline()` emits `undefined` for `upper` / `lower` when the rolling
  window is flat (`sd === 0`)** — matching `outliers()`'s behavior. Before,
  a zero-width band would cause a naive `value > upper || value < lower`
  filter to flag every non-equal point as anomalous inside a constant run.
  The `avg` and `sd` columns still report their true values; only the band
  edges collapse to `undefined`.

### Changed

- Internal: consolidated a duplicate `OptionalNumberCol` type alias into
  the pre-existing `OptionalNumberColumn`. No surface change.
- Docs: walked back an over-claim in `outliers()`'s docstring. It was
  documented as "sugar over `baseline().filter()`" but is implemented
  independently. Now says the two are conceptually equivalent.

## [0.5.9] — 2026-04-23

### Added

- **`TimeSeries.baseline(col, opts)`** — rolling-stats primitive. Runs one
  rolling pass and appends four optional number columns (`avg`, `sd`,
  `upper = avg + σ·sd`, `lower = avg - σ·sd`) to the source schema. Band
  charts read `toPoints('upper')` / `toPoints('lower')` directly; outlier
  filters compare against `upper` / `lower`. Replaces the band-plus-outliers
  two-pass pattern with one call. Custom column names via `{ names }` if the
  defaults collide.

[0.8.2]: https://github.com/pjm17971/pond-ts/compare/v0.8.1...v0.8.2
[0.8.1]: https://github.com/pjm17971/pond-ts/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/pjm17971/pond-ts/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/pjm17971/pond-ts/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/pjm17971/pond-ts/compare/v0.5.11...v0.6.0
[0.5.11]: https://github.com/pjm17971/pond-ts/compare/v0.5.10...v0.5.11
[0.5.10]: https://github.com/pjm17971/pond-ts/compare/v0.5.9...v0.5.10
[0.5.9]: https://github.com/pjm17971/pond-ts/compare/v0.5.8...v0.5.9

## [0.5.8] — 2026-04-23

### Added

- **`TimeSeries.outliers(col, { window, sigma, alignment? })`** —
  rolling-baseline outlier detection. Returns `TimeSeries<S>` filtered to
  events whose value deviates from the trailing rolling average by more than
  `sigma · rolling_stdev`. Composes directly with aggregate, groupBy, etc.
- **`TimeSeries.prototype.toPoints(col)`** — flat `{ ts, value }[]` export
  matching conventional chart-library shape (Recharts, Observable Plot, d3).
  Filters `undefined` values; returns a frozen array.
- **`TimeSeries.fromPoints(points, { schema, name? })`** — inverse
  constructor for round-tripping chart-style points back into pond-native
  operations. Schema must have exactly two columns.

[0.5.8]: https://github.com/pjm17971/pond-ts/compare/v0.5.7...v0.5.8

## [0.5.7] — 2026-04-23

### Added

- **`smooth('ema', { warmup: N })`** — drops the first `N` output rows so
  callers don't have to write `.slice(N)` after every EMA call. The smoother
  still processes those events, so kept rows are computed against a warm EMA.
  `warmup: 0` is a no-op; warmup ≥ series length returns an empty series.

[0.5.7]: https://github.com/pjm17971/pond-ts/compare/v0.5.6...v0.5.7

## [0.5.6] — 2026-04-23

### Added

- **`useCurrent` reference stability** — the returned record and each of its
  fields are reference-stable across renders when structurally unchanged. A
  no-op push (same aggregate values) hands back the previous references;
  downstream `useMemo([current.host], ...)` only re-runs when that specific
  field changes. Scalar fields compare via `===`; array fields compare length
  then elementwise.

[0.5.6]: https://github.com/pjm17971/pond-ts/compare/v0.5.5...v0.5.6

## [0.5.5] — 2026-04-23

### Added

- **Narrow return types for `rolling` + `aggregate` output-map overloads.**
  `rolling(w, { avg: { from: 'cpu', using: 'avg' }, ... })` now returns
  `TimeSeries<RollingOutputMapSchema<S, M>>` — `e.get('avg')` narrows to
  `number | undefined` instead of `ColumnValue | undefined`, and `e.key()`
  preserves the source's first-column kind. Same fix on `aggregate`'s
  output-map overload.

### Fixed

- `min` / `max` were missing from the numeric-reducer list in `ReduceResult`
  (v0.5.2 regression). Both reducers have `outputKind: 'number'` at runtime;
  the type now agrees. `reduce({ cpu: 'max' })` narrows to `number | undefined`.

[0.5.5]: https://github.com/pjm17971/pond-ts/compare/v0.5.4...v0.5.5

## [0.5.4] — 2026-04-23

### Added

- **`rolling` accepts `AggregateOutputMap`** — feature parity with
  `aggregate`. Multi-reducer-per-column now works in one pass:
  ```ts
  series.rolling('1m', {
    avg: { from: 'cpu', using: 'avg' },
    sd: { from: 'cpu', using: 'stdev' },
  });
  ```
  Two new overloads on both window-only and sequence-driven forms.

### Changed

- `rolling`'s internal column walker now routes through the shared
  `normalizeAggregateColumns` helper. Schema-column order is preserved for
  `AggregateMap` inputs so the runtime layout continues to match
  `RollingSchema<S, M>`.

[0.5.4]: https://github.com/pjm17971/pond-ts/compare/v0.5.3...v0.5.4

## [0.5.3] — 2026-04-23

### Added

- **Source-kind narrowing on array-output reducers in `ReduceResult`.**
  `unique` and `` `top${number}` `` now narrow their output to
  `ReadonlyArray<T>` where `T` is the source column's element type:
  ```ts
  series.reduce({ host: 'unique' }).host;
  //    ^ ReadonlyArray<string> | undefined (was ReadonlyArray<ScalarValue>)
  ```
  Array-kind source columns fall back to the wide `ReadonlyArray<ScalarValue>`
  union since element kind isn't schema-visible.

[0.5.3]: https://github.com/pjm17971/pond-ts/compare/v0.5.2...v0.5.3

## [0.5.2] — 2026-04-23

### Added

- **`TimeSeries.reduce` per-entry type narrowing.** Numeric reducers
  (`sum`/`avg`/`count`/`median`/`stdev`/`difference`/`pNN`) narrow to
  `number | undefined`; `unique`/`top${N}` narrow to `ReadonlyArray<…> |
undefined`; `first`/`last`/`keep` preserve the source column kind. Custom
  reducer functions and `AggregateOutputSpec` entries keep the wide
  `ColumnValue | undefined` fallback. Narrowing lives in the new
  `types-reduce.ts` — same file-split pattern used later for the output-map
  narrowing.

### Changed

- `useCurrent` now aliases `ReduceResult<S, Mapping>` directly; the hook's
  duplicated narrowing logic is gone.

[0.5.2]: https://github.com/pjm17971/pond-ts/compare/v0.5.1...v0.5.2

## [0.5.1] — 2026-04-23

### Added

- **`TimeSeries.tail(duration?)`** — trailing temporal slice, the
  counterpart to `Array.slice(-n)`. Called with no argument, returns the
  whole series. Composes with every other `TimeSeries` method.
- **`useCurrent` hook (`@pond-ts/react`)** — subscribes to a live source and
  returns the current value of a reducer mapping. Signature:
  `useCurrent(source, mapping, { tail?, throttle? })`. Stable-shape record
  even while the source is empty, so destructuring on first render is safe.

[0.5.1]: https://github.com/pjm17971/pond-ts/compare/v0.5.0...v0.5.1

## [0.5.0] — 2026-04-23

### Added

- **First-class `'array'` column kind.** New `ArrayValue = ReadonlyArray<ScalarValue>`
  and `ColumnValue = ScalarValue | ArrayValue` types. Array columns are inert
  with respect to numerical operators (`diff`, `rate`, `cumulative`,
  `rolling`-over-numbers skip them automatically via `NumericColumnNameForSchema`).
- **`unique` reducer** — distinct sorted values; works in `reduce`,
  `aggregate`, and `rolling`. Flattens array-kind sources one level (set union
  across arrays in a bucket).
- **`top(n)` reducer** — top N values by frequency with deterministic
  tie-break. String-pattern dispatch (`'top3'`, `'top10'`) parallel to `pNN`,
  plus a `top(n)` helper that returns the typed string literal. Incremental
  bucket + rolling state via a count map. Also flattens array-kind sources.
- **Five array-prefixed operators on `TimeSeries`**:
  - `arrayContains(col, value)` — has this one
  - `arrayContainsAll(col, values)` — has every one (AND / subset)
  - `arrayContainsAny(col, values)` — has at least one (OR / intersection)
  - `arrayAggregate(col, reducer, { as?, kind? })` — per-event reduction
    reusing the full reducer registry (count, sum, avg, unique, custom, etc.).
    Replace in place or append via `as`.
  - `arrayExplode(col, { as?, kind? })` — fan each event out into one event
    per array element. Replace the array column or keep it alongside a scalar
    sibling.
- **LiveSeries accepts `kind: 'array'`** on its schema with array cells
  frozen on push.
- **JSON round-trip** for array cells works unchanged (toJSON / fromJSON
  pass arrays through naturally).
- **Docs**: new `guides/arrays.mdx` reference page;
  `examples/error-rate-dashboard.mdx` scenario walkthrough backed 1:1 by an
  E2E test; `reducer-reference.mdx` expanded with concrete input/output
  examples for `unique` and `top(n)`.

### Changed

- **`reduce()` / `ReduceResult` / `CustomAggregateReducer` return types** widened
  from `ScalarValue | undefined` to `ColumnValue | undefined`. Narrowed
  annotations (`: number | undefined`) keep working; only callers with
  explicit `: ScalarValue | undefined` annotations need to widen.
  (v0.5.2 narrows these further per-entry.)

[0.5.0]: https://github.com/pjm17971/pond-ts/compare/v0.4.3...v0.5.0

## [0.4.3] — 2026-04-22

### Added

- `useLiveQuery` and `useLatest` hooks in `@pond-ts/react`.

### Fixed

- LiveView eviction mirroring (uses `EMITS_EVICT` symbol to safely detect
  evict-capable sources; avoids duck-typing that broke on `LiveAggregation`).
- Type narrowing through `LiveAggregation` / `LiveRollingAggregation` via
  `Out` type parameter.
- `Time.toDate()` convenience method.
- `useWindow` under React StrictMode (view creation moved to `useEffect`).
- `TimeSeries[Symbol.iterator]` and `toArray()` for ergonomic iteration.
- `useSnapshot` accepts `SnapshotSource<S>` structural type (no casts for
  `LiveAggregation` input).

[0.4.3]: https://github.com/pjm17971/pond-ts/compare/v0.4.2...v0.4.3

## [0.4.2] — 2026-04-21

### Changed

- First release using npm OIDC Trusted Publisher (no stored tokens).

[0.4.2]: https://github.com/pjm17971/pond-ts/compare/v0.4.1...v0.4.2

## [0.4.1] — 2026-04-21

Administrative — no behavioral changes.

[0.4.1]: https://github.com/pjm17971/pond-ts/compare/v0.4.0...v0.4.1

## [0.4.0] — 2026-04-21

### Added

- **`@pond-ts/react` package** — React hooks for live series
  (`useLiveSeries`, `useTimeSeries`, `useSnapshot`, `useWindow`, `useDerived`,
  `takeSnapshot`). Monorepo restructure completed.
- **LiveView + LiveSource composition** — `filter`, `map`, `select`,
  `window` views that compose with `LiveAggregation` / `LiveRollingAggregation`
  via a shared `LiveSource<S>` interface.
- **Live per-event and carry-forward transforms** — `diff`, `rate`,
  `pctChange`, `fill`, `cumulative` available as LiveView variants.
- **Grace period on `LiveAggregation`** — delays bucket closing so
  out-of-order events within a window accumulate into their correct bucket.
  Defaults from source `LiveSeries`'s `graceWindow`.
- **Streaming dashboard example** with E2E tests.
- **Benchmark suite** comparing `pond-ts` vs `pondjs`.

[0.4.0]: https://github.com/pjm17971/pond-ts/compare/v0.3.0...v0.4.0

## [0.3.0] — 2026-04-21

### Added

- **`LiveSeries`** — mutable, append-optimized streaming buffer sharing the
  same schema type as `TimeSeries`. Retention policies (`maxEvents`,
  `maxAge`, `maxBytes`). Synchronous subscriptions (`event`, `batch`,
  `evict`). Ordering modes (`strict`, `drop`, `reorder`).
- **`LiveAggregation`** — incremental bucketed aggregation over a
  `LiveSource`.
- **`LiveRollingAggregation`** — sliding-window reduction over a
  `LiveSource`.

[0.3.0]: https://github.com/pjm17971/pond-ts/compare/v0.2.0...v0.3.0

## [0.2.0] — 2026-04-16

### Added

- **Phase 2 batch expansion**: `reduce`, `groupBy`, `diff`, `rate`, `fill`.
- **Phase 2.5 columnar primitives**: `pctChange`, `cumulative`, `shift`,
  `bfill` fill strategy.
- **Aggregator parity with pondjs**: `median`, `stdev`, `percentile`
  (`pNN`), `difference`, `keep`.

[0.2.0]: https://github.com/pjm17971/pond-ts/compare/v0.1.4...v0.2.0

## [0.1.x] — 2026-04-16

Phase 0 (core performance) and Phase 1 (batch hardening) releases. Five
critical O(N²) hot paths optimized (172× aggregate, 182× rolling, 15×
movingAverage, 7.5× loess, 819× includesKey, 134× alignLinearAt).
`toJSON`/`fromJSON` round-trip, custom aggregate reducers, edge-case
coverage across every analytical primitive.

See [tag history](https://github.com/pjm17971/pond-ts/tags) for details.
