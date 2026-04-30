# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
`pond-ts` and `@pond-ts/react` release together under a single `v*` tag, so this
file covers both packages. Pre-1.0: minor bumps may include new features and
type-level changes; patch bumps are strictly additive.

[Unreleased]: https://github.com/pjm17971/pond-ts/compare/v0.11.7...HEAD

## [Unreleased]

### Added

- **`rolling.sample(sequence)`** on `LiveRollingAggregation` — taps a
  rolling aggregation and emits one snapshot of the rolling state each
  time a source event crosses an epoch-aligned boundary of `sequence`.
  Closes the frontend-telemetry gap: collect high-frequency timing
  events, sample p50/p95 to a backend every 30 s, while the same
  rolling drives an in-app live display (no duplicated deque).

  ```ts
  const rolling = timings.rolling('1m', {
    p50: { from: 'latency', using: 'p50' },
    p95: { from: 'latency', using: 'p95' },
  });

  // One sampler → backend report every 30 s of event time
  const reported = rolling.sample(Sequence.every('30s'));
  reported.on('event', (e) =>
    fetch('/api/telemetry', { method: 'POST', body: JSON.stringify(e.data()) }),
  );

  // Same rolling drives the UI live display
  useLiveQuery(timings, () => rolling.value());
  ```

  Emission is **data-driven**: no `setInterval`. If the source goes
  quiet, no events fire. A single source event spanning multiple
  boundaries fires exactly one event at the new bucket. Snapshot is
  taken after the boundary-crossing event is ingested by the rolling,
  so the emitted value includes that event's contribution.

  **Independent lifetimes.** `sample.dispose()` only detaches the
  sampler from the rolling; the rolling's lifecycle stays the user's
  responsibility. One rolling can power multiple `.sample()` cadences
  plus direct `rolling.value()` reads without coupling.

- **`LiveSequenceRollingAggregation` exported** from package root with
  full `LiveSource<Out>` surface and the same view-transform set as
  `LiveRollingAggregation` (`filter`, `map`, `select`, `window`,
  `diff`, `rate`, `pctChange`, `fill`, `cumulative`, `rolling`,
  `aggregate`).

- **Telemetry-reporting recipe** at
  `website/docs/recipes/telemetry-reporting.mdx` — end-to-end
  frontend-collection → backend-summary pattern using `.sample()`,
  plus the React in-app display via `useLiveQuery`.

## [0.11.7] — 2026-04-29

### Added

- **`LiveView.count()` and `LiveView.eventRate()` terminal accessors.**
  Read the current event count and events-per-second over a windowed
  view directly — closes the
  `useCurrent(live, { cpu: 'count' }, { tail: '1m' }).cpu / 60`
  boilerplate surfaced by the gRPC experiment.
  ```ts
  const eventsPerSec = live.window('1m').eventRate(); // events/sec
  const eventsInWindow = live.window('1m').count();
  ```
  `eventRate()` requires a time-based window (`window('1m')`) and
  throws on count-based windows (`window(100)`) — there's no
  denominator to use. Distinct from `LiveView.rate(columns)`,
  which is the per-column derivative operator (rate-of-change of
  values).
- `LiveView.{filter,map,select}` now propagate the parent's window
  duration to the child view, so chains like
  `live.window('1m').filter(...).eventRate()` work as expected.
- `@pond-ts/react` ships **`useEventRate(source, '1m')`** — a
  reactive hook returning the events-per-second number, throttled
  on `'event'` like `useSnapshot`. Hooks mounted on already-
  populated sources render the actual rate on first paint via
  lazy `useState` init.
  ```tsx
  const eventsPerSec = useEventRate(liveSeries, '1m');
  // <div>EVENT RATE {eventsPerSec.toFixed(1)}/s</div>
  ```

[0.11.7]: https://github.com/pjm17971/pond-ts/compare/v0.11.6...v0.11.7

## [0.11.6] — 2026-04-29

### Added

- **`LiveSeries.toJSON()` return-type narrowing on `rowFormat`.**
  Overloads keyed on `rowFormat: 'array' | 'object'` so consumers
  read `result.rows` without a cast. Tuple form returns
  `TimeSeriesJsonOutputArray<S>`; object form returns
  `TimeSeriesJsonOutputObject<S>`. Both new types exported from
  `pond-ts/types`. The companion narrowing on `TimeSeries.toJSON`
  is still parked — it cascades TS2394 errors through unrelated
  overload sets in `TimeSeries.ts`. See PLAN.md.
- New types: `TimeSeriesJsonOutputArray<S>` and
  `TimeSeriesJsonOutputObject<S>`. Use these for typed assignment
  (`const out: TimeSeriesJsonOutputArray<S> = ts.toJSON()`) or
  cast (`ts.toJSON() as TimeSeriesJsonOutputArray<S>`) until the
  `TimeSeries.toJSON` narrowing lands.

### Documentation

- `count` reducer JSDoc clarifies that **duplicate temporal keys
  do not collapse** — multiple events sharing one `Time` key each
  contribute independently to the count. Walks the per-column
  value array, not unique keys. Behavior is consistent across
  `reduce`, `aggregate`, `rolling`, `LiveAggregation`, and
  `LiveRollingAggregation` — pinned by `test/duplicate-keys.test.ts`
  (9 tests covering every layer including the
  "dashboard-defaults" 480-events-at-8/s scenario from the gRPC
  experiment's M1 friction notes).

[0.11.6]: https://github.com/pjm17971/pond-ts/compare/v0.11.5...v0.11.6

## [0.11.5] — 2026-04-29

### Fixed

- Published tarballs for both `pond-ts` and `@pond-ts/react` now
  include `README.md`, `LICENSE`, and `CHANGELOG.md`. Earlier
  releases shipped only `dist/` + `package.json`, which left the
  npm page rendering as "This package does not have a README"
  despite the comprehensive root README. The repo-root files were
  invisible to `npm pack` because npm publishes from the package
  directory and only auto-includes README/LICENSE when those files
  live in the package dir itself. Each package now has a `prepack`
  step that copies them in from the repo root before build.

[0.11.5]: https://github.com/pjm17971/pond-ts/compare/v0.11.4...v0.11.5

## [0.11.4] — 2026-04-29

### Added

- **`LiveSeries` snapshot/append primitives** — closes the gap
  where networked `LiveSeries` setups (gRPC, WebSocket fanout) had
  to hand-roll the parallel APIs that already existed on
  `TimeSeries`.
  - **Codec-agnostic typed-tuple primitives:** `LiveSeries.toRows()`,
    `LiveSeries.toObjects()`, `LiveSeries.pushMany(rows)`,
    `Event.toRow(schema)`. Operate in `RowForSchema<S>` typed
    tuples — JSON, MessagePack, protobuf, anything else applies at
    the application boundary, not inside the library.
  - **JSON sugar layered on top:** `LiveSeries.toJSON()`,
    `LiveSeries.fromJSON(input, options?)`,
    `LiveSeries.pushJson(rows)`, `Event.toJsonRow(schema)`. Closes
    the wire→push safety hole — `pushJson` validates a
    `JsonRowForSchema<S>` against the schema at compile time, so
    schema evolution breaks the call site instead of swallowing
    via `live.push(row as never)`.
  - **`pushMany(rows)` is non-variadic.** Pair with the existing
    variadic `push(...rows)` (now a one-line wrapper); reach for
    `pushMany` when ingesting a snapshot or any large array —
    variadic spread allocates a stack frame per element and can
    blow on multi-thousand-row snapshots.

  Surfaced by the gRPC experiment's M1 milestone
  ([pond-grpc-experiment#3](https://github.com/pjm17971/pond-grpc-experiment/pull/3)).
  See PLAN.md Phase 4 for the deferred adaptor-extraction
  framing (codec strategies parked until two real codecs exist
  in working code).

### Changed

- `LiveSeries.push(...rows)` is now a wrapper around
  `LiveSeries.pushMany(rows)`. Behavior is identical — same
  validation, listener fires, and retention pass.

[0.11.4]: https://github.com/pjm17971/pond-ts/compare/v0.11.3...v0.11.4

## [0.11.3] — 2026-04-28

### Added

- **`pond-ts/types` subpath export** — type-only entry point that
  exposes the schema-shape, row-shape, and JSON-shape types
  (`SeriesSchema`, `ColumnDef`, `RowForSchema`,
  `JsonRowForSchema`, etc.) without dragging in the runtime.
  Schema-as-contract consumers — packages whose only job is to
  declare the `as const` schema flowing through producer /
  aggregator / web — can now constrain literals via
  `satisfies SeriesSchema` without adding `pond-ts` as a runtime
  dependency. Surfaced by the gRPC experiment's `packages/shared`,
  where `import { SeriesSchema } from 'pond-ts'` would have
  pulled in the whole library for one type.

  ```ts
  import type { SeriesSchema } from 'pond-ts/types';
  export const schema = [
    { name: 'time', kind: 'time' },
    { name: 'cpu', kind: 'number' },
  ] as const satisfies SeriesSchema;
  ```

  Existing `import { SeriesSchema } from 'pond-ts'` calls keep
  working unchanged.

[0.11.3]: https://github.com/pjm17971/pond-ts/compare/v0.11.2...v0.11.3

## [0.11.2] — 2026-04-28

### Added

- `minSamples` option on `TimeSeries.rolling`,
  `PartitionedTimeSeries.rolling`, `LiveRollingAggregation`, and
  the `LivePartitionedSeries` rolling sugar — suppresses output
  rows whose window contains fewer than the configured number of
  source events. Forwarded to `TimeSeries.baseline` and
  `TimeSeries.outliers` (and their per-partition variants), which
  pass it to their internal rolling pass. Defaults to `0` (no
  gate) so existing call sites are unaffected. Use it on noisy
  rolling stats (e.g. the rolling stdev that feeds
  `baseline()`'s ±σ bands) to hide the warm-up region where a
  tiny-sample stdev would collapse the band tight enough to
  false-flag normal events.

[0.11.2]: https://github.com/pjm17971/pond-ts/compare/v0.11.1...v0.11.2

## [0.11.1] — 2026-04-27

Closes a packaging footgun the dashboard agent surfaced while
upgrading from `pond-ts@0.10.1` to `pond-ts@0.11.0`.

When users had `@pond-ts/react@0.10.1` (which declared
`dependencies: { "pond-ts": "^0.10.0" }`) and bumped only
`pond-ts` to `0.11.0`, npm satisfied the react package's `^0.10.0`
range by nesting a _second_ copy of `pond-ts@0.10.1` under
`@pond-ts/react/node_modules/`. Two pond-ts copies meant two
distinct `Sequence` / `Time` / etc. classes with non-shared JS
private (`#`) brands. TypeScript surfaced this as
`Property '#private' refers to a different member`, which is
opaque without the package context.

### Changed

- **`@pond-ts/react`**: moved `pond-ts` from `dependencies` to
  `peerDependencies` (range unchanged: `^0.11.0`). With peer-dep
  semantics, npm refuses to install a duplicate `pond-ts`; instead
  it warns at install time about peer-version mismatch — concrete,
  actionable feedback rather than a runtime brand-check failure.

  This is the standard pattern for packages that wrap another
  library's classes (`react-dom` peer-deps `react`, etc.):
  `@pond-ts/react`'s hooks return and operate on `pond-ts`
  instances, so they MUST share class identity with the consumer's
  `pond-ts`.

  **Mild break:** consumers who installed only `@pond-ts/react`
  and relied on the transitive `pond-ts` will now get an npm
  warning and need to add `pond-ts` to their direct dependencies.
  In practice anyone using `@pond-ts/react` is already importing
  `pond-ts` types/classes, so the typical setup already has it
  declared explicitly.

### Notes

- **Why caret (`^0.11.0`) and not exact pin?** Pre-1.0 caret
  semver only accepts patches within the same minor (so
  `^0.11.0` matches 0.11.x but not 0.12.0). That already
  enforces minor-level lockstep — exact pinning would force
  consumers to bump both packages for every patch, even when one
  package's bump is a lockstep no-op.

[0.11.1]: https://github.com/pjm17971/pond-ts/compare/v0.11.0...v0.11.1

## [0.11.0] — 2026-04-27

The "live partitioning" release. Closes the cross-entity
correctness story end-to-end — the per-partition primitives we
shipped in v0.9.0 / v0.10.0 for batch now have a live counterpart
that handles ingestion, retention, grace, and stateful pipelines
on multi-host streams.

Without this, every multi-host live pipeline (rolling avg, fill,
diff, rate, cumulative, pctChange) silently mixes data across
entities — the same hazard the partitionBy work resolved for
batch, but live-side. Dashboard agent's v0.9.0 round-2 feedback
explicitly named "LivePartitionedSeries would be the obvious next
step" as the missing piece.

### Added

- **`liveSeries.partitionBy(col, options?)`** — returns
  `LivePartitionedSeries<S, K>`, the live counterpart to
  `PartitionedTimeSeries`. Routes events from a source
  `LiveSource<S>` into per-partition `LiveSeries<S>` sub-buffers,
  each with its own retention, grace window, and stateful
  operator pipeline.

  Per-partition semantics (settled in design):
  - Retention applies per partition (a chatty host can't squeeze
    a quiet one out of the buffer)
  - Grace windows apply per partition (late events touch only
    their own partition)
  - Aggregation timing is per partition (one host's rolling avg
    fires when that host has enough data)
  - Auto-spawn on new partition values; optional `groups` for
    typed declared partitions (mirrors batch typed-groups)

  Terminals:
  - `.toMap()` → `Map<K, LiveSource<S>>` for direct per-partition
    subscription
  - `.collect()` → unified `LiveSeries<S>` (append-only fan-in)
  - `.apply(factory)` → unified `LiveSeries<R>` with per-
    partition operator chains
  - `.dispose()` cleans up source subscription, all per-partition
    pipeline subscribers, and `toMap`-created factory chains

- **Typed chainable sugar** — `partitioned.fill(...).rolling(...).collect()`
  matches the batch chainable view. Sugar coverage on both
  `LivePartitionedSeries` and the chained `LivePartitionedView`:
  `fill`, `diff`, `rate`, `pctChange`, `cumulative`, `rolling`.

  ```ts
  const cpuSmoothed = live
    .partitionBy('host')
    .fill({ cpu: 'hold' })
    .rolling('1m', { cpu: 'avg', host: 'last' })
    .collect();
  ```

  `LivePartitionedView<SBase, R, K>` is a lazy chain step holding
  a composed factory; terminals delegate to the root partitioned
  series. Auto-spawn flows through the chain — a new partition
  triggers a fresh factory invocation.

- **`LivePartitionedView`** exported from package root.

- **`ARCHITECTURE.md`** at repo root — first-pass document for
  contributors (human or AI) reading the codebase cold. Covers
  layered model, stateful primitives, recurring patterns
  (typed-groups, trusted construction via `static #foo`,
  factory-based per-partition state, append-only fan-in vs
  mirrored materialization, per-method JSDoc warnings, perf-
  check discipline), decision log, and conventions.

### Changed

- **CLAUDE.md** points to `ARCHITECTURE.md` so future sessions
  discover it alongside `PLAN.md`.

### Notes

- **Append-only fan-in semantics** for `collect()` and `apply()`
  on `LivePartitionedSeries` — per-partition retention/grace
  evictions do NOT propagate to the unified buffer. Documented
  via JSDoc; the unified buffer's own retention is independent.
  Use `toMap()` for current per-partition state.

- **Post-commit error semantics for partition rejection** — when
  the partition view throws inside the source's event listener
  (rogue value, partition ordering rejection), the source has
  already committed the event. Documented in
  `LiveSeries.partitionBy` JSDoc; recommend upstream input
  validation if source/partition atomicity matters.

- **Rolling drops partition column unless explicitly added.**
  `LiveSeries.rolling` (and the partitioned chain via it) only
  retains columns named in `mapping` — include `host: 'last'` (or
  similar) to keep the partition tag visible in the unified
  output. Documented in `rolling`'s JSDoc on both the
  `LivePartitionedSeries` and `LivePartitionedView` surfaces.

### Performance

- Routing overhead measured at ~88ms for 100k events × 10 hosts
  (50ms over bare push). Apples-to-apples vs equivalent un-
  partitioned operator chains: ~1.8-2.6× cost. Constant per
  event (~0.8 µs); cardinality scales flat (Map lookup is O(1)).
  See `scripts/perf-live-partitioned.mjs`.

- An `_acceptEvent` private-method optimization to bypass row
  re-validation in partition routing was scoped and rejected for
  v0.11 — the benefit (~0.3-0.4 µs/event saved) is marginal for
  typical telemetry workloads (1-10k events/sec) and the cost
  (validation-bypass primitive on the public API surface) wasn't
  justified. May revisit if a high-throughput user surfaces the
  bottleneck with real workload data.

[0.11.0]: https://github.com/pjm17971/pond-ts/compare/v0.10.1...v0.11.0

## [0.10.1] — 2026-04-27

Strictly additive over v0.10.0. Closes the export gap surfaced by
the Codex CSV-cleaner v0.10 retest:

> `MaterializeSchema` exists in `dist/types.d.ts` but is not
> exported from the package root, so the script had to spell out
> the materialized schema locally for strict typing.

### Added

- **`MaterializeSchema<S>`** now exported from the package root.
  Users typing `materialize` output (or composing it into wrapper
  utilities) can import the type directly from `pond-ts` instead
  of digging into the dist-types.
- **`DedupeKeep<S>`** also exported (was the same gap — the type
  for the `dedupe({ keep })` resolver function shape). Closes the
  same friction for callers writing custom dedupe resolvers in
  isolation.

[0.10.1]: https://github.com/pjm17971/pond-ts/compare/v0.10.0...v0.10.1

## [0.10.0] — 2026-04-27

The "round-2 dashboard agent feedback" release. After v0.9.0
shipped the cross-entity correctness wave, three independent
agents (Codex CSV-cleaner, fresh CSV-cleaner eval, dashboard
agent) flagged refinements. v0.10 delivers all three:

- A grid-completion primitive that doesn't pre-pick a fill method
  (Codex's "regularize without filling" friction)
- A terminal `toMap` that materializes the partition view directly
  to a Map keyed by partition value (dashboard agent's
  `.collect().groupBy(col, fn)` chain pain)
- Typed partition declaration via `groups` for narrowed Map keys
  and declared-order iteration (dashboard agent's third
  refinement; mirrors `pivotByGroup({ groups })`)

Strictly additive over v0.9.x — no behavior changes for existing
callers.

### Added

- **`series.materialize(sequence, options?)`** — emits one
  time-keyed row per sequence bucket, populating value columns
  from a chosen source event in the bucket (or `undefined` for
  empty buckets). Does only the grid step; pairs naturally with
  `fill()` for explicit fill-policy control:

  ```ts
  series
    .partitionBy('host')
    .materialize(Sequence.every('1m'))
    .fill({ cpu: 'linear' }, { maxGap: '3m' })
    .collect();
  ```

  Three `select` modes: `'first'` / `'last'` (default) /
  `'nearest'` — all bucket-bounded; empty buckets emit
  `undefined` regardless. Three `sample` anchors:
  `'begin'` (default) / `'center'` / `'end'`. Output schema
  widens value columns to optional (`MaterializeSchema<S>`).

  The `PartitionedTimeSeries.materialize` sugar auto-populates
  the partition column on every output row, including
  empty-bucket rows — without this, downstream code would need a
  `.fill({ host: 'hold' })` step that fails for partitions where
  every event sits in a long-outage gap.

  Distinct from `align()` (which mandates a `'hold'` or
  `'linear'` fill method and returns interval-keyed) and
  `aggregate()` (which applies a per-column reducer). See
  `cleaning.mdx` for the full operator-comparison table.

- **`PartitionedTimeSeries.toMap(transform?)`** — terminal that
  returns `Map<key, TimeSeries<S>>` (or `Map<key, R>` with a
  transform) directly from the partition view. Replaces the
  `.collect().groupBy(col, fn)` chain dashboard code was using.

  Three overloads cover the common shapes: bare per-partition
  `TimeSeries`, transform that returns `TimeSeries<R>`, and
  transform that returns arbitrary `R`. Map iteration order
  matches the order each partition was first encountered in the
  source events (or declared order when `groups` is set).

  Map keys are stringified partition values for single-column
  partitions (preserving the natural string representation:
  `'api-1'`, `'eu'`, etc.), or JSON arrays for composite
  partitions (`'["api-1","eu"]'`). `undefined` partition values
  use the leading-space sentinel `' undefined'` to avoid
  collision with the literal string `'undefined'` — distinct
  from `groupBy`'s bare `'undefined'` key, which silently
  collapses the two cases. Documented as an intentional
  improvement; migrators changing from `.get('undefined')` to
  `.get(' undefined')`.

  **3.3× faster than the `.collect().groupBy(col, fn)` chain it
  replaces** at 100k events × 10 hosts (33 ms vs 108 ms,
  measured by `scripts/perf-partitioned-toMap.mjs`).

- **`series.partitionBy(col, { groups })` typed declaration**
  — pre-declares the expected partition values, narrowing the
  partition view's `K` type from `string` to the literal union.
  Propagates through every sugar method's return type and through
  `toMap`'s `Map` key:

  ```ts
  const HOSTS = ['api-1', 'api-2', 'api-3'] as const;
  const byHost = series
    .partitionBy('host', { groups: HOSTS })
    .fill({ cpu: 'linear' })
    .toMap();
  // byHost: Map<'api-1' | 'api-2' | 'api-3', TimeSeries<S>>
  ```

  Mirrors `pivotByGroup({ groups })` — same design vocabulary,
  same discipline: declared-order iteration, empty declared
  groups produce empty entries, partition values not in `groups`
  throw at construction time, empty `groups: []` and duplicate
  values throw upfront, single-column only (composite + groups
  throws). Numeric and boolean partition columns are stringified
  by the encoder, so declared groups must be the stringified
  form (`groups: ['1', '2']` for a numeric column).

- **Per-method `**Multi-entity series:**` JSDoc warnings**
  remain on every stateful operator (shipped in v0.9.0); the
  v0.10 operators (`materialize`, `toMap`) inherit the same
  discoverability.

### Changed

- **CLAUDE.md adds a perf-check policy.** New operators that walk
  events, allocate per-event, or scale with input dimensions
  must have an analytical complexity statement, a benchmark
  script (`packages/core/scripts/perf-<operator>.mjs`), and
  before/after numbers in the commit message. Surfaces in the
  Layer 1 self-review checklist. Every v0.10 PR followed this:
  `materialize` got `perf-materialize.mjs` (and two optimization
  passes that landed –41% on the partitioned variant);
  `toMap` got `perf-partitioned-toMap.mjs` (3.3× speedup
  measurement); typed `groups` got `perf-partitionby-groups.mjs`
  (zero chain-step regression via the class-private trusted
  factory).

[0.10.0]: https://github.com/pjm17971/pond-ts/compare/v0.9.1...v0.10.0

## [0.9.1] — 2026-04-26

Strictly additive over v0.9.0. Closes a sugar-method type bug
identified independently by two agents (a fresh CSV-cleaner eval
against v0.9.0 and Codex on a v0.9.0 retest), plus folds in two
fresh-agent doc improvements.

### Fixed

- **`PartitionedTimeSeries.fill` now accepts `maxGap`.** PR #78
  added `maxGap` to `TimeSeries.fill` for v0.9.0 but the partitioned
  sugar's option type was not widened, so the headline v0.9.0 chain —
  `partitionBy('host').fill('linear', { maxGap: '5m' })` — failed
  type checking and forced callers into `.apply()`. The underlying
  impl already passed options through, so this is a one-line type
  widening: `{ limit?: number; maxGap?: DurationInput }`.

### Added

- **9 new tests** under `TimeSeries.partitionBy.test.ts`:
  - 4 regression tests pinning the partitioned `fill(maxGap)` chain
    works (bare `maxGap`, all-or-nothing per-partition span,
    `limit + maxGap` composition, full `partitionBy + dedupe +
fill(maxGap)` chain).
  - 5 composite-key round-trip tests addressing a refinement flagged
    by the dashboard agent: `partitionBy(['host', 'region'])`
    preserves both key columns in the schema, on every output event,
    keeps `(host, region)` tuples distinct (no collapse on host
    alone), and round-trips through `apply()` and the full chain.
- **`cleaning.mdx` "Schema first — `required: false`" section.**
  Leads the page; documents why optional cells need the flag and
  surfaces the `fromJSON`/`null` workaround for the known
  `RowForSchema` variance limitation. Previously this prose only
  lived in the 0.8.2 changelog (fresh-agent feedback).
- **`cleaning.mdx` "End-to-end multi-entity cleaning pipeline"
  section.** The unified `partitionBy + dedupe + fill(maxGap)`
  chain in one place plus a step-by-step hazard table.
  Previously split across three sections (fresh-agent feedback).

[0.9.1]: https://github.com/pjm17971/pond-ts/compare/v0.9.0...v0.9.1

## [0.9.0] — 2026-04-26

The "cross-entity correctness + cleaning hygiene" release. Three
independent CSV-cleaner agent runs (Codex, Claude, Gemini) all hit
the same shape: stateful transforms (`fill('linear')`, `rolling`,
`diff`, etc.) silently mix data across entities on multi-host
series, and `fill('linear', { limit: 3 })` fabricates interpolated
data across long outages instead of leaving the unknown unknown.

v0.9.0 ships three operator-level fixes plus a discoverability pass
on every affected method's JSDoc.

### Added

- **`series.partitionBy(col).<op>(...).collect()`** — chainable
  per-partition view over `TimeSeries`. Sugar methods for every
  stateful operator (`fill`, `align`, `rolling`, `smooth`,
  `baseline`, `outliers`, `diff`, `rate`, `pctChange`, `cumulative`,
  `shift`, `aggregate`, `dedupe`) run the underlying transform per
  partition. `.collect()` materializes back to `TimeSeries<S>`.
  `.apply(g => /* arbitrary chain */)` is the terminal escape hatch.
  One primitive covers the cross-entity hazard for every at-risk
  method, instead of adding a `partitionBy` option to each.
- **`series.dedupe({ keep })`** — first-class deduplication with
  policies: `'first' | 'last' | 'error' | 'drop' | { min: col } |
{ max: col } | (events) => Event`. Default key is the full event
  key (`begin` for time-keyed, `begin+end` for time-range,
  `begin+end+value` for interval-keyed); default resolution is
  `'last'`. `partitionBy('host').dedupe()` is the multi-entity
  pattern.
- **`fill(strategy, { maxGap })`** — duration-based gap cap,
  complements the existing count-based `limit`. Both compose; most
  restrictive wins.

### Changed

- **`fill` is now all-or-nothing.** A gap either fits both caps and
  is filled entirely, or exceeds either cap and is left fully
  unfilled. Previously `limit: 3` on a 5-cell gap filled 3 cells and
  left 2 unfilled — propagating stale `'hold'` values past their
  useful lifetime and inventing misleading `'linear'` slopes across
  long outages. Existing `limit` callers see strictly more
  conservative behavior; to opt back in to partial fill, set
  `limit`/`maxGap` larger than any gap you want filled.
- **Every stateful TimeSeries method's JSDoc** now includes a
  `**Multi-entity series:**` warning paragraph naming the operator's
  specific cross-entity hazard and pointing at the
  `partitionBy(col).<method>(...).collect()` pattern. Discoverable
  in LSP hover, IDE quick-help, and any tool that reads type
  definitions.
- **`PartitionedTimeSeries` view** preserves partition state across
  every sugar call, so multi-step per-partition chains compose
  cleanly without re-partitioning at each step.

### Fixed

- Pre-existing brand-check bug on `series.filter(...).diff(...)`
  and similar chains: events constructed via
  `#fromTrustedEvents` (which uses `Object.create` to bypass the
  constructor) hit a JS-private brand check on `#diffOrRate` and
  threw. Refactored to a class-static private (`static
#diffOrRate`) — runtime-private without the per-instance brand
  failure.

[0.9.0]: https://github.com/pjm17971/pond-ts/compare/v0.8.2...v0.9.0

## [0.8.2] — 2026-04-26

Strictly additive over v0.8.1. Closes friction surfaced by two
independent agent runs against a realistic CSV-cleaning task —
specifically, the missing fan-in primitive that forces callers out
of the typed contract when reassembling per-host transformed
subseries.

### Added

- **`TimeSeries.concat([s1, s2, ...])`** — concatenates the events of
  N same-schema `TimeSeries` instances, re-sorted by key. The
  row-append / vertical-stack counterpart to `joinMany` (which
  column-merges by key). Matches `Array.prototype.concat` /
  `pandas.concat(axis=0)` / SQL `UNION ALL` semantics. Closes the
  round-trip after `groupBy(col, fn)` + per-group transforms without
  forcing callers to unwrap events back to row tuples.

  ```ts
  const filledByHost = series.groupBy('host', (g) =>
    g.fill({ cpu: 'linear' }, { limit: 2 }),
  );
  const combined = TimeSeries.concat([...filledByHost.values()]);
  // back to one TimeSeries<S>; events from all hosts re-sorted.
  ```

  Schemas must match column-by-column on `name` and `kind`; throws
  upfront on mismatch. Same-key events from different inputs are
  both kept (row-append, not key-dedupe).

  Coming from pondjs: `timeSeriesListMerge`'s concatenation case
  maps to `TimeSeries.concat([...])`; its column-union case maps to
  `TimeSeries.joinMany([...])`.

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
