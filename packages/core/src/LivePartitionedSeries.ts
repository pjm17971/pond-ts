import { LiveSeries, type LiveSeriesOptions } from './LiveSeries.js';
import { LiveRollingAggregation } from './LiveRollingAggregation.js';
import {
  makeCumulativeView,
  makeDiffView,
  makeFillView,
  type LiveFillMapping,
  type LiveFillStrategy,
} from './LiveView.js';
import {
  type AggregateMap,
  type DiffSchema,
  type EventDataForSchema,
  type EventForSchema,
  type LiveSource,
  type NumericColumnNameForSchema,
  type RollingSchema,
  type RowForSchema,
  type SeriesSchema,
} from './types.js';
import type { DurationInput } from './utils/duration.js';
import type { RollingWindow } from './LiveRollingAggregation.js';

type SpawnListener<S extends SeriesSchema, K extends string> = (
  key: K,
  partition: LiveSource<S>,
) => void;

/**
 * Per-partition retention and grace settings for a partitioned live
 * view. Each partition is its own bounded buffer with these limits;
 * the source `LiveSource`'s own retention does not propagate.
 */
export type LivePartitionedOptions<K extends string> = {
  /** Declared partition values (mirrors batch `partitionBy({ groups })`). */
  groups?: ReadonlyArray<K>;
  /** Retention applied to each partition's sub-buffer independently. */
  retention?: NonNullable<LiveSeriesOptions<SeriesSchema>['retention']>;
  /** Grace window applied per partition for late events. */
  graceWindow?: DurationInput;
  /** Ordering mode for each partition. Defaults to `'strict'`. */
  ordering?: NonNullable<LiveSeriesOptions<SeriesSchema>['ordering']>;
};

/** Encoder for partition values → keys. Mirrors the batch single-column case. */
function partitionKey(
  event: { data(): Record<string, unknown> },
  col: string,
): string {
  const v = event.data()[col];
  return v === undefined ? ' undefined' : `${String(v)}`;
}

/**
 * Live counterpart to {@link PartitionedTimeSeries}. Routes events
 * from a source `LiveSource<S>` into per-partition `LiveSeries<S>`
 * sub-buffers, each with its own retention, grace window, and
 * stateful operator pipeline.
 *
 * **Per-partition semantics** (settled in the v0.11 design pass):
 *
 * - **Retention** applies to each partition independently. A
 *   chatty host can't squeeze a quiet one out of the buffer.
 * - **Grace windows** apply per partition. A late event for
 *   `host-A` does not perturb `host-B`'s emission. **Caveat:**
 *   per-partition grace is bounded by the source's grace
 *   window. If the source rejects an event (because it's older
 *   than the source's grace), it never reaches the partitioned
 *   view. Setting `partitionBy('host', { graceWindow: '10m' })`
 *   on a source with `graceWindow: '1m'` silently uses the
 *   smaller window.
 * - **Aggregation timing** is per-partition. `host-A`'s rolling
 *   avg fires when `host-A` has enough data, regardless of
 *   `host-B`.
 * - **Auto-spawn** on new partition values: the first time a
 *   value not seen before arrives, allocate a sub-buffer.
 *   Optional `{ groups }` upfront declares the expected set
 *   (mirrors the batch typed-groups pattern); when set, unknown
 *   partition values throw on ingest.
 *
 * **v0.11 PR 1 scope** — foundation only. Compose operators per
 * partition via `apply((sub) => sub.fill(...).rolling(...))`.
 * Typed chainable sugar methods (`fill(...).rolling(...).collect()`)
 * arrive in v0.11 PR 2.
 *
 * @example
 * ```ts
 * const live = new LiveSeries({ ... });
 *
 * // Per-host event lookup — direct subscription per partition.
 * const byHost = live.partitionBy('host').toMap();
 * byHost.get('api-1')?.on('event', (e) => { ... });
 *
 * // Apply a chain of live operators per partition; collect into a
 * // unified LiveSeries.
 * const cpuSmoothed = live.partitionBy('host').apply((sub) =>
 *   sub.fill({ cpu: 'hold' }).rolling('1m', { cpu: 'avg' }),
 * );
 * ```
 */
export class LivePartitionedSeries<
  S extends SeriesSchema,
  K extends string = string,
> {
  // NOTE: `LivePartitionedSeries` is intentionally NOT a `LiveSource` —
  // it has no `on()` method and does not emit events directly.
  // Consumers obtain a `LiveSource` from `collect()` (unified buffer),
  // `apply()` (per-partition factory output), or `toMap()` (per-partition
  // sources). We deliberately do not declare `EMITS_EVICT` so that
  // `LiveView`'s duck-typed eviction subscription (`EMITS_EVICT in
  // source`) doesn't trip on this class.
  readonly name: string;
  readonly schema: S;
  readonly by: keyof EventDataForSchema<S> & string;
  readonly groups?: ReadonlyArray<K>;

  readonly #partitions: Map<K, LiveSeries<S>>;
  readonly #partitionOptions: {
    retention: LiveSeriesOptions<S>['retention'];
    graceWindow: LiveSeriesOptions<S>['graceWindow'];
    ordering: LiveSeriesOptions<S>['ordering'];
  };
  readonly #onSpawn: Set<SpawnListener<S, K>>;
  readonly #disposers: Set<() => void>;
  readonly #unsubscribeSource: () => void;

  constructor(
    source: LiveSource<S>,
    by: keyof EventDataForSchema<S> & string,
    options: LivePartitionedOptions<K> = {},
  ) {
    this.name = source.name;
    this.schema = source.schema;
    this.by = by;

    if (!source.schema.some((c) => c.name === by)) {
      throw new TypeError(
        `LivePartitionedSeries: column "${String(by)}" not in schema`,
      );
    }

    if (options.groups !== undefined) {
      if (options.groups.length === 0) {
        throw new TypeError('LivePartitionedSeries: `groups` cannot be empty.');
      }
      const seen = new Set<string>();
      for (const g of options.groups) {
        if (seen.has(g)) {
          throw new TypeError(
            `LivePartitionedSeries: duplicate value ${JSON.stringify(g)} in \`groups\`.`,
          );
        }
        seen.add(g);
      }
      this.groups = options.groups;
    }

    this.#partitions = new Map();
    this.#partitionOptions = {
      retention: options.retention,
      graceWindow: options.graceWindow,
      ordering: options.ordering,
    };
    this.#onSpawn = new Set();
    this.#disposers = new Set();

    if (this.groups) {
      for (const g of this.groups) {
        this.#spawnPartition(g);
      }
    }

    // Replay source's existing events into the right partitions.
    for (let i = 0; i < source.length; i++) {
      this.#routeEvent(source.at(i)!);
    }

    // Subscribe to new events from the source.
    this.#unsubscribeSource = source.on('event', (event) => {
      this.#routeEvent(event);
    });
  }

  /**
   * Materialize the partitioned view as a `Map<key, LiveSource<S>>`,
   * one entry per spawned partition. Map iteration order matches
   * spawn order (declared order if `groups` was set, insertion
   * order otherwise).
   */
  toMap(): Map<K, LiveSource<S>> {
    return new Map(this.#partitions);
  }

  /**
   * Fan in events from every partition into a single unified
   * `LiveSeries<S>`. Subscribes to per-partition output `'event'`
   * streams and pushes each event into the unified buffer.
   *
   * **Append-only semantics.** This is a fan-in sink, not a
   * mirrored materialization. When per-partition retention or
   * grace evicts events from a sub-buffer, those evictions are
   * NOT propagated to the unified buffer. The unified buffer
   * keeps every event it ever received until evicted by its own
   * retention. To control its size, pass a `retention` option to
   * `collect`. To inspect the current per-partition state, use
   * `toMap()` and snapshot each partition independently.
   *
   * **Ordering caveat:** the unified `LiveSeries` defaults to
   * `'strict'` ordering. If the source uses `ordering: 'reorder'`
   * (i.e., accepts late events out-of-order), reordered events
   * will arrive at the unified buffer out of order and throw.
   * Pass `{ ordering: 'reorder', graceWindow: ... }` to `collect`
   * when the source is in reorder mode.
   */
  collect(options?: Partial<LiveSeriesOptions<S>>): LiveSeries<S> {
    const unifiedOptions: LiveSeriesOptions<S> = {
      name: options?.name ?? this.name,
      schema: this.schema,
    };
    if (options?.ordering !== undefined)
      unifiedOptions.ordering = options.ordering;
    if (options?.graceWindow !== undefined)
      unifiedOptions.graceWindow = options.graceWindow;
    if (options?.retention !== undefined)
      unifiedOptions.retention = options.retention;

    const unified = new LiveSeries<S>(unifiedOptions);

    const subscribeToPartition = (partition: LiveSource<S>): (() => void) => {
      return partition.on('event', (event) => {
        unified.push(eventToRow(event, this.schema));
      });
    };

    // Sort existing events from all partitions by time, then push
    // them into the unified buffer in order. Without this prefix
    // pass, collect() only catches new events going forward and
    // misses anything pre-existing in the partition sub-buffers.
    type Existing = { time: number; event: EventForSchema<S> };
    const existing: Existing[] = [];
    for (const partition of this.#partitions.values()) {
      for (let i = 0; i < partition.length; i++) {
        const e = partition.at(i)!;
        existing.push({ time: e.begin(), event: e });
      }
    }
    existing.sort((a, b) => a.time - b.time);
    for (const { event } of existing) {
      unified.push(eventToRow(event, this.schema));
    }

    for (const partition of this.#partitions.values()) {
      this.#disposers.add(subscribeToPartition(partition));
    }
    this.#onSpawn.add((_, partition) => {
      this.#disposers.add(subscribeToPartition(partition));
    });

    return unified;
  }

  /**
   * Apply `factory` per-partition and fan in the outputs into a
   * single unified `LiveSeries<R>`. The factory is called once per
   * partition (current and future); each call receives the
   * partition's `LiveSource<S>` and should return a `LiveSource<R>`
   * derived from it (typically by composing `LiveSeries`-style
   * operators like `sub.fill(...).rolling(...)`).
   *
   * The unified series subscribes to every factory output and
   * pushes events as they arrive. Auto-spawn propagates: a new
   * partition value triggers a fresh factory invocation and the
   * resulting `LiveSource` is subscribed to.
   *
   * **Append-only semantics.** Same as `collect()` — this is a
   * fan-in sink. Per-partition output evictions (e.g. from a
   * window operator inside the factory) are NOT propagated to
   * the unified buffer. Use the `options` argument to set the
   * unified buffer's own retention.
   *
   * **History replay.** When `apply()` is called on a partitioned
   * view that already has events distributed across multiple
   * partitions, existing factory-output events are gathered from
   * every output, sorted globally by time, and pushed into the
   * unified buffer in time order. This preserves strict ordering
   * for the unified buffer.
   *
   * **Factory contract.** The factory must be **pure and
   * re-runnable**: side-effect-free, no closure-captured state
   * that mutates across calls, no external subscriptions on the
   * input or output. The implementation invokes the factory once
   * upfront on a stub `LiveSeries<S>` (to capture the output
   * schema synchronously) and again once per partition (current
   * and future). Factories that don't satisfy the contract may
   * leak state across the stub call and the real per-partition
   * calls.
   *
   * **Ordering caveat:** same as `collect()` — pass `{ ordering:
   * 'reorder' }` if the source uses reorder mode and reordered
   * events will reach the unified buffer.
   */
  apply<R extends SeriesSchema>(
    factory: (sub: LiveSeries<S>) => LiveSource<R>,
    options?: Partial<LiveSeriesOptions<R>>,
  ): LiveSeries<R> {
    // Capture the output schema upfront by running the factory on
    // an empty stub LiveSeries. The stub is never connected to a
    // source — it only exists to let `factory` declare its output
    // schema synchronously, before any partitions exist.
    const stub = new LiveSeries<S>({
      name: `${this.name}/_stub`,
      schema: this.schema,
    });
    const stubOut = factory(stub);
    const outSchema: R = stubOut.schema;

    const opts: LiveSeriesOptions<R> = {
      name: options?.name ?? this.name,
      schema: outSchema,
    };
    if (options?.ordering !== undefined) opts.ordering = options.ordering;
    if (options?.graceWindow !== undefined)
      opts.graceWindow = options.graceWindow;
    if (options?.retention !== undefined) opts.retention = options.retention;
    const unified = new LiveSeries<R>(opts);

    // Build factory outputs for all existing partitions first, then
    // globally sort their existing events by time and push them in
    // order. Without this two-phase pass, the unified buffer would
    // receive partition-A's history fully before partition-B's,
    // producing out-of-order pushes and tripping unified's strict
    // ordering when histories interleave (e.g. a@0, b@60k, a@120k).
    const outputs: Array<{ key: K; out: LiveSource<R> }> = [];
    for (const [key, partition] of this.#partitions) {
      outputs.push({ key, out: factory(partition as LiveSeries<S>) });
    }

    type ExistingR = { time: number; event: EventForSchema<R>; outSchema: R };
    const existing: ExistingR[] = [];
    for (const { out } of outputs) {
      for (let i = 0; i < out.length; i++) {
        const e = out.at(i)!;
        existing.push({ time: e.begin(), event: e, outSchema: out.schema });
      }
    }
    existing.sort((a, b) => a.time - b.time);
    for (const { event, outSchema: s } of existing) {
      unified.push(eventToRow(event, s));
    }

    // Subscribe each factory output to the unified buffer for live
    // forwarding.
    for (const { out } of outputs) {
      const unsub = out.on('event', (event) => {
        unified.push(eventToRow(event, out.schema));
      });
      this.#disposers.add(unsub);
    }

    // Auto-spawn: when a new partition appears, run the factory
    // for it and subscribe its output. The new partition is empty
    // at spawn time (events are pushed AFTER spawn listeners fire),
    // so no historical replay is needed for the new partition's
    // factory output.
    this.#onSpawn.add((_, partition) => {
      const out = factory(partition as LiveSeries<S>);
      const unsub = out.on('event', (event) => {
        unified.push(eventToRow(event, out.schema));
      });
      this.#disposers.add(unsub);
    });

    return unified;
  }

  // ─── Chainable typed sugar (returns LivePartitionedView) ──────
  //
  // Each sugar method returns a `LivePartitionedView<NewSchema, K>`
  // — a chained view that composes the operator factory with any
  // future chain steps. Use these when you want the full
  // operator chain at the type level:
  //
  //   live.partitionBy('host').fill(...).rolling(...).collect()
  //
  // For one-shot per-partition factories (no chain), use `apply()`
  // instead.

  /** Per-partition `fill`. See {@link LiveSeries.fill}. */
  fill(
    strategy: LiveFillStrategy | LiveFillMapping<S>,
    options?: { limit?: number },
  ): LivePartitionedView<S, S, K> {
    return new LivePartitionedView<S, S, K>(this, (sub) =>
      makeFillView(sub, strategy, options),
    );
  }

  /** Per-partition `diff`. See {@link LiveSeries.diff}. */
  diff<const Target extends NumericColumnNameForSchema<S>>(
    columns: Target | readonly Target[],
    options?: { drop?: boolean },
  ): LivePartitionedView<S, DiffSchema<S, Target>, K> {
    return new LivePartitionedView<S, DiffSchema<S, Target>, K>(
      this,
      (sub) =>
        makeDiffView(sub, 'diff', columns, options) as unknown as LiveSource<
          DiffSchema<S, Target>
        >,
    );
  }

  /** Per-partition `rate`. See {@link LiveSeries.rate}. */
  rate<const Target extends NumericColumnNameForSchema<S>>(
    columns: Target | readonly Target[],
    options?: { drop?: boolean },
  ): LivePartitionedView<S, DiffSchema<S, Target>, K> {
    return new LivePartitionedView<S, DiffSchema<S, Target>, K>(
      this,
      (sub) =>
        makeDiffView(sub, 'rate', columns, options) as unknown as LiveSource<
          DiffSchema<S, Target>
        >,
    );
  }

  /** Per-partition `pctChange`. See {@link LiveSeries.pctChange}. */
  pctChange<const Target extends NumericColumnNameForSchema<S>>(
    columns: Target | readonly Target[],
    options?: { drop?: boolean },
  ): LivePartitionedView<S, DiffSchema<S, Target>, K> {
    return new LivePartitionedView<S, DiffSchema<S, Target>, K>(
      this,
      (sub) =>
        makeDiffView(
          sub,
          'pctChange',
          columns,
          options,
        ) as unknown as LiveSource<DiffSchema<S, Target>>,
    );
  }

  /** Per-partition `cumulative`. See {@link LiveSeries.cumulative}. */
  cumulative<const Targets extends NumericColumnNameForSchema<S>>(spec: {
    [P in Targets]:
      | 'sum'
      | 'max'
      | 'min'
      | 'count'
      | ((acc: number, value: number) => number);
  }): LivePartitionedView<S, DiffSchema<S, Targets>, K> {
    return new LivePartitionedView<S, DiffSchema<S, Targets>, K>(
      this,
      (sub) =>
        makeCumulativeView(sub, spec) as unknown as LiveSource<
          DiffSchema<S, Targets>
        >,
    );
  }

  /** Per-partition `rolling`. See {@link LiveSeries.rolling}. */
  rolling<const M extends AggregateMap<S>>(
    window: RollingWindow,
    mapping: M,
  ): LivePartitionedView<S, RollingSchema<S, M>, K> {
    return new LivePartitionedView<S, RollingSchema<S, M>, K>(
      this,
      (sub) =>
        new LiveRollingAggregation(
          sub,
          window,
          mapping as AggregateMap<S>,
        ) as unknown as LiveSource<RollingSchema<S, M>>,
    );
  }

  /**
   * Dispose of the partitioned view: unsubscribe from the source,
   * disconnect every per-partition pipeline subscriber (created
   * by `collect()` and `apply()`), and drop spawn listeners. Safe
   * to call multiple times.
   *
   * **Note:** this does not clear the per-partition `LiveSeries`
   * sub-buffers themselves. Their event arrays linger until the
   * `LivePartitionedSeries` instance becomes unreferenced and is
   * garbage-collected. If you want to free the sub-buffer memory
   * eagerly, drop your reference to the `LivePartitionedSeries`
   * after `dispose()`.
   */
  dispose(): void {
    this.#unsubscribeSource();
    for (const dispose of this.#disposers) dispose();
    this.#disposers.clear();
    this.#onSpawn.clear();
  }

  // ─── Internal ─────────────────────────────────────────────────

  #spawnPartition(key: K): LiveSeries<S> {
    const opts: LiveSeriesOptions<S> = {
      name: `${this.name}/${String(key)}`,
      schema: this.schema,
    };
    if (this.#partitionOptions.ordering !== undefined)
      opts.ordering = this.#partitionOptions.ordering;
    if (this.#partitionOptions.graceWindow !== undefined)
      opts.graceWindow = this.#partitionOptions.graceWindow;
    if (this.#partitionOptions.retention !== undefined)
      opts.retention = this.#partitionOptions.retention;

    const part = new LiveSeries<S>(opts);
    this.#partitions.set(key, part);
    for (const fn of this.#onSpawn) fn(key, part);
    return part;
  }

  #routeEvent(event: EventForSchema<S>): void {
    const key = partitionKey(event, this.by) as K;
    let part = this.#partitions.get(key);
    if (!part) {
      if (this.groups && !this.groups.includes(key)) {
        throw new TypeError(
          `LivePartitionedSeries: encountered partition value ${JSON.stringify(
            key === ' undefined' ? undefined : key,
          )} for column "${String(this.by)}" which is not in declared groups ` +
            `[${this.groups.map((g) => JSON.stringify(g)).join(', ')}].`,
        );
      }
      part = this.#spawnPartition(key);
    }
    part.push(eventToRow(event, this.schema));
  }
}

// Convert an Event back to a row tuple (for re-pushing into a
// LiveSeries). Mirrors the conversion in LiveSeries.toTimeSeries.
function eventToRow<S extends SeriesSchema>(
  event: EventForSchema<S>,
  schema: S,
): RowForSchema<S> {
  const row: unknown[] = [event.key()];
  for (let i = 1; i < schema.length; i++) {
    row.push(event.get((schema[i] as { name: string }).name as never));
  }
  return row as RowForSchema<S>;
}

/**
 * Chained typed view over a {@link LivePartitionedSeries}. Returned
 * by every sugar method on the leaf and on this view, composing the
 * operator factory at each step.
 *
 * The view is **lazy**: factories aren't run until a terminal
 * (`collect()`, `apply()`, `toMap()`) is called. Each terminal
 * delegates back to the leaf's per-partition state, applying the
 * composed factory chain to each partition's `LiveSeries`.
 *
 * Dispose is **inherited** from the leaf — chained views don't
 * register their own subscriptions on the source, so there's
 * nothing for them to unsubscribe from. Disposing the leaf disposes
 * everything (terminals subscribed to factory outputs are tracked
 * on the leaf's internal disposers).
 *
 * @example
 * ```ts
 * const cpuSmoothed = live
 *   .partitionBy('host')
 *   .fill({ cpu: 'hold' })       // → LivePartitionedView<S, S, K>
 *   .rolling('1m', { cpu: 'avg' }) // → LivePartitionedView<S, R, K>
 *   .collect();                    // → LiveSeries<R>
 * ```
 *
 * @typeParam SBase - schema of the root partitioned series's
 *   per-partition `LiveSeries` (kept so the composed factory's
 *   input type is correct).
 * @typeParam R - schema of the current chained output.
 * @typeParam K - partition key type.
 */
export class LivePartitionedView<
  SBase extends SeriesSchema,
  R extends SeriesSchema,
  K extends string = string,
> {
  readonly #root: LivePartitionedSeries<SBase, K>;
  readonly #factory: (sub: LiveSeries<SBase>) => LiveSource<R>;

  /**
   * Schema of the chained output. Captured by running the factory
   * once on a stub `LiveSeries<SBase>` at construction.
   */
  readonly schema: R;

  /** @internal — used by sugar methods to chain. */
  constructor(
    root: LivePartitionedSeries<SBase, K>,
    factory: (sub: LiveSeries<SBase>) => LiveSource<R>,
  ) {
    this.#root = root;
    this.#factory = factory;
    // Capture output schema upfront via a stub invocation. The stub
    // is never connected — same pattern as
    // {@link LivePartitionedSeries.apply}.
    const stub = new LiveSeries<SBase>({
      name: `${root.name}/_stub`,
      schema: root.schema,
    });
    const stubOut = factory(stub);
    this.schema = stubOut.schema;
  }

  /** Same as {@link LivePartitionedSeries.collect}, applied through the factory chain. */
  collect(options?: Partial<LiveSeriesOptions<R>>): LiveSeries<R> {
    return this.#root.apply(this.#factory, options);
  }

  /**
   * Apply a further per-partition transform on top of the existing
   * factory chain. Equivalent to chaining one more sugar method
   * via a custom function. Returns a unified `LiveSeries<R2>`.
   */
  apply<R2 extends SeriesSchema>(
    factory: (sub: LiveSource<R>) => LiveSource<R2>,
    options?: Partial<LiveSeriesOptions<R2>>,
  ): LiveSeries<R2> {
    const composed = this.#factory;
    return this.#root.apply(
      (sub) => factory(composed(sub)) as unknown as LiveSource<R2>,
      options,
    );
  }

  /**
   * Materialize the chained view per-partition as a
   * `Map<K, LiveSource<R>>`. Runs the composed factory once per
   * existing partition; auto-spawn from the leaf is *not*
   * propagated into this map (the snapshot reflects partitions at
   * the time of the call).
   *
   * For a live-updating per-partition view, subscribe to the leaf's
   * `partitionBy` directly with `toMap()` and call the factory
   * yourself, or use `collect()` for a unified buffer.
   */
  toMap(): Map<K, LiveSource<R>> {
    const result = new Map<K, LiveSource<R>>();
    const partitions = this.#root.toMap();
    for (const [key, sub] of partitions) {
      result.set(key, this.#factory(sub as LiveSeries<SBase>));
    }
    return result;
  }

  // ─── Chainable sugar (composes the factory) ──────────────────

  fill(
    strategy: LiveFillStrategy | LiveFillMapping<R>,
    options?: { limit?: number },
  ): LivePartitionedView<SBase, R, K> {
    const prev = this.#factory;
    return new LivePartitionedView<SBase, R, K>(this.#root, (sub) =>
      makeFillView(prev(sub), strategy, options),
    );
  }

  diff<const Target extends NumericColumnNameForSchema<R>>(
    columns: Target | readonly Target[],
    options?: { drop?: boolean },
  ): LivePartitionedView<SBase, DiffSchema<R, Target>, K> {
    const prev = this.#factory;
    return new LivePartitionedView<SBase, DiffSchema<R, Target>, K>(
      this.#root,
      (sub) =>
        makeDiffView(
          prev(sub),
          'diff',
          columns,
          options,
        ) as unknown as LiveSource<DiffSchema<R, Target>>,
    );
  }

  rate<const Target extends NumericColumnNameForSchema<R>>(
    columns: Target | readonly Target[],
    options?: { drop?: boolean },
  ): LivePartitionedView<SBase, DiffSchema<R, Target>, K> {
    const prev = this.#factory;
    return new LivePartitionedView<SBase, DiffSchema<R, Target>, K>(
      this.#root,
      (sub) =>
        makeDiffView(
          prev(sub),
          'rate',
          columns,
          options,
        ) as unknown as LiveSource<DiffSchema<R, Target>>,
    );
  }

  pctChange<const Target extends NumericColumnNameForSchema<R>>(
    columns: Target | readonly Target[],
    options?: { drop?: boolean },
  ): LivePartitionedView<SBase, DiffSchema<R, Target>, K> {
    const prev = this.#factory;
    return new LivePartitionedView<SBase, DiffSchema<R, Target>, K>(
      this.#root,
      (sub) =>
        makeDiffView(
          prev(sub),
          'pctChange',
          columns,
          options,
        ) as unknown as LiveSource<DiffSchema<R, Target>>,
    );
  }

  cumulative<const Targets extends NumericColumnNameForSchema<R>>(spec: {
    [P in Targets]:
      | 'sum'
      | 'max'
      | 'min'
      | 'count'
      | ((acc: number, value: number) => number);
  }): LivePartitionedView<SBase, DiffSchema<R, Targets>, K> {
    const prev = this.#factory;
    return new LivePartitionedView<SBase, DiffSchema<R, Targets>, K>(
      this.#root,
      (sub) =>
        makeCumulativeView(prev(sub), spec) as unknown as LiveSource<
          DiffSchema<R, Targets>
        >,
    );
  }

  rolling<const M extends AggregateMap<R>>(
    window: RollingWindow,
    mapping: M,
  ): LivePartitionedView<SBase, RollingSchema<R, M>, K> {
    const prev = this.#factory;
    return new LivePartitionedView<SBase, RollingSchema<R, M>, K>(
      this.#root,
      (sub) =>
        new LiveRollingAggregation(
          prev(sub),
          window,
          mapping as AggregateMap<R>,
        ) as unknown as LiveSource<RollingSchema<R, M>>,
    );
  }
}
