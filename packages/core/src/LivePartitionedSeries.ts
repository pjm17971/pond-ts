import { LiveSeries, type LiveSeriesOptions } from './LiveSeries.js';
import {
  type EventDataForSchema,
  type EventForSchema,
  type LiveSource,
  type RowForSchema,
  type SeriesSchema,
} from './types.js';
import type { DurationInput } from './utils/duration.js';

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
   * Materialize the partitioned view as a single unified
   * `LiveSeries<S>`. Subscribes to every per-partition output and
   * fans events into a single buffer in arrival order.
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
   * Apply `factory` per-partition and collect the outputs into a
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

    const wirePartition = (sub: LiveSeries<S>): void => {
      const out = factory(sub);
      // Replay output's existing events into the unified buffer.
      for (let i = 0; i < out.length; i++) {
        unified.push(eventToRow(out.at(i)!, out.schema));
      }
      const unsub = out.on('event', (event) => {
        unified.push(eventToRow(event, out.schema));
      });
      this.#disposers.add(unsub);
    };

    for (const partition of this.#partitions.values()) {
      wirePartition(partition as LiveSeries<S>);
    }
    this.#onSpawn.add((_, partition) => {
      wirePartition(partition as LiveSeries<S>);
    });

    return unified;
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
