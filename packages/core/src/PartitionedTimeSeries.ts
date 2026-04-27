import { TimeSeries } from './TimeSeries.js';
import type { BoundedSequence } from './BoundedSequence.js';
import type { Sequence } from './Sequence.js';
import type { DurationInput } from './utils/duration.js';
import type { TemporalLike } from './temporal.js';
import type {
  AggregateMap,
  AggregateOutputMap,
  AggregateSchema,
  AlignSchema,
  BaselineSchema,
  DedupeKeep,
  DiffSchema,
  EventDataForSchema,
  EventForSchema,
  FillMapping,
  FillStrategy,
  MaterializeSchema,
  NumericColumnNameForSchema,
  RollingAlignment,
  RollingSchema,
  SeriesSchema,
  SmoothAppendSchema,
  SmoothMethod,
  SmoothSchema,
} from './types.js';
import type {
  AggregateOutputMapResultSchema,
  RollingOutputMapSchema,
} from './types-aggregate.js';

type SequenceLike = Sequence | BoundedSequence;
type AlignMethod = 'hold' | 'linear';
type AlignSample = 'begin' | 'center' | 'end';

/**
 * View over a `TimeSeries` that scopes stateful transforms to within
 * each partition. Created by `TimeSeries.partitionBy(by)`.
 *
 * Most pond-ts stateful operators read from neighboring events when
 * computing each output. On a multi-entity series (events for many
 * hosts interleaved by time), those neighbors silently cross entity
 * boundaries: a `fill('linear')` for `host-A` would interpolate using
 * `host-B`'s value as a "neighbor"; a `rolling('5m', { cpu: 'avg' })`
 * would average across all hosts in the window.
 *
 * `partitionBy` runs the transform independently on each partition's
 * events. The view is **persistent across chains** — each sugar method
 * returns another `PartitionedTimeSeries` carrying the same partition
 * columns, so multi-step per-partition workflows compose cleanly:
 *
 * ```ts
 * const cleaned = ts
 *   .partitionBy('host')
 *   .dedupe({ keep: 'last' })   // per-host
 *   .fill({ cpu: 'linear' })    // per-host
 *   .rolling('5m', { cpu: 'avg' })  // per-host
 *   .collect();                 // back to TimeSeries<S>
 * ```
 *
 * Call `.collect()` (or `.apply(fn)` for arbitrary transforms) to
 * materialize back to a regular `TimeSeries`. Without `.collect()`,
 * the chain stays in partition view.
 *
 * @example
 * ```ts
 * // Per-host fill
 * const filled = series.partitionBy('host').fill({ cpu: 'linear' }).collect();
 *
 * // Composite partitioning by host + region
 * const filled = series.partitionBy(['host', 'region']).fill({ cpu: 'linear' }).collect();
 *
 * // Arbitrary transform via apply (terminal — returns TimeSeries directly)
 * const custom = series.partitionBy('host').apply(g =>
 *   g.fill({ cpu: 'linear' }).rolling('5m', { cpu: 'avg' }),
 * );
 * ```
 */
export class PartitionedTimeSeries<S extends SeriesSchema> {
  readonly source: TimeSeries<S>;
  readonly by: ReadonlyArray<keyof EventDataForSchema<S> & string>;

  constructor(
    source: TimeSeries<S>,
    by:
      | (keyof EventDataForSchema<S> & string)
      | ReadonlyArray<keyof EventDataForSchema<S> & string>,
  ) {
    this.source = source;
    this.by = (Array.isArray(by) ? by : [by]) as ReadonlyArray<
      keyof EventDataForSchema<S> & string
    >;
    if (this.by.length === 0) {
      throw new TypeError(
        'PartitionedTimeSeries requires at least one partition column.',
      );
    }
    for (const col of this.by) {
      if (!source.schema.some((c) => c.name === col)) {
        throw new TypeError(
          `PartitionedTimeSeries: column "${String(col)}" not in schema`,
        );
      }
    }
  }

  /**
   * Materialize the partitioned view back into a regular `TimeSeries`.
   * Terminal operation — call this at the end of a chain to "collect"
   * the per-partition results. Equivalent to `.apply(g => g)` but
   * cheaper (no fn dispatch, just returns the source as-is).
   *
   * @example
   * ```ts
   * const cleaned = ts
   *   .partitionBy('host')
   *   .fill({ cpu: 'linear' })
   *   .rolling('5m', { cpu: 'avg' })
   *   .collect();  // <- TimeSeries<S>
   * ```
   */
  collect(): TimeSeries<S> {
    return this.source;
  }

  /**
   * Run a transform `fn` independently on each partition and return a
   * `TimeSeries<R>` directly (terminal — does not stay in the
   * partitioned view). The escape hatch for compositions or operators
   * not exposed as sugar.
   *
   * To keep the partition after a custom transform, use the sugar
   * methods (which preserve partition state) or call `.partitionBy(...)`
   * again on the result.
   *
   * @example
   * ```ts
   * // chain two stateful ops within each partition (one shot)
   * const out = series.partitionBy('host').apply(g =>
   *   g.fill({ cpu: 'linear' }).rolling('5m', { cpu: 'avg' }),
   * );
   * ```
   */
  apply<R extends SeriesSchema>(
    fn: (group: TimeSeries<S>) => TimeSeries<R>,
  ): TimeSeries<R> {
    return PartitionedTimeSeries.applyToSource(this.source, this.by, fn);
  }

  // Internal helper used by both `apply` (terminal) and the sugar
  // methods (which re-wrap the result back into a partitioned view).
  private static applyToSource<SX extends SeriesSchema, R extends SeriesSchema>(
    source: TimeSeries<SX>,
    by: ReadonlyArray<keyof EventDataForSchema<SX> & string>,
    fn: (group: TimeSeries<SX>) => TimeSeries<R>,
  ): TimeSeries<R> {
    // Empty source: apply fn to an empty group so the output schema
    // and name come from fn, not from inferring R structurally.
    if (source.events.length === 0) {
      const empty = TimeSeries.fromEvents(
        [] as ReadonlyArray<EventForSchema<SX>>,
        {
          schema: source.schema,
          name: source.name,
        },
      );
      return fn(empty);
    }

    const compositeKey = (event: EventForSchema<SX>): string => {
      const data = event.data() as Record<string, unknown>;
      // Single-column case: avoid the encoding overhead.
      if (by.length === 1) {
        const v = data[by[0]!];
        return v === undefined ? ' undefined' : `${String(v)}`;
      }
      // Multi-column case: JSON.stringify guarantees no collision
      // because it encodes strings with quotes and escapes. A naive
      // separator approach (e.g. join('|')) would collide on values
      // containing the separator.
      const parts: unknown[] = new Array(by.length);
      for (let i = 0; i < by.length; i += 1) {
        parts[i] = data[by[i]!] ?? null;
      }
      return JSON.stringify(parts);
    };

    const buckets = new Map<string, EventForSchema<SX>[]>();
    for (const event of source.events) {
      const key = compositeKey(event);
      let bucket = buckets.get(key);
      if (!bucket) {
        bucket = [];
        buckets.set(key, bucket);
      }
      bucket.push(event);
    }

    const transformed: TimeSeries<R>[] = [];
    for (const events of buckets.values()) {
      const sub = TimeSeries.fromEvents(events, {
        schema: source.schema,
        name: source.name,
      });
      transformed.push(fn(sub));
    }

    return TimeSeries.concat(transformed);
  }

  // Wrap a transform result back into a PartitionedTimeSeries with the
  // same partition columns. Used by the sugar methods to keep the chain
  // in partition view. Cast at the boundary because R may not preserve
  // the partition columns type-narrowly (e.g. RollingSchema<S, M> may
  // drop columns); runtime constructor validates that the partition
  // columns are still present in the result schema.
  private rewrap<R extends SeriesSchema>(
    out: TimeSeries<R>,
  ): PartitionedTimeSeries<R> {
    return new PartitionedTimeSeries(
      out,
      this.by as unknown as ReadonlyArray<keyof EventDataForSchema<R> & string>,
    );
  }

  // ─── Sugar: stateful ops, applied per partition ─────────────────────
  //
  // Each method's overload signatures mirror the corresponding
  // `TimeSeries` method but return `PartitionedTimeSeries<NewSchema>`
  // instead of `TimeSeries<NewSchema>`, so the chain stays in partition
  // view. Call `.collect()` to materialize back. Each impl runs the
  // underlying op per-partition via `applyToSource` and re-wraps.

  /** Per-partition `fill`. See {@link TimeSeries.fill}. */
  fill(
    strategy: FillStrategy | FillMapping<S>,
    options?: { limit?: number; maxGap?: DurationInput },
  ): PartitionedTimeSeries<S> {
    return this.rewrap(
      PartitionedTimeSeries.applyToSource(this.source, this.by, (g) =>
        g.fill(strategy, options),
      ),
    );
  }

  /**
   * Per-partition `dedupe`. The duplicate key becomes "same partition
   * columns AND same timestamp" — `partitionBy` provides the partition
   * segregation, `dedupe` handles the within-partition timestamp
   * collapse. The most common dedupe shape for multi-entity ingest.
   *
   * See {@link TimeSeries.dedupe}.
   */
  dedupe(options?: { keep?: DedupeKeep<S> }): PartitionedTimeSeries<S> {
    return this.rewrap(
      PartitionedTimeSeries.applyToSource(this.source, this.by, (g) =>
        g.dedupe(options),
      ),
    );
  }

  /** Per-partition `align`. See {@link TimeSeries.align}. */
  align(
    sequence: SequenceLike,
    options?: {
      method?: AlignMethod;
      sample?: AlignSample;
      range?: TemporalLike;
    },
  ): PartitionedTimeSeries<AlignSchema<S>> {
    return this.rewrap(
      PartitionedTimeSeries.applyToSource(this.source, this.by, (g) =>
        g.align(sequence, options),
      ),
    );
  }

  /**
   * Per-partition `materialize`. See {@link TimeSeries.materialize}.
   *
   * **Bonus over the bare `TimeSeries.materialize` call:** every
   * output row, including empty-bucket rows, gets the partition
   * columns auto-populated from the partition's known key values.
   * Without this, empty buckets would emit rows with `undefined`
   * partition columns — forcing a follow-up
   * `.fill({ host: 'hold' })` step that fails for partitions where
   * every event sits in a long-outage gap.
   */
  materialize(
    sequence: SequenceLike,
    options?: {
      sample?: AlignSample;
      select?: 'first' | 'last' | 'nearest';
      range?: TemporalLike;
    },
  ): PartitionedTimeSeries<MaterializeSchema<S>> {
    const partitionCols = this.by as ReadonlyArray<string>;
    return this.rewrap(
      PartitionedTimeSeries.applyToSource(this.source, this.by, (g) => {
        const out = g.materialize(sequence, options);
        if (g.events.length === 0) return out;

        // Detect whether any output row needs partition-column patching
        // (i.e., whether any bucket was empty). If the source covered
        // the grid, every row already carries the partition columns
        // from its source event — skip the map() pass entirely. This
        // avoids the per-event closure-call + new event allocation
        // cost when no patching is required.
        const events = out.events;
        let needsPatch = false;
        outer: for (let i = 0; i < events.length; i += 1) {
          const data = events[i]!.data() as Record<string, unknown>;
          for (let c = 0; c < partitionCols.length; c += 1) {
            if (data[partitionCols[c]!] === undefined) {
              needsPatch = true;
              break outer;
            }
          }
        }
        if (!needsPatch) return out;

        // Patch partition columns where undefined (empty-bucket rows).
        // All events in this partition share the partition columns —
        // capture them once from the first source event.
        const firstData = g.events[0]!.data() as Record<string, unknown>;
        const partValues: Record<string, unknown> = {};
        for (const col of partitionCols) {
          partValues[col] = firstData[col];
        }
        return out.map(out.schema, (event) => {
          const data = event.data() as Record<string, unknown>;
          let result = event;
          for (const col of partitionCols) {
            if (data[col] === undefined) {
              // eslint-disable-next-line @typescript-eslint/no-explicit-any
              result = (result as any).set(col, partValues[col]);
            }
          }
          return result;
        });
      }),
    );
  }

  /** Per-partition `rolling`. See {@link TimeSeries.rolling}. */
  rolling<const Mapping extends AggregateMap<S>>(
    window: DurationInput,
    mapping: Mapping,
    options?: { alignment?: RollingAlignment },
  ): PartitionedTimeSeries<RollingSchema<S, Mapping>>;
  rolling<const Mapping extends AggregateOutputMap<S>>(
    window: DurationInput,
    mapping: Mapping,
    options?: { alignment?: RollingAlignment },
  ): PartitionedTimeSeries<RollingOutputMapSchema<S, Mapping>>;
  rolling<const Mapping extends AggregateMap<S>>(
    sequence: SequenceLike,
    window: DurationInput,
    mapping: Mapping,
    options?: {
      alignment?: RollingAlignment;
      sample?: AlignSample;
      range?: TemporalLike;
    },
  ): PartitionedTimeSeries<AggregateSchema<S, Mapping>>;
  rolling<const Mapping extends AggregateOutputMap<S>>(
    sequence: SequenceLike,
    window: DurationInput,
    mapping: Mapping,
    options?: {
      alignment?: RollingAlignment;
      sample?: AlignSample;
      range?: TemporalLike;
    },
  ): PartitionedTimeSeries<AggregateOutputMapResultSchema<S, Mapping>>;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  rolling(...args: any[]): any {
    return this.rewrap(
      PartitionedTimeSeries.applyToSource(this.source, this.by, (g) =>
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (g.rolling as any)(...args),
      ),
    );
  }

  /** Per-partition `smooth`. See {@link TimeSeries.smooth}. */
  smooth<
    const Target extends NumericColumnNameForSchema<S>,
    const Output extends string | undefined = undefined,
  >(
    column: Target,
    method: SmoothMethod,
    options:
      | { alpha: number; warmup?: number; output?: Output }
      | { window: DurationInput; alignment?: RollingAlignment; output?: Output }
      | { span: number; output?: Output },
  ): PartitionedTimeSeries<
    Output extends string
      ? SmoothAppendSchema<S, Output>
      : SmoothSchema<S, Target>
  > {
    return this.rewrap(
      PartitionedTimeSeries.applyToSource(this.source, this.by, (g) =>
        g.smooth(column, method, options),
      ),
    );
  }

  /** Per-partition `baseline`. See {@link TimeSeries.baseline}. */
  baseline<
    const Col extends NumericColumnNameForSchema<S>,
    const AvgName extends string = 'avg',
    const SdName extends string = 'sd',
    const UpperName extends string = 'upper',
    const LowerName extends string = 'lower',
  >(
    col: Col,
    options: {
      window: DurationInput;
      sigma: number;
      alignment?: RollingAlignment;
      names?: {
        avg?: AvgName;
        sd?: SdName;
        upper?: UpperName;
        lower?: LowerName;
      };
    },
  ): PartitionedTimeSeries<
    BaselineSchema<S, AvgName, SdName, UpperName, LowerName>
  > {
    return this.rewrap(
      PartitionedTimeSeries.applyToSource(this.source, this.by, (g) =>
        g.baseline(col, options),
      ),
    );
  }

  /** Per-partition `outliers`. See {@link TimeSeries.outliers}. */
  outliers<const Col extends NumericColumnNameForSchema<S>>(
    col: Col,
    options: {
      window: DurationInput;
      sigma: number;
      alignment?: RollingAlignment;
    },
  ): PartitionedTimeSeries<S> {
    return this.rewrap(
      PartitionedTimeSeries.applyToSource(this.source, this.by, (g) =>
        g.outliers(col, options),
      ),
    );
  }

  /** Per-partition `diff`. See {@link TimeSeries.diff}. */
  diff<const Target extends NumericColumnNameForSchema<S>>(
    columns: Target | readonly Target[],
    options?: { drop?: boolean },
  ): PartitionedTimeSeries<DiffSchema<S, Target>> {
    return this.rewrap(
      PartitionedTimeSeries.applyToSource(this.source, this.by, (g) =>
        g.diff(columns, options),
      ),
    );
  }

  /** Per-partition `rate`. See {@link TimeSeries.rate}. */
  rate<const Target extends NumericColumnNameForSchema<S>>(
    columns: Target | readonly Target[],
    options?: { drop?: boolean },
  ): PartitionedTimeSeries<DiffSchema<S, Target>> {
    return this.rewrap(
      PartitionedTimeSeries.applyToSource(this.source, this.by, (g) =>
        g.rate(columns, options),
      ),
    );
  }

  /** Per-partition `pctChange`. See {@link TimeSeries.pctChange}. */
  pctChange<const Target extends NumericColumnNameForSchema<S>>(
    columns: Target | readonly Target[],
    options?: { drop?: boolean },
  ): PartitionedTimeSeries<DiffSchema<S, Target>> {
    return this.rewrap(
      PartitionedTimeSeries.applyToSource(this.source, this.by, (g) =>
        g.pctChange(columns, options),
      ),
    );
  }

  /** Per-partition `cumulative`. See {@link TimeSeries.cumulative}. */
  cumulative<const Targets extends NumericColumnNameForSchema<S>>(spec: {
    [K in Targets]:
      | 'sum'
      | 'max'
      | 'min'
      | 'count'
      | ((acc: number, value: number) => number);
  }): PartitionedTimeSeries<DiffSchema<S, Targets>> {
    return this.rewrap(
      PartitionedTimeSeries.applyToSource(this.source, this.by, (g) =>
        g.cumulative(spec),
      ),
    );
  }

  /** Per-partition `shift`. See {@link TimeSeries.shift}. */
  shift<const Target extends NumericColumnNameForSchema<S>>(
    columns: Target | readonly Target[],
    n: number,
  ): PartitionedTimeSeries<DiffSchema<S, Target>> {
    return this.rewrap(
      PartitionedTimeSeries.applyToSource(this.source, this.by, (g) =>
        g.shift(columns, n),
      ),
    );
  }

  /** Per-partition `aggregate`. See {@link TimeSeries.aggregate}. */
  aggregate<const Mapping extends AggregateMap<S>>(
    sequence: SequenceLike,
    mapping: Mapping,
    options?: { range?: TemporalLike },
  ): PartitionedTimeSeries<AggregateSchema<S, Mapping>>;
  aggregate<const Mapping extends AggregateOutputMap<S>>(
    sequence: SequenceLike,
    mapping: Mapping,
    options?: { range?: TemporalLike },
  ): PartitionedTimeSeries<AggregateOutputMapResultSchema<S, Mapping>>;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  aggregate(...args: any[]): any {
    return this.rewrap(
      PartitionedTimeSeries.applyToSource(this.source, this.by, (g) =>
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (g.aggregate as any)(...args),
      ),
    );
  }
}
