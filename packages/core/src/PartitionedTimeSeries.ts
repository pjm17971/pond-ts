import { TimeSeries } from './TimeSeries.js';
import type { BoundedSequence } from './BoundedSequence.js';
import type { Sequence } from './Sequence.js';
import type { DurationInput } from './utils/duration.js';
import type { TemporalLike } from './temporal.js';
import type {
  AggregateMap,
  AggregateOutputMap,
  AggregateSchema,
  BaselineSchema,
  DiffSchema,
  EventDataForSchema,
  EventForSchema,
  FillMapping,
  FillStrategy,
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
 * View over a `TimeSeries` that scopes stateful transforms to within each
 * partition (defined by one or more value-column values). Created by
 * `TimeSeries.partitionBy(by)`.
 *
 * Most pond-ts stateful operators — `fill`, `align`, `rolling`, `smooth`,
 * `baseline`, `outliers`, `diff`, `rate`, `pctChange`, `cumulative`,
 * `shift`, `aggregate` — read from neighboring events when computing each
 * output. When a series interleaves multiple entities (host, region,
 * device, …), those neighbors silently cross entity boundaries: a
 * `fill('linear')` for `host-A` would interpolate using `host-B`'s
 * value as a "neighbor", and a `rolling('5m', { cpu: 'avg' })` would
 * average across all hosts in the window.
 *
 * `partitionBy` runs the transform independently on each partition's
 * events and reassembles the output back into one `TimeSeries`. The
 * return type is always `TimeSeries`, not `PartitionedTimeSeries` —
 * each operation is a single step. Re-`partitionBy` after to chain
 * another partitioned op; this is by design (chains that cross the
 * partition boundary are honest about what's per-partition vs.
 * cross-partition).
 *
 * For ops that aren't on the sugar surface (or for arbitrary
 * compositions), use {@link PartitionedTimeSeries.apply} — it runs `fn`
 * on each partition and reassembles.
 *
 * @example
 * ```ts
 * // per-host fill (no interpolation across hosts)
 * const filled = series.partitionBy('host').fill({ cpu: 'linear' });
 *
 * // per-host rolling avg
 * const smoothed = series.partitionBy('host').rolling('5m', { cpu: 'avg' });
 *
 * // composite partitioning by host + region
 * const filled = series.partitionBy(['host', 'region']).fill({ cpu: 'linear' });
 *
 * // arbitrary transform via apply()
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
   * Run a transform `fn` independently on each partition and reassemble
   * the outputs into one `TimeSeries`. The escape hatch for
   * compositions or operators not exposed as sugar on this view.
   *
   * @example
   * ```ts
   * // chain two stateful ops within each partition
   * const out = series.partitionBy('host').apply(g =>
   *   g.fill({ cpu: 'linear' }).rolling('5m', { cpu: 'avg' }),
   * );
   * ```
   */
  apply<R extends SeriesSchema>(
    fn: (group: TimeSeries<S>) => TimeSeries<R>,
  ): TimeSeries<R> {
    // Empty source: apply fn to an empty group so the output schema and
    // name come from fn, not from inferring R structurally.
    if (this.source.events.length === 0) {
      const empty = TimeSeries.fromEvents(
        [] as ReadonlyArray<EventForSchema<S>>,
        {
          schema: this.source.schema,
          name: this.source.name,
        },
      );
      return fn(empty);
    }

    const compositeKey = (event: EventForSchema<S>): string => {
      const data = event.data() as Record<string, unknown>;
      // Single-column case: avoid the join overhead.
      if (this.by.length === 1) {
        const v = data[this.by[0]!];
        return v === undefined ? 'undefined' : String(v);
      }
      const parts: string[] = new Array(this.by.length);
      for (let i = 0; i < this.by.length; i += 1) {
        const v = data[this.by[i]!];
        parts[i] = v === undefined ? 'undefined' : String(v);
      }
      // Use a separator unlikely to collide with stringified column values.
      return parts.join(' ');
    };

    const buckets = new Map<string, EventForSchema<S>[]>();
    for (const event of this.source.events) {
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
        schema: this.source.schema,
        name: this.source.name,
      });
      transformed.push(fn(sub));
    }

    return TimeSeries.concat(transformed);
  }

  // ─── Sugar: stateful ops, applied per partition ─────────────────────
  //
  // Each method's overload signatures mirror the corresponding
  // `TimeSeries` method exactly so callers see identical narrowing.
  // The implementation in each case is `this.apply(g => g.method(...))`.

  /** Per-partition `fill`. See {@link TimeSeries.fill}. */
  fill(
    strategy: FillStrategy | FillMapping<S>,
    options?: { limit?: number },
  ): TimeSeries<S> {
    return this.apply((g) => g.fill(strategy, options));
  }

  /** Per-partition `align`. See {@link TimeSeries.align}. */
  align(
    sequence: SequenceLike,
    options?: {
      method?: AlignMethod;
      sample?: AlignSample;
      range?: TemporalLike;
    },
  ): TimeSeries<S> {
    // Cast: AlignSchema<S> is structurally a TimeSeries<S> with the
    // value columns optionalized; concat preserves that.
    return this.apply((g) =>
      g.align(sequence, options),
    ) as unknown as TimeSeries<S>;
  }

  /** Per-partition `rolling`. See {@link TimeSeries.rolling}. */
  rolling<const Mapping extends AggregateMap<S>>(
    window: DurationInput,
    mapping: Mapping,
    options?: { alignment?: RollingAlignment },
  ): TimeSeries<RollingSchema<S, Mapping>>;
  rolling<const Mapping extends AggregateOutputMap<S>>(
    window: DurationInput,
    mapping: Mapping,
    options?: { alignment?: RollingAlignment },
  ): TimeSeries<RollingOutputMapSchema<S, Mapping>>;
  rolling<const Mapping extends AggregateMap<S>>(
    sequence: SequenceLike,
    window: DurationInput,
    mapping: Mapping,
    options?: {
      alignment?: RollingAlignment;
      sample?: AlignSample;
      range?: TemporalLike;
    },
  ): TimeSeries<AggregateSchema<S, Mapping>>;
  rolling<const Mapping extends AggregateOutputMap<S>>(
    sequence: SequenceLike,
    window: DurationInput,
    mapping: Mapping,
    options?: {
      alignment?: RollingAlignment;
      sample?: AlignSample;
      range?: TemporalLike;
    },
  ): TimeSeries<AggregateOutputMapResultSchema<S, Mapping>>;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  rolling(...args: any[]): any {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return this.apply((g) => (g.rolling as any)(...args));
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
  ): TimeSeries<
    Output extends string
      ? SmoothAppendSchema<S, Output>
      : SmoothSchema<S, Target>
  > {
    return this.apply((g) => g.smooth(column, method, options));
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
  ): TimeSeries<BaselineSchema<S, AvgName, SdName, UpperName, LowerName>> {
    return this.apply((g) => g.baseline(col, options));
  }

  /** Per-partition `outliers`. See {@link TimeSeries.outliers}. */
  outliers<const Col extends NumericColumnNameForSchema<S>>(
    col: Col,
    options: {
      window: DurationInput;
      sigma: number;
      alignment?: RollingAlignment;
    },
  ): TimeSeries<S> {
    return this.apply((g) => g.outliers(col, options));
  }

  /** Per-partition `diff`. See {@link TimeSeries.diff}. */
  diff<const Target extends NumericColumnNameForSchema<S>>(
    columns: Target | readonly Target[],
    options?: { drop?: boolean },
  ): TimeSeries<DiffSchema<S, Target>> {
    return this.apply((g) => g.diff(columns, options));
  }

  /** Per-partition `rate`. See {@link TimeSeries.rate}. */
  rate<const Target extends NumericColumnNameForSchema<S>>(
    columns: Target | readonly Target[],
    options?: { drop?: boolean },
  ): TimeSeries<DiffSchema<S, Target>> {
    return this.apply((g) => g.rate(columns, options));
  }

  /** Per-partition `pctChange`. See {@link TimeSeries.pctChange}. */
  pctChange<const Target extends NumericColumnNameForSchema<S>>(
    columns: Target | readonly Target[],
    options?: { drop?: boolean },
  ): TimeSeries<DiffSchema<S, Target>> {
    return this.apply((g) => g.pctChange(columns, options));
  }

  /** Per-partition `cumulative`. See {@link TimeSeries.cumulative}. */
  cumulative<const Targets extends NumericColumnNameForSchema<S>>(spec: {
    [K in Targets]:
      | 'sum'
      | 'max'
      | 'min'
      | 'count'
      | ((acc: number, value: number) => number);
  }): TimeSeries<DiffSchema<S, Targets>> {
    return this.apply((g) => g.cumulative(spec));
  }

  /** Per-partition `shift`. See {@link TimeSeries.shift}. */
  shift<const Target extends NumericColumnNameForSchema<S>>(
    columns: Target | readonly Target[],
    n: number,
  ): TimeSeries<DiffSchema<S, Target>> {
    return this.apply((g) => g.shift(columns, n));
  }

  /** Per-partition `aggregate`. See {@link TimeSeries.aggregate}. */
  aggregate<const Mapping extends AggregateMap<S>>(
    sequence: SequenceLike,
    mapping: Mapping,
    options?: { range?: TemporalLike },
  ): TimeSeries<AggregateSchema<S, Mapping>>;
  aggregate<const Mapping extends AggregateOutputMap<S>>(
    sequence: SequenceLike,
    mapping: Mapping,
    options?: { range?: TemporalLike },
  ): TimeSeries<AggregateOutputMapResultSchema<S, Mapping>>;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  aggregate(...args: any[]): any {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return this.apply((g) => (g.aggregate as any)(...args));
  }
}
