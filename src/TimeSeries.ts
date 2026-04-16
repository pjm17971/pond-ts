import type {
  AlignSchema,
  AggregateFunction,
  AggregateMap,
  AggregateSchema,
  CollapseSchema,
  EventDataForSchema,
  EventForSchema,
  FirstColKind,
  IntervalKeyedSchema,
  JsonObjectRowForSchema,
  JsonRowForSchema,
  JoinConflictMode,
  JoinManySchema,
  JoinSchema,
  JoinType,
  NumericColumnNameForSchema,
  NormalizedRowForSchema,
  PrefixedJoinManySchema,
  PrefixedJoinSchema,
  RenameMap,
  RenameSchema,
  RollingAlignment,
  RollingSchema,
  ScalarValue,
  SmoothMethod,
  SmoothAppendSchema,
  SmoothSchema,
  SelectSchema,
  SeriesSchema,
  TimeKeyedSchema,
  TimeSeriesJsonInput,
  TimeSeriesInput,
  TimeRangeKeyedSchema,
  ValueColumnsForSchema,
} from './types.js';
import { BoundedSequence } from './BoundedSequence.js';
import { parseTimestampString, type TimeZoneOptions } from './calendar.js';
import { Interval } from './Interval.js';
import { Time } from './Time.js';
import { TimeRange } from './TimeRange.js';
import type {
  EventKey,
  IntervalInput,
  IntervalValue,
  TemporalLike,
  TimeRangeInput,
  TimestampInput,
} from './temporal.js';
import { Sequence } from './Sequence.js';
import { validateAndNormalize } from './validate.js';
import type { DurationInput } from './Sequence.js';

type RangeLike = EventKey | TimeRangeInput | IntervalInput;
type BoundaryLike = EventKey | TimestampInput;
type KeyLike = EventKey | TimestampInput | TimeRangeInput | IntervalInput;
type SeriesRangeLike = TemporalLike | { timeRange(): TimeRange | undefined };
type AlignMethod = 'hold' | 'linear';
type AlignSample = 'begin' | 'center';
type SequenceLike = Sequence | BoundedSequence;
type ErrorJoinOptions = { type?: JoinType; onConflict?: 'error' };
type PrefixJoinOptions<Prefixes extends readonly string[]> = {
  type?: JoinType;
  onConflict: 'prefix';
  prefixes: Prefixes;
};
type JoinOptions = ErrorJoinOptions | PrefixJoinOptions<readonly string[]>;
type SeriesTuple = readonly [
  TimeSeries<SeriesSchema>,
  ...TimeSeries<SeriesSchema>[],
];
type SchemasForSeriesTuple<T extends SeriesTuple> = {
  [I in keyof T]: T[I] extends TimeSeries<infer Schema> ? Schema : never;
} extends infer Result
  ? Result extends readonly [SeriesSchema, ...SeriesSchema[]]
    ? Result
    : never
  : never;

function isObjectRow<S extends SeriesSchema>(
  value: JsonRowForSchema<S> | JsonObjectRowForSchema<S>,
): value is JsonObjectRowForSchema<S> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function parseJsonTimestamp(
  value: unknown,
  options: TimeZoneOptions = {},
): number {
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw new TypeError('expected finite timestamp');
    }
    return value;
  }
  if (typeof value === 'string') {
    return parseTimestampString(value, options);
  }
  if (value instanceof Date) {
    return value.getTime();
  }
  throw new TypeError('expected timestamp as number or string');
}

function parseJsonKey(
  kind: FirstColKind,
  value: unknown,
  options: TimeZoneOptions = {},
): EventKey {
  if (
    value instanceof Time ||
    value instanceof TimeRange ||
    value instanceof Interval
  ) {
    return value;
  }

  switch (kind) {
    case 'time':
      return new Time(parseJsonTimestamp(value, options));
    case 'timeRange':
      if (Array.isArray(value) && value.length === 2) {
        return new TimeRange({
          start: parseJsonTimestamp(value[0], options),
          end: parseJsonTimestamp(value[1], options),
        });
      }
      if (
        typeof value === 'object' &&
        value !== null &&
        'start' in value &&
        'end' in value &&
        !('value' in value)
      ) {
        return new TimeRange({
          start: parseJsonTimestamp(
            (value as { start: unknown }).start,
            options,
          ),
          end: parseJsonTimestamp((value as { end: unknown }).end, options),
        });
      }
      throw new TypeError(
        'expected timeRange as [start, end] or { start, end }',
      );
    case 'interval':
      if (Array.isArray(value) && value.length === 3) {
        return new Interval({
          value: value[0] as string | number,
          start: parseJsonTimestamp(value[1], options),
          end: parseJsonTimestamp(value[2], options),
        });
      }
      if (
        typeof value === 'object' &&
        value !== null &&
        'value' in value &&
        'start' in value &&
        'end' in value
      ) {
        return new Interval({
          value: (value as { value: string | number }).value,
          start: parseJsonTimestamp(
            (value as { start: unknown }).start,
            options,
          ),
          end: parseJsonTimestamp((value as { end: unknown }).end, options),
        });
      }
      throw new TypeError(
        'expected interval as [value, start, end] or { value, start, end }',
      );
  }
}

function parseJsonRows<S extends SeriesSchema>(
  schema: S,
  rows: TimeSeriesJsonInput<S>['rows'],
  options: TimeZoneOptions = {},
): TimeSeriesInput<S>['rows'] {
  return rows.map((row) => {
    const values = isObjectRow(row)
      ? schema.map((column) => row[column.name as keyof typeof row])
      : row;

    return Object.freeze(
      values.map((value, index) => {
        if (value === null) {
          return undefined;
        }

        const column = schema[index]!;
        if (index === 0) {
          return parseJsonKey(column.kind as FirstColKind, value, options);
        }
        return value;
      }),
    ) as TimeSeriesInput<S>['rows'][number];
  }) as TimeSeriesInput<S>['rows'];
}
type PrefixesForSeriesTuple<T extends SeriesTuple> = {
  [I in keyof T]: string;
} extends infer Result
  ? Result extends readonly [string, ...string[]]
    ? Result
    : never
  : never;

function toRows<S extends SeriesSchema>(
  schema: S,
  events: ReadonlyArray<EventForSchema<S>>,
): TimeSeriesInput<S>['rows'] {
  return events.map((event) => {
    const data = event.data();
    return Object.freeze([
      event.key(),
      ...schema
        .slice(1)
        .map((column) => data[column.name as keyof typeof data]),
    ]) as TimeSeriesInput<S>['rows'][number];
  }) as TimeSeriesInput<S>['rows'];
}

function isEventKey(value: unknown): value is EventKey {
  return (
    typeof value === 'object' &&
    value !== null &&
    'begin' in value &&
    'end' in value
  );
}

function toBoundaryTimestamp(value: BoundaryLike): number {
  if (isEventKey(value)) {
    return value.begin();
  }
  return value instanceof Date ? value.getTime() : value;
}

function toKey(value: KeyLike): EventKey {
  if (isEventKey(value)) {
    return value;
  }
  if (Array.isArray(value)) {
    if (value.length === 2) {
      return new TimeRange(value as TimeRangeInput);
    }
    return new Interval(value as IntervalInput);
  }
  if (typeof value === 'object' && value !== null) {
    if ('value' in value) {
      return new Interval(value as Extract<KeyLike, { value: unknown }>);
    }
    if ('start' in value && 'end' in value) {
      return new TimeRange(value as TimeRangeInput);
    }
  }
  return new Time(value as TimestampInput);
}

function toSelectionRange(value: RangeLike): TimeRange {
  if (value instanceof TimeRange) {
    return value;
  }
  if (value instanceof Interval) {
    return value.timeRange();
  }
  if (isEventKey(value)) {
    return new TimeRange({ start: value.begin(), end: value.end() });
  }
  if (Array.isArray(value)) {
    if (value.length === 2) {
      return new TimeRange(value as TimeRangeInput);
    }
    return new Interval(value as IntervalInput).timeRange();
  }
  if ('value' in value) {
    return new Interval(
      value as Extract<RangeLike, { value: unknown }>,
    ).timeRange();
  }
  return new TimeRange(value as TimeRangeInput);
}

function toOptionalSeriesRange(value: SeriesRangeLike): TimeRange | undefined {
  if (
    typeof value === 'object' &&
    value !== null &&
    'timeRange' in value &&
    typeof value.timeRange === 'function'
  ) {
    return value.timeRange() ?? undefined;
  }
  return toSelectionRange(value as RangeLike);
}

function makeAlignedSchema<S extends SeriesSchema>(schema: S): AlignSchema<S> {
  return Object.freeze([
    { name: 'interval', kind: 'interval' as const },
    ...schema.slice(1).map((column) => ({
      ...column,
      required: false as const,
    })),
  ]) as AlignSchema<S>;
}

function sampleTime(interval: Interval, sample: AlignSample): number {
  return sample === 'center'
    ? interval.begin() + interval.duration() / 2
    : interval.begin();
}

function eventAnchorTime(key: EventKey): number {
  return key instanceof Time ? key.begin() : key.timeRange().midpoint();
}

function loessAt(
  x: number,
  anchors: ReadonlyArray<number>,
  values: ReadonlyArray<number | undefined>,
  span: number,
): number | undefined {
  const points = anchors.flatMap((anchor, index) => {
    const value = values[index];
    return value === undefined ? [] : [{ x: anchor, y: value }];
  });

  if (points.length === 0) {
    return undefined;
  }

  if (points.length === 1) {
    return points[0]!.y;
  }

  const neighborCount = Math.max(
    2,
    Math.min(points.length, Math.ceil(span * points.length)),
  );
  const sortedDistances = points
    .map((point) => ({ point, distance: Math.abs(point.x - x) }))
    .sort((left, right) => left.distance - right.distance);
  const bandwidth = sortedDistances[neighborCount - 1]!.distance;

  if (bandwidth === 0) {
    const coincident = sortedDistances
      .filter((entry) => entry.distance === 0)
      .map((entry) => entry.point.y);
    return (
      coincident.reduce((sum, value) => sum + value, 0) / coincident.length
    );
  }

  let weightedCount = 0;
  let sumW = 0;
  let sumWX = 0;
  let sumWY = 0;
  let sumWXX = 0;
  let sumWXY = 0;

  for (const { point, distance } of sortedDistances.slice(0, neighborCount)) {
    const ratio = distance / bandwidth;
    const weight = ratio >= 1 ? 0 : (1 - ratio ** 3) ** 3;
    if (weight === 0) {
      continue;
    }
    weightedCount += 1;
    sumW += weight;
    sumWX += weight * point.x;
    sumWY += weight * point.y;
    sumWXX += weight * point.x * point.x;
    sumWXY += weight * point.x * point.y;
  }

  if (weightedCount === 0 || sumW === 0) {
    return undefined;
  }

  const denominator = sumW * sumWXX - sumWX * sumWX;
  if (Math.abs(denominator) < Number.EPSILON) {
    return sumWY / sumW;
  }

  const intercept = (sumWY * sumWXX - sumWX * sumWXY) / denominator;
  const slope = (sumW * sumWXY - sumWX * sumWY) / denominator;
  return intercept + slope * x;
}

function makeSmoothSchema<
  S extends SeriesSchema,
  Target extends NumericColumnNameForSchema<S>,
>(schema: S, target: Target): SmoothSchema<S, Target>;
function makeSmoothSchema<
  S extends SeriesSchema,
  Target extends NumericColumnNameForSchema<S>,
  Name extends string,
>(schema: S, target: Target, output: Name): SmoothAppendSchema<S, Name>;
function makeSmoothSchema<
  S extends SeriesSchema,
  Target extends NumericColumnNameForSchema<S>,
>(
  schema: S,
  target: Target,
  output?: string,
): SmoothSchema<S, Target> | SmoothAppendSchema<S, string> {
  if (output === undefined || output === target) {
    return Object.freeze([
      schema[0],
      ...schema.slice(1).map((column) =>
        column.name === target
          ? {
              name: column.name,
              kind: 'number' as const,
              required: false as const,
            }
          : column,
      ),
    ]) as unknown as SmoothSchema<S, Target>;
  }

  if (schema.slice(1).some((column) => column.name === output)) {
    throw new TypeError(`smooth output column '${output}' already exists`);
  }

  return Object.freeze([
    schema[0],
    ...schema.slice(1),
    { name: output, kind: 'number' as const, required: false as const },
  ]) as unknown as SmoothAppendSchema<S, string>;
}

function toBoundedSequence(
  sequence: SequenceLike,
  range: TemporalLike,
  sample: AlignSample,
): BoundedSequence {
  return sequence instanceof BoundedSequence
    ? sequence
    : sequence.bounded(range, { sample });
}

function isTimeKeyed<S extends SeriesSchema>(series: TimeSeries<S>): boolean {
  return series.firstColumnKind === 'time';
}

function bucketContainsHalfOpen(bucket: Interval, timestamp: number): boolean {
  return timestamp >= bucket.begin() && timestamp < bucket.end();
}

function bucketOverlapsHalfOpen(bucket: Interval, event: EventKey): boolean {
  if (event.begin() === event.end()) {
    return bucketContainsHalfOpen(bucket, event.begin());
  }
  return event.begin() < bucket.end() && bucket.begin() < event.end();
}

function aggregateValues(
  operation: AggregateFunction,
  values: ReadonlyArray<ScalarValue | undefined>,
): ScalarValue | undefined {
  const defined = values.filter(
    (value): value is ScalarValue => value !== undefined,
  );
  const numeric = defined.filter(
    (value): value is number => typeof value === 'number',
  );

  switch (operation) {
    case 'count':
      return defined.length;
    case 'sum':
      return numeric.reduce((sum, value) => sum + value, 0);
    case 'avg':
      return numeric.length === 0
        ? undefined
        : numeric.reduce((sum, value) => sum + value, 0) / numeric.length;
    case 'min':
      return numeric.length === 0
        ? undefined
        : numeric.reduce((left, right) => (left <= right ? left : right));
    case 'max':
      return numeric.length === 0
        ? undefined
        : numeric.reduce((left, right) => (left >= right ? left : right));
    case 'first':
      return defined[0];
    case 'last':
      return defined[defined.length - 1];
  }
}

function parseDurationInput(value: DurationInput): number {
  if (typeof value === 'number') {
    if (!Number.isFinite(value) || value <= 0) {
      throw new TypeError(
        'rolling window must be a positive finite number of milliseconds',
      );
    }
    return value;
  }

  const match = /^(\d+)(ms|s|m|h|d)$/.exec(value);
  if (!match) {
    throw new TypeError(`unsupported duration '${value}'`);
  }

  const amount = Number(match[1]);
  const unit = match[2];
  const multiplier =
    unit === 'ms'
      ? 1
      : unit === 's'
        ? 1_000
        : unit === 'm'
          ? 60_000
          : unit === 'h'
            ? 3_600_000
            : 86_400_000;
  return amount * multiplier;
}

function duplicateValueColumnNames(
  schemas: ReadonlyArray<SeriesSchema>,
): string[] {
  const counts = new Map<string, number>();
  for (const schema of schemas) {
    for (const column of schema.slice(1)) {
      counts.set(column.name, (counts.get(column.name) ?? 0) + 1);
    }
  }
  return [...counts.entries()]
    .filter(([, count]) => count > 1)
    .map(([name]) => name)
    .sort();
}

function assertDistinctValueColumns(
  schemas: ReadonlyArray<SeriesSchema>,
  message: string,
): void {
  const seen = new Set<string>();
  const duplicates = new Set<string>();
  for (const schema of schemas) {
    for (const column of schema.slice(1)) {
      if (seen.has(column.name)) {
        duplicates.add(column.name);
      } else {
        seen.add(column.name);
      }
    }
  }
  if (duplicates.size > 0) {
    throw new TypeError(`${message}: ${[...duplicates].sort().join(', ')}`);
  }
}

function buildConflictRenameMap(
  schema: SeriesSchema,
  duplicates: ReadonlySet<string>,
  prefix: string,
): Partial<Record<string, string>> {
  const renameMap: Partial<Record<string, string>> = {};
  for (const column of schema.slice(1)) {
    if (duplicates.has(column.name)) {
      renameMap[column.name] = `${prefix}_${column.name}`;
    }
  }
  return renameMap;
}

function prepareSeriesForJoin<T extends SeriesTuple>(
  series: T,
  options: JoinOptions,
): T {
  const conflictMode: JoinConflictMode = options.onConflict ?? 'error';
  const duplicates = duplicateValueColumnNames(
    series.map((item) => item.schema),
  );

  if (duplicates.length === 0) {
    return series;
  }

  if (conflictMode === 'error') {
    throw new TypeError(
      `cannot join series with duplicate column names: ${duplicates.join(', ')}`,
    );
  }

  const prefixOptions = options as PrefixJoinOptions<readonly string[]>;

  if (prefixOptions.prefixes.length !== series.length) {
    throw new TypeError(
      `prefix conflict handling requires exactly ${series.length} prefixes`,
    );
  }

  const duplicateSet = new Set(duplicates);
  const renamedSeries = series.map((item, index) => {
    const renameMap = buildConflictRenameMap(
      item.schema,
      duplicateSet,
      prefixOptions.prefixes[index]!,
    );
    return item.rename(renameMap);
  }) as unknown as T;

  assertDistinctValueColumns(
    renamedSeries.map((item) => item.schema),
    'prefix conflict handling still produced duplicate column names',
  );

  return renamedSeries;
}

/**
 * An immutable ordered collection of typed events sharing a common schema.
 *
 * @example
 * ```ts
 * const schema = [
 *   { name: "time", kind: "time" },
 *   { name: "cpu", kind: "number" },
 *   { name: "host", kind: "string" },
 * ] as const;
 *
 * const series = new TimeSeries({
 *   name: "cpu-usage",
 *   schema,
 *   rows: [[new Date("2025-01-01T00:00:00.000Z"), 0.42, "api-1"]],
 * });
 *
 * series.first()?.get("cpu"); // 0.42
 * series.timeRange(); // overall extent of the series
 * series.within(new TimeRange({ start: 0, end: Date.now() })); // fully contained events
 * series.align(Sequence.every("1m")); // uses the series range over an epoch-anchored minute grid
 * ```
 */
export class TimeSeries<S extends SeriesSchema> {
  readonly name: string;
  readonly schema: S;
  readonly events: ReadonlyArray<EventForSchema<S>>;

  /**
   * Example: `TimeSeries.joinMany([cpu.align(seq), memory.align(seq), errors.align(seq)])`.
   * Performs an exact-key n-ary join across many series.
   *
   * Use `join(...)` for the binary case and `joinMany(...)` when you want to build one wide series
   * from several aligned or aggregated inputs. This avoids repeated manual pairwise joins in
   * feature-building, reporting, and dashboard pipelines.
   *
   * Defaults:
   * - `type`: `"outer"`
   * - `onConflict`: `"error"`
   */
  static joinMany<const T extends SeriesTuple>(
    series: T,
    options?: ErrorJoinOptions,
  ): TimeSeries<JoinManySchema<SchemasForSeriesTuple<T>>>;
  static joinMany<
    const T extends SeriesTuple,
    const Prefixes extends PrefixesForSeriesTuple<T>,
  >(
    series: T,
    options: PrefixJoinOptions<Prefixes>,
  ): TimeSeries<PrefixedJoinManySchema<SchemasForSeriesTuple<T>, Prefixes>>;
  static joinMany<const T extends SeriesTuple>(
    series: T,
    options: JoinOptions = {},
  ): TimeSeries<SeriesSchema> {
    const prepared = prepareSeriesForJoin(
      series as unknown as SeriesTuple,
      options,
    );
    const [first, ...rest] = prepared;
    let joined: TimeSeries<SeriesSchema> = first;

    for (const next of rest) {
      joined =
        options.type === undefined
          ? (joined.join(next) as unknown as TimeSeries<SeriesSchema>)
          : (joined.join(next, {
              type: options.type,
            }) as unknown as TimeSeries<SeriesSchema>);
    }

    return joined;
  }

  /**
   * Example: `TimeSeries.fromJSON({ name, schema, rows, parse: { timeZone: "Europe/Madrid" } })`.
   * Creates a typed series from JSON-style row arrays or object rows keyed by schema column names.
   *
   * `null` values are treated as missing values. Ambiguous local timestamp strings are parsed using
   * the supplied `parse.timeZone`, which defaults to `UTC`.
   */
  static fromJSON<S extends SeriesSchema>(
    input: TimeSeriesJsonInput<S> & { parse?: TimeZoneOptions },
  ): TimeSeries<S> {
    return new TimeSeries({
      name: input.name,
      schema: input.schema,
      rows: parseJsonRows(input.schema, input.rows, input.parse),
    });
  }

  /** Example: `new TimeSeries({ name, schema, rows })`. Creates an immutable time series from a schema and row-oriented input data. */
  constructor(input: TimeSeriesInput<S>) {
    this.name = input.name;
    this.schema = Object.freeze(input.schema.slice()) as S;
    this.events = validateAndNormalize(input);
    Object.freeze(this);
  }

  /** Example: `series.firstColumnKind`. Returns the first-column kind from the series schema. */
  get firstColumnKind(): FirstColKind {
    return this.schema[0]!.kind;
  }

  /** Example: `series.rows`. Returns the normalized row view of the series. */
  get rows(): ReadonlyArray<NormalizedRowForSchema<S>> {
    return toRows(this.schema, this.events) as ReadonlyArray<
      NormalizedRowForSchema<S>
    >;
  }

  /** Example: `series.at(0)`. Returns the event at the supplied zero-based position, if present. */
  at(index: number): EventForSchema<S> | undefined {
    return this.events[index];
  }

  /** Example: `series.first()`. Returns the first event in the series, if present. */
  first(): EventForSchema<S> | undefined {
    return this.at(0);
  }

  /** Example: `series.last()`. Returns the last event in the series, if present. */
  last(): EventForSchema<S> | undefined {
    return this.events.length === 0
      ? undefined
      : this.events[this.events.length - 1];
  }

  /** Example: `series.map(nextSchema, event => event)`. Maps each event into a new typed schema and returns a new series. */
  map<NextSchema extends SeriesSchema>(
    schema: NextSchema,
    mapper: (
      event: EventForSchema<S>,
      index: number,
    ) => EventForSchema<NextSchema>,
  ): TimeSeries<NextSchema> {
    const mappedEvents = this.events.map((event, index) =>
      mapper(event, index),
    );

    return new TimeSeries({
      name: this.name,
      schema,
      rows: toRows(schema, mappedEvents),
    });
  }

  /** Example: `series.asTime({ at: "center" })`. Converts the series key type to `"time"` using the supplied anchor within each event extent. */
  asTime(
    options: { at?: 'begin' | 'center' | 'end' } = {},
  ): TimeSeries<TimeKeyedSchema<S>> {
    const schema = Object.freeze([
      { name: 'time', kind: 'time' as const },
      ...this.schema.slice(1),
    ]) as TimeKeyedSchema<S>;

    return new TimeSeries({
      name: this.name,
      schema,
      rows: toRows(
        schema,
        this.events.map((event) => event.asTime(options)) as EventForSchema<
          typeof schema
        >[],
      ),
    });
  }

  /** Example: `series.asTimeRange()`. Converts the series key type to `"timeRange"` while preserving each event extent. */
  asTimeRange(): TimeSeries<TimeRangeKeyedSchema<S>> {
    const schema = Object.freeze([
      { name: 'timeRange', kind: 'timeRange' as const },
      ...this.schema.slice(1),
    ]) as TimeRangeKeyedSchema<S>;

    return new TimeSeries({
      name: this.name,
      schema,
      rows: toRows(
        schema,
        this.events.map((event) => event.asTimeRange()) as EventForSchema<
          typeof schema
        >[],
      ),
    });
  }

  /** Example: `series.asInterval(event => event.begin())`. Converts the series key type to `"interval"` while preserving each event extent and supplying interval labels. */
  asInterval(value: IntervalValue): TimeSeries<IntervalKeyedSchema<S>>;
  asInterval(
    value: (event: EventForSchema<S>, index: number) => IntervalValue,
  ): TimeSeries<IntervalKeyedSchema<S>>;
  asInterval(
    value:
      | IntervalValue
      | ((event: EventForSchema<S>, index: number) => IntervalValue),
  ): TimeSeries<IntervalKeyedSchema<S>> {
    const schema = Object.freeze([
      { name: 'interval', kind: 'interval' as const },
      ...this.schema.slice(1),
    ]) as IntervalKeyedSchema<S>;
    const nextEvents = this.events.map((event, index) => {
      return typeof value === 'function'
        ? event.asInterval(() => value(event, index))
        : event.asInterval(value);
    }) as EventForSchema<typeof schema>[];

    return new TimeSeries({
      name: this.name,
      schema,
      rows: toRows(schema, nextEvents),
    });
  }

  /**
   * Example: `left.join(right, { type: "left" })`.
   * Performs an exact-key join of two series with the same key kind.
   *
   * Join types:
   * - `"outer"`: keep keys from either side
   * - `"left"`: keep all keys from the left series
   * - `"right"`: keep all keys from the right series
   * - `"inner"`: keep only keys present on both sides
   *
   * Defaults:
   * - `type`: `"outer"`
   * - `onConflict`: `"error"`
   *
   * Value columns from both series are included in the result and are optional because joined rows
   * may have missing values on either side. If both series use the same payload column name,
   * you can either rename one side before joining or use `{ onConflict: "prefix", prefixes: [...] }`.
   */
  join<Other extends SeriesSchema>(
    other: TimeSeries<Other>,
    options?: ErrorJoinOptions,
  ): TimeSeries<JoinSchema<S, Other>>;
  join<
    Other extends SeriesSchema,
    const Prefixes extends readonly [string, string],
  >(
    other: TimeSeries<Other>,
    options: PrefixJoinOptions<Prefixes>,
  ): TimeSeries<PrefixedJoinSchema<S, Other, Prefixes>>;
  join<Other extends SeriesSchema>(
    other: TimeSeries<Other>,
    options: JoinOptions = {},
  ): TimeSeries<SeriesSchema> {
    const [left, right] = prepareSeriesForJoin([this, other], options) as [
      TimeSeries<SeriesSchema>,
      TimeSeries<SeriesSchema>,
    ];
    const joinType = options.type ?? 'outer';

    if (left.firstColumnKind !== right.firstColumnKind) {
      throw new TypeError('cannot join series with different key kinds');
    }

    const resultSchema = Object.freeze([
      left.schema[0],
      ...left.schema
        .slice(1)
        .map((column) => ({ ...column, required: false as const })),
      ...right.schema
        .slice(1)
        .map((column) => ({ ...column, required: false as const })),
    ]) as unknown as SeriesSchema;

    const joinedEvents: EventForSchema<SeriesSchema>[] = [];
    let leftIndex = 0;
    let rightIndex = 0;

    while (leftIndex < left.events.length || rightIndex < right.events.length) {
      const leftEvent = left.events[leftIndex];
      const rightEvent = right.events[rightIndex];

      if (leftEvent && !rightEvent) {
        if (joinType === 'left' || joinType === 'outer') {
          joinedEvents.push(
            leftEvent.merge({}) as unknown as EventForSchema<SeriesSchema>,
          );
        }
        leftIndex += 1;
        continue;
      }

      if (rightEvent && !leftEvent) {
        if (joinType === 'right' || joinType === 'outer') {
          joinedEvents.push(
            rightEvent.merge({}) as unknown as EventForSchema<SeriesSchema>,
          );
        }
        rightIndex += 1;
        continue;
      }

      const comparison = leftEvent!.key().compare(rightEvent!.key());
      if (comparison === 0) {
        joinedEvents.push(
          leftEvent!.merge(
            rightEvent!.data(),
          ) as unknown as EventForSchema<SeriesSchema>,
        );
        leftIndex += 1;
        rightIndex += 1;
      } else if (comparison < 0) {
        if (joinType === 'left' || joinType === 'outer') {
          joinedEvents.push(
            leftEvent!.merge({}) as unknown as EventForSchema<SeriesSchema>,
          );
        }
        leftIndex += 1;
      } else {
        if (joinType === 'right' || joinType === 'outer') {
          joinedEvents.push(
            rightEvent!.merge({}) as unknown as EventForSchema<SeriesSchema>,
          );
        }
        rightIndex += 1;
      }
    }

    return new TimeSeries({
      name: left.name,
      schema: resultSchema,
      rows: toRows(resultSchema, joinedEvents),
    });
  }

  /**
   * Example: `series.align(Sequence.every("1m"))`.
   * Aligns the series onto a `Sequence` grid or `BoundedSequence` and returns an interval-keyed series.
   *
   * `hold` carries forward the latest known value to each sample position. `linear` interpolates
   * numeric columns between neighboring time-keyed events and falls back to hold behavior for
   * non-numeric columns. Aligned columns are optional because edge buckets may have no value.
   *
   * Defaults:
   * - `method`: `"hold"`
   * - `sample`: `"begin"`
   * - `range`: `series.timeRange()`
   *
   * For `Sequence` inputs, the sequence anchor still comes from the grid definition itself. For
   * procedural sequences created with `Sequence.every(...)`, that anchor defaults to Unix epoch
   * `0`. The `range` only decides which finite slice of that grid is bounded for this alignment.
   *
   * When a `BoundedSequence` is supplied, its intervals are used directly.
   *
   * Example:
   * - `Sequence.every("1m")` defines an epoch-anchored minute grid
   * - `series.align(Sequence.every("1m"))` aligns onto the slice of that minute grid spanning the
   *   current series extent
   */
  align(
    sequence: SequenceLike,
    options: {
      method?: AlignMethod;
      sample?: AlignSample;
      range?: TemporalLike;
    } = {},
  ): TimeSeries<AlignSchema<S>> {
    const method = options.method ?? 'hold';
    const sample = options.sample ?? 'begin';
    const range = options.range ?? this.timeRange();
    const resultSchema = makeAlignedSchema(this.schema);

    if (!range) {
      return new TimeSeries({
        name: this.name,
        schema: resultSchema as unknown as SeriesSchema,
        rows: [],
      }) as unknown as TimeSeries<AlignSchema<S>>;
    }

    if (method === 'linear' && !isTimeKeyed(this)) {
      throw new TypeError(
        'linear alignment currently requires a time-keyed series',
      );
    }

    const intervals = toBoundedSequence(sequence, range, sample).intervals();
    const valueColumns = this.schema.slice(1) as ValueColumnsForSchema<S>;

    const alignedRows = intervals.map((interval) => {
      const t = sampleTime(interval, sample);
      const data =
        method === 'linear'
          ? this.#alignLinearAt(t, valueColumns)
          : this.#alignHoldAt(t);

      return Object.freeze([
        interval,
        ...resultSchema
          .slice(1)
          .map((column) => data[column.name as keyof typeof data]),
      ]);
    });

    return new TimeSeries({
      name: this.name,
      schema: resultSchema as unknown as SeriesSchema,
      rows: alignedRows as unknown as TimeSeriesInput<SeriesSchema>['rows'],
    }) as unknown as TimeSeries<AlignSchema<S>>;
  }

  /**
   * Example: `series.aggregate(Sequence.every("1m"), { value: "avg" })`.
   * Aggregates events into sequence buckets using built-in reducer names.
   *
   * Buckets use half-open membership semantics: `[begin, end)`. Point events contribute to the
   * bucket containing their timestamp. Interval-like events contribute to every bucket they
   * overlap under half-open overlap rules.
   *
   * Defaults:
   * - `range`: `series.timeRange()`
   *
   * As with `align(...)`, `Sequence` defines the underlying grid and `range` selects which portion
   * of that grid is bounded. With `Sequence.every(...)`, the default grid anchor is Unix epoch `0`,
   * but the default aggregation range is always the source series extent. When a
   * `BoundedSequence` is supplied, its intervals are used directly.
   *
   * Override `range` when you need multiple series aggregated over the same reporting window,
   * including leading or trailing empty buckets outside an individual series extent.
   *
   * To align buckets to the beginning of the current series instead of epoch boundaries, override
   * the sequence anchor rather than the aggregation range:
   *
   * ```ts
   * const range = series.timeRange();
   * if (!range) {
   *   throw new Error("empty series");
   * }
   *
   * const aggregated = series.aggregate(
   *   Sequence.every("1m", { anchor: range.begin() }),
   *   { value: "avg" },
   * );
   * ```
   */
  aggregate<const Mapping extends AggregateMap<S>>(
    sequence: SequenceLike,
    mapping: Mapping,
    options: { range?: TemporalLike } = {},
  ): TimeSeries<AggregateSchema<S, Mapping>> {
    const range = options.range ?? this.timeRange();
    const resultSchema = Object.freeze([
      { name: 'interval', kind: 'interval' as const },
      ...this.schema
        .slice(1)
        .filter((column) => column.name in (mapping as Record<string, unknown>))
        .map((column) => {
          const operation = mapping[
            column.name as keyof Mapping
          ] as AggregateFunction;
          return {
            name: column.name,
            kind:
              operation === 'sum' ||
              operation === 'avg' ||
              operation === 'count'
                ? 'number'
                : column.kind,
            required: false as const,
          };
        }),
    ]) as unknown as AggregateSchema<S, Mapping>;

    if (!range) {
      return new TimeSeries({
        name: this.name,
        schema: resultSchema as unknown as SeriesSchema,
        rows: [],
      }) as unknown as TimeSeries<AggregateSchema<S, Mapping>>;
    }

    const buckets = toBoundedSequence(sequence, range, 'begin').intervals();
    const resultRows = buckets.map((bucket) => {
      const contributors = this.events.filter((event) =>
        bucketOverlapsHalfOpen(bucket, event.key()),
      );
      const aggregated = resultSchema.slice(1).map((column) => {
        const operation = mapping[
          column.name as keyof Mapping
        ] as AggregateFunction;
        const values = contributors.map((event) => {
          const data = event.data();
          return data[column.name as keyof typeof data];
        }) as ReadonlyArray<ScalarValue | undefined>;
        return aggregateValues(operation, values);
      });
      return Object.freeze([bucket, ...aggregated]);
    });

    return new TimeSeries({
      name: this.name,
      schema: resultSchema as unknown as SeriesSchema,
      rows: resultRows as unknown as TimeSeriesInput<SeriesSchema>['rows'],
    }) as unknown as TimeSeries<AggregateSchema<S, Mapping>>;
  }

  /**
   * Example: `series.rolling("1h", { value: "avg" })`.
   * Computes event-driven rolling aggregations over the ordered series.
   *
   * Example: `series.rolling(Sequence.every("1m"), "5m", { value: "avg" })`.
   * Computes sequence-driven rolling aggregations and returns an interval-keyed series on the
   * supplied grid.
   *
   * Rolling windows are anchored either at each event's `begin()` time or at the sample point of
   * each sequence bucket. Membership is determined from source event `begin()` times.
   *
   * Supported alignments:
   * - `"trailing"`: `(t - window, t]`
   * - `"leading"`: `[t, t + window)`
   * - `"centered"`: `[t - window/2, t + window/2)`
   *
   * Defaults:
   * - `alignment`: `"trailing"`
   * - sequence-driven only: `sample: "begin"`
   * - sequence-driven only: `range: series.timeRange()`
   */
  rolling<const Mapping extends AggregateMap<S>>(
    window: DurationInput,
    mapping: Mapping,
    options?: { alignment?: RollingAlignment },
  ): TimeSeries<RollingSchema<S, Mapping>>;
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
  rolling<const Mapping extends AggregateMap<S>>(
    sequenceOrWindow: SequenceLike | DurationInput,
    windowOrMapping: DurationInput | Mapping,
    mappingOrOptions?:
      | Mapping
      | {
          alignment?: RollingAlignment;
          sample?: AlignSample;
          range?: TemporalLike;
        },
    maybeOptions: {
      alignment?: RollingAlignment;
      sample?: AlignSample;
      range?: TemporalLike;
    } = {},
  ):
    | TimeSeries<RollingSchema<S, Mapping>>
    | TimeSeries<AggregateSchema<S, Mapping>> {
    const buildResultColumns = () =>
      this.schema
        .slice(1)
        .filter((column) => column.name in (mapping as Record<string, unknown>))
        .map((column) => {
          const operation = mapping[
            column.name as keyof Mapping
          ] as AggregateFunction;
          return {
            name: column.name,
            kind:
              operation === 'sum' ||
              operation === 'avg' ||
              operation === 'count'
                ? 'number'
                : column.kind,
            required: false as const,
          };
        });

    let mapping: Mapping;
    let options: {
      alignment?: RollingAlignment;
      sample?: AlignSample;
      range?: TemporalLike;
    };
    let sequence: SequenceLike | undefined;
    let window: DurationInput;

    if (
      sequenceOrWindow instanceof Sequence ||
      sequenceOrWindow instanceof BoundedSequence
    ) {
      sequence = sequenceOrWindow;
      window = windowOrMapping as DurationInput;
      mapping = mappingOrOptions as Mapping;
      options = maybeOptions;
    } else {
      window = sequenceOrWindow;
      mapping = windowOrMapping as Mapping;
      options =
        (mappingOrOptions as { alignment?: RollingAlignment } | undefined) ??
        {};
    }

    const windowMs = parseDurationInput(window);
    const alignment = options.alignment ?? 'trailing';
    const anchorInWindow = (candidate: number, anchor: number): boolean => {
      if (alignment === 'trailing') {
        return candidate > anchor - windowMs && candidate <= anchor;
      }
      if (alignment === 'leading') {
        return candidate >= anchor && candidate < anchor + windowMs;
      }
      const halfWindow = windowMs / 2;
      return (
        candidate >= anchor - halfWindow && candidate < anchor + halfWindow
      );
    };

    if (sequence) {
      const sample = options.sample ?? 'begin';
      const range = options.range ?? this.timeRange();
      const resultSchema = Object.freeze([
        { name: 'interval', kind: 'interval' as const },
        ...buildResultColumns(),
      ]) as unknown as AggregateSchema<S, Mapping>;

      if (!range) {
        return new TimeSeries({
          name: this.name,
          schema: resultSchema as unknown as SeriesSchema,
          rows: [],
        }) as unknown as TimeSeries<AggregateSchema<S, Mapping>>;
      }

      const buckets = toBoundedSequence(sequence, range, sample).intervals();
      const resultRows = buckets.map((bucket) => {
        const anchor = sampleTime(bucket, sample);
        const contributors = this.events.filter((candidate) =>
          anchorInWindow(candidate.begin(), anchor),
        );
        const aggregated = resultSchema.slice(1).map((column) => {
          const operation = mapping[
            column.name as keyof Mapping
          ] as AggregateFunction;
          const values = contributors.map((candidate) => {
            const data = candidate.data();
            return data[column.name as keyof typeof data];
          }) as ReadonlyArray<ScalarValue | undefined>;
          return aggregateValues(operation, values);
        });

        return Object.freeze([bucket, ...aggregated]);
      });

      return new TimeSeries({
        name: this.name,
        schema: resultSchema as unknown as SeriesSchema,
        rows: resultRows as unknown as TimeSeriesInput<SeriesSchema>['rows'],
      }) as unknown as TimeSeries<AggregateSchema<S, Mapping>>;
    }

    const resultSchema = Object.freeze([
      this.schema[0],
      ...buildResultColumns(),
    ]) as unknown as RollingSchema<S, Mapping>;

    const resultRows = this.events.map((event) => {
      const anchor = event.begin();
      const contributors = this.events.filter((candidate) =>
        anchorInWindow(candidate.begin(), anchor),
      );
      const aggregated = resultSchema.slice(1).map((column) => {
        const operation = mapping[
          column.name as keyof Mapping
        ] as AggregateFunction;
        const values = contributors.map((candidate) => {
          const data = candidate.data();
          return data[column.name as keyof typeof data];
        }) as ReadonlyArray<ScalarValue | undefined>;
        return aggregateValues(operation, values);
      });

      return Object.freeze([event.key(), ...aggregated]);
    });

    return new TimeSeries({
      name: this.name,
      schema: resultSchema as unknown as SeriesSchema,
      rows: resultRows as unknown as TimeSeriesInput<SeriesSchema>['rows'],
    }) as unknown as TimeSeries<RollingSchema<S, Mapping>>;
  }

  /**
   * Example: `series.smooth("value", "ema", { alpha: 0.2 })`.
   * Applies a smoothing transform to one numeric payload column while preserving the original key
   * type, key values, and all non-target payload fields.
   *
   * Example: `series.smooth("value", "movingAverage", { window: "5m", alignment: "centered", output: "valueAvg" })`.
   * Computes a moving average over the selected numeric column using anchor points derived from
   * event keys. `Time` keys use their timestamp. `TimeRange` and `Interval` keys use the midpoint
   * of their extent.
   *
   * Example: `series.smooth("value", "loess", { span: 0.75 })`.
   * Computes a LOESS-smoothed value for the selected numeric column using local weighted linear
   * regression over those same anchor points.
   *
   * When `output` is omitted, the smoothed values replace the target column. When `output` is
   * supplied, the smoothed values are appended as a new optional numeric column.
   */
  smooth<
    const Target extends NumericColumnNameForSchema<S>,
    const Output extends string | undefined = undefined,
  >(
    column: Target,
    method: SmoothMethod,
    options:
      | { alpha: number; output?: Output }
      | { window: DurationInput; alignment?: RollingAlignment; output?: Output }
      | { span: number; output?: Output },
  ): TimeSeries<
    Output extends string
      ? SmoothAppendSchema<S, Output>
      : SmoothSchema<S, Target>
  > {
    const output = options.output;
    const resultSchema =
      output === undefined
        ? makeSmoothSchema(this.schema, column)
        : makeSmoothSchema(this.schema, column, output);

    const anchors = this.events.map((event) => eventAnchorTime(event.key()));
    const sourceValues: ReadonlyArray<number | undefined> = this.events.map(
      (event) => {
        const raw = event.get(column);
        return typeof raw === 'number' ? raw : undefined;
      },
    );

    if (method === 'ema') {
      if (!('alpha' in options)) {
        throw new TypeError('ema smoothing requires an alpha option');
      }
      const alpha = options.alpha;
      if (
        typeof alpha !== 'number' ||
        !Number.isFinite(alpha) ||
        alpha <= 0 ||
        alpha > 1
      ) {
        throw new TypeError(
          'ema smoothing requires alpha to be a finite number in the range (0, 1]',
        );
      }

      let previous: number | undefined;
      const resultRows = this.events.map((event) => {
        const raw = event.get(column);
        const smoothed =
          typeof raw !== 'number'
            ? undefined
            : previous === undefined
              ? raw
              : alpha * raw + (1 - alpha) * previous;

        if (smoothed !== undefined) {
          previous = smoothed;
        }

        const nextEvent =
          output === undefined
            ? event.set(column, smoothed as EventDataForSchema<S>[Target])
            : event.merge({ [output]: smoothed });
        return Object.freeze([
          nextEvent.key(),
          ...resultSchema
            .slice(1)
            .map(
              (nextColumn) =>
                nextEvent.data()[
                  nextColumn.name as keyof ReturnType<typeof nextEvent.data>
                ],
            ),
        ]);
      });

      return new TimeSeries({
        name: this.name,
        schema: resultSchema as unknown as SeriesSchema,
        rows: resultRows as unknown as TimeSeriesInput<SeriesSchema>['rows'],
      }) as unknown as TimeSeries<
        Output extends string
          ? SmoothAppendSchema<S, Output>
          : SmoothSchema<S, Target>
      >;
    }

    if (method === 'loess') {
      if (!('span' in options)) {
        throw new TypeError('loess smoothing requires a span option');
      }
      const span = options.span;
      if (
        typeof span !== 'number' ||
        !Number.isFinite(span) ||
        span <= 0 ||
        span > 1
      ) {
        throw new TypeError(
          'loess smoothing requires span to be a finite number in the range (0, 1]',
        );
      }

      const resultRows = this.events.map((event, index) => {
        const smoothed = loessAt(anchors[index]!, anchors, sourceValues, span);
        const nextEvent =
          output === undefined
            ? event.set(column, smoothed as EventDataForSchema<S>[Target])
            : event.merge({ [output]: smoothed });
        return Object.freeze([
          nextEvent.key(),
          ...resultSchema
            .slice(1)
            .map(
              (nextColumn) =>
                nextEvent.data()[
                  nextColumn.name as keyof ReturnType<typeof nextEvent.data>
                ],
            ),
        ]);
      });

      return new TimeSeries({
        name: this.name,
        schema: resultSchema as unknown as SeriesSchema,
        rows: resultRows as unknown as TimeSeriesInput<SeriesSchema>['rows'],
      }) as unknown as TimeSeries<
        Output extends string
          ? SmoothAppendSchema<S, Output>
          : SmoothSchema<S, Target>
      >;
    }

    if (!('window' in options)) {
      throw new TypeError('movingAverage smoothing requires a window option');
    }
    const window = options.window;
    const windowMs = parseDurationInput(window!);
    const alignment = options.alignment ?? 'trailing';
    const anchorInWindow = (candidate: number, anchor: number): boolean => {
      if (alignment === 'trailing') {
        return candidate > anchor - windowMs && candidate <= anchor;
      }
      if (alignment === 'leading') {
        return candidate >= anchor && candidate < anchor + windowMs;
      }
      const halfWindow = windowMs / 2;
      return (
        candidate >= anchor - halfWindow && candidate < anchor + halfWindow
      );
    };

    const resultRows = this.events.map((event, index) => {
      const anchor = anchors[index]!;
      const values = sourceValues
        .filter((_, candidateIndex) =>
          anchorInWindow(anchors[candidateIndex]!, anchor),
        )
        .flatMap((value) => (value === undefined ? [] : [value]));
      const smoothed =
        values.length === 0
          ? undefined
          : values.reduce((sum, value) => sum + value, 0) / values.length;

      const nextEvent =
        output === undefined
          ? event.set(column, smoothed as EventDataForSchema<S>[Target])
          : event.merge({ [output]: smoothed });
      return Object.freeze([
        nextEvent.key(),
        ...resultSchema
          .slice(1)
          .map(
            (nextColumn) =>
              nextEvent.data()[
                nextColumn.name as keyof ReturnType<typeof nextEvent.data>
              ],
          ),
      ]);
    });

    return new TimeSeries({
      name: this.name,
      schema: resultSchema as unknown as SeriesSchema,
      rows: resultRows as unknown as TimeSeriesInput<SeriesSchema>['rows'],
    }) as unknown as TimeSeries<
      Output extends string
        ? SmoothAppendSchema<S, Output>
        : SmoothSchema<S, Target>
    >;
  }

  /** Example: `series.slice(0, 10)`. Returns a positional half-open slice of the series. */
  slice(beginIndex?: number, endIndex?: number): TimeSeries<S> {
    return new TimeSeries({
      name: this.name,
      schema: this.schema,
      rows: toRows(this.schema, this.events.slice(beginIndex, endIndex)),
    });
  }

  /** Example: `series.filter(event => event.get("active"))`. Returns a new series containing only events that match the predicate. */
  filter(
    predicate: (event: EventForSchema<S>, index: number) => boolean,
  ): TimeSeries<S> {
    return new TimeSeries({
      name: this.name,
      schema: this.schema,
      rows: toRows(
        this.schema,
        this.events.filter((event, index) => predicate(event, index)),
      ),
    });
  }

  /** Example: `series.find(event => event.get("value") > 0)`. Returns the first event that matches the predicate, if any. */
  find(
    predicate: (event: EventForSchema<S>, index: number) => boolean,
  ): EventForSchema<S> | undefined {
    return this.events.find((event, index) => predicate(event, index));
  }

  /** Example: `series.some(event => event.get("healthy"))`. Returns `true` when at least one event matches the predicate. */
  some(
    predicate: (event: EventForSchema<S>, index: number) => boolean,
  ): boolean {
    return this.events.some((event, index) => predicate(event, index));
  }

  /** Example: `series.every(event => event.get("healthy"))`. Returns `true` when every event matches the predicate. */
  every(
    predicate: (event: EventForSchema<S>, index: number) => boolean,
  ): boolean {
    return this.events.every((event, index) => predicate(event, index));
  }

  /** Example: `series.includesKey(new Time(Date.now()))`. Returns `true` when the series contains an event with an exactly matching key. */
  includesKey(key: KeyLike): boolean {
    const normalizedKey = toKey(key);
    return this.events.some((event) => event.key().equals(normalizedKey));
  }

  /** Example: `series.bisect(new Time(Date.now()))`. Returns the insertion index for the supplied key in the ordered event sequence. */
  bisect(key: KeyLike): number {
    const normalizedKey = toKey(key);
    let low = 0;
    let high = this.events.length;

    while (low < high) {
      const mid = Math.floor((low + high) / 2);
      if (this.events[mid]!.key().compare(normalizedKey) < 0) {
        low = mid + 1;
      } else {
        high = mid;
      }
    }

    return low;
  }

  /** Example: `series.atOrBefore(new Time(Date.now()))`. Returns the event with the exact key or the nearest earlier event, if any. */
  atOrBefore(key: KeyLike): EventForSchema<S> | undefined {
    const normalizedKey = toKey(key);
    const index = this.bisect(normalizedKey);

    if (
      index < this.events.length &&
      this.events[index]!.key().equals(normalizedKey)
    ) {
      return this.events[index];
    }

    return index === 0 ? undefined : this.events[index - 1];
  }

  /** Example: `series.atOrAfter(new Time(Date.now()))`. Returns the event with the exact key or the nearest later event, if any. */
  atOrAfter(key: KeyLike): EventForSchema<S> | undefined {
    return this.events[this.bisect(key)];
  }

  /** Example: `series.timeRange()`. Returns the overall temporal extent of the series, if the series is not empty. */
  timeRange(): TimeRange | undefined {
    const first = this.first();
    if (!first) {
      return undefined;
    }

    const start = first.begin();
    const end = this.events.reduce(
      (maxEnd, event) => Math.max(maxEnd, event.end()),
      first.end(),
    );
    return new TimeRange({ start, end });
  }

  /** Example: `series.overlaps(range)`. Returns `true` when the overall series extent overlaps the supplied temporal value. */
  overlaps(other: SeriesRangeLike): boolean {
    const range = this.timeRange();
    const otherRange = toOptionalSeriesRange(other);
    if (!range || !otherRange) {
      return false;
    }
    return range.overlaps(otherRange);
  }

  /** Example: `series.contains(range)`. Returns `true` when the overall series extent fully contains the supplied temporal value. */
  contains(other: SeriesRangeLike): boolean {
    const range = this.timeRange();
    const otherRange = toOptionalSeriesRange(other);
    if (!range || !otherRange) {
      return false;
    }
    return range.contains(otherRange);
  }

  /** Example: `series.intersection(range)`. Returns the overlap between the overall series extent and the supplied temporal value, if any. */
  intersection(other: SeriesRangeLike): TimeRange | undefined {
    const range = this.timeRange();
    const otherRange = toOptionalSeriesRange(other);
    if (!range || !otherRange) {
      return undefined;
    }
    return range.intersection(otherRange);
  }

  /**
   * Example: `series.overlapping(range)`.
   * Returns the portion of the series whose event extents overlap the supplied range.
   *
   * Unlike `within(...)`, this keeps partially overlapping events without modifying their keys.
   * Use `trim(...)` when you want those overlapping keys clipped to the supplied range.
   */
  overlapping(range: RangeLike): TimeSeries<S> {
    return this.filter((event) => event.overlaps(range));
  }

  /**
   * Example: `series.containedBy(range)`.
   * Returns the portion of the series whose event extents are fully contained by the supplied range.
   *
   * This is the strict containment selector:
   * events must start at or after the range start and end at or before the range end.
   * Unlike `overlapping(...)`, partially overlapping events are excluded.
   */
  containedBy(range: RangeLike): TimeSeries<S> {
    const selectionRange = toSelectionRange(range);
    return this.filter((event) => selectionRange.contains(event));
  }

  /**
   * Example: `series.trim(range)`.
   * Returns the series trimmed to the supplied range by clipping overlapping event keys.
   *
   * Non-overlapping events are dropped. Overlapping `TimeRange` and `Interval` keys are clipped
   * to the supplied range. Overlapping `Time` keys are preserved unchanged.
   */
  trim(range: RangeLike): TimeSeries<S> {
    const trimmedEvents = this.events
      .map((event) => event.trim(range))
      .filter((event): event is EventForSchema<S> => event !== undefined);

    return new TimeSeries({
      name: this.name,
      schema: this.schema,
      rows: toRows(this.schema, trimmedEvents),
    });
  }

  /** Example: `series.before(Date.now())`. Returns the events ending strictly before the supplied temporal boundary. */
  before(boundary: BoundaryLike): TimeSeries<S> {
    const limit = toBoundaryTimestamp(boundary);
    return this.filter((event) => event.end() < limit);
  }

  /** Example: `series.after(Date.now())`. Returns the events beginning strictly after the supplied temporal boundary. */
  after(boundary: BoundaryLike): TimeSeries<S> {
    const limit = toBoundaryTimestamp(boundary);
    return this.filter((event) => event.begin() > limit);
  }

  /**
   * Example: `series.within(start, end)`.
   * Returns the portion of the series fully contained by the supplied inclusive temporal range.
   *
   * This is equivalent in behavior to `containedBy(...)`, but accepts either explicit begin/end
   * boundaries or a single range-like value.
   */
  within(begin: TimestampInput, end: TimestampInput): TimeSeries<S>;
  /**
   * Example: `series.within(range)`.
   * Returns the portion of the series fully contained by the supplied inclusive temporal range.
   *
   * Use `overlapping(...)` for intersection-based selection or `trim(...)` for clipped output.
   */
  within(range: RangeLike): TimeSeries<S>;
  within(
    beginOrRange: TimestampInput | RangeLike,
    end?: TimestampInput,
  ): TimeSeries<S> {
    const range =
      end === undefined
        ? toSelectionRange(beginOrRange as RangeLike)
        : new TimeRange({ start: beginOrRange as TimestampInput, end });
    return this.filter(
      (event) => event.begin() >= range.begin() && event.end() <= range.end(),
    );
  }

  /** Example: `series.select("cpu", "healthy")`. Returns a new series containing only the selected payload fields. */
  select<const Keys extends readonly (keyof EventDataForSchema<S>)[]>(
    ...keys: Keys
  ): TimeSeries<SelectSchema<S, Keys[number] & string>> {
    const firstColumn = this.schema[0]!;
    const selectedColumns = this.schema
      .slice(1)
      .filter((column) => keys.includes(column.name as Keys[number]));
    const resultSchema = Object.freeze([
      firstColumn,
      ...selectedColumns,
    ]) as unknown as SelectSchema<S, Keys[number] & string>;

    const resultEvents = this.events.map((event) => {
      const selectedEvent = event.select(...keys);
      return selectedEvent;
    });

    return new TimeSeries({
      name: this.name,
      schema: resultSchema as unknown as SeriesSchema,
      rows: toRows(
        resultSchema as unknown as SeriesSchema,
        resultEvents as unknown as EventForSchema<SeriesSchema>[],
      ),
    }) as unknown as TimeSeries<SelectSchema<S, Keys[number] & string>>;
  }

  /** Example: `series.rename({ cpu: "usage" })`. Returns a new series with payload field names renamed according to the supplied mapping. */
  rename<const Mapping extends RenameMap<EventDataForSchema<S>>>(
    mapping: Mapping,
  ): TimeSeries<RenameSchema<S, Mapping>> {
    const firstColumn = this.schema[0]!;
    const renamedColumns = this.schema.slice(1).map((column) => ({
      ...column,
      name:
        (mapping as Partial<Record<string, string>>)[column.name] ??
        column.name,
    }));
    const resultSchema = Object.freeze([
      firstColumn,
      ...renamedColumns,
    ]) as unknown as RenameSchema<S, Mapping>;

    const resultEvents = this.events.map((event) => {
      const renamedEvent = event.rename(mapping);
      return renamedEvent;
    });

    return new TimeSeries({
      name: this.name,
      schema: resultSchema as unknown as SeriesSchema,
      rows: toRows(
        resultSchema as unknown as SeriesSchema,
        resultEvents as unknown as EventForSchema<SeriesSchema>[],
      ),
    }) as unknown as TimeSeries<RenameSchema<S, Mapping>>;
  }

  /** Example: `series.collapse(["in", "out"], "avg", fn)`. Collapses selected payload fields into a single derived field across each event in the series. */
  collapse<
    const Keys extends readonly (keyof EventDataForSchema<S>)[],
    Name extends string,
    R extends ScalarValue,
  >(
    keys: Keys,
    output: Name,
    reducer: (values: Pick<EventDataForSchema<S>, Keys[number]>) => R,
  ): TimeSeries<CollapseSchema<S, Keys[number] & string, Name, R>>;

  collapse<
    const Keys extends readonly (keyof EventDataForSchema<S>)[],
    Name extends string,
    R extends ScalarValue,
  >(
    keys: Keys,
    output: Name,
    reducer: (values: Pick<EventDataForSchema<S>, Keys[number]>) => R,
    options: { append: true },
  ): TimeSeries<CollapseSchema<S, Keys[number] & string, Name, R, true>>;

  collapse<
    const Keys extends readonly (keyof EventDataForSchema<S>)[],
    Name extends string,
    R extends ScalarValue,
  >(
    keys: Keys,
    output: Name,
    reducer: (values: Pick<EventDataForSchema<S>, Keys[number]>) => R,
    options?: { append?: boolean },
  ): TimeSeries<CollapseSchema<S, Keys[number] & string, Name, R, boolean>> {
    const nextEvents = this.events.map((event) => {
      if (options?.append === true) {
        return event.collapse(keys, output, reducer, { append: true });
      }
      return event.collapse(keys, output, reducer);
    });

    const firstColumn = this.schema[0]!;
    const append = options?.append === true;
    const keptColumns = append
      ? this.schema.slice(1)
      : this.schema
          .slice(1)
          .filter((column) => !keys.includes(column.name as Keys[number]));

    const resultSchema = Object.freeze([
      firstColumn,
      ...keptColumns,
      {
        name: output,
        kind:
          typeof nextEvents[0]?.get(output) === 'number'
            ? 'number'
            : typeof nextEvents[0]?.get(output) === 'boolean'
              ? 'boolean'
              : 'string',
      },
    ]) as unknown as CollapseSchema<S, Keys[number] & string, Name, R, boolean>;

    return new TimeSeries({
      name: this.name,
      schema: resultSchema as unknown as SeriesSchema,
      rows: toRows(
        resultSchema as unknown as SeriesSchema,
        nextEvents as unknown as EventForSchema<SeriesSchema>[],
      ),
    }) as unknown as TimeSeries<
      CollapseSchema<S, Keys[number] & string, Name, R, boolean>
    >;
  }

  /** Example: `series.length`. Returns the number of events in the series. */
  get length(): number {
    return this.events.length;
  }

  #alignHoldAt(t: number): EventDataForSchema<S> {
    const event = this.atOrBefore(new Time(t));
    return (event?.data() ?? {}) as EventDataForSchema<S>;
  }

  #alignLinearAt(
    t: number,
    valueColumns: ValueColumnsForSchema<S>,
  ): EventDataForSchema<S> {
    const exact = this.find((event) => event.begin() === t);
    if (exact) {
      return exact.data() as EventDataForSchema<S>;
    }

    const previous = this.atOrBefore(new Time(t));
    const next = this.atOrAfter(new Time(t));
    if (!previous || !next || previous.begin() === next.begin()) {
      return (previous?.data() ?? {}) as EventDataForSchema<S>;
    }

    const ratio = (t - previous.begin()) / (next.begin() - previous.begin());
    const result: Record<string, unknown> = {};
    const previousData = previous.data();
    const nextData = next.data();

    for (const column of valueColumns) {
      const previousValue =
        previousData[column.name as keyof typeof previousData];
      const nextValue = nextData[column.name as keyof typeof nextData];

      if (
        column.kind === 'number' &&
        typeof previousValue === 'number' &&
        typeof nextValue === 'number'
      ) {
        result[column.name] =
          previousValue + (nextValue - previousValue) * ratio;
        continue;
      }

      result[column.name] = previousValue;
    }

    return result as EventDataForSchema<S>;
  }
}
