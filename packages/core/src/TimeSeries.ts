import type {
  AlignSchema,
  AggregateFunction,
  AggregateReducer,
  AggregateOutputMap,
  AggregateOutputSpec,
  AggregateMap,
  AggregateSchema,
  CollapseSchema,
  EventDataForSchema,
  EventForSchema,
  FirstColKind,
  IntervalKeyedSchema,
  JsonObjectRowForSchema,
  JsonRowFormat,
  JsonRowForSchema,
  JsonValueForKind,
  JoinConflictMode,
  JoinManySchema,
  JoinSchema,
  JoinType,
  NumericColumnNameForSchema,
  NormalizedObjectRow,
  NormalizedObjectRowForSchema,
  NormalizedRowForSchema,
  PrefixedJoinManySchema,
  PrefixedJoinSchema,
  ReduceResult,
  RenameMap,
  RenameSchema,
  RollingAlignment,
  RollingSchema,
  CustomAggregateReducer,
  DiffSchema,
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
import { Event } from './Event.js';
import { Sequence } from './Sequence.js';
import { validateAndNormalize } from './validate.js';
import type { DurationInput } from './utils/duration.js';
import { parseDuration } from './utils/duration.js';
import {
  resolveReducer,
  type AggregateBucketState,
  type RollingReducerState,
} from './reducers/index.js';

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
type AlignCursor = { index: number };
type JoinOptions = ErrorJoinOptions | PrefixJoinOptions<readonly string[]>;
type FillStrategy = 'hold' | 'bfill' | 'linear' | 'zero';
type FillMapping<S extends SeriesSchema> = {
  [K in ValueColumnsForSchema<S>[number]['name']]?: FillStrategy | ScalarValue;
};
type ResolvedFillSpec =
  | { mode: FillStrategy }
  | { mode: 'literal'; value: ScalarValue };
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

function serializeJsonKey(
  kind: FirstColKind,
  key: EventKey,
  rowFormat: JsonRowFormat,
): JsonValueForKind<FirstColKind> {
  if (kind === 'time') {
    return key.begin();
  }

  if (kind === 'timeRange') {
    return rowFormat === 'object'
      ? { start: key.begin(), end: key.end() }
      : [key.begin(), key.end()];
  }

  const interval = key as Interval;
  return rowFormat === 'object'
    ? { value: interval.value, start: interval.begin(), end: interval.end() }
    : [interval.value, interval.begin(), interval.end()];
}

function serializeJsonValue(value: unknown): unknown {
  return value === undefined ? null : value;
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

function toObjects<S extends SeriesSchema>(
  schema: S,
  events: ReadonlyArray<EventForSchema<S>>,
): ReadonlyArray<NormalizedObjectRow> {
  const keyColumn = schema[0]!;
  const dataColumns = schema.slice(1);
  return events.map((event) => {
    const row: Record<string, unknown> = {
      [keyColumn.name]: event.key(),
    };
    const data = event.data();

    for (const column of dataColumns) {
      row[column.name] = data[column.name as keyof typeof data];
    }

    return Object.freeze(row) as NormalizedObjectRowForSchema<S>;
  }) as ReadonlyArray<NormalizedObjectRow>;
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
  values: ReadonlyArray<number>,
  span: number,
): number | undefined {
  if (anchors.length === 0) {
    return undefined;
  }

  if (anchors.length === 1) {
    return values[0];
  }

  const neighborCount = Math.max(
    2,
    Math.min(anchors.length, Math.ceil(span * anchors.length)),
  );
  let start = 0;
  if (neighborCount < anchors.length) {
    let low = 0;
    let high = anchors.length - neighborCount;
    while (low < high) {
      const mid = Math.floor((low + high) / 2);
      if (x - anchors[mid]! > anchors[mid + neighborCount]! - x) {
        low = mid + 1;
      } else {
        high = mid;
      }
    }
    start = low;
  }

  const end = start + neighborCount;
  const bandwidth = Math.max(
    Math.abs(x - anchors[start]!),
    Math.abs(anchors[end - 1]! - x),
  );

  if (bandwidth === 0) {
    const coincidentStart = lowerBound(anchors, x);
    const coincidentEnd = upperBound(anchors, x);
    let coincidentSum = 0;
    for (let index = coincidentStart; index < coincidentEnd; index++) {
      coincidentSum += values[index]!;
    }
    return coincidentSum / (coincidentEnd - coincidentStart);
  }

  let weightedCount = 0;
  let sumW = 0;
  let sumWX = 0;
  let sumWY = 0;
  let sumWXX = 0;
  let sumWXY = 0;

  for (let index = start; index < end; index++) {
    const pointX = anchors[index]!;
    const pointY = values[index]!;
    const distance = Math.abs(pointX - x);
    const ratio = distance / bandwidth;
    const weight = ratio >= 1 ? 0 : (1 - ratio ** 3) ** 3;
    if (weight === 0) {
      continue;
    }
    weightedCount += 1;
    sumW += weight;
    sumWX += weight * pointX;
    sumWY += weight * pointY;
    sumWXX += weight * pointX * pointX;
    sumWXY += weight * pointX * pointY;
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

function lowerBound(values: ReadonlyArray<number>, target: number): number {
  let low = 0;
  let high = values.length;

  while (low < high) {
    const mid = Math.floor((low + high) / 2);
    if (values[mid]! < target) {
      low = mid + 1;
    } else {
      high = mid;
    }
  }

  return low;
}

function upperBound(values: ReadonlyArray<number>, target: number): number {
  let low = 0;
  let high = values.length;

  while (low < high) {
    const mid = Math.floor((low + high) / 2);
    if (values[mid]! <= target) {
      low = mid + 1;
    } else {
      high = mid;
    }
  }

  return low;
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
  return resolveReducer(operation).reduce(defined, numeric);
}

function isBuiltInAggregateReducer(
  reducer: AggregateReducer,
): reducer is AggregateFunction {
  return typeof reducer === 'string';
}

function applyAggregateReducer(
  reducer: AggregateReducer,
  values: ReadonlyArray<ScalarValue | undefined>,
): ScalarValue | undefined {
  return isBuiltInAggregateReducer(reducer)
    ? aggregateValues(reducer, values)
    : (reducer as CustomAggregateReducer)(values);
}

type AggregateColumnSpec = {
  output: string;
  source: string;
  reducer: AggregateReducer;
  kind: 'number' | 'string' | 'boolean';
};

function isAggregateOutputSpec<S extends SeriesSchema>(
  value: unknown,
): value is AggregateOutputSpec<S> {
  return (
    typeof value === 'object' &&
    value !== null &&
    'from' in value &&
    'using' in value
  );
}

function normalizeAggregateColumns<S extends SeriesSchema>(
  schema: S,
  mapping: AggregateMap<S> | AggregateOutputMap<S>,
): AggregateColumnSpec[] {
  const columnsByName = new Map(
    schema.slice(1).map((column) => [column.name, column] as const),
  );
  const normalized: AggregateColumnSpec[] = [];

  for (const [outputName, raw] of Object.entries(mapping)) {
    const sourceName = isAggregateOutputSpec<S>(raw) ? raw.from : outputName;
    const sourceColumn = columnsByName.get(sourceName);
    if (!sourceColumn) {
      throw new TypeError(
        `aggregate mapping references unknown source column '${sourceName}'`,
      );
    }
    if (
      sourceColumn.kind !== 'number' &&
      sourceColumn.kind !== 'string' &&
      sourceColumn.kind !== 'boolean'
    ) {
      throw new TypeError(
        `aggregate source column '${sourceName}' must be a scalar value column`,
      );
    }
    const reducer = isAggregateOutputSpec<S>(raw) ? raw.using : raw;
    if (typeof reducer !== 'string' && typeof reducer !== 'function') {
      throw new TypeError(
        `aggregate reducer for '${outputName}' must be a built-in name or function`,
      );
    }
    const explicitKind = isAggregateOutputSpec<S>(raw) ? raw.kind : undefined;
    const resolvedKind =
      explicitKind ??
      (typeof reducer === 'string' &&
      resolveReducer(reducer).outputKind === 'number'
        ? 'number'
        : sourceColumn.kind);
    normalized.push({
      output: outputName,
      source: sourceName,
      reducer,
      kind: resolvedKind,
    });
  }

  return normalized;
}

function createAggregateBucketState(
  operation: AggregateFunction,
): AggregateBucketState {
  return resolveReducer(operation).bucketState();
}

function createRollingReducerState(
  operation: AggregateFunction,
): RollingReducerState {
  return resolveReducer(operation).rollingState();
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
  ): any {
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

  /**
   * Example: `series.toJSON({ rowFormat: "object" })`.
   * Serializes the series into the JSON-friendly shape accepted by `TimeSeries.fromJSON(...)`.
   *
   * Timestamps are emitted as numbers to avoid time zone ambiguity. Missing payload values are
   * emitted as `null`. By default rows are emitted as arrays; use `rowFormat: "object"` for rows
   * keyed by schema column names.
   */
  toJSON(
    options: { rowFormat?: JsonRowFormat } = {},
  ): TimeSeriesJsonInput<SeriesSchema> {
    const rowFormat = options.rowFormat ?? 'array';
    const dataColumns = this.schema.slice(1);

    if (rowFormat === 'object') {
      const keyColumn = this.schema[0]!;
      const rows = this.events.map((event) => {
        const row: Record<string, unknown> = {
          [keyColumn.name]: serializeJsonKey(
            keyColumn.kind,
            event.key(),
            rowFormat,
          ),
        };
        const data = event.data();

        for (const column of dataColumns) {
          row[column.name] = serializeJsonValue(
            data[column.name as keyof typeof data],
          );
        }

        return Object.freeze(row) as JsonObjectRowForSchema<SeriesSchema>;
      });

      return {
        name: this.name,
        schema: this.schema as SeriesSchema,
        rows,
      };
    }

    const rows = this.events.map((event) => {
      const data = event.data();
      return Object.freeze([
        serializeJsonKey(this.schema[0]!.kind, event.key(), rowFormat),
        ...dataColumns.map((column) =>
          serializeJsonValue(data[column.name as keyof typeof data]),
        ),
      ]) as JsonRowForSchema<SeriesSchema>;
    });

    return {
      name: this.name,
      schema: this.schema as SeriesSchema,
      rows,
    };
  }

  /**
   * Builds a series from event data that has already been validated and ordered by the caller.
   *
   * This is intentionally private and only used by transforms that preserve the existing event
   * order and normalized key invariants.
   */
  static #fromTrustedEvents<NextSchema extends SeriesSchema>(
    name: string,
    schema: NextSchema,
    events: ReadonlyArray<EventForSchema<NextSchema>>,
  ): TimeSeries<NextSchema> {
    const series = Object.create(TimeSeries.prototype) as {
      name: string;
      schema: NextSchema;
      events: ReadonlyArray<EventForSchema<NextSchema>>;
    };

    series.name = name;
    series.schema = Object.freeze(schema.slice()) as NextSchema;
    series.events = Object.freeze(events.slice()) as ReadonlyArray<
      EventForSchema<NextSchema>
    >;

    return Object.freeze(series) as TimeSeries<NextSchema>;
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

  /** Example: `series.toRows()`. Returns normalized row arrays using `Time`/`TimeRange`/`Interval` keys and `undefined` for missing payload values. */
  toRows(): ReadonlyArray<NormalizedRowForSchema<S>> {
    return this.rows;
  }

  /** Example: `series.toObjects()`. Returns normalized schema-keyed object rows using temporal key objects and `undefined` for missing payload values. */
  toObjects(): ReadonlyArray<NormalizedObjectRow> {
    return toObjects(this.schema, this.events);
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

    const resultEvents = this.events.map((event) => event.asTime(options));

    if ((options.at ?? 'begin') === 'begin') {
      return TimeSeries.#fromTrustedEvents(
        this.name,
        schema,
        resultEvents as EventForSchema<typeof schema>[],
      );
    }

    return new TimeSeries({
      name: this.name,
      schema,
      rows: toRows(schema, resultEvents as EventForSchema<typeof schema>[]),
    });
  }

  /** Example: `series.asTimeRange()`. Converts the series key type to `"timeRange"` while preserving each event extent. */
  asTimeRange(): TimeSeries<TimeRangeKeyedSchema<S>> {
    const schema = Object.freeze([
      { name: 'timeRange', kind: 'timeRange' as const },
      ...this.schema.slice(1),
    ]) as TimeRangeKeyedSchema<S>;

    const resultEvents = this.events.map((event) => event.asTimeRange());

    return TimeSeries.#fromTrustedEvents(
      this.name,
      schema,
      resultEvents as EventForSchema<typeof schema>[],
    );
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

    return TimeSeries.#fromTrustedEvents(this.name, schema, nextEvents);
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
  ): any {
    const [left, right] = prepareSeriesForJoin(
      [
        this as unknown as TimeSeries<SeriesSchema>,
        other as unknown as TimeSeries<SeriesSchema>,
      ],
      options,
    ) as [TimeSeries<SeriesSchema>, TimeSeries<SeriesSchema>];
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

    return TimeSeries.#fromTrustedEvents(left.name, resultSchema, joinedEvents);
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
    const resultColumns = resultSchema.slice(1);

    const alignedRows =
      method === 'linear'
        ? (() => {
            const cursor: AlignCursor = { index: 0 };
            const rows = new Array(intervals.length);

            for (let i = 0; i < intervals.length; i += 1) {
              const interval = intervals[i]!;
              const t = sampleTime(interval, sample);
              const data = alignLinearAt(this, t, valueColumns, cursor);
              const row = new Array(resultColumns.length + 1);
              row[0] = interval;

              for (let j = 0; j < resultColumns.length; j += 1) {
                const column = resultColumns[j]!;
                row[j + 1] = data[column.name as keyof typeof data];
              }

              rows[i] = Object.freeze(row);
            }

            return rows;
          })()
        : intervals.map((interval) => {
            const t = sampleTime(interval, sample);
            const data = alignHoldAt(this, t);

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
   * Aggregates events into sequence buckets using built-in reducer names or custom reducers.
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
   * Custom reducer contract:
   * - input: `ReadonlyArray<ScalarValue | undefined>`
   * - output: `ScalarValue | undefined`
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
    options?: { range?: TemporalLike },
  ): TimeSeries<AggregateSchema<S, Mapping>>;
  aggregate<const Mapping extends AggregateOutputMap<S>>(
    sequence: SequenceLike,
    mapping: Mapping,
    options?: { range?: TemporalLike },
  ): TimeSeries<SeriesSchema>;
  aggregate(
    sequence: SequenceLike,
    mapping: AggregateMap<S> | AggregateOutputMap<S>,
    options: { range?: TemporalLike } = {},
  ): any {
    return aggregateInternal(this, sequence, mapping, options);
  }

  /**
   * Example: `series.reduce("value", "avg")`.
   * Collapses the entire series to a single scalar value using the specified reducer.
   *
   * Example: `series.reduce({ cpu: "avg", requests: "sum" })`.
   * Collapses the entire series to a record with one entry per mapped column.
   *
   * Uses the same reducer specs as `aggregate(...)` — built-in names like `"avg"`, `"sum"`,
   * `"count"`, or custom functions `(values) => result`. Where `aggregate` buckets by time and
   * produces a new `TimeSeries`, `reduce` treats the whole series as one bucket and produces
   * a plain value or record.
   */
  reduce(
    column: ValueColumnsForSchema<S>[number]['name'],
    reducer: AggregateReducer,
  ): ScalarValue | undefined;
  reduce<const Mapping extends AggregateMap<S>>(
    mapping: Mapping,
  ): ReduceResult<S, Mapping>;
  reduce<const Mapping extends AggregateOutputMap<S>>(
    mapping: Mapping,
  ): ReduceResult<S, Mapping>;
  reduce(
    columnOrMapping:
      | ValueColumnsForSchema<S>[number]['name']
      | AggregateMap<S>
      | AggregateOutputMap<S>,
    reducer?: AggregateReducer,
  ): ScalarValue | undefined | Record<string, ScalarValue | undefined> {
    if (typeof columnOrMapping === 'string') {
      const values = this.events.map((event) => {
        const data = event.data();
        return data[columnOrMapping as keyof typeof data];
      }) as ReadonlyArray<ScalarValue | undefined>;
      return applyAggregateReducer(reducer!, values);
    }

    const columns = normalizeAggregateColumns(this.schema, columnOrMapping);
    const result: Record<string, ScalarValue | undefined> = {};
    for (const col of columns) {
      const values = this.events.map((event) => {
        const data = event.data();
        return data[col.source as keyof typeof data];
      }) as ReadonlyArray<ScalarValue | undefined>;
      result[col.output] = applyAggregateReducer(col.reducer, values);
    }
    return result;
  }

  /**
   * Example: `series.groupBy("host")`.
   * Partitions the series into groups keyed by the distinct values of a payload column.
   * Each group is a `TimeSeries` with the same schema, preserving event order.
   *
   * Example: `series.groupBy("host", group => group.rolling("5m", { cpu: "avg" }))`.
   * When a transform callback is supplied, it is applied to each group and the result
   * map contains the transform outputs instead of raw sub-series.
   */
  groupBy(
    column: keyof EventDataForSchema<S> & string,
  ): Map<string, TimeSeries<S>>;
  groupBy<R>(
    column: keyof EventDataForSchema<S> & string,
    transform: (group: TimeSeries<S>, key: string) => R,
  ): Map<string, R>;
  groupBy<R>(
    column: keyof EventDataForSchema<S> & string,
    transform?: (group: TimeSeries<S>, key: string) => R,
  ): Map<string, TimeSeries<S>> | Map<string, R> {
    const buckets = new Map<string, EventForSchema<S>[]>();

    for (const event of this.events) {
      const raw = event.data()[column as keyof EventDataForSchema<S>];
      const key = raw === undefined ? 'undefined' : String(raw);
      let bucket = buckets.get(key);
      if (!bucket) {
        bucket = [];
        buckets.set(key, bucket);
      }
      bucket.push(event);
    }

    const buildGroup = (events: EventForSchema<S>[]): TimeSeries<S> =>
      new TimeSeries({
        name: this.name,
        schema: this.schema,
        rows: toRows(this.schema, events) as TimeSeriesInput<S>['rows'],
      });

    if (transform) {
      const result = new Map<string, R>();
      for (const [key, events] of buckets) {
        result.set(key, transform(buildGroup(events), key));
      }
      return result;
    }

    const result = new Map<string, TimeSeries<S>>();
    for (const [key, events] of buckets) {
      result.set(key, buildGroup(events));
    }
    return result;
  }

  /**
   * Example: `series.diff("requests")`.
   * Computes per-event differences for the specified numeric columns.
   * Non-specified columns pass through unchanged. The first event gets
   * `undefined` in affected columns unless `{ drop: true }` is passed,
   * which removes the first event entirely.
   *
   * Example: `series.diff(["requests", "cpu"])`.
   * Multiple columns can be diffed in a single call.
   *
   * Example: `series.diff("requests", { drop: true })`.
   * Drops the first event instead of keeping it with undefined values.
   */
  diff<const Target extends NumericColumnNameForSchema<S>>(
    columns: Target | readonly Target[],
    options?: { drop?: boolean },
  ): TimeSeries<DiffSchema<S, Target>> {
    return this.#diffOrRate('diff', columns, options);
  }

  /**
   * Example: `series.rate("requests")`.
   * Computes the per-second rate of change for the specified numeric columns.
   * Non-specified columns pass through unchanged. The first event gets
   * `undefined` in affected columns unless `{ drop: true }` is passed,
   * which removes the first event entirely.
   *
   * Example: `series.rate(["requests", "cpu"])`.
   * Multiple columns can be rated in a single call.
   *
   * Example: `series.rate("requests", { drop: true })`.
   * Drops the first event instead of keeping it with undefined values.
   */
  rate<const Target extends NumericColumnNameForSchema<S>>(
    columns: Target | readonly Target[],
    options?: { drop?: boolean },
  ): TimeSeries<DiffSchema<S, Target>> {
    return this.#diffOrRate('rate', columns, options);
  }

  /**
   * Example: `series.pctChange("requests")`.
   * Computes the percentage change `(curr - prev) / prev` for the specified
   * numeric columns. Non-specified columns pass through unchanged. The first
   * event gets `undefined` in affected columns unless `{ drop: true }` is
   * passed.
   */
  pctChange<const Target extends NumericColumnNameForSchema<S>>(
    columns: Target | readonly Target[],
    options?: { drop?: boolean },
  ): TimeSeries<DiffSchema<S, Target>> {
    return this.#diffOrRate('pctChange', columns, options);
  }

  #diffOrRate<Target extends NumericColumnNameForSchema<S>>(
    mode: 'diff' | 'rate' | 'pctChange',
    columns: Target | readonly Target[],
    options?: { drop?: boolean },
  ): TimeSeries<DiffSchema<S, Target>> {
    type OutSchema = DiffSchema<S, Target>;

    const cols = typeof columns === 'string' ? [columns] : columns;
    const drop = options?.drop === true;

    if (cols.length === 0) {
      throw new Error(`${mode}() requires at least one column name`);
    }

    const targetSet = new Set<string>(cols);

    const outSchema = Object.freeze(
      this.schema.map((col, i) => {
        if (i === 0) return col;
        if (targetSet.has(col.name)) {
          return { ...col, kind: 'number' as const, required: false as const };
        }
        return col;
      }),
    ) as unknown as OutSchema;

    const events = this.events;
    if (events.length === 0) {
      return TimeSeries.#fromTrustedEvents<OutSchema>(this.name, outSchema, []);
    }

    const resultEvents: EventForSchema<OutSchema>[] = [];

    if (!drop) {
      const firstData = { ...events[0]!.data() } as Record<string, unknown>;
      for (const col of cols) {
        firstData[col] = undefined;
      }
      resultEvents.push(
        new Event(
          events[0]!.key(),
          firstData,
        ) as unknown as EventForSchema<OutSchema>,
      );
    }

    for (let i = 1; i < events.length; i++) {
      const prev = events[i - 1]!;
      const curr = events[i]!;
      const data = { ...curr.data() } as Record<string, unknown>;

      const dt =
        mode === 'rate' ? (curr.begin() - prev.begin()) / 1000 : undefined;

      for (const col of cols) {
        const prevVal = (prev.data() as Record<string, unknown>)[col];
        const currVal = data[col];

        if (typeof currVal === 'number' && typeof prevVal === 'number') {
          const delta = currVal - prevVal;
          if (mode === 'pctChange') {
            data[col] = prevVal !== 0 ? delta / prevVal : undefined;
          } else if (mode === 'rate') {
            data[col] = dt !== 0 ? delta / dt! : undefined;
          } else {
            data[col] = delta;
          }
        } else {
          data[col] = undefined;
        }
      }

      resultEvents.push(
        new Event(curr.key(), data) as unknown as EventForSchema<OutSchema>,
      );
    }

    return TimeSeries.#fromTrustedEvents<OutSchema>(
      this.name,
      outSchema,
      resultEvents,
    );
  }

  /**
   * Example: `series.cumulative({ requests: "sum" })`.
   * Computes running accumulations for the specified numeric columns.
   * Non-accumulated columns pass through unchanged.
   *
   * Built-in accumulators: `"sum"`, `"max"`, `"min"`, `"count"`.
   * Custom accumulators: `(acc: number, value: number) => number`.
   */
  cumulative<const Targets extends NumericColumnNameForSchema<S>>(spec: {
    [K in Targets]:
      | 'sum'
      | 'max'
      | 'min'
      | 'count'
      | ((acc: number, value: number) => number);
  }): TimeSeries<DiffSchema<S, Targets>> {
    type OutSchema = DiffSchema<S, Targets>;

    const entries = Object.entries(spec) as [
      string,
      (
        | 'sum'
        | 'max'
        | 'min'
        | 'count'
        | ((acc: number, value: number) => number)
      ),
    ][];

    if (entries.length === 0) {
      throw new Error('cumulative() requires at least one column');
    }

    const targetSet = new Set<string>(entries.map(([name]) => name));

    const outSchema = Object.freeze(
      this.schema.map((col, i) => {
        if (i === 0) return col;
        if (targetSet.has(col.name)) {
          return { ...col, kind: 'number' as const, required: false as const };
        }
        return col;
      }),
    ) as unknown as OutSchema;

    const events = this.events;
    if (events.length === 0) {
      return TimeSeries.#fromTrustedEvents<OutSchema>(this.name, outSchema, []);
    }

    const state = new Map<
      string,
      {
        acc: number | undefined;
        apply: (acc: number | undefined, value: number) => number;
      }
    >();
    for (const [name, reducer] of entries) {
      if (typeof reducer === 'function') {
        const fn = reducer;
        state.set(name, {
          acc: undefined,
          apply: (acc, v) => (acc === undefined ? v : fn(acc, v)),
        });
      } else {
        switch (reducer) {
          case 'sum':
            state.set(name, {
              acc: undefined,
              apply: (acc, v) => (acc ?? 0) + v,
            });
            break;
          case 'count':
            state.set(name, { acc: undefined, apply: (acc) => (acc ?? 0) + 1 });
            break;
          case 'max':
            state.set(name, {
              acc: undefined,
              apply: (acc, v) => (acc === undefined || v > acc ? v : acc),
            });
            break;
          case 'min':
            state.set(name, {
              acc: undefined,
              apply: (acc, v) => (acc === undefined || v < acc ? v : acc),
            });
            break;
        }
      }
    }

    const resultEvents: EventForSchema<OutSchema>[] = [];
    for (const event of events) {
      const data = { ...event.data() } as Record<string, unknown>;
      for (const [name, s] of state) {
        const raw = data[name];
        if (typeof raw === 'number') {
          s.acc = s.apply(s.acc, raw);
          data[name] = s.acc;
        } else {
          data[name] = s.acc;
        }
      }
      resultEvents.push(
        new Event(event.key(), data) as unknown as EventForSchema<OutSchema>,
      );
    }

    return TimeSeries.#fromTrustedEvents<OutSchema>(
      this.name,
      outSchema,
      resultEvents,
    );
  }

  /**
   * Example: `series.shift("value", 1)`.
   * Lags column values by N events (positive N) or leads them (negative N).
   * Vacated positions get `undefined`.
   */
  shift<const Target extends NumericColumnNameForSchema<S>>(
    columns: Target | readonly Target[],
    n: number,
  ): TimeSeries<DiffSchema<S, Target>> {
    type OutSchema = DiffSchema<S, Target>;

    const cols = typeof columns === 'string' ? [columns] : columns;

    if (cols.length === 0) {
      throw new Error('shift() requires at least one column name');
    }
    if (!Number.isInteger(n)) {
      throw new Error('shift() requires an integer offset');
    }

    const targetSet = new Set<string>(cols);

    const outSchema = Object.freeze(
      this.schema.map((col, i) => {
        if (i === 0) return col;
        if (targetSet.has(col.name)) {
          return { ...col, kind: 'number' as const, required: false as const };
        }
        return col;
      }),
    ) as unknown as OutSchema;

    const events = this.events;
    if (events.length === 0) {
      return TimeSeries.#fromTrustedEvents<OutSchema>(this.name, outSchema, []);
    }

    const resultEvents: EventForSchema<OutSchema>[] = [];
    for (let i = 0; i < events.length; i++) {
      const data = { ...events[i]!.data() } as Record<string, unknown>;
      const srcIdx = i - n;
      for (const col of cols) {
        if (srcIdx >= 0 && srcIdx < events.length) {
          data[col] = (events[srcIdx]!.data() as Record<string, unknown>)[col];
        } else {
          data[col] = undefined;
        }
      }
      resultEvents.push(
        new Event(
          events[i]!.key(),
          data,
        ) as unknown as EventForSchema<OutSchema>,
      );
    }

    return TimeSeries.#fromTrustedEvents<OutSchema>(
      this.name,
      outSchema,
      resultEvents,
    );
  }

  /**
   * Example: `series.fill("hold")`.
   * Fills `undefined` values using the given strategy for all payload columns.
   *
   * Example: `series.fill({ cpu: "linear", host: "hold" })`.
   * Per-column fill strategies. Unmentioned columns are left as-is.
   * Strategy names: `"hold"` (forward fill), `"linear"` (time-interpolated),
   * `"zero"` (fill with 0). A non-string value is used as a literal fill value.
   *
   * Example: `series.fill("hold", { limit: 3 })`.
   * Caps consecutive fills per column. After `limit` consecutive fills, further
   * `undefined` values are left as-is until a real value resets the counter.
   *
   * `"linear"` requires known values on both sides of a gap to interpolate.
   * Leading and trailing `undefined` runs are left unfilled.
   */
  fill(
    strategy: FillStrategy | FillMapping<S>,
    options?: { limit?: number },
  ): TimeSeries<S> {
    if (this.events.length === 0) {
      return this;
    }

    const colNames = this.schema.slice(1).map((c) => c.name);
    const specs: Map<string, ResolvedFillSpec> = new Map();

    if (typeof strategy === 'string') {
      for (const name of colNames) {
        specs.set(name, { mode: strategy });
      }
    } else {
      const strategies: Set<string> = new Set([
        'hold',
        'bfill',
        'linear',
        'zero',
      ]);
      for (const [name, spec] of Object.entries(strategy)) {
        if (typeof spec === 'string' && strategies.has(spec)) {
          specs.set(name, { mode: spec as FillStrategy });
        } else {
          specs.set(name, { mode: 'literal', value: spec as ScalarValue });
        }
      }
    }

    const limit = options?.limit;
    const n = this.events.length;

    const columns: Record<string, (ScalarValue | undefined)[]> = {};
    for (const name of colNames) {
      columns[name] = new Array(n);
    }
    for (let i = 0; i < n; i++) {
      const data = this.events[i]!.data();
      for (const name of colNames) {
        columns[name]![i] = data[name as keyof typeof data] as
          | ScalarValue
          | undefined;
      }
    }

    const times = new Array<number>(n);
    for (let i = 0; i < n; i++) {
      times[i] = this.events[i]!.begin();
    }

    for (const [name, spec] of specs) {
      const col = columns[name];
      if (!col) continue;

      switch (spec.mode) {
        case 'hold': {
          let last: ScalarValue | undefined;
          let consecutive = 0;
          for (let i = 0; i < n; i++) {
            if (col[i] !== undefined) {
              last = col[i];
              consecutive = 0;
            } else if (last !== undefined) {
              consecutive++;
              if (limit === undefined || consecutive <= limit) {
                col[i] = last;
              }
            }
          }
          break;
        }
        case 'bfill': {
          let next: ScalarValue | undefined;
          let consecutive = 0;
          for (let i = n - 1; i >= 0; i--) {
            if (col[i] !== undefined) {
              next = col[i];
              consecutive = 0;
            } else if (next !== undefined) {
              consecutive++;
              if (limit === undefined || consecutive <= limit) {
                col[i] = next;
              }
            }
          }
          break;
        }
        case 'zero': {
          let consecutive = 0;
          for (let i = 0; i < n; i++) {
            if (col[i] !== undefined) {
              consecutive = 0;
            } else {
              consecutive++;
              if (limit === undefined || consecutive <= limit) {
                col[i] = 0;
              }
            }
          }
          break;
        }
        case 'literal': {
          let consecutive = 0;
          for (let i = 0; i < n; i++) {
            if (col[i] !== undefined) {
              consecutive = 0;
            } else {
              consecutive++;
              if (limit === undefined || consecutive <= limit) {
                col[i] = spec.value;
              }
            }
          }
          break;
        }
        case 'linear': {
          let gapStart = -1;
          for (let i = 0; i < n; i++) {
            if (col[i] !== undefined) {
              if (gapStart >= 0 && gapStart > 0) {
                const before = col[gapStart - 1] as number;
                const after = col[i] as number;
                const t0 = times[gapStart - 1]!;
                const t1 = times[i]!;
                const span = t1 - t0;
                const gapLen = i - gapStart;
                for (let j = gapStart; j < i; j++) {
                  const fillIndex = j - gapStart + 1;
                  if (limit !== undefined && fillIndex > limit) break;
                  if (span === 0) {
                    col[j] = before;
                  } else {
                    const ratio = (times[j]! - t0) / span;
                    col[j] = before + (after - before) * ratio;
                  }
                }
              }
              gapStart = -1;
            } else if (gapStart < 0) {
              gapStart = i;
            }
          }
          break;
        }
      }
    }

    const resultEvents: EventForSchema<S>[] = [];
    for (let i = 0; i < n; i++) {
      const data: Record<string, unknown> = {};
      for (const name of colNames) {
        data[name] = columns[name]![i];
      }
      resultEvents.push(
        new Event(this.events[i]!.key(), data) as unknown as EventForSchema<S>,
      );
    }

    return TimeSeries.#fromTrustedEvents<S>(
      this.name,
      this.schema,
      resultEvents,
    );
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
          ] as AggregateReducer;
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

    const windowMs = parseDuration(window);
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
          const reducer = mapping[
            column.name as keyof Mapping
          ] as AggregateReducer;
          const values = contributors.map((candidate) => {
            const data = candidate.data();
            return data[column.name as keyof typeof data];
          }) as ReadonlyArray<ScalarValue | undefined>;
          return applyAggregateReducer(reducer, values);
        });

        return Object.freeze([bucket, ...aggregated]);
      });

      return new TimeSeries({
        name: this.name,
        schema: resultSchema as unknown as SeriesSchema,
        rows: resultRows as unknown as TimeSeriesInput<SeriesSchema>['rows'],
      }) as unknown as TimeSeries<AggregateSchema<S, Mapping>>;
    }

    const resultColumns = buildResultColumns();
    const resultSchema = Object.freeze([
      this.schema[0],
      ...resultColumns,
    ]) as unknown as RollingSchema<S, Mapping>;
    const reducerStates = resultColumns.map((column) => {
      const reducer = mapping[column.name as keyof Mapping] as AggregateReducer;
      return isBuiltInAggregateReducer(reducer)
        ? createRollingReducerState(reducer)
        : null;
    });
    const beginTimes = this.events.map((event) => event.begin());
    const resultRows: TimeSeriesInput<SeriesSchema>['rows'][number][] =
      new Array(this.events.length);
    let windowStart = 0;
    let windowEnd = 0;
    const addEvent = (index: number): void => {
      const event = this.events[index]!;
      const data = event.data();
      for (let i = 0; i < reducerStates.length; i++) {
        const state = reducerStates[i];
        if (state) {
          const column = resultColumns[i]!;
          state.add(
            index,
            data[column.name as keyof typeof data] as ScalarValue | undefined,
          );
        }
      }
    };
    const removeEvent = (index: number): void => {
      const event = this.events[index]!;
      const data = event.data();
      for (let i = 0; i < reducerStates.length; i++) {
        const state = reducerStates[i];
        if (state) {
          const column = resultColumns[i]!;
          state.remove(
            index,
            data[column.name as keyof typeof data] as ScalarValue | undefined,
          );
        }
      }
    };
    const snapshotWindow = (): (ScalarValue | undefined)[] =>
      resultColumns.map((column, i) => {
        const state = reducerStates[i];
        if (state) return state.snapshot();
        const reducer = mapping[
          column.name as keyof Mapping
        ] as AggregateReducer;
        const values = this.events
          .slice(windowStart, windowEnd)
          .map((event) => {
            const data = event.data();
            return data[column.name as keyof typeof data];
          }) as ReadonlyArray<ScalarValue | undefined>;
        return applyAggregateReducer(reducer, values);
      });

    if (alignment === 'trailing') {
      for (let groupStart = 0; groupStart < this.events.length; ) {
        const anchor = beginTimes[groupStart]!;
        let groupEnd = groupStart + 1;
        while (
          groupEnd < this.events.length &&
          beginTimes[groupEnd] === anchor
        ) {
          groupEnd += 1;
        }

        while (
          windowEnd < this.events.length &&
          beginTimes[windowEnd]! <= anchor
        ) {
          addEvent(windowEnd);
          windowEnd += 1;
        }

        const lowerBound = anchor - windowMs;
        while (
          windowStart < windowEnd &&
          beginTimes[windowStart]! <= lowerBound
        ) {
          removeEvent(windowStart);
          windowStart += 1;
        }

        const aggregated = snapshotWindow();
        for (let index = groupStart; index < groupEnd; index++) {
          resultRows[index] = Object.freeze([
            this.events[index]!.key(),
            ...aggregated,
          ]) as unknown as TimeSeriesInput<SeriesSchema>['rows'][number];
        }

        groupStart = groupEnd;
      }
    } else if (alignment === 'leading') {
      for (let groupStart = 0; groupStart < this.events.length; ) {
        const anchor = beginTimes[groupStart]!;
        let groupEnd = groupStart + 1;
        while (
          groupEnd < this.events.length &&
          beginTimes[groupEnd] === anchor
        ) {
          groupEnd += 1;
        }

        const lowerBound = anchor;
        while (
          windowStart < windowEnd &&
          beginTimes[windowStart]! < lowerBound
        ) {
          removeEvent(windowStart);
          windowStart += 1;
        }

        const upperBound = anchor + windowMs;
        while (
          windowEnd < this.events.length &&
          beginTimes[windowEnd]! < upperBound
        ) {
          addEvent(windowEnd);
          windowEnd += 1;
        }

        const aggregated = snapshotWindow();
        for (let index = groupStart; index < groupEnd; index++) {
          resultRows[index] = Object.freeze([
            this.events[index]!.key(),
            ...aggregated,
          ]) as unknown as TimeSeriesInput<SeriesSchema>['rows'][number];
        }

        groupStart = groupEnd;
      }
    } else {
      const halfWindow = windowMs / 2;
      for (let groupStart = 0; groupStart < this.events.length; ) {
        const anchor = beginTimes[groupStart]!;
        let groupEnd = groupStart + 1;
        while (
          groupEnd < this.events.length &&
          beginTimes[groupEnd] === anchor
        ) {
          groupEnd += 1;
        }

        const lowerBound = anchor - halfWindow;
        while (
          windowStart < windowEnd &&
          beginTimes[windowStart]! < lowerBound
        ) {
          removeEvent(windowStart);
          windowStart += 1;
        }

        const upperBound = anchor + halfWindow;
        while (
          windowEnd < this.events.length &&
          beginTimes[windowEnd]! < upperBound
        ) {
          addEvent(windowEnd);
          windowEnd += 1;
        }

        const aggregated = snapshotWindow();
        for (let index = groupStart; index < groupEnd; index++) {
          resultRows[index] = Object.freeze([
            this.events[index]!.key(),
            ...aggregated,
          ]) as unknown as TimeSeriesInput<SeriesSchema>['rows'][number];
        }

        groupStart = groupEnd;
      }
    }

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

      const loessAnchors: number[] = [];
      const loessValues: number[] = [];
      for (let index = 0; index < anchors.length; index++) {
        const value = sourceValues[index];
        if (typeof value === 'number') {
          loessAnchors.push(anchors[index]!);
          loessValues.push(value);
        }
      }

      const resultRows = this.events.map((event, index) => {
        const smoothed = loessAt(
          anchors[index]!,
          loessAnchors,
          loessValues,
          span,
        );
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
    const windowMs = parseDuration(window!);
    const alignment = options.alignment ?? 'trailing';
    const resultValues = new Array<number | undefined>(this.events.length);
    let windowStart = 0;
    let windowEnd = 0;
    let numericSum = 0;
    let numericCount = 0;
    const addEvent = (index: number): void => {
      const value = sourceValues[index];
      if (typeof value === 'number') {
        numericSum += value;
        numericCount += 1;
      }
    };
    const removeEvent = (index: number): void => {
      const value = sourceValues[index];
      if (typeof value === 'number') {
        numericSum -= value;
        numericCount -= 1;
      }
    };
    const snapshot = (): number | undefined =>
      numericCount === 0 ? undefined : numericSum / numericCount;

    for (let groupStart = 0; groupStart < this.events.length; ) {
      const anchor = anchors[groupStart]!;
      let groupEnd = groupStart + 1;
      while (groupEnd < this.events.length && anchors[groupEnd] === anchor) {
        groupEnd += 1;
      }

      if (alignment === 'trailing') {
        while (
          windowEnd < this.events.length &&
          anchors[windowEnd]! <= anchor
        ) {
          addEvent(windowEnd);
          windowEnd += 1;
        }

        const lowerBound = anchor - windowMs;
        while (windowStart < windowEnd && anchors[windowStart]! <= lowerBound) {
          removeEvent(windowStart);
          windowStart += 1;
        }
      } else if (alignment === 'leading') {
        while (windowStart < windowEnd && anchors[windowStart]! < anchor) {
          removeEvent(windowStart);
          windowStart += 1;
        }

        const upperBound = anchor + windowMs;
        while (
          windowEnd < this.events.length &&
          anchors[windowEnd]! < upperBound
        ) {
          addEvent(windowEnd);
          windowEnd += 1;
        }
      } else {
        const halfWindow = windowMs / 2;
        while (
          windowStart < windowEnd &&
          anchors[windowStart]! < anchor - halfWindow
        ) {
          removeEvent(windowStart);
          windowStart += 1;
        }

        const upperBound = anchor + halfWindow;
        while (
          windowEnd < this.events.length &&
          anchors[windowEnd]! < upperBound
        ) {
          addEvent(windowEnd);
          windowEnd += 1;
        }
      }

      const smoothed = snapshot();
      for (let index = groupStart; index < groupEnd; index++) {
        resultValues[index] = smoothed;
      }

      groupStart = groupEnd;
    }

    const resultRows = this.events.map((event, index) => {
      const smoothed = resultValues[index];
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
    return TimeSeries.#fromTrustedEvents(
      this.name,
      this.schema,
      this.events.slice(beginIndex, endIndex),
    );
  }

  /** Example: `series.filter(event => event.get("active"))`. Returns a new series containing only events that match the predicate. */
  filter(
    predicate: (event: EventForSchema<S>, index: number) => boolean,
  ): TimeSeries<S> {
    return TimeSeries.#fromTrustedEvents(
      this.name,
      this.schema,
      this.events.filter((event, index) => predicate(event, index)),
    );
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
    const index = this.bisect(normalizedKey);
    return (
      index < this.events.length &&
      this.events[index]!.key().equals(normalizedKey)
    );
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

    return TimeSeries.#fromTrustedEvents(this.name, this.schema, trimmedEvents);
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

    return TimeSeries.#fromTrustedEvents(
      this.name,
      resultSchema as unknown as SeriesSchema,
      resultEvents as unknown as EventForSchema<SeriesSchema>[],
    ) as unknown as TimeSeries<SelectSchema<S, Keys[number] & string>>;
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

    return TimeSeries.#fromTrustedEvents(
      this.name,
      resultSchema as unknown as SeriesSchema,
      resultEvents as unknown as EventForSchema<SeriesSchema>[],
    ) as unknown as TimeSeries<RenameSchema<S, Mapping>>;
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
  ): any {
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

    return TimeSeries.#fromTrustedEvents(
      this.name,
      resultSchema as unknown as SeriesSchema,
      nextEvents as unknown as EventForSchema<SeriesSchema>[],
    ) as unknown as TimeSeries<
      CollapseSchema<S, Keys[number] & string, Name, R, boolean>
    >;
  }

  /** Example: `series.length`. Returns the number of events in the series. */
  get length(): number {
    return this.events.length;
  }

  /** Example: `for (const event of series) { ... }`. Iterates events in order. */
  [Symbol.iterator](): Iterator<EventForSchema<S>> {
    let index = 0;
    const events = this.events;
    return {
      next(): IteratorResult<EventForSchema<S>> {
        if (index < events.length) {
          return { value: events[index++]!, done: false };
        }
        return { value: undefined as any, done: true };
      },
    };
  }

  /** Example: `series.toArray()`. Returns a shallow copy of the event array. */
  toArray(): EventForSchema<S>[] {
    return this.events.slice();
  }
}

function aggregateInternal<S extends SeriesSchema>(
  series: TimeSeries<S>,
  sequence: SequenceLike,
  mapping: AggregateMap<S> | AggregateOutputMap<S>,
  options: { range?: TemporalLike } = {},
): TimeSeries<SeriesSchema> {
  const range = options.range ?? series.timeRange();
  const aggregateColumns = normalizeAggregateColumns(series.schema, mapping);
  const resultSchema = Object.freeze([
    { name: 'interval', kind: 'interval' as const },
    ...aggregateColumns.map((column) => ({
      name: column.output,
      kind: column.kind,
      required: false as const,
    })),
  ]) as unknown as SeriesSchema;

  if (!range) {
    return new TimeSeries({
      name: series.name,
      schema: resultSchema,
      rows: [],
    });
  }

  const buckets = toBoundedSequence(sequence, range, 'begin').intervals();
  const columns = aggregateColumns;

  if (isTimeKeyed(series)) {
    const builtInOnly = columns.every((column) =>
      isBuiltInAggregateReducer(column.reducer),
    );
    let eventIndex = 0;
    const resultRows = buckets.map((bucket) => {
      const states = builtInOnly
        ? columns.map((column) =>
            createAggregateBucketState(column.reducer as AggregateFunction),
          )
        : undefined;

      while (
        eventIndex < series.events.length &&
        series.events[eventIndex]!.begin() < bucket.begin()
      ) {
        eventIndex += 1;
      }

      const bucketStart = eventIndex;
      let scanIndex = bucketStart;
      while (
        scanIndex < series.events.length &&
        series.events[scanIndex]!.begin() < bucket.end()
      ) {
        if (states) {
          const data = series.events[scanIndex]!.data();
          for (let index = 0; index < columns.length; index += 1) {
            const column = columns[index]!;
            states[index]!.add(
              data[column.source as keyof typeof data] as
                | ScalarValue
                | undefined,
            );
          }
        }
        scanIndex += 1;
      }

      eventIndex = scanIndex;
      if (states) {
        return Object.freeze([
          bucket,
          ...states.map((state) => state.snapshot()),
        ]);
      }
      const contributors = series.events.slice(bucketStart, scanIndex);
      const aggregated = columns.map((column) => {
        const values = contributors.map((event) => {
          const data = event.data();
          return data[column.source as keyof typeof data];
        }) as ReadonlyArray<ScalarValue | undefined>;
        return applyAggregateReducer(column.reducer, values);
      });
      return Object.freeze([bucket, ...aggregated]);
    });

    return new TimeSeries({
      name: series.name,
      schema: resultSchema as unknown as SeriesSchema,
      rows: resultRows as unknown as TimeSeriesInput<SeriesSchema>['rows'],
    });
  }

  const resultRows = buckets.map((bucket) => {
    const contributors = series.events.filter((event) =>
      bucketOverlapsHalfOpen(bucket, event.key()),
    );
    const aggregated = columns.map((column) => {
      const values = contributors.map((event) => {
        const data = event.data();
        return data[column.source as keyof typeof data];
      }) as ReadonlyArray<ScalarValue | undefined>;
      return applyAggregateReducer(column.reducer, values);
    });
    return Object.freeze([bucket, ...aggregated]);
  });

  return new TimeSeries({
    name: series.name,
    schema: resultSchema,
    rows: resultRows as unknown as TimeSeriesInput<SeriesSchema>['rows'],
  });
}

function alignHoldAt<S extends SeriesSchema>(
  series: TimeSeries<S>,
  t: number,
): EventDataForSchema<S> {
  const event = series.atOrBefore(new Time(t));
  return (event?.data() ?? {}) as EventDataForSchema<S>;
}

function alignLinearAt<S extends SeriesSchema>(
  series: TimeSeries<S>,
  t: number,
  valueColumns: ValueColumnsForSchema<S>,
  cursor?: AlignCursor,
): EventDataForSchema<S> {
  const events = series.events;
  const hasCursor = cursor !== undefined;
  let index = hasCursor ? cursor.index : series.bisect(t);

  if (hasCursor) {
    while (index < events.length && events[index]!.begin() < t) {
      index += 1;
    }
    cursor.index = index;
  }

  if (index < events.length && events[index]!.begin() === t) {
    return events[index]!.data() as EventDataForSchema<S>;
  }

  if (index === 0) {
    return {} as EventDataForSchema<S>;
  }

  const previous = events[index - 1]!;
  const next = events[index];
  if (!next || previous.begin() === next.begin()) {
    return previous.data() as EventDataForSchema<S>;
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
      result[column.name] = previousValue + (nextValue - previousValue) * ratio;
      continue;
    }

    result[column.name] = previousValue;
  }

  return result as EventDataForSchema<S>;
}
