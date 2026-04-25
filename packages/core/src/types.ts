import type {
  EventKey,
  IntervalInput,
  TimeRangeInput,
  TimestampInput,
} from './temporal.js';
import type { Event } from './Event.js';
import type { Interval } from './Interval.js';
import type { Time } from './Time.js';
import type { TimeRange } from './TimeRange.js';

/** Marker symbol for sources that emit `'evict'` events. @internal */
export const EMITS_EVICT: unique symbol = Symbol.for('pond-ts:emitsEvict');

export type ScalarKind = 'number' | 'string' | 'boolean' | 'array';
export type ScalarValue = number | string | boolean;
/**
 * A read-only array of scalars. Array-kind columns carry values of this type.
 * Currently populated by reducers that collapse a bucket into a list
 * (e.g. `unique`). Inert with respect to numerical operators (`diff`, `rate`,
 * `cumulative`, `rolling`) — those filter to `kind: 'number'` columns.
 */
export type ArrayValue = ReadonlyArray<ScalarValue>;
/**
 * Anything a value column cell may hold at runtime. Widens `ScalarValue`
 * with `ArrayValue` for columns declared `kind: 'array'`.
 */
export type ColumnValue = ScalarValue | ArrayValue;
export type FirstColKind = 'time' | 'interval' | 'timeRange';

export type ColumnDef<Name extends string, Kind extends string> = {
  name: Name;
  kind: Kind;
  required?: boolean;
};

export type FirstColumn =
  | ColumnDef<'time', 'time'>
  | ColumnDef<'interval', 'interval'>
  | ColumnDef<'timeRange', 'timeRange'>;

export type ValueColumn<Name extends string = string> = ColumnDef<
  Name,
  ScalarKind
>;

export type SeriesSchema = readonly [FirstColumn, ...ValueColumn[]];

export type ValueForKind<K extends string> = K extends 'time'
  ? TimestampInput | Time
  : K extends 'interval'
    ? IntervalInput | Interval
    : K extends 'timeRange'
      ? TimeRangeInput | TimeRange
      : K extends 'number'
        ? number
        : K extends 'string'
          ? string
          : K extends 'boolean'
            ? boolean
            : K extends 'array'
              ? ArrayValue
              : never;

export type RowForSchema<S extends readonly ColumnDef<string, string>[]> = {
  [I in keyof S]: S[I] extends ColumnDef<any, infer K>
    ? ValueForKind<K>
    : never;
};

export type TimeSeriesInput<S extends SeriesSchema> = {
  name: string;
  schema: S;
  rows: ReadonlyArray<RowForSchema<S>>;
};

export type JsonTimestampInput = number | string;
export type JsonTimeRangeInput =
  | readonly [start: JsonTimestampInput, end: JsonTimestampInput]
  | { start: JsonTimestampInput; end: JsonTimestampInput };
export type JsonIntervalInput =
  | readonly [
      value: string | number,
      start: JsonTimestampInput,
      end: JsonTimestampInput,
    ]
  | {
      value: string | number;
      start: JsonTimestampInput;
      end: JsonTimestampInput;
    };

export type JsonValueForKind<K extends string> = K extends 'time'
  ? JsonTimestampInput
  : K extends 'timeRange'
    ? JsonTimeRangeInput
    : K extends 'interval'
      ? JsonIntervalInput
      : K extends 'number'
        ? number
        : K extends 'string'
          ? string
          : K extends 'boolean'
            ? boolean
            : K extends 'array'
              ? ArrayValue
              : never;

export type JsonRowForSchema<S extends readonly ColumnDef<string, string>[]> = {
  [I in keyof S]: S[I] extends ColumnDef<any, infer K>
    ? JsonValueForKind<K> | null
    : never;
};

export type JsonObjectRowForSchema<S extends SeriesSchema> = {
  [C in S[number] as C['name']]: C extends ColumnDef<any, infer K>
    ? JsonValueForKind<K> | null
    : never;
};

export type TimeSeriesJsonInput<S extends SeriesSchema> = {
  name: string;
  schema: S;
  rows: ReadonlyArray<JsonRowForSchema<S> | JsonObjectRowForSchema<S>>;
};

export type JsonRowFormat = 'array' | 'object';

export type NormalizedValueForKind<K extends string> = K extends 'time'
  ? Time
  : K extends 'timeRange'
    ? TimeRange
    : K extends 'interval'
      ? Interval
      : K extends 'number'
        ? number
        : K extends 'string'
          ? string
          : K extends 'boolean'
            ? boolean
            : K extends 'array'
              ? ArrayValue
              : never;

export type NormalizedRowForSchema<
  S extends readonly ColumnDef<string, string>[],
> = {
  [I in keyof S]: S[I] extends ColumnDef<any, infer K>
    ? NormalizedValueForKind<K>
    : never;
};

type NormalizedDataValueForColumn<C extends ColumnDef<string, string>> =
  C extends ColumnDef<any, infer K>
    ? K extends FirstColKind
      ? EventKeyForKind<K>
      : C['required'] extends false
        ? NormalizedValueForKind<K> | undefined
        : NormalizedValueForKind<K>
    : never;

export type NormalizedObjectRowForSchema<S extends SeriesSchema> = Partial<{
  [C in S[number] as C['name']]: NormalizedDataValueForColumn<C>;
}>;

export type NormalizedObjectRow = Readonly<
  Record<string, EventKey | ColumnValue | undefined>
>;

type DataValueForColumn<C extends ColumnDef<string, string>> =
  C extends ColumnDef<any, infer K>
    ? C['required'] extends false
      ? NormalizedValueForKind<K> | undefined
      : NormalizedValueForKind<K>
    : never;

type DataColumnsForSchema<S extends SeriesSchema> = S extends readonly [
  FirstColumn,
  ...infer Rest,
]
  ? Rest extends readonly ValueColumn[]
    ? Rest
    : never
  : never;

export type EventDataForSchema<S extends SeriesSchema> = {
  [C in DataColumnsForSchema<S>[number] as C['name']]: DataValueForColumn<C>;
};

/**
 * Wide-row shape returned by `TimeSeries.toPoints()`: `ts` plus every
 * value column from the schema. Each value column is `T | undefined`
 * regardless of the schema's `required` flag.
 *
 * Why ignore `required`? Charting workflows are the dominant consumer.
 * Even on a `required: true` schema, transformed series can produce
 * rows with `undefined` cells — `baseline()` adds optional `avg` /
 * `sd` / `upper` / `lower` columns, `align()` produces gaps before
 * the first source event, and aggregations on partial buckets emit
 * `undefined` for some reducers. Chart libraries handle the
 * `T | undefined` shape natively (rendering gaps via
 * `connectNulls={false}`); narrowing to `T` would force every caller
 * to widen back. If you have a fully-required schema and want strict
 * narrowing, work with `EventDataForSchema<S>` directly instead of
 * `toPoints()`.
 *
 * Caveat: a value column literally named `ts` would collide with the
 * timestamp key. The library doesn't currently guard against this;
 * pick a different column name if it matters.
 */
export type PointRowForSchema<S extends SeriesSchema> = { ts: number } & {
  [C in DataColumnsForSchema<S>[number] as C['name']]:
    | NormalizedValueForKind<C['kind']>
    | undefined;
};

export type EventKeyForKind<K extends FirstColKind> = K extends 'time'
  ? Time
  : K extends 'timeRange'
    ? TimeRange
    : K extends 'interval'
      ? Interval
      : never;

export type EventKeyForSchema<S extends SeriesSchema> =
  S[0] extends ColumnDef<any, infer K>
    ? K extends FirstColKind
      ? EventKeyForKind<K>
      : EventKey
    : EventKey;

export type EventForSchema<S extends SeriesSchema> = Event<
  EventKeyForSchema<S>,
  EventDataForSchema<S>
>;

export interface LiveSource<S extends SeriesSchema> {
  readonly name: string;
  readonly schema: S;
  readonly length: number;
  at(index: number): EventForSchema<S> | undefined;
  on(type: 'event', fn: (event: EventForSchema<S>) => void): () => void;
}

export type RekeySchema<
  S extends SeriesSchema,
  First extends FirstColumn,
> = readonly [First, ...ValueColumnsForSchema<S>];

export type TimeKeyedSchema<S extends SeriesSchema> = RekeySchema<
  S,
  ColumnDef<'time', 'time'>
>;
export type TimeRangeKeyedSchema<S extends SeriesSchema> = RekeySchema<
  S,
  ColumnDef<'timeRange', 'timeRange'>
>;
export type IntervalKeyedSchema<S extends SeriesSchema> = RekeySchema<
  S,
  ColumnDef<'interval', 'interval'>
>;

export type ValueColumnsForSchema<S extends SeriesSchema> = S extends readonly [
  FirstColumn,
  ...infer Rest,
]
  ? Rest extends readonly ValueColumn[]
    ? Rest
    : never
  : never;

export type KindForValue<V extends ScalarValue> = V extends number
  ? 'number'
  : V extends string
    ? 'string'
    : 'boolean';

export type CollapseData<
  D,
  Keys extends keyof D,
  Name extends string,
  R extends ScalarValue,
  Append extends boolean = false,
> = Append extends true
  ? Readonly<D & Record<Name, R>>
  : Readonly<Omit<D, Keys> & Record<Name, R>>;

type DropSelectedColumns<
  Columns extends readonly ValueColumn[],
  Keys extends string,
> = Columns extends readonly [infer Head, ...infer Tail]
  ? Head extends ValueColumn
    ? Tail extends readonly ValueColumn[]
      ? Head['name'] extends Keys
        ? DropSelectedColumns<Tail, Keys>
        : [Head, ...DropSelectedColumns<Tail, Keys>]
      : []
    : []
  : [];

type OutputColumn<Name extends string, R extends ScalarValue> = ColumnDef<
  Name,
  KindForValue<R>
>;

export type CollapseColumns<
  Columns extends readonly ValueColumn[],
  Keys extends string,
  Name extends string,
  R extends ScalarValue,
  Append extends boolean = false,
> = Append extends true
  ? [...Columns, OutputColumn<Name, R>]
  : [...DropSelectedColumns<Columns, Keys>, OutputColumn<Name, R>];

export type CollapseSchema<
  S extends SeriesSchema,
  Keys extends keyof EventDataForSchema<S> & string,
  Name extends string,
  R extends ScalarValue,
  Append extends boolean = false,
> = readonly [
  S[0],
  ...CollapseColumns<ValueColumnsForSchema<S>, Keys, Name, R, Append>,
];

export type SelectData<D, Keys extends keyof D> = Readonly<Pick<D, Keys>>;

type PickSelectedColumns<
  Columns extends readonly ValueColumn[],
  Keys extends string,
> = Columns extends readonly [infer Head, ...infer Tail]
  ? Head extends ValueColumn
    ? Tail extends readonly ValueColumn[]
      ? Head['name'] extends Keys
        ? [Head, ...PickSelectedColumns<Tail, Keys>]
        : PickSelectedColumns<Tail, Keys>
      : []
    : []
  : [];

export type SelectSchema<
  S extends SeriesSchema,
  Keys extends keyof EventDataForSchema<S> & string,
> = readonly [S[0], ...PickSelectedColumns<ValueColumnsForSchema<S>, Keys>];

export type RenameMap<D> = Partial<{
  [K in keyof D & string]: string;
}>;

export type RenameData<D, Mapping extends RenameMap<D>> = Readonly<{
  [Name in keyof D & string as Name extends keyof Mapping
    ? Mapping[Name] extends string
      ? Mapping[Name]
      : Name
    : Name]: D[Name];
}>;

type RenameColumn<
  Column extends ValueColumn,
  Mapping extends Partial<Record<string, string>>,
> =
  Column extends ColumnDef<infer Name, infer Kind>
    ? ColumnDef<
        Name extends keyof Mapping
          ? Mapping[Name] extends string
            ? Mapping[Name]
            : Name
          : Name,
        Kind
      >
    : never;

type RenameColumns<
  Columns extends readonly ValueColumn[],
  Mapping extends Partial<Record<string, string>>,
> = Columns extends readonly [infer Head, ...infer Tail]
  ? Head extends ValueColumn
    ? Tail extends readonly ValueColumn[]
      ? [RenameColumn<Head, Mapping>, ...RenameColumns<Tail, Mapping>]
      : []
    : []
  : [];

export type RenameSchema<
  S extends SeriesSchema,
  Mapping extends RenameMap<EventDataForSchema<S>>,
> = readonly [S[0], ...RenameColumns<ValueColumnsForSchema<S>, Mapping>];

type OptionalizeColumn<Column extends ValueColumn> =
  Column extends ColumnDef<infer Name, infer Kind>
    ? ColumnDef<Name, Kind> & { readonly required: false }
    : never;

type OptionalizeColumns<Columns extends readonly ValueColumn[]> =
  Columns extends readonly [infer Head, ...infer Tail]
    ? Head extends ValueColumn
      ? Tail extends readonly ValueColumn[]
        ? [OptionalizeColumn<Head>, ...OptionalizeColumns<Tail>]
        : []
      : []
    : [];

export type AlignSchema<S extends SeriesSchema> = readonly [
  ColumnDef<'interval', 'interval'>,
  ...OptionalizeColumns<ValueColumnsForSchema<S>>,
];

export type AggregateFunction =
  | 'sum'
  | 'avg'
  | 'min'
  | 'max'
  | 'count'
  | 'first'
  | 'last'
  | 'median'
  | 'stdev'
  | 'difference'
  | 'keep'
  | 'unique'
  | `p${number}`
  | `top${number}`;
/**
 * Custom aggregate reducers receive every value in a bucket (including
 * `undefined`) and return a single result. The return type is widened to
 * `ColumnValue` so reducers may emit an array — the resulting column's
 * schema kind is inferred as `'array'` when the custom reducer output is
 * declared via `AggregateOutputSpec.kind`.
 */
export type CustomAggregateReducer = (
  values: ReadonlyArray<ColumnValue | undefined>,
) => ColumnValue | undefined;
export type AggregateReducer = AggregateFunction | CustomAggregateReducer;
export type RollingAlignment = 'trailing' | 'leading' | 'centered';
export type SmoothMethod = 'ema' | 'movingAverage' | 'loess';
export type JoinType = 'inner' | 'left' | 'right' | 'outer';
export type JoinConflictMode = 'error' | 'prefix';

type AggregateFunctionsForKind<Kind extends ScalarKind> = Kind extends 'number'
  ? AggregateReducer
  : Kind extends 'array'
    ?
        | 'count'
        | 'first'
        | 'last'
        | 'keep'
        | 'unique'
        | `top${number}`
        | CustomAggregateReducer
    :
        | 'count'
        | 'first'
        | 'last'
        | 'keep'
        | 'unique'
        | `top${number}`
        | CustomAggregateReducer;

type AggregateMapEntries<S extends SeriesSchema> = {
  [C in ValueColumnsForSchema<S>[number] as C['name']]?: AggregateFunctionsForKind<
    C['kind']
  >;
};

export type AggregateMap<S extends SeriesSchema> = Readonly<
  AggregateMapEntries<S>
>;

type ValueColumnByName<
  S extends SeriesSchema,
  Name extends ValueColumnsForSchema<S>[number]['name'],
> = Extract<ValueColumnsForSchema<S>[number], ColumnDef<Name, ScalarKind>>;

type AggregateReducerForColumn<
  S extends SeriesSchema,
  Name extends ValueColumnsForSchema<S>[number]['name'],
> = AggregateFunctionsForKind<ValueColumnByName<S, Name>['kind']>;

export type AggregateOutputSpec<
  S extends SeriesSchema,
  Name extends ValueColumnsForSchema<S>[number]['name'] =
    ValueColumnsForSchema<S>[number]['name'],
> = Readonly<{
  from: Name;
  using: AggregateReducerForColumn<S, Name>;
  kind?: ScalarKind;
}>;

export type AggregateOutputMap<S extends SeriesSchema> = Readonly<
  Record<string, AggregateOutputSpec<S>>
>;

type AggregateKindForColumn<
  Column extends ValueColumn,
  Op extends AggregateReducer,
> = Op extends AggregateFunction
  ? Op extends 'sum' | 'avg' | 'count'
    ? 'number'
    : Op extends 'unique' | `top${number}`
      ? 'array'
      : Column['kind']
  : Column['kind'];

type AggregateColumnForMap<
  Column extends ValueColumn,
  Mapping,
> = Column['name'] extends keyof Mapping
  ? Mapping[Column['name']] extends AggregateReducer
    ? ColumnDef<
        Column['name'],
        AggregateKindForColumn<Column, Mapping[Column['name']]>
      > & {
        readonly required: false;
      }
    : never
  : never;

type AggregateColumns<
  Columns extends readonly ValueColumn[],
  Mapping,
> = Columns extends readonly [infer Head, ...infer Tail]
  ? Head extends ValueColumn
    ? Tail extends readonly ValueColumn[]
      ? Head['name'] extends keyof Mapping
        ? [
            AggregateColumnForMap<Head, Mapping>,
            ...AggregateColumns<Tail, Mapping>,
          ]
        : AggregateColumns<Tail, Mapping>
      : []
    : []
  : [];

export type AggregateSchema<S extends SeriesSchema, Mapping> = readonly [
  ColumnDef<'interval', 'interval'>,
  ...AggregateColumns<ValueColumnsForSchema<S>, Mapping>,
];

export type RollingSchema<S extends SeriesSchema, Mapping> = readonly [
  S[0],
  ...AggregateColumns<ValueColumnsForSchema<S>, Mapping>,
];
// `ReduceResult` / `ReduceResultForMap` live in `./types-reduce.ts`.
// Moved out of this file so the narrow conditional type doesn't interact
// with the overload-compatibility checker on `arrayAggregate` /
// `arrayExplode` (TS2394 when both live in the same compilation unit).
export type { ReduceResult } from './types-reduce.js';

export type NumericColumnNameForSchema<S extends SeriesSchema> = Extract<
  ValueColumnsForSchema<S>[number],
  ColumnDef<string, 'number'>
>['name'];

/**
 * Names of value columns whose declared kind is `'array'`. Used as the
 * parameter constraint on array-column operators (`includes`, `count`,
 * `contains`, `explode`).
 */
export type ArrayColumnNameForSchema<S extends SeriesSchema> = Extract<
  ValueColumnsForSchema<S>[number],
  ColumnDef<string, 'array'>
>['name'];

type ReplaceColumnKind<
  Columns extends readonly ValueColumn[],
  Target extends string,
  NewKind extends ScalarKind,
> = Columns extends readonly [infer Head, ...infer Tail]
  ? Head extends ValueColumn
    ? Tail extends readonly ValueColumn[]
      ? Head['name'] extends Target
        ? [
            ColumnDef<Head['name'], NewKind>,
            ...ReplaceColumnKind<Tail, Target, NewKind>,
          ]
        : [Head, ...ReplaceColumnKind<Tail, Target, NewKind>]
      : []
    : []
  : [];

/**
 * Aggregate functions that always produce a numeric result regardless of
 * source column kind. Matches the reducer registry's `outputKind: 'number'`.
 */
type NumericAggregateFunction =
  | 'sum'
  | 'avg'
  | 'count'
  | 'min'
  | 'max'
  | 'median'
  | 'stdev'
  | 'difference'
  | `p${number}`;

/**
 * Output column kind for `arrayAggregate(col, reducer, { kind? })`.
 * Numeric reducers → `'number'`, `'unique'` → `'array'`, `'first'`/`'last'`/
 * `'keep'` and custom functions → `'string'` unless the caller passes an
 * explicit `kind`.
 */
export type ArrayAggregateKind<
  Op extends AggregateReducer,
  ExplicitKind extends ScalarKind | undefined = undefined,
> = ExplicitKind extends ScalarKind
  ? ExplicitKind
  : Op extends NumericAggregateFunction
    ? 'number'
    : Op extends 'unique' | `top${number}`
      ? 'array'
      : 'string';

type AppendColumn<
  S extends SeriesSchema,
  Name extends string,
  Kind extends ScalarKind,
> = readonly [S[0], ...ValueColumnsForSchema<S>, ColumnDef<Name, Kind>];

/**
 * Schema for `baseline(col, { sigma, ... })`: source schema preserved,
 * plus four optional number columns — the rolling average, standard
 * deviation, and the `avg ± sigma * sd` band edges. All four are
 * `required: false` because the rolling window may not have a baseline
 * yet in the opening events.
 */
export type BaselineSchema<
  S extends SeriesSchema,
  AvgName extends string = 'avg',
  SdName extends string = 'sd',
  UpperName extends string = 'upper',
  LowerName extends string = 'lower',
> = readonly [
  S[0],
  ...ValueColumnsForSchema<S>,
  OptionalNumberColumn<AvgName>,
  OptionalNumberColumn<SdName>,
  OptionalNumberColumn<UpperName>,
  OptionalNumberColumn<LowerName>,
];

/**
 * Schema for `arrayAggregate(col, reducer)` replacing the array column
 * in place with the reducer's output kind.
 */
export type ArrayAggregateReplaceSchema<
  S extends SeriesSchema,
  Col extends ArrayColumnNameForSchema<S>,
  Op extends AggregateReducer,
  ExplicitKind extends ScalarKind | undefined = undefined,
> = readonly [
  S[0],
  ...ReplaceColumnKind<
    ValueColumnsForSchema<S>,
    Col,
    ArrayAggregateKind<Op, ExplicitKind>
  >,
];

/**
 * Schema for `arrayAggregate(col, reducer, { as })` — appends a new column
 * carrying the reducer's output and keeps the source array column intact.
 */
export type ArrayAggregateAppendSchema<
  S extends SeriesSchema,
  Name extends string,
  Op extends AggregateReducer,
  ExplicitKind extends ScalarKind | undefined = undefined,
> = AppendColumn<S, Name, ArrayAggregateKind<Op, ExplicitKind>>;

/**
 * Schema for `arrayExplode(col)` replacing the array column in place with
 * a scalar column (default kind `'string'`).
 */
export type ArrayExplodeReplaceSchema<
  S extends SeriesSchema,
  Col extends ArrayColumnNameForSchema<S>,
  OutputKind extends ScalarKind = 'string',
> = readonly [
  S[0],
  ...ReplaceColumnKind<ValueColumnsForSchema<S>, Col, OutputKind>,
];

/**
 * Schema for `arrayExplode(col, { as })` — appends a scalar column with the
 * per-element value and keeps the source array column intact; each output
 * event still carries the full array on that source column.
 */
export type ArrayExplodeAppendSchema<
  S extends SeriesSchema,
  Name extends string,
  OutputKind extends ScalarKind = 'string',
> = AppendColumn<S, Name, OutputKind>;

type OptionalNumberColumn<Name extends string> = ColumnDef<Name, 'number'> & {
  readonly required: false;
};

type ReplaceSmoothedColumn<
  Columns extends readonly ValueColumn[],
  Target extends string,
> = Columns extends readonly [infer Head, ...infer Tail]
  ? Head extends ValueColumn
    ? Tail extends readonly ValueColumn[]
      ? Head['name'] extends Target
        ? [
            OptionalNumberColumn<Head['name']>,
            ...ReplaceSmoothedColumn<Tail, Target>,
          ]
        : [Head, ...ReplaceSmoothedColumn<Tail, Target>]
      : []
    : []
  : [];

export type SmoothSchema<
  S extends SeriesSchema,
  Target extends NumericColumnNameForSchema<S>,
> = readonly [S[0], ...ReplaceSmoothedColumn<ValueColumnsForSchema<S>, Target>];

export type SmoothAppendSchema<
  S extends SeriesSchema,
  Name extends string,
> = readonly [S[0], ...ValueColumnsForSchema<S>, OptionalNumberColumn<Name>];

export type DiffSchema<
  S extends SeriesSchema,
  Targets extends NumericColumnNameForSchema<S>,
> = readonly [
  S[0],
  ...ReplaceSmoothedColumn<ValueColumnsForSchema<S>, Targets>,
];

type JoinColumns<
  Left extends readonly ValueColumn[],
  Right extends readonly ValueColumn[],
> = [...OptionalizeColumns<Left>, ...OptionalizeColumns<Right>];

export type JoinSchema<
  Left extends SeriesSchema,
  Right extends SeriesSchema,
> = readonly [
  Left[0],
  ...JoinColumns<ValueColumnsForSchema<Left>, ValueColumnsForSchema<Right>>,
];

type ColumnNamesForSchema<S extends SeriesSchema> =
  ValueColumnsForSchema<S>[number]['name'];
type DuplicateNamesForPair<
  Left extends SeriesSchema,
  Right extends SeriesSchema,
> = Extract<ColumnNamesForSchema<Left>, ColumnNamesForSchema<Right>>;

type PrefixNameIfDuplicate<
  Name extends string,
  Duplicates extends string,
  Prefix extends string,
> = Name extends Duplicates ? `${Prefix}_${Name}` : Name;

type PrefixedOptionalizeColumn<
  Column extends ValueColumn,
  Duplicates extends string,
  Prefix extends string,
> =
  Column extends ColumnDef<infer Name, infer Kind>
    ? ColumnDef<PrefixNameIfDuplicate<Name, Duplicates, Prefix>, Kind> & {
        readonly required: false;
      }
    : never;

type PrefixedOptionalizeColumns<
  Columns extends readonly ValueColumn[],
  Duplicates extends string,
  Prefix extends string,
> = Columns extends readonly [infer Head, ...infer Tail]
  ? Head extends ValueColumn
    ? Tail extends readonly ValueColumn[]
      ? [
          PrefixedOptionalizeColumn<Head, Duplicates, Prefix>,
          ...PrefixedOptionalizeColumns<Tail, Duplicates, Prefix>,
        ]
      : []
    : []
  : [];

export type PrefixedJoinSchema<
  Left extends SeriesSchema,
  Right extends SeriesSchema,
  Prefixes extends readonly [string, string],
> = readonly [
  Left[0],
  ...PrefixedOptionalizeColumns<
    ValueColumnsForSchema<Left>,
    DuplicateNamesForPair<Left, Right>,
    Prefixes[0]
  >,
  ...PrefixedOptionalizeColumns<
    ValueColumnsForSchema<Right>,
    DuplicateNamesForPair<Left, Right>,
    Prefixes[1]
  >,
];

type JoinManySchemaHelper<
  Acc extends SeriesSchema,
  Rest extends readonly SeriesSchema[],
> = Rest extends readonly [infer Head, ...infer Tail]
  ? Head extends SeriesSchema
    ? Tail extends readonly SeriesSchema[]
      ? JoinManySchemaHelper<JoinSchema<Acc, Head>, Tail>
      : never
    : never
  : Acc;

export type JoinManySchema<
  Schemas extends readonly [SeriesSchema, ...SeriesSchema[]],
> = Schemas extends readonly [infer Head, ...infer Tail]
  ? Head extends SeriesSchema
    ? Tail extends readonly SeriesSchema[]
      ? JoinManySchemaHelper<Head, Tail>
      : never
    : never
  : never;

type DuplicateNamesAcrossSchemasHelper<
  Schemas extends readonly SeriesSchema[],
  Seen extends string = never,
  Duplicates extends string = never,
> = Schemas extends readonly [infer Head, ...infer Tail]
  ? Head extends SeriesSchema
    ? Tail extends readonly SeriesSchema[]
      ? DuplicateNamesAcrossSchemasHelper<
          Tail,
          Seen | ColumnNamesForSchema<Head>,
          Duplicates | Extract<ColumnNamesForSchema<Head>, Seen>
        >
      : Duplicates
    : never
  : Duplicates;

type DuplicateNamesAcrossSchemas<
  Schemas extends readonly [SeriesSchema, ...SeriesSchema[]],
> = DuplicateNamesAcrossSchemasHelper<Schemas>;

type PrefixTupleForSchemas<
  Schemas extends readonly [SeriesSchema, ...SeriesSchema[]],
> = {
  [K in keyof Schemas]: string;
};

type PrefixedJoinManyColumns<
  Schemas extends readonly [SeriesSchema, ...SeriesSchema[]],
  Prefixes extends PrefixTupleForSchemas<Schemas>,
  Duplicates extends string = DuplicateNamesAcrossSchemas<Schemas>,
> = Schemas extends readonly [infer Head, ...infer Tail]
  ? Prefixes extends readonly [infer PrefixHead, ...infer PrefixTail]
    ? Head extends SeriesSchema
      ? PrefixHead extends string
        ? Tail extends readonly [SeriesSchema, ...SeriesSchema[]]
          ? PrefixTail extends PrefixTupleForSchemas<Tail>
            ? [
                ...PrefixedOptionalizeColumns<
                  ValueColumnsForSchema<Head>,
                  Duplicates,
                  PrefixHead
                >,
                ...PrefixedJoinManyColumns<Tail, PrefixTail, Duplicates>,
              ]
            : never
          : [
              ...PrefixedOptionalizeColumns<
                ValueColumnsForSchema<Head>,
                Duplicates,
                PrefixHead
              >,
            ]
        : never
      : never
    : never
  : [];

export type PrefixedJoinManySchema<
  Schemas extends readonly [SeriesSchema, ...SeriesSchema[]],
  Prefixes extends PrefixTupleForSchemas<Schemas>,
> = readonly [Schemas[0][0], ...PrefixedJoinManyColumns<Schemas, Prefixes>];
