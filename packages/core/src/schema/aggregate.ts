import type {
  AppendColumn,
  ArrayColumnNameForSchema,
  ColumnDef,
  ColumnValue,
  OptionalizeColumns,
  ReplaceColumnKind,
  ScalarKind,
  SeriesSchema,
  ValueColumn,
  ValueColumnsForSchema,
} from './series.js';

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
  | 'samples'
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

type AggregateFunctionsForKind<Kind extends ScalarKind> = Kind extends 'number'
  ? AggregateReducer
  : Kind extends 'array'
    ?
        | 'count'
        | 'first'
        | 'last'
        | 'keep'
        | 'unique'
        | 'samples'
        | `top${number}`
        | CustomAggregateReducer
    :
        | 'count'
        | 'first'
        | 'last'
        | 'keep'
        | 'unique'
        | 'samples'
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
    : Op extends 'unique' | 'samples' | `top${number}`
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

export type AggregateColumns<
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

export type AlignSchema<S extends SeriesSchema> = readonly [
  ColumnDef<'interval', 'interval'>,
  ...OptionalizeColumns<ValueColumnsForSchema<S>>,
];

/**
 * Output schema of `TimeSeries.materialize(...)`. The first column is
 * always `time` (regardless of input key kind — materialize emits one
 * row per sequence bucket sample point, time-keyed by design). Value
 * columns are widened to optional because empty buckets emit
 * `undefined` cells.
 */
export type MaterializeSchema<S extends SeriesSchema> = readonly [
  ColumnDef<'time', 'time'>,
  ...OptionalizeColumns<ValueColumnsForSchema<S>>,
];

// ---------------------------------------------------------------------------
// AggregateOutputMap output-kind widening — formerly in types-aggregate.ts
// ---------------------------------------------------------------------------

/**
 * Infer the output column kind for one `AggregateOutputSpec` entry.
 *
 * Precedence:
 *
 * 1. Explicit `kind` on the spec wins — `{ from, using, kind: 'number' }`
 *    always emits `'number'` regardless of reducer.
 * 2. Numeric-output reducers (`'sum'`, `'avg'`, `'count'`, `'median'`,
 *    `'stdev'`, `'difference'`, any `` `p${number}` ``) → `'number'`.
 * 3. Array-output reducers (`'unique'`, any `` `top${number}` ``) →
 *    `'array'`.
 * 4. Source-preserving reducers (`'first'`, `'last'`, `'keep'`) →
 *    the source column's kind looked up by `spec.from`.
 * 5. Custom reducer functions → `ScalarKind` fallback (output kind is
 *    runtime-determined).
 *
 * Same branch set as `ReduceResult` in `./reduce.ts`. Keep the
 * two in sync if the reducer registry grows.
 */
type OutputSpecKind<S extends SeriesSchema, Spec> = Spec extends {
  kind: infer K extends ScalarKind;
}
  ? K
  : Spec extends { from: infer From extends string; using: infer Using }
    ? Using extends
        | 'sum'
        | 'avg'
        | 'count'
        | 'min'
        | 'max'
        | 'median'
        | 'stdev'
        | 'difference'
        | `p${number}`
      ? 'number'
      : Using extends 'unique' | `top${number}`
        ? 'array'
        : Using extends 'first' | 'last' | 'keep'
          ? From extends ValueColumnsForSchema<S>[number]['name']
            ? Extract<ValueColumnsForSchema<S>[number], { name: From }>['kind']
            : ScalarKind
          : ScalarKind
    : ScalarKind;

/**
 * Union of typed `ColumnDef`s — one per entry in the output-map
 * mapping. Used as the `...Rest` of the schema tuple; the result is a
 * `readonly [FirstColumn, ...Array<ColumnDefUnion>]`. `DataColumnsForSchema`
 * + `EventDataForSchema` flatten that union into the right combined
 * record so `event.get(outputName)` narrows correctly per output key.
 */
type AggregateOutputMapColumns<S extends SeriesSchema, Mapping> = {
  [K in keyof Mapping & string]: ColumnDef<K, OutputSpecKind<S, Mapping[K]>> & {
    readonly required: false;
  };
}[keyof Mapping & string];

/**
 * Schema for `rolling(window, mapping)` where `mapping` is an
 * `AggregateOutputMap`. Preserves the source's first-column kind
 * (same as `RollingSchema`) and narrows each value column per the
 * output-map spec.
 */
export type RollingOutputMapSchema<S extends SeriesSchema, Mapping> = readonly [
  S[0],
  ...Array<AggregateOutputMapColumns<S, Mapping>>,
];

/**
 * Schema for sequence-driven `rolling(seq, window, mapping)` and for
 * `aggregate(seq, mapping)` where `mapping` is an `AggregateOutputMap`.
 * The first column is the `'interval'` key produced by the sequence.
 */
export type AggregateOutputMapResultSchema<
  S extends SeriesSchema,
  Mapping,
> = readonly [
  ColumnDef<'interval', 'interval'>,
  ...Array<AggregateOutputMapColumns<S, Mapping>>,
];

// ---------------------------------------------------------------------------
// Array-aggregate and array-explode output schemas
// ---------------------------------------------------------------------------

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
    : Op extends 'unique' | 'samples' | `top${number}`
      ? 'array'
      : 'string';

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
