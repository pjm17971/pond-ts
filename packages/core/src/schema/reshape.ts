import type { EventDataForSchema, EventForSchema } from './events.js';
import type {
  ColumnDef,
  KindForValue,
  NumericColumnNameForSchema,
  ScalarValue,
  SeriesSchema,
  ValueColumn,
  ValueColumnKindForName,
  ValueColumnsForSchema,
} from './series.js';

// ---------------------------------------------------------------------------
// Fill
// ---------------------------------------------------------------------------

/**
 * Strategies accepted by `TimeSeries.fill(strategy, options?)` and the
 * partitioned variant `series.partitionBy(col).fill(...)`.
 *
 * - `'hold'` — forward fill: carry the most recent known value forward.
 * - `'bfill'` — backward fill: use the next known value.
 * - `'linear'` — time-interpolated; numeric only. Leading and trailing
 *    null runs are left unfilled (no neighbor on one side).
 * - `'zero'` — fill with `0`.
 */
export type FillStrategy = 'hold' | 'bfill' | 'linear' | 'zero';

/**
 * Per-column fill spec for `TimeSeries.fill(mapping, options?)` and the
 * partitioned variant. Values are either a {@link FillStrategy} name or
 * a literal `ScalarValue` (the latter is treated as a constant fill,
 * e.g. `host: 'unknown'`). Columns not listed are left untouched.
 */
export type FillMapping<S extends SeriesSchema> = {
  [K in ValueColumnsForSchema<S>[number]['name']]?: FillStrategy | ScalarValue;
};

// ---------------------------------------------------------------------------
// Dedupe
// ---------------------------------------------------------------------------

/**
 * Resolution policy for `TimeSeries.dedupe({ keep })`. Determines which
 * event survives when multiple events share a key (timestamp for the
 * batch operator; timestamp + partition columns for
 * `partitionBy(...).dedupe(...)`).
 *
 * - `'first'` — keep the first event encountered at each key.
 * - `'last'` — keep the last event encountered (default, matches the
 *   "WebSocket replay" / "newer wins" intuition for retried ingest).
 * - `'error'` — throw on any duplicate key. Useful for ingestion paths
 *   that want a hard failure on data-shape violations.
 * - `'drop'` — discard every event at any duplicate key. Conservative;
 *   the value of `'1.5 events at this timestamp'` is rarely defensible.
 * - `{ min: col }` / `{ max: col }` — keep the event with the smallest
 *   / largest value at the named numeric column. Ties keep the first
 *   tied event encountered.
 * - function — custom resolver. Receives all events sharing the key
 *   (always length ≥ 2 by the time it's invoked) and returns one. Use
 *   for merge logic that combines fields across duplicates.
 */
export type DedupeKeep<S extends SeriesSchema> =
  | 'first'
  | 'last'
  | 'error'
  | 'drop'
  | { min: NumericColumnNameForSchema<S> }
  | { max: NumericColumnNameForSchema<S> }
  | ((events: ReadonlyArray<EventForSchema<S>>) => EventForSchema<S>);

// ---------------------------------------------------------------------------
// Pivot by group
// ---------------------------------------------------------------------------

/**
 * Output column tuple for the typed `pivotByGroup` overload — recursive
 * helper that builds one `ValueColumn` per declared group in declaration
 * order.
 */
type PivotByGroupColumns<
  S extends SeriesSchema,
  V extends string,
  Groups extends readonly string[],
  Acc extends ValueColumn[] = [],
> = Groups extends readonly [infer Head, ...infer Tail]
  ? Head extends string
    ? Tail extends readonly string[]
      ? PivotByGroupColumns<
          S,
          V,
          Tail,
          [
            ...Acc,
            {
              name: `${Head}_${V}`;
              kind: ValueColumnKindForName<S, V>;
              required: false;
            },
          ]
        >
      : Acc
    : Acc
  : Acc;

/**
 * Output schema of `pivotByGroup(group, value, { groups: [...] as const })`.
 * Builds `[time, ...{name: '${G}_${V}', kind: <V's kind>, required: false}[]]`,
 * preserving the time column from `S` and emitting one value column per
 * declared group in declaration order.
 */
export type PivotByGroupSchema<
  S extends SeriesSchema,
  V extends string,
  Groups extends readonly string[],
> = readonly [S[0], ...PivotByGroupColumns<S, V, Groups>];

// ---------------------------------------------------------------------------
// Collapse
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Select
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Rename
// ---------------------------------------------------------------------------

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
