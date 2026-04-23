import type {
  ColumnValue,
  SeriesSchema,
  ValueColumnsForSchema,
} from './types.js';

/**
 * Lookup the declared column definition for a given column name in a
 * schema. Narrows to the matching `ColumnDef`, or `never` if `Name`
 * isn't a value-column name in `S`.
 */
type ColumnByName<S extends SeriesSchema, Name extends string> = Extract<
  ValueColumnsForSchema<S>[number],
  { name: Name }
>;

/**
 * Per-entry narrowed output type for `TimeSeries.reduce(mapping)`. For
 * an `AggregateMap` with literal reducer names, each field narrows to
 * the specific value kind the reducer produces:
 *
 * ```ts
 * series.reduce({ cpu: 'avg', host: 'unique' });
 * //    ^ { cpu: number | undefined;
 * //        host: ReadonlyArray<ScalarValue> | undefined }
 * ```
 *
 * The branches are enumerated inline (rather than delegated to
 * `AggregateKindForColumn` + `NormalizedValueForKind`) because the
 * inlined form is the only shape TypeScript accepts in the same
 * compilation unit as the `arrayAggregate` / `arrayExplode` overloads —
 * more-delegated variants trip TS2394 on those overloads' compatibility
 * with their implementation signature. The narrow logic is intentionally
 * duplicated here; keep it in sync with `AggregateKindForColumn` in
 * `types.ts` if the set of numeric / array-producing reducers changes.
 *
 * Branches:
 *
 * - Numeric-output reducers (`'sum'`, `'avg'`, `'count'`, `'median'`,
 *   `'stdev'`, `'difference'`, any `p${number}`) → `number | undefined`.
 * - Array-output reducers (`'unique'`, any `top${number}`) →
 *   `ReadonlyArray<T> | undefined`, where `T` is the source column's
 *   element type — `ReadonlyArray<string>` for a `kind: 'string'`
 *   column, `ReadonlyArray<number>` for `kind: 'number'`, etc.
 *   Array-kind source columns fall back to the wide
 *   `ReadonlyArray<ScalarValue> | undefined`.
 * - Source-preserving reducers (`'first'`, `'last'`, `'keep'`) → the
 *   source column's value type (`number`, `string`, or `boolean` —
 *   `undefined` included). Array-kind source columns fall back to
 *   `ColumnValue | undefined` because tracking element kind is out of
 *   scope for the schema.
 * - Custom reducer functions and `AggregateOutputSpec` entries fall
 *   back to `ColumnValue | undefined` — their output kind is set at
 *   runtime and the type system can't see through it.
 */
export type ReduceResult<S extends SeriesSchema, Mapping> = {
  [K in keyof Mapping & string]: Mapping[K] extends
    | 'sum'
    | 'avg'
    | 'count'
    | 'min'
    | 'max'
    | 'median'
    | 'stdev'
    | 'difference'
    | `p${number}`
    ? number | undefined
    : Mapping[K] extends 'unique' | `top${number}`
      ? K extends ValueColumnsForSchema<S>[number]['name']
        ? ColumnByName<S, K>['kind'] extends 'number'
          ? ReadonlyArray<number> | undefined
          : ColumnByName<S, K>['kind'] extends 'string'
            ? ReadonlyArray<string> | undefined
            : ColumnByName<S, K>['kind'] extends 'boolean'
              ? ReadonlyArray<boolean> | undefined
              : ReadonlyArray<string | number | boolean> | undefined
        : ReadonlyArray<string | number | boolean> | undefined
      : Mapping[K] extends 'first' | 'last' | 'keep'
        ? K extends ValueColumnsForSchema<S>[number]['name']
          ? ColumnByName<S, K>['kind'] extends 'number'
            ? number | undefined
            : ColumnByName<S, K>['kind'] extends 'string'
              ? string | undefined
              : ColumnByName<S, K>['kind'] extends 'boolean'
                ? boolean | undefined
                : ColumnValue | undefined
          : ColumnValue | undefined
        : ColumnValue | undefined;
};
