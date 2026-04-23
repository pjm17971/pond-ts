import type {
  ColumnDef,
  SeriesSchema,
  ScalarKind,
  ValueColumnsForSchema,
} from './types.js';

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
 * Same branch set as `ReduceResult` in `./types-reduce.ts`. Keep the
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
