import type {
  NumericColumnNameForSchema,
  OptionalNumberColumn,
  SeriesSchema,
  ValueColumn,
  ValueColumnsForSchema,
} from './series.js';

export type SmoothMethod = 'ema' | 'movingAverage' | 'loess';

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
