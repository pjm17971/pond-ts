import type { ArrayValue, ColumnDef, SeriesSchema } from './series.js';

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

/**
 * `toJSON()` output narrowed to the array (tuple) row form.
 * Returned when `rowFormat` is omitted or set to `'array'`.
 */
export type TimeSeriesJsonOutputArray<S extends SeriesSchema> = {
  name: string;
  schema: S;
  rows: ReadonlyArray<JsonRowForSchema<S>>;
};

/**
 * `toJSON()` output narrowed to the object (schema-keyed) row form.
 * Returned when `rowFormat` is set to `'object'`.
 */
export type TimeSeriesJsonOutputObject<S extends SeriesSchema> = {
  name: string;
  schema: S;
  rows: ReadonlyArray<JsonObjectRowForSchema<S>>;
};

export type JsonRowFormat = 'array' | 'object';
