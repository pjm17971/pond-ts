/**
 * Type-only entry point at `pond-ts/types`.
 *
 * Lets schema-as-contract consumers — packages whose only job is to
 * declare the `as const` schema that flows through producer /
 * aggregator / web — constrain literals via `satisfies SeriesSchema`
 * without taking a runtime dependency on `pond-ts`.
 *
 * @example
 * ```ts
 * import type { SeriesSchema } from 'pond-ts/types';
 *
 * export const schema = [
 *   { name: 'time', kind: 'time' },
 *   { name: 'cpu', kind: 'number' },
 *   { name: 'host', kind: 'string' },
 * ] as const satisfies SeriesSchema;
 * ```
 *
 * Curated to schema-shape, row-shape, and wire (JSON-shape) types.
 * Operator-derived schema types (`RollingSchema`, `AggregateSchema`,
 * etc.) live on the main entry where you'd already be using the
 * runtime that produces them.
 */
export type {
  ArrayValue,
  ColumnDef,
  ColumnValue,
  EventDataForSchema,
  EventForSchema,
  EventKeyForKind,
  EventKeyForSchema,
  FirstColKind,
  FirstColumn,
  JsonIntervalInput,
  JsonObjectRowForSchema,
  JsonRowForSchema,
  JsonRowFormat,
  JsonTimeRangeInput,
  JsonTimestampInput,
  JsonValueForKind,
  RowForSchema,
  ScalarKind,
  ScalarValue,
  SeriesSchema,
  TimeSeriesInput,
  TimeSeriesJsonInput,
  ValueColumn,
  ValueForKind,
} from './types.js';
