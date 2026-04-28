/**
 * Type-only entry point at `pond-ts/types`.
 *
 * Lets schema-as-contract consumers — packages whose only job is to
 * declare the `as const` schema that flows through producer /
 * aggregator / web — constrain literals via `satisfies SeriesSchema`
 * without taking a runtime dependency on `pond-ts`. The emitted JS
 * for this entry is `export {};` (zero runtime); the .d.ts is a
 * curated re-export of `types.ts`, so the source of truth doesn't
 * drift.
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
 * **What's included:** schema-shape types (`SeriesSchema`,
 * `ColumnDef`, `FirstColumn`, `ValueColumn`, `ScalarKind`,
 * `ColumnValue`, `ArrayValue`, ...), row-shape types (`RowForSchema`,
 * `EventDataForSchema`, `EventForSchema`, `EventKeyForKind`,
 * `EventKeyForSchema`), and wire (JSON-shape) types
 * (`TimeSeriesJsonInput`, `JsonRowForSchema`,
 * `JsonObjectRowForSchema`, `JsonValueForKind`, `JsonRowFormat`,
 * `JsonTimestampInput`, `JsonTimeRangeInput`, `JsonIntervalInput`).
 *
 * **What's deliberately not included:**
 * - **Operator-derived schema types** (`RollingSchema`,
 *   `AggregateSchema`, etc.) — if you're using rolling, you're using
 *   the runtime, and the main entry already covers you.
 * - **`LiveSource` and other source interfaces** — they describe a
 *   runtime contract (event subscription, dispatch). Schema-as-
 *   contract consumers don't implement sources; if you're writing
 *   one, import from the main entry.
 *
 * Existing `import { SeriesSchema } from 'pond-ts'` calls keep
 * working unchanged.
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
