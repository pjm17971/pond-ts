/**
 * Type tests for the `pond-ts/types` subpath entry. Verifies that
 * the curated schema-shape types let an external consumer constrain
 * `as const` literals via `satisfies SeriesSchema` without importing
 * any runtime symbol from the main entry.
 *
 * Imports from the in-tree path; the published subpath
 * `pond-ts/types` resolves to the same module via `package.json`'s
 * `exports."./types"`.
 */
import type {
  ColumnDef,
  EventDataForSchema,
  EventForSchema,
  RowForSchema,
  ScalarKind,
  SeriesSchema,
  ValueColumn,
} from '../src/types-public.js';

// Schema definition with `satisfies` — the canonical use case.
// Compile error if any column's kind isn't a real ScalarKind.
const cpuSchema = [
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'number' },
  { name: 'host', kind: 'string' },
] as const satisfies SeriesSchema;

// Row tuple type derived from the schema.
type CpuRow = RowForSchema<typeof cpuSchema>;

// Event-shape utilities are usable without runtime import.
type CpuData = EventDataForSchema<typeof cpuSchema>;
type CpuEvent = EventForSchema<typeof cpuSchema>;

// Catch the unrelated-type error if these get accidentally typed
// `unknown` (which would happen if the curated re-export silently
// dropped its generic param). Each branch has to discriminate.
function check(row: CpuRow, data: CpuData, event: CpuEvent): void {
  // RowForSchema<...> first slot is the time-key value. Don't pin
  // the exact runtime shape; just confirm it's not `never` /
  // `unknown` — the typeof of a non-empty tuple is `object`.
  const _row: object = row;
  const _data: object = data;
  const _event: object = event;
  void _row;
  void _data;
  void _event;
}
void check;

// Negative checks — these would not compile if the curated types
// were wrong. Use a typed annotation rather than `satisfies` so the
// error lands on a predictable line, where the expect-error directive
// can sit directly above it.

// @ts-expect-error 'oops' is not a ScalarKind
const wrongKind: SeriesSchema = [
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'oops' },
] as const;
void wrongKind;

const wrongFirst: SeriesSchema = [
  // @ts-expect-error first column must be one of time / interval / timeRange
  { name: 'time', kind: 'number' },
  { name: 'cpu', kind: 'number' },
] as const;
void wrongFirst;

// Building blocks compose. Constructing column types by hand should
// type-check.
type CustomCol = ColumnDef<'load', 'number'>;
const _custom: CustomCol = { name: 'load', kind: 'number' };
void _custom;

type Numeric = ValueColumn<'gauge'>;
const _gauge: Numeric = { name: 'gauge', kind: 'number' };
void _gauge;

// Sanity: the kind union still includes all four scalars.
const kinds: ScalarKind[] = ['number', 'string', 'boolean', 'array'];
void kinds;
