/**
 * Type tests for `TimeSeries.mapColumns` — the per-cell column value
 * transform. Pins the load-bearing type contract:
 *
 * - mappers are keyed by *value* column name (not the key column);
 * - each mapper is `(value: T) => T` where `T` is that column's kind,
 *   so the transform is **same-kind** (a kind-changing return fails to
 *   compile) and the result stays `TimeSeries<S>` (schema unchanged).
 *
 * Checked by `npm run test:type` (tsc over tsconfig.types.json); the
 * `@ts-expect-error` lines must each actually error.
 */

import { TimeSeries } from '../src/index.js';
import { expectTypeOf } from 'vitest';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'v', kind: 'number' },
  { name: 'host', kind: 'string' },
] as const;

const s = new TimeSeries({
  name: 's',
  schema,
  rows: [
    [0, 10, 'a'],
    [1000, 20, 'b'],
  ],
});

// Same-kind mappers compile, and the result type is unchanged.
expectTypeOf(s.mapColumns({ v: (x) => x * 2 })).toEqualTypeOf<typeof s>();
expectTypeOf(s.mapColumns({ host: (h) => h.toUpperCase() })).toEqualTypeOf<
  typeof s
>();
// The mapper's parameter is narrowed to the column's value type.
s.mapColumns({
  v: (x) => {
    expectTypeOf(x).toEqualTypeOf<number>();
    return x;
  },
  host: (h) => {
    expectTypeOf(h).toEqualTypeOf<string>();
    return h;
  },
});

// @ts-expect-error — kind-changing return (number column, string out) rejected.
s.mapColumns({ v: (x) => `${x}` });

// @ts-expect-error — kind-changing return (string column, number out) rejected.
s.mapColumns({ host: (h) => h.length });

// @ts-expect-error — unknown column name rejected.
s.mapColumns({ nope: (x) => x });

// @ts-expect-error — the key column is not mappable.
s.mapColumns({ time: (t) => t });
