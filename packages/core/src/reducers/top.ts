import type { ColumnValue, ScalarValue } from '../types.js';
import type { ReducerDef } from './types.js';

const TOP_RE = /^top(\d+)$/;

/**
 * Parse a `'topN'` reducer name (e.g. `'top3'`) into its integer N.
 * Returns `undefined` if the string isn't a valid top-N name. Used by the
 * reducer registry to dispatch parameterized reducer names.
 */
export function parseTopN(name: string): number | undefined {
  const match = TOP_RE.exec(name);
  if (!match) return undefined;
  const n = Number(match[1]);
  if (!Number.isInteger(n) || n <= 0) return undefined;
  return n;
}

/**
 * Deterministic scalar ordering used to break ties when multiple values
 * share a frequency count. Numbers before strings before booleans; within
 * a kind, JS default comparison.
 */
function compareScalars(a: ScalarValue, b: ScalarValue): number {
  const ta = typeof a;
  const tb = typeof b;
  if (ta !== tb) {
    const rank = (t: string): number =>
      t === 'number' ? 0 : t === 'string' ? 1 : 2;
    return rank(ta) - rank(tb);
  }
  if (a === b) return 0;
  return a < b ? -1 : 1;
}

function isScalar(v: ColumnValue | undefined): v is ScalarValue {
  return (
    typeof v === 'number' || typeof v === 'string' || typeof v === 'boolean'
  );
}

/**
 * Materialize the top N values from a frequency map.
 * Order: frequency descending, ties broken by `compareScalars`.
 */
function topFromCounts(
  counts: Map<ScalarValue, number>,
  n: number,
): ScalarValue[] {
  const entries = Array.from(counts.entries());
  entries.sort((a, b) => {
    if (a[1] !== b[1]) return b[1] - a[1];
    return compareScalars(a[0], b[0]);
  });
  const out: ScalarValue[] = [];
  const limit = Math.min(n, entries.length);
  for (let i = 0; i < limit; i += 1) out.push(entries[i]![0]);
  return out;
}

/**
 * Factory: returns a `ReducerDef` that emits the top N values by
 * frequency, sorted by count descending with deterministic tie-break.
 * Used internally by `resolveReducer` when parsing a `'topN'` string name.
 */
export function topReducer(n: number): ReducerDef {
  if (!Number.isInteger(n) || n <= 0) {
    throw new TypeError(`top requires a positive integer N, got ${n}`);
  }
  return {
    outputKind: 'array',
    reduce(defined) {
      const counts = new Map<ScalarValue, number>();
      for (const v of defined) {
        if (isScalar(v)) counts.set(v, (counts.get(v) ?? 0) + 1);
      }
      return topFromCounts(counts, n);
    },
    bucketState() {
      const counts = new Map<ScalarValue, number>();
      return {
        add(v) {
          if (isScalar(v)) counts.set(v, (counts.get(v) ?? 0) + 1);
        },
        snapshot() {
          return topFromCounts(counts, n);
        },
      };
    },
    rollingState() {
      const counts = new Map<ScalarValue, number>();
      return {
        add(_i, v) {
          if (!isScalar(v)) return;
          counts.set(v, (counts.get(v) ?? 0) + 1);
        },
        remove(_i, v) {
          if (!isScalar(v)) return;
          const next = (counts.get(v) ?? 0) - 1;
          if (next <= 0) counts.delete(v);
          else counts.set(v, next);
        },
        snapshot() {
          return topFromCounts(counts, n);
        },
      };
    },
  };
}

/**
 * Ergonomic helper: `top(3)` returns the literal `'top3'` typed as
 * `` `top${3}` ``. Use wherever an `AggregateReducer` name is accepted:
 *
 * ```ts
 * series.aggregate(Sequence.every('1m'), { host: top(3) });
 * series.arrayAggregate('tags', top(5));
 * ```
 *
 * Equivalent to typing the string literal `'top3'` directly.
 */
export function top<N extends number>(n: N): `top${N}` {
  return `top${n}` as `top${N}`;
}
