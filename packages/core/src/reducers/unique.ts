import type { ColumnValue, ScalarValue } from '../types.js';
import type { ReducerDef } from './types.js';

/**
 * Sort scalar values into a stable, deterministic order:
 *   numbers < strings < booleans,
 * within each bucket use the language's default comparison.
 *
 * Having a stable sort makes bucket-to-bucket comparisons and test
 * expectations predictable even when the input order varies.
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

function snapshotSet(set: Set<ScalarValue>): ScalarValue[] {
  const out = Array.from(set);
  out.sort(compareScalars);
  return out;
}

/**
 * Distinct non-undefined scalar values from a bucket, returned as a sorted
 * array. Non-scalar inputs (array cells on the source column) are skipped
 * rather than dedup'd by reference — that keeps the output meaningful when
 * `unique` is applied to an already-aggregated array column.
 */
export const unique: ReducerDef = {
  outputKind: 'array',
  reduce(defined) {
    const set = new Set<ScalarValue>();
    for (const v of defined) {
      if (isScalar(v)) set.add(v);
    }
    return snapshotSet(set);
  },
  bucketState() {
    const set = new Set<ScalarValue>();
    return {
      add(v) {
        if (isScalar(v)) set.add(v);
      },
      snapshot() {
        return snapshotSet(set);
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
        const out = Array.from(counts.keys());
        out.sort(compareScalars);
        return out;
      },
    };
  },
};
