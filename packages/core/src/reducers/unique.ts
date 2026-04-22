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

/** Push each scalar from `v` into `set`; flattens a single level of array. */
function collectInto(set: Set<ScalarValue>, v: ColumnValue | undefined): void {
  if (v === undefined) return;
  if (isScalar(v)) {
    set.add(v);
    return;
  }
  for (const element of v) {
    if (isScalar(element)) set.add(element);
  }
}

function incrementCounts(
  counts: Map<ScalarValue, number>,
  v: ColumnValue | undefined,
): void {
  if (v === undefined) return;
  if (isScalar(v)) {
    counts.set(v, (counts.get(v) ?? 0) + 1);
    return;
  }
  for (const element of v) {
    if (isScalar(element)) {
      counts.set(element, (counts.get(element) ?? 0) + 1);
    }
  }
}

function decrementCounts(
  counts: Map<ScalarValue, number>,
  v: ColumnValue | undefined,
): void {
  if (v === undefined) return;
  if (isScalar(v)) {
    const next = (counts.get(v) ?? 0) - 1;
    if (next <= 0) counts.delete(v);
    else counts.set(v, next);
    return;
  }
  for (const element of v) {
    if (!isScalar(element)) continue;
    const next = (counts.get(element) ?? 0) - 1;
    if (next <= 0) counts.delete(element);
    else counts.set(element, next);
  }
}

function snapshotSet(set: Set<ScalarValue>): ScalarValue[] {
  const out = Array.from(set);
  out.sort(compareScalars);
  return out;
}

/**
 * Distinct non-undefined scalar values from a bucket, returned as a sorted
 * array. Array-kind inputs are _flattened_ one level: a bucket of
 * `[['a','b'], ['b','c']]` produces `['a','b','c']`. This makes "what
 * distinct tags showed up this minute?" work naturally when the source is
 * already a tag-list array column.
 */
export const unique: ReducerDef = {
  outputKind: 'array',
  reduce(defined) {
    const set = new Set<ScalarValue>();
    for (const v of defined) collectInto(set, v);
    return snapshotSet(set);
  },
  bucketState() {
    const set = new Set<ScalarValue>();
    return {
      add(v) {
        collectInto(set, v);
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
        incrementCounts(counts, v);
      },
      remove(_i, v) {
        decrementCounts(counts, v);
      },
      snapshot() {
        const out = Array.from(counts.keys());
        out.sort(compareScalars);
        return out;
      },
    };
  },
};
