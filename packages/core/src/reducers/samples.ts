import type { ColumnValue, ScalarValue } from '../types.js';
import type { ReducerDef } from './types.js';

function isScalar(v: ColumnValue | undefined): v is ScalarValue {
  return (
    typeof v === 'number' || typeof v === 'string' || typeof v === 'boolean'
  );
}

/** Push each scalar from `v` into `out`; flattens a single level of array. */
function collectInto(out: ScalarValue[], v: ColumnValue | undefined): void {
  if (v === undefined) return;
  if (isScalar(v)) {
    out.push(v);
    return;
  }
  for (const element of v) {
    if (isScalar(element)) out.push(element);
  }
}

/**
 * All defined scalar values from a bucket, returned as an array in
 * arrival order (duplicates preserved). Array-kind inputs are
 * flattened one level: a bucket of `[['a','b'], ['b','c']]` produces
 * `['a','b','b','c']`. This makes "give me every value the rolling
 * window has seen" work naturally on either scalar or tag-list
 * columns.
 *
 * Sits beside `unique` (which deduplicates) and `top${N}` (which
 * bounds and frequency-orders). Reach for `samples` when you need
 * the full value list, with duplicates and arrival order preserved
 * — typically because a downstream computation needs the raw
 * values (anomaly density against a baseline, custom thresholding,
 * histogramming).
 *
 * **Use on bounded windows.** Memory is O(window size); per-event
 * cost is O(1) `add` and O(1) `remove` (Map-keyed by event index);
 * `snapshot` is O(N) for the array copy.
 */
export const samples: ReducerDef = {
  outputKind: 'array',
  reduce(defined) {
    const out: ScalarValue[] = [];
    for (const v of defined) collectInto(out, v);
    return out;
  },
  bucketState() {
    const items: ScalarValue[] = [];
    return {
      add(v) {
        collectInto(items, v);
      },
      snapshot() {
        return items.slice();
      },
    };
  },
  rollingState() {
    // Map keyed by event index so `remove` is O(1). `Array.from(map.values())`
    // returns insertion order, which matches arrival order.
    const items = new Map<number, ScalarValue[]>();
    return {
      add(index, v) {
        const collected: ScalarValue[] = [];
        collectInto(collected, v);
        if (collected.length > 0) items.set(index, collected);
      },
      remove(index, _v) {
        items.delete(index);
      },
      snapshot() {
        const out: ScalarValue[] = [];
        for (const arr of items.values()) {
          for (const v of arr) out.push(v);
        }
        return out;
      },
    };
  },
};
