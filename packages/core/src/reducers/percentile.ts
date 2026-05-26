import type { Float64Column } from '../columnar/index.js';
import type { ReducerDef } from './types.js';
import { rollingSortedArray } from './rolling.js';

export function percentileOfSorted(sorted: number[], q: number): number {
  const rank = (q / 100) * (sorted.length - 1);
  const lo = Math.floor(rank);
  const hi = Math.ceil(rank);
  if (lo === hi) return sorted[lo]!;
  return sorted[lo]! + (sorted[hi]! - sorted[lo]!) * (rank - lo);
}

export function parsePercentile(op: string): number | undefined {
  if (op.length > 1 && op.charCodeAt(0) === 112) {
    const q = Number(op.slice(1));
    if (q >= 0 && q <= 100) return q;
  }
  return undefined;
}

/**
 * Shared `reduceColumn` body for percentile-shaped reducers
 * (`median`, `p50`, `p95`, etc.). Walks the validity bitmap to
 * gather defined non-NaN cells into a dense `Float64Array`, sorts
 * it in place (typed-array sort is faster than `Array.sort` since
 * the comparator is intrinsic), then reads the percentile from
 * the sorted view.
 *
 * **NaN filtered out.** `Float64Column` accepts NaN cells via
 * trusted construction (the row-API rejects NaN at intake but
 * lower-level factories permit it). `Float64Array.prototype.sort`
 * puts NaN deterministically at the end, which would shift the
 * percentile index by the NaN count — giving a different answer
 * than the row-API `Array.sort((a,b) => a-b)` path, where NaN
 * makes the comparator return NaN and the sort order is undefined.
 * Treating NaN as "not a valid percentile sample" matches the
 * row-API's documented contract (`assertCellKind('number', value)`
 * rejects NaN at intake) and gives both paths the same answer for
 * any input that reached the column. Closed L2 review finding on
 * PR #153.
 *
 * `Float64Array.prototype.sort` defaults to numeric compare
 * (unlike `Array.prototype.sort`'s lexicographic default), so we
 * skip the comparator function call per swap.
 */
export function reducePercentileColumn(
  col: Float64Column,
  q: number,
): number | undefined {
  const validity = col.validity;
  const values = col.values;
  let dense: Float64Array;
  let denseLength = 0;
  if (validity === undefined) {
    if (col.length === 0) return undefined;
    // Optimistic pre-size to col.length; the actual length is
    // `col.length - nanCount`. Shrink at the end via subarray if NaN
    // cells are present.
    dense = new Float64Array(col.length);
    for (let i = 0; i < col.length; i += 1) {
      const v = values[i]!;
      if (Number.isNaN(v)) continue;
      dense[denseLength] = v;
      denseLength += 1;
    }
  } else {
    const definedCount = validity.definedCount;
    if (definedCount === 0) return undefined;
    dense = new Float64Array(definedCount);
    const bits = validity.bits;
    for (let i = 0; i < col.length; i += 1) {
      if ((bits[i >> 3]! & (1 << (i & 7))) === 0) continue;
      const v = values[i]!;
      if (Number.isNaN(v)) continue;
      dense[denseLength] = v;
      denseLength += 1;
    }
  }
  if (denseLength === 0) return undefined;
  // Sort only the populated prefix. `Float64Array.sort` doesn't
  // accept a range; subarray gives a view sharing the same buffer.
  const view = dense.subarray(0, denseLength);
  view.sort();
  const rank = (q / 100) * (denseLength - 1);
  const lo = Math.floor(rank);
  const hi = Math.ceil(rank);
  if (lo === hi) return view[lo]!;
  return view[lo]! + (view[hi]! - view[lo]!) * (rank - lo);
}

export function percentileReducer(q: number): ReducerDef {
  return {
    outputKind: 'number',
    reduce(_d, numeric) {
      if (numeric.length === 0) return undefined;
      const sorted = numeric.slice().sort((a, b) => a - b);
      return percentileOfSorted(sorted, q);
    },
    reduceColumn(col) {
      return reducePercentileColumn(col, q);
    },
    bucketState() {
      const collected: number[] = [];
      return {
        add(v) {
          if (typeof v === 'number') collected.push(v);
        },
        snapshot() {
          if (collected.length === 0) return undefined;
          const sorted = collected.slice().sort((a, b) => a - b);
          return percentileOfSorted(sorted, q);
        },
      };
    },
    rollingState() {
      const arr = rollingSortedArray();
      return {
        add: arr.add,
        remove: arr.remove,
        snapshot() {
          return arr.sorted.length === 0
            ? undefined
            : percentileOfSorted(arr.sorted, q);
        },
      };
    },
  };
}
