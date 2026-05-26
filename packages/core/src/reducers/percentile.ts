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
 * gather defined cells into a dense `Float64Array` clone, sorts
 * it in place (typed-array sort is faster than `Array.sort` since
 * the comparator is intrinsic), then reads the percentile from
 * the sorted view.
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
  let dense: Float64Array;
  if (validity === undefined) {
    if (col.length === 0) return undefined;
    dense = new Float64Array(col.values.subarray(0, col.length));
  } else {
    const definedCount = validity.definedCount;
    if (definedCount === 0) return undefined;
    dense = new Float64Array(definedCount);
    const values = col.values;
    const bits = validity.bits;
    let outIdx = 0;
    for (let i = 0; i < col.length; i += 1) {
      if ((bits[i >> 3]! & (1 << (i & 7))) !== 0) {
        dense[outIdx] = values[i]!;
        outIdx += 1;
      }
    }
  }
  dense.sort();
  const rank = (q / 100) * (dense.length - 1);
  const lo = Math.floor(rank);
  const hi = Math.ceil(rank);
  if (lo === hi) return dense[lo]!;
  return dense[lo]! + (dense[hi]! - dense[lo]!) * (rank - lo);
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
