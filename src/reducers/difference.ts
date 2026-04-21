import type { ReducerDef } from './types.js';
import { rollingSortedArray } from './rolling.js';

export const difference: ReducerDef = {
  outputKind: 'number',
  reduce(_d, numeric) {
    if (numeric.length === 0) return undefined;
    let lo = numeric[0]!;
    let hi = numeric[0]!;
    for (let i = 1; i < numeric.length; i++) {
      const v = numeric[i]!;
      if (v < lo) lo = v;
      if (v > hi) hi = v;
    }
    return hi - lo;
  },
  bucketState() {
    let lo: number | undefined;
    let hi: number | undefined;
    return {
      add(v) {
        if (typeof v !== 'number') return;
        if (lo === undefined || v < lo) lo = v;
        if (hi === undefined || v > hi) hi = v;
      },
      snapshot() {
        return lo !== undefined && hi !== undefined ? hi - lo : undefined;
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
          : arr.sorted[arr.sorted.length - 1]! - arr.sorted[0]!;
      },
    };
  },
};
