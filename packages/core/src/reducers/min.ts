import type { ReducerDef } from './types.js';
import { rollingMonotoneDeque } from './rolling.js';

export const min: ReducerDef = {
  outputKind: 'number',
  reduce(_d, numeric) {
    return numeric.length === 0
      ? undefined
      : numeric.reduce((a, b) => (a <= b ? a : b));
  },
  reduceColumn(col) {
    const values = col.values;
    const validity = col.validity;
    let lo: number | undefined;
    if (validity === undefined) {
      if (col.length === 0) return undefined;
      lo = values[0]!;
      for (let i = 1; i < col.length; i += 1) {
        const v = values[i]!;
        if (v < lo) lo = v;
      }
      return lo;
    }
    const bits = validity.bits;
    for (let i = 0; i < col.length; i += 1) {
      if ((bits[i >> 3]! & (1 << (i & 7))) === 0) continue;
      const v = values[i]!;
      if (lo === undefined || v < lo) lo = v;
    }
    return lo;
  },
  bucketState() {
    let lo: number | undefined;
    return {
      add(v) {
        if (typeof v === 'number' && (lo === undefined || v < lo)) lo = v;
      },
      snapshot() {
        return lo;
      },
    };
  },
  rollingState() {
    return rollingMonotoneDeque((existing, incoming) => existing <= incoming);
  },
};
