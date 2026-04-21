import type { ReducerDef } from './types.js';
import { rollingMonotoneDeque } from './rolling.js';

export const max: ReducerDef = {
  outputKind: 'number',
  reduce(_d, numeric) {
    return numeric.length === 0
      ? undefined
      : numeric.reduce((a, b) => (a >= b ? a : b));
  },
  bucketState() {
    let hi: number | undefined;
    return {
      add(v) {
        if (typeof v === 'number' && (hi === undefined || v > hi)) hi = v;
      },
      snapshot() {
        return hi;
      },
    };
  },
  rollingState() {
    return rollingMonotoneDeque((existing, incoming) => existing >= incoming);
  },
};
