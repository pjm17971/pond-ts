import type { ReducerDef } from './types.js';
import { rollingMonotoneDeque } from './rolling.js';

export const min: ReducerDef = {
  outputKind: 'number',
  reduce(_d, numeric) {
    return numeric.length === 0
      ? undefined
      : numeric.reduce((a, b) => (a <= b ? a : b));
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
