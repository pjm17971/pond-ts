import type { ReducerDef } from './types.js';

export const avg: ReducerDef = {
  outputKind: 'number',
  reduce(_d, numeric) {
    return numeric.length === 0
      ? undefined
      : numeric.reduce((s, v) => s + v, 0) / numeric.length;
  },
  bucketState() {
    let s = 0;
    let n = 0;
    return {
      add(v) {
        if (typeof v === 'number') {
          s += v;
          n++;
        }
      },
      snapshot() {
        return n === 0 ? undefined : s / n;
      },
    };
  },
  rollingState() {
    let s = 0;
    let n = 0;
    return {
      add(_i, v) {
        if (typeof v === 'number') {
          s += v;
          n++;
        }
      },
      remove(_i, v) {
        if (typeof v === 'number') {
          s -= v;
          n--;
        }
      },
      snapshot() {
        return n === 0 ? undefined : s / n;
      },
    };
  },
};
