import type { ReducerDef } from './types.js';

export const sum: ReducerDef = {
  outputKind: 'number',
  reduce(_d, numeric) {
    return numeric.reduce((s, v) => s + v, 0);
  },
  bucketState() {
    let s = 0;
    return {
      add(v) {
        if (typeof v === 'number') s += v;
      },
      snapshot() {
        return s;
      },
    };
  },
  rollingState() {
    let s = 0;
    return {
      add(_i, v) {
        if (typeof v === 'number') s += v;
      },
      remove(_i, v) {
        if (typeof v === 'number') s -= v;
      },
      snapshot() {
        return s;
      },
    };
  },
};
