import type { ReducerDef } from './types.js';

export const count: ReducerDef = {
  outputKind: 'number',
  reduce(defined) {
    return defined.length;
  },
  bucketState() {
    let n = 0;
    return {
      add(v) {
        if (v !== undefined) n++;
      },
      snapshot() {
        return n;
      },
    };
  },
  rollingState() {
    let n = 0;
    return {
      add(_i, v) {
        if (v !== undefined) n++;
      },
      remove(_i, v) {
        if (v !== undefined) n--;
      },
      snapshot() {
        return n;
      },
    };
  },
};
