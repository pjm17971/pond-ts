import type { ReducerDef } from './types.js';

export const stdev: ReducerDef = {
  outputKind: 'number',
  reduce(_d, numeric) {
    if (numeric.length === 0) return undefined;
    const mean = numeric.reduce((s, v) => s + v, 0) / numeric.length;
    const variance =
      numeric.reduce((s, v) => s + (v - mean) ** 2, 0) / numeric.length;
    return Math.sqrt(variance);
  },
  bucketState() {
    let s = 0;
    let sq = 0;
    let n = 0;
    return {
      add(v) {
        if (typeof v === 'number') {
          s += v;
          sq += v * v;
          n++;
        }
      },
      snapshot() {
        if (n === 0) return undefined;
        const mean = s / n;
        return Math.sqrt(sq / n - mean * mean);
      },
    };
  },
  rollingState() {
    let s = 0;
    let sq = 0;
    let n = 0;
    return {
      add(_i, v) {
        if (typeof v === 'number') {
          s += v;
          sq += v * v;
          n++;
        }
      },
      remove(_i, v) {
        if (typeof v === 'number') {
          s -= v;
          sq -= v * v;
          n--;
        }
      },
      snapshot() {
        if (n === 0) return undefined;
        const mean = s / n;
        return Math.sqrt(Math.max(0, sq / n - mean * mean));
      },
    };
  },
};
