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
  reduceColumn(col) {
    // Sum-of-values + sum-of-squares — mirrors the bucket state's
    // E[X²] − E[X]² formulation. Matches the bucket/rolling
    // path's numerical behavior exactly so column / row paths
    // produce identical results.
    const values = col.values;
    const validity = col.validity;
    let s = 0;
    let sq = 0;
    let n = 0;
    if (validity === undefined) {
      n = col.length;
      if (n === 0) return undefined;
      for (let i = 0; i < n; i += 1) {
        const v = values[i]!;
        s += v;
        sq += v * v;
      }
    } else {
      n = validity.definedCount;
      if (n === 0) return undefined;
      const bits = validity.bits;
      for (let i = 0; i < col.length; i += 1) {
        if ((bits[i >> 3]! & (1 << (i & 7))) === 0) continue;
        const v = values[i]!;
        s += v;
        sq += v * v;
      }
    }
    const mean = s / n;
    return Math.sqrt(Math.max(0, sq / n - mean * mean));
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
