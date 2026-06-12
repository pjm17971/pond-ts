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
    // **Two-pass formula** matches the row-API `reduce` path:
    //   mean = Σv / n
    //   variance = Σ(v − mean)² / n
    // The one-pass formula `sq/n − mean²` (used by `bucketState` /
    // `rollingState` because they need incremental updates) suffers
    // catastrophic cancellation for near-equal large-magnitude
    // values — e.g. `[1e10, 1e10+1, 1e10+2, 1e10+3]` (population
    // stdev = sqrt(5/4) ≈ 1.118) gives exactly 0 from one-pass vs
    // 1.118 from two-pass. The row-API `reduce(_d, numeric)` does
    // the two-pass; the column path does the same to keep results
    // identical across paths. Closed L2 review finding on PR #153.
    //
    // `bucketState` now uses **Welford's online variance** (stable,
    // O(1) per add, no buffer) so the bucketed `aggregate()` row path
    // matches this two-pass column path to floating-point precision —
    // closing the §1.1 fast-path-vs-row-path divergence (and the
    // missing-clamp NaN crash) without regressing the live layer,
    // which shares `bucketState` and depends on O(1) state. The two
    // formulas agree to ~1e-15, not bit-for-bit (Welford accumulates
    // in add-order, the two-pass in array-order) — far below the gross
    // cancellation the old one-pass `sq/n − mean²` produced (0 instead
    // of ~1.118 on near-equal large values).
    //
    // `rollingState` keeps the one-pass form: its sliding window needs
    // `remove`, which Welford can't do stably (windowed removal
    // drifts), and it already has the `Math.max(0, …)` clamp.
    // Stabilising rolling stdev is a separate, deferred item.
    const values = col._values;
    const validity = col.validity;
    let s = 0;
    let n = 0;
    if (validity === undefined) {
      n = col.length;
      if (n === 0) return undefined;
      for (let i = 0; i < n; i += 1) s += values[i]!;
    } else {
      n = validity.definedCount;
      if (n === 0) return undefined;
      const bits = validity.bits;
      for (let i = 0; i < col.length; i += 1) {
        if ((bits[i >> 3]! & (1 << (i & 7))) !== 0) s += values[i]!;
      }
    }
    const mean = s / n;
    let variance = 0;
    if (validity === undefined) {
      for (let i = 0; i < n; i += 1) {
        const d = values[i]! - mean;
        variance += d * d;
      }
    } else {
      const bits = validity.bits;
      for (let i = 0; i < col.length; i += 1) {
        if ((bits[i >> 3]! & (1 << (i & 7))) === 0) continue;
        const d = values[i]! - mean;
        variance += d * d;
      }
    }
    return Math.sqrt(variance / n);
  },
  bucketState() {
    // Welford's online variance — see the note in `reduceColumn`. `m2` is the
    // running sum of squared deviations from the current mean; it is ≥ 0 by
    // construction, so the `Math.max(0, …)` only absorbs FP round-off (−0 /
    // sub-ULP), never the gross negative the old one-pass `sq/n − mean²` hit.
    let n = 0;
    let mean = 0;
    let m2 = 0;
    return {
      add(v) {
        if (typeof v === 'number') {
          n += 1;
          const delta = v - mean;
          mean += delta / n;
          m2 += delta * (v - mean);
        }
      },
      snapshot() {
        if (n === 0) return undefined;
        return Math.sqrt(Math.max(0, m2 / n));
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
