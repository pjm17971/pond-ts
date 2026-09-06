/**
 * **Up / down legs of a per-bar change** — one array of changes split into
 * the non-negative part and the non-negative *magnitude* of the negative
 * part:
 *
 * ```
 * up[i]   = max(delta[i], 0)
 * down[i] = max(−delta[i], 0)
 * ```
 *
 * Every "how much of the recent move was upward" oscillator starts here and
 * differs only in what it does next: {@link rsi} Wilder-smooths the two legs
 * and reads their ratio, {@link chandeMomentum} sums them unsmoothed and
 * reads their normalised difference, {@link intradayMomentumIndex} runs the
 * same sums over candle **bodies** rather than close-to-close changes. Three
 * studies, one split — so the split lives here.
 *
 * It is deliberately **not** a diff: the caller supplies the changes, because
 * the three consumers do not agree on what a change is (`close[i] −
 * close[i−1]` for two of them, `close[i] − open[i]` for the third). Splitting
 * is the shared part; deriving is not.
 *
 * ## Zero and missing are different answers
 *
 * A bar that did not move contributes `0` to **both** legs — an honest zero,
 * which is what makes a flat window sum to `0 / 0` and read as *no answer*
 * rather than as a reading of zero (the {@link rsi} precedent). A bar whose
 * change is unknown contributes `NaN` to both, which the rolling kernels'
 * array door then carries: every window containing it is missing, rather
 * than being averaged over one bar fewer.
 *
 * Both are load-bearing and neither is the default a naive `Math.max(d, 0)`
 * would give — `Math.max(NaN, 0)` is `NaN`, but `d > 0 ? d : 0` maps a `NaN`
 * change to `0`, silently reading an unknown bar as a flat one.
 *
 * O(N), one pass, two allocations.
 */
export function upDownLegValues(deltas: Float64Array): {
  up: Float64Array;
  down: Float64Array;
} {
  const length = deltas.length;
  const up = new Float64Array(length);
  const down = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    const d = deltas[i]!;
    if (Number.isNaN(d)) {
      up[i] = NaN;
      down[i] = NaN;
      continue;
    }
    up[i] = d > 0 ? d : 0;
    down[i] = d < 0 ? -d : 0;
  }
  return { up, down };
}
