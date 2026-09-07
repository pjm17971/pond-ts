import { rollingMeanValues } from './rolling-mean.js';
import { trueRangeValues } from './true-range.js';

/**
 * **The Random Walk Index's two legs** (E. Michael Poulos, TASC 1991) — how
 * far price actually travelled over each of the look-backs `2 … period`,
 * measured in units of what a random walk of the same length would be
 * expected to cover, and reported as the **largest** such reading:
 *
 * ```
 * rwiHigh[i] = max over n = 2 … period of  (high[i]  − low[i−n])  / (meanTR(n)[i] · √n)
 * rwiLow[i]  = max over n = 2 … period of  (high[i−n] − low[i])   / (meanTR(n)[i] · √n)
 * ```
 *
 * The `√n` is the whole idea: a symmetric random walk covers a distance
 * proportional to `√n` after `n` steps, and one step's size is estimated by
 * the mean true range, so the ratio is "how many random-walk-widths did this
 * move actually cover". A reading above 1 is a move too large to be chance
 * at that horizon; the max over horizons asks the question at every scale
 * the `period` allows and reports the strongest answer.
 *
 * ## Why this is a kernel and not a study loop — the G2 "multi-column
 * window" shape
 *
 * The corpus assessment (§5, **G2**) flags this as the awkward one: it is a
 * window, but the reducer needs a *different* rolling statistic at every
 * horizon inside the window, which none of `rollingValues`,
 * `rollingColumns`, {@link rollingExtremesValues} or {@link foldRows} can
 * express. So the horizon sweep lives here, once, rather than as a nested
 * loop inside the study.
 *
 * ## `meanTR(n)` is the **`n`-bar mean** of true range, not Wilder's ATR
 *
 * This is the one definitional choice the formula forces, and it is made in
 * favour of Poulos' own words ("the average true range over the last `n`
 * periods") for a structural reason as well as a textual one: the `√n`
 * scaling is a statement about `n` **independent steps**, so the denominator
 * has to be the average step size over exactly those `n` bars. Wilder's
 * recursion is an infinite-memory smoothing whose effective window is not
 * `n` at all, so `atr(n)` at horizon `n` would be scaled by the wrong
 * quantity — and, unlike a plain mean, its value at bar `i` depends on
 * history from before the horizon being tested. The alternative is measured
 * rather than dismissed: the generator computes the whole study with
 * `wilder` denominators and asserts the separation.
 *
 * ## Cost — O(N · period), deliberately, with O(N) memory
 *
 * Each horizon costs one {@link rollingMeanValues} pass over the shared true
 * range plus one pass to fold into the running maximum, so the kernel is
 * `O(N · period)` in time — linear in the look-back, like
 * {@link rollingMeanAbsDevValues} and for the same reason (there is no
 * sliding update for a max over `period` *different* statistics). It is
 * **not** `O(N · period)` in memory: the horizons are folded in one at a
 * time and each mean array is discarded, so two outputs plus two scratch
 * arrays is the whole allocation regardless of `period`. Measured at 1M bars
 * in `scripts/perf-studies.mjs`, which carries a `period 14` and a
 * `period 50` entry side by side so the linearity stays visible.
 *
 * ## Missing cells and the strict max
 *
 * `Math.max` propagates `NaN`, so a bar reads `NaN` unless **every** horizon
 * has a value — the strict rule, and the one the reading needs: a "maximum
 * over 2 … 14" taken over the four horizons that happened to be available is
 * not that maximum. `rollingMeanValues` is itself strict (every one of the
 * `n` true ranges finite), and `trueRangeValues` leaves bar 0 `NaN`, so on
 * gap-free input the first value lands on bar **`period`** — exactly where
 * `low[i − period]` first exists too.
 *
 * A zero denominator (`meanTR(n) = 0`: every bar in that horizon completely
 * flat) reads `NaN` rather than an infinity. Nothing forces the numerator to
 * zero with it — `low[i−n]` is the low of a bar *outside* the true-range
 * window, which may have had any range at all — so it is a real number over
 * zero, not a `0/0`, and `withColumn` would reject the infinity anyway (it
 * throws on a non-finite cell rather than recording one — measured).
 *
 * ## The `i < n` half of the `blank` test is REDUNDANT, deliberately
 *
 * `meanTR(n)` is strict, so on any row before bar `n` it is already `NaN`,
 * which makes `denominator` `NaN`, which makes the term `NaN` with or
 * without the index test. Removing that half therefore changes no output and
 * a mutation of it survives every test — measured, and the same situation
 * `rollingExtremesValues`' `Number.isFinite` guard records.
 *
 * It stays for two reasons. It says at the point of use *which* rows are
 * warm-up, rather than leaving that to be re-derived from the strictness of
 * a kernel two files away. And it keeps the loop from reading
 * `low[i − n]` at a negative index, which is defined in JavaScript
 * (`undefined`, then `NaN` through the arithmetic) but is not something the
 * next editor should have to check.
 */
export function randomWalkValues(
  high: Float64Array,
  low: Float64Array,
  close: Float64Array,
  period: number,
): { rwiHigh: Float64Array; rwiLow: Float64Array } {
  const length = close.length;
  const rwiHigh = new Float64Array(length).fill(-Infinity);
  const rwiLow = new Float64Array(length).fill(-Infinity);
  const trueRange = trueRangeValues(high, low, close);

  for (let n = 2; n <= period; n += 1) {
    const meanTrueRange = rollingMeanValues(trueRange, n);
    const scale = Math.sqrt(n);
    for (let i = 0; i < length; i += 1) {
      const denominator = meanTrueRange[i]! * scale;
      // A zero mean true range is a real number over zero (see above), so it
      // is masked here rather than allowed to reach the max as an infinity.
      const blank = i < n || denominator === 0;
      const termHigh = blank ? NaN : (high[i]! - low[i - n]!) / denominator;
      const termLow = blank ? NaN : (high[i - n]! - low[i]!) / denominator;
      // `Math.max` propagates NaN, which IS the strict rule: one horizon
      // without a value leaves the bar without a maximum. It is also what
      // turns the `i < n` rows above into the warm-up — the longest horizon
      // writes `NaN` over every row before bar `period`, and a `NaN` in the
      // running maximum can never be displaced by a later horizon.
      rwiHigh[i] = Math.max(rwiHigh[i]!, termHigh);
      rwiLow[i] = Math.max(rwiLow[i]!, termLow);
    }
  }
  return { rwiHigh, rwiLow };
}
