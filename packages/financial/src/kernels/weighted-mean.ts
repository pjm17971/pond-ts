import { rollingMeanSdInto } from './ranged.js';

/**
 * **Rolling weighted mean** — over each trailing `period`-bar window,
 *
 * ```
 * out[i] = Σ values·weights / Σ weights
 * ```
 *
 * VWAP is this with typical price for `values` and volume for `weights`;
 * a volume-weighted moving average is the same thing over close. The
 * loop is shared rather than owned by either.
 *
 * ## Built on the range-exact kernel
 *
 * Both sums come from {@link rollingMeanSdInto} — as *means*, whose shared
 * divisor cancels in the ratio — so the weighted mean inherits that kernel's
 * range exactness and its shifted-frame conditioning rather than carrying a
 * second sliding accumulator with its own rounding history. The cost is two
 * passes instead of one, both on the cheap (no-σ) path.
 *
 * ## Missing cells
 *
 * `NaN` marks a gap ([PND-STUDYBOX]). A row whose value **or** weight is
 * missing is dropped from **both** sums — the weight array is masked to
 * the product's gaps before summing — so the ratio is always taken over
 * one set of rows. Without that, a missing price would drop its term from
 * the numerator while its volume stayed in the denominator, and the answer
 * would be quietly biased toward zero rather than honestly computed over
 * fewer bars.
 *
 * As with every count-window study, a window is emitted once it spans
 * `period` **rows** and is computed from whichever of them are finite; a
 * window with no finite contributor is `NaN`.
 *
 * **Zero total weight** — a window of no volume at all — is `NaN`: there is
 * nothing to weight by, so there is no answer. There is deliberately **no
 * guard** for it, because there is nothing for one to do: with non-negative
 * weights, a zero denominator means every weight in the window was zero,
 * so every product was too, and `0 / 0` is already `NaN`. (An earlier draft
 * carried a `d === 0 ? NaN : …` guard and claimed it was load-bearing;
 * mutation-testing showed no test could tell it was there, because it
 * wasn't doing anything.) The only way to reach `x / 0` is a negative
 * weight, which volume is not — and if one ever arrives, the `±Infinity`
 * is rejected loudly at `withColumn` rather than mapped to a silent gap.
 *
 * O(N), two passes, three allocations.
 */
export function rollingWeightedMeanValues(
  values: Float64Array,
  weights: Float64Array,
  period: number,
): Float64Array {
  const length = values.length;
  const product = new Float64Array(length);
  const masked = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    const p = values[i]! * weights[i]!;
    product[i] = p;
    // A gap in either input drops the row from BOTH sums (see above).
    masked[i] = Number.isNaN(p) ? NaN : weights[i]!;
  }

  const numerator = new Float64Array(length);
  const denominator = new Float64Array(length);
  rollingMeanSdInto(product, period, 0, length, numerator, undefined);
  rollingMeanSdInto(masked, period, 0, length, denominator, undefined);

  // Reuse the numerator buffer for the result — it is fresh and nothing else
  // reads it. A no-volume window divides 0 by 0 here and is NaN on its own
  // (see above).
  for (let i = 0; i < length; i += 1) {
    numerator[i] = numerator[i]! / denominator[i]!;
  }
  return numerator;
}
