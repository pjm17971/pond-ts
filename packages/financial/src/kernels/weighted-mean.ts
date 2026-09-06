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
    // A gap in either input drops the row from BOTH sums (see above). The
    // test is `isFinite`, the same one `rollingMeanSdInto` counts by, so a
    // product that overflows to ±Infinity leaves both sums too rather than
    // desynchronising their counts.
    masked[i] = Number.isFinite(p) ? weights[i]! : NaN;
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

/** The 4-bar symmetric weights `(1, 2, 2, 1)`, and their total. Named so the
 *  loop below reads as the definition rather than as four magic numbers. */
const SWMA_WEIGHTS = [1, 2, 2, 1] as const;
const SWMA_TOTAL = 6;

/**
 * **Symmetric weighted moving average (SWMA)** — the fixed 4-bar
 * `(1, 2, 2, 1) / 6` smoother:
 *
 * ```
 * swma[i] = (x[i] + 2·x[i−1] + 2·x[i−2] + x[i−3]) / 6
 * ```
 *
 * A tiny, *symmetric* low-pass filter: unlike the K2 engine's `wma` (linear
 * weights `1…n`, heaviest on the newest bar) it weights the two middle bars
 * equally and the two outer bars equally, so it does not lean on the most
 * recent bar. That is the whole reason it exists as its own thing — it is
 * John Ehlers' smoother, and it is what TradingView's `swma()` and its
 * Relative Vigor Index are defined in terms of.
 *
 * ## Why the width is fixed rather than a `period`
 *
 * Because the definition is. "SWMA" names *these* weights, not a family:
 * TradingView's built-in takes no length, and the studies that cite it
 * (RVI's numerator, denominator and signal line — three calls from one
 * study) all mean the 4-bar form. A `period` parameter would need a weight
 * *rule* to generalise, and there is no published one — a knob whose other
 * settings nobody defines is a speculative parameter, not a feature. The K2
 * engine is the answer for "smooth this over n bars"; this is the answer for
 * "the SWMA".
 *
 * ## Edges
 *
 * - The first **three** rows are `NaN` — the window is not yet full,
 *   length-preserving as everywhere else.
 * - `NaN` marks a gap ([PND-STUDYBOX]) and propagates through the weighted
 *   sum on its own, so the gap bar and the three after it are missing. A
 *   positional weight cannot skip a cell without reweighting the rest — the
 *   `wma` rule, for the same reason.
 *
 * O(N), one pass, one allocation.
 */
export function symmetricWeightedValues(values: Float64Array): Float64Array {
  const length = values.length;
  const out = new Float64Array(length);
  const width = SWMA_WEIGHTS.length;
  for (let i = 0; i < length; i += 1) {
    if (i < width - 1) {
      out[i] = NaN;
      continue;
    }
    let sum = 0;
    for (let k = 0; k < width; k += 1) sum += SWMA_WEIGHTS[k]! * values[i - k]!;
    out[i] = sum / SWMA_TOTAL;
  }
  return out;
}
