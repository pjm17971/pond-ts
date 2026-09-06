/**
 * **Percent rate of change over a raw array** — `(x[i] / x[i−periods] − 1) ×
 * 100`, kernel **K4** of the corpus assessment
 * (`docs/notes/financial-indicators-assessment-2026-07.md` §3).
 *
 * The arithmetic is one line; what is worth sharing is the **two edge rules**
 * that go with it, because a study that re-derives them differs from
 * `percentChange` in exactly the places nobody looks:
 *
 * - **No predecessor yet** (`i < periods`) → `NaN`. A length-preserving
 *   warm-up of `periods` rows, not `periods − 1`: the look-back reads a bar,
 *   not a window.
 * - **A zero base** → `NaN`. `x / 0` is `±Infinity`, which is not a percent
 *   change; there is no honest answer for "how far above zero is this", and
 *   `withColumn` would reject the infinity loudly rather than record it.
 *   (Note this is `=== 0`, so a *negative* base still produces a number —
 *   percent change off a negative level is defined, if unusual, and clamping
 *   it would be inventing a rule.)
 *
 * `NaN` marks a gap ([PND-STUDYBOX]) and propagates through the ratio on its
 * own, so a missing bar costs itself and the bar `periods` later, and nothing
 * else — a window kernel's "recovers once the gap leaves" rule with a window
 * of one.
 *
 * Over an **array** rather than a column because most callers derive their
 * input: TRIX takes the 1-bar rate of change of a triple-smoothed EMA,
 * Coppock sums two of these before smoothing, KST four.
 * {@link percentChange} is the column case and routes through here, so the
 * study and the derived-input callers are one definition.
 *
 * O(N), one pass, one allocation.
 */
export function percentChangeValues(
  values: Float64Array,
  periods: number,
): Float64Array {
  const length = values.length;
  const out = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    if (i < periods) {
      out[i] = NaN;
      continue;
    }
    const previous = values[i - periods]!;
    out[i] = previous === 0 ? NaN : (values[i]! / previous - 1) * 100;
  }
  return out;
}
