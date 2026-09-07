/**
 * **Shinohara's four intensity terms** — the per-bar numerators and
 * denominators the A and B ratios sum over a window:
 *
 * ```
 * strongUp[i]   = high[i] − open[i]          strongDown[i] = open[i] − low[i]
 * weakUp[i]     = high[i] − close[i−1]       weakDown[i]   = close[i−1] − low[i]
 * ```
 *
 * The A ratio ("strong") measures how far a bar travelled **above its own
 * open** against how far it fell below it; the B ratio ("weak") measures the
 * same two distances against the **previous close**, which is what makes one
 * a reading of the session's own conviction and the other a reading of the
 * gap the session opened into.
 *
 * ## Why a kernel
 *
 * Four row-aligned derivations, one of which reads a **shifted** column, and
 * every one of them has to be blanked on exactly the bars the others are
 * (see below). That is a loop, and the studies README puts loops here. The
 * study is then four {@link rollingMeanValues} calls and two divisions.
 *
 * ## Bar 0, and why the pairs blank together
 *
 * The two **weak** terms read `close[i−1]`, so both are `NaN` at bar 0 — the
 * B ratio's window therefore starts one bar after the A ratio's, which is
 * the per-column warm-up rule the package applies everywhere else.
 *
 * Within each pair, numerator and denominator are `NaN` on exactly the same
 * bars **by construction**, because each pair reads the same three inputs:
 * `strong` reads `open` with `high` or `low`, `weak` reads the previous
 * `close` with `high` or `low`. A bar missing its `high` blanks the up-term
 * only, so the ratio for any window containing it is missing anyway (its
 * numerator is), and the surviving denominator is never read. That is the
 * #710 rule satisfied without a mask rather than in spite of one — worth
 * stating, because the obvious extension (letting the caller point `high`
 * and `low` at different columns with different gaps) would break it.
 *
 * Nothing is clamped: a caller who redirects `open` at a column outside the
 * bar's range gets negative terms, honestly, and the study documents what a
 * negative sum means for the reading.
 *
 * O(N), one pass, four allocations.
 */
export function shinoharaTermsValues(
  open: Float64Array,
  high: Float64Array,
  low: Float64Array,
  close: Float64Array,
): {
  strongUp: Float64Array;
  strongDown: Float64Array;
  weakUp: Float64Array;
  weakDown: Float64Array;
} {
  const length = high.length;
  const strongUp = new Float64Array(length);
  const strongDown = new Float64Array(length);
  const weakUp = new Float64Array(length);
  const weakDown = new Float64Array(length);
  if (length === 0) {
    return { strongUp, strongDown, weakUp, weakDown };
  }

  weakUp[0] = NaN;
  weakDown[0] = NaN;
  for (let i = 0; i < length; i += 1) {
    strongUp[i] = high[i]! - open[i]!;
    strongDown[i] = open[i]! - low[i]!;
    if (i > 0) {
      const prevClose = close[i - 1]!;
      weakUp[i] = high[i]! - prevClose;
      weakDown[i] = prevClose - low[i]!;
    }
  }
  return { strongUp, strongDown, weakUp, weakDown };
}
