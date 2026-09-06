/**
 * **Typical price** — `(high + low + close) / 3`, the one-number summary of
 * a bar that the volume-weighted and money-flow studies price on: VWAP
 * weights it by volume, and CCI, MFI and the Keltner centre line all read
 * it. A named, reusable quantity, so it lives here rather than in any one
 * of them.
 *
 * `NaN` marks a gap ([PND-STUDYBOX]) and propagates through the sum, so a
 * bar with any of its three prices missing has no typical price.
 *
 * O(N), one pass, one allocation.
 */
export function typicalPriceValues(
  high: Float64Array,
  low: Float64Array,
  close: Float64Array,
): Float64Array {
  const length = high.length;
  const out = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    out[i] = (high[i]! + low[i]! + close[i]!) / 3;
  }
  return out;
}

/**
 * **Median price** — `(high + low) / 2`, the midpoint of a bar's range and
 * the *other* one-number bar summary the corpus keeps asking for: the
 * Awesome Oscillator is a difference of two SMAs of it, Bill Williams'
 * Alligator smooths it with three displaced SMMAs, the Gator histogram is
 * built from those, and High-Low Bands are a percent envelope around an MA
 * of it (assessment §6.1 / §6.2).
 *
 * It lives beside {@link typicalPriceValues} rather than in a file of its own
 * because it is the same idea at a different arity — a named per-bar price
 * summary — and because the alternative is a `(h + l) / 2` loop inside a
 * study, which is exactly what the studies README's "studies contain no data
 * loop" rule exists to stop. One consumer ships today ({@link
 * awesomeOscillator}); the three named above are the reason it is exported
 * rather than kept private to it.
 *
 * `NaN` marks a gap ([PND-STUDYBOX]) and propagates through the sum, so a bar
 * missing either price has no median price.
 *
 * O(N), one pass, one allocation.
 */
export function medianPriceValues(
  high: Float64Array,
  low: Float64Array,
): Float64Array {
  const length = high.length;
  const out = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    out[i] = (high[i]! + low[i]!) / 2;
  }
  return out;
}

/**
 * **Bar range** — `high − low`, the span a bar covered *within itself*, and
 * the third per-bar summary this file names (after {@link
 * typicalPriceValues} and {@link medianPriceValues}).
 *
 * ## Why it is here rather than inside a study
 *
 * Two studies want exactly this array and nothing else from the bar:
 * {@link chaikinVolatility} smooths it with an EMA and reads that EMA's rate
 * of change, and {@link massIndex} takes the ratio of one EMA of it to a
 * second EMA of the first. Sharing the derivation is what stops the two from
 * disagreeing about what "the range" is — the studies README's rule that a
 * study is options-validation plus kernel calls.
 *
 * **This is plain range, not {@link trueRangeValues}.** True range widens a
 * bar that opened away from the previous close; this does not, so a gap up
 * shows here only as whatever the bar traded through afterwards. Both
 * definitions are in the corpus and the two families do not mix: the
 * volatility studies built on Wilder's work (ATR, Keltner, the Choppiness
 * Index) take *true* range, while Chaikin's and Dorsey's take the plain one,
 * because that is what each author defined. Neither study exposes a knob to
 * swap them — that would be two indicators behind a flag.
 *
 * `NaN` marks a gap ([PND-STUDYBOX]) and propagates through the subtraction,
 * so a bar missing either price has no range. Nothing clamps the sign: a
 * caller who redirects `high` and `low` at two columns that cross gets
 * negative ranges, honestly, and each consumer documents what that means for
 * it.
 *
 * O(N), one pass, one allocation.
 */
export function barRangeValues(
  high: Float64Array,
  low: Float64Array,
): Float64Array {
  const length = high.length;
  const out = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    out[i] = high[i]! - low[i]!;
  }
  return out;
}
