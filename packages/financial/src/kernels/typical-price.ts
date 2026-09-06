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
