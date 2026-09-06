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
