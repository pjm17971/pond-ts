/**
 * **Signed volume** — each bar's volume carrying the sign of its close
 * change, the per-bar term On-Balance Volume accumulates:
 *
 * ```
 * sv[i] = sign(close[i] − close[i−1]) · volume[i]
 * ```
 *
 * An up-close counts the whole bar's volume as buying, a down-close as
 * selling, and an unchanged close as neither (`sign(0) = 0`). This is the
 * derivation half of OBV; {@link cumulativeValues} is the accumulation
 * half, and `obv` composes the two.
 *
 * ## The first bar
 *
 * Bar 0 has no previous close, so its sign is undefined — and **TA-Lib's
 * convention is to count it as accumulation**: `OBV[0] = volume[0]`. That
 * convention lives here, as `sv[first] = volume[first]`, rather than in the
 * study, because it belongs to the *term*: a cumulative sum that steps over
 * a leading `NaN` would otherwise seed on bar 1 and sit `volume[0]` below
 * TA-Lib for the whole series. (Some implementations seed at `0` instead.
 * OBV is read for its *shape*, not its level, so either is defensible; this
 * one is chosen so the oracle can assert equality rather than a bound.)
 *
 * `first` is the first bar at which **both** close and volume are finite.
 * A leading gap in either input therefore shifts the seed to the first
 * usable bar — the answer TA-Lib's own wrapper gives (it strips leading
 * `NaN`s from every input before calling the C function), and the answer
 * that lets OBV run over a study output whose warm-up leaves missing rows
 * at the head.
 *
 * ## Missing cells after the first bar
 *
 * `NaN` marks a gap ([PND-STUDYBOX]). `Math.sign(NaN)` is `NaN` and
 * `0 · NaN` is `NaN`, so a bar with a missing close, a missing previous
 * close, **or** a missing volume has an unknown term — including a flat
 * bar with unknown volume, where a case could be made for `0`; it is not
 * made, because "unknown times zero" is a rule nobody would expect to have
 * to know. Note that a missing close costs **two** bars: its own, and the
 * next one, whose sign reads it as the previous close.
 *
 * O(N), one pass, one allocation.
 */
export function signedVolumeValues(
  close: Float64Array,
  volume: Float64Array,
): Float64Array {
  const length = close.length;
  const out = new Float64Array(length);

  let first = 0;
  while (
    first < length &&
    (Number.isNaN(close[first]!) || Number.isNaN(volume[first]!))
  ) {
    first += 1;
  }
  for (let i = 0; i < first; i += 1) out[i] = NaN;
  if (first >= length) return out;

  out[first] = volume[first]!;
  for (let i = first + 1; i < length; i += 1) {
    out[i] = Math.sign(close[i]! - close[i - 1]!) * volume[i]!;
  }
  return out;
}
