/**
 * **Running sum** — `out[i] = Σ values[0..i]`, the accumulation step of the
 * cumulative volume studies. OBV is `cumulative(signedVolume)`; the
 * Accumulation/Distribution line is `cumulative(clv · volume)`, Price-Volume
 * Trend is `cumulative(pctChange · volume)`, and an anchored VWAP is a ratio
 * of two of these. One loop, shared, rather than one per study.
 *
 * ## Missing cells
 *
 * `NaN` marks a gap ([PND-STUDYBOX]), and a running sum is the one shape
 * where a gap has no local answer: every value after an unknown term is a
 * known sum plus an unknown, which is unknown. So:
 *
 * - **A leading run of gaps shifts the start.** The sum begins at the first
 *   finite term; rows before it are `NaN`. That is what lets a study run
 *   over another study's output, whose own warm-up leaves missing rows at
 *   the head, without coming back empty — the same rule
 *   {@link wilderValues} follows for its seed.
 * - **An interior gap propagates to the end.** `sum += NaN` is `NaN`, and
 *   stays `NaN`; nothing here resets it. Skipping the gap (treating the
 *   unknown term as `0`) would report a *level* that is silently off by the
 *   missing contribution for the rest of the series, which is worse than
 *   reporting no level. Fill before summing if you need continuity.
 *
 * That is the same asymmetry the Wilder recursion has, for the same reason:
 * a recursion carries state forward forever, and a running sum is the
 * simplest recursion there is.
 *
 * O(N), one pass, one allocation. The interior-gap rule costs nothing — it
 * is IEEE arithmetic doing what it does.
 */
export function cumulativeValues(values: Float64Array): Float64Array {
  const length = values.length;
  const out = new Float64Array(length);

  let first = 0;
  while (first < length && Number.isNaN(values[first]!)) first += 1;
  for (let i = 0; i < first; i += 1) out[i] = NaN;

  let sum = 0;
  for (let i = first; i < length; i += 1) {
    sum += values[i]!;
    out[i] = sum;
  }
  return out;
}
