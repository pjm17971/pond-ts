import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import { movingAverageValues } from '../kernels/moving-average.js';
import { rollingMeanValues } from '../kernels/rolling-mean.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';
import { barRangeValues } from '../kernels/typical-price.js';

export interface MassIndexOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Span of **each** of the two chained EMAs, in **bars**. **Default `9`.** */
  emaPeriod?: number;
  /** Summation look-back over the EMA ratio, in **bars**. **Default `25`.** */
  sumPeriod?: number;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'mass'`.** */
  output?: Output;
}

/**
 * **Mass Index** (Donald Dorsey) — a running sum of how much a smoothed bar
 * range exceeds its *own* smoothing, which is a measure of range **expansion**
 * that ignores direction entirely:
 *
 * ```
 * E1 = EMA(high − low, emaPeriod)
 * E2 = EMA(E1, emaPeriod)
 * ${output} = Σ over sumPeriod of (E1 / E2)
 * ```
 *
 * Appends one column. Reads **high and low**, each named by an option
 * defaulting to its `DEFAULT_OHLCV` name (the {@link atr} precedent).
 *
 * Dorsey's reading: the ratio sits near `1` while ranges are steady and
 * climbs while they widen, so a 25-bar sum of it near `25` is a quiet market.
 * The signal he named is the **reversal bulge** — the index rising through
 * 27 and then falling back below 26.5 — which warns that a trend is about to
 * turn *without saying which way*. The index has no direction of its own,
 * which is the whole point of it: pair it with a trend study.
 *
 * ## Definition — Dorsey's 9 / 25, and what is pinned
 *
 * **TA-Lib has no Mass Index**, so the oracle is a pandas replication with
 * the analytic first-valid bar asserted and a measured separation from the
 * plausible wrong turn (using the ratio's *mean* rather than its sum, which
 * is the same shape divided by 25 and would sit inside a chart's noise if it
 * were not asserted apart).
 *
 * - **9 and 25 are Dorsey's own.** Both are options because the two lengths
 *   do different jobs, but the defaults are the published pair and the 27 /
 *   26.5 bulge thresholds only mean anything at `sumPeriod: 25` — a caller
 *   who changes it has to rescale the thresholds too, which is said here
 *   rather than left to be discovered.
 * - **A sum, not an average.** The index is `Σ ratio`, so it lives on a
 *   `~sumPeriod` scale (about 25 for a steady market). Dividing by
 *   `sumPeriod` would be a perfectly good indicator and is *not* this one;
 *   every published threshold is on the sum.
 * - **The EMAs are pond's**, first-sample seed, `α = 2/(emaPeriod + 1)` —
 *   the {@link macd} / {@link trix} convention, so the chain cannot disagree
 *   with `ema()` inside the package.
 * - **Plain range, not true range** ({@link barRangeValues}) — Dorsey's, and
 *   the same fork {@link chaikinVolatility} takes. {@link atr} and
 *   {@link choppinessIndex} take the other one.
 *
 * ## Warm-up: the EMA∘EMA chain, on the {@link trix} rule
 *
 * Stage 2 steps over stage 1's warm-up rather than poisoning its seed with
 * it (that is the K2 array door's rule for derived inputs), so on gap-free
 * input `E1` lands on bar `emaPeriod − 1`, `E2` on **`2·emaPeriod − 2`**, and
 * the summation `sumPeriod − 1` bars after that: **`2·emaPeriod + sumPeriod
 * − 3`**, which is bar 40 at the defaults. Length-preserving.
 *
 * ## Edges
 *
 * - **Scale-invariant, and shift-invariant.** The ratio of two linear filters
 *   of the same non-negative array is unchanged by scaling every price, and
 *   the range is a difference, so it is unchanged by shifting them too. Both
 *   are pinned by property tests.
 * - **A steady market reads `≈ sumPeriod`, never `0`.** The natural
 *   comparison level is `25`, not zero — this is not an oscillator around a
 *   zero line.
 * - **A zero denominator reports `undefined`, and needs no guard at all.**
 *   Two ways to reach one, and the summation absorbs both. On real bars
 *   `E2 = 0` requires every range from the series' start to that bar to be
 *   exactly zero (a first-sample-seeded EMA of non-negative values is zero
 *   only if all of them are), which forces `E1 = 0` too, so the ratio is
 *   `0/0` — already `NaN`. On **crossing** columns (`high` and `low`
 *   redirected at two fields that swap order) the range changes sign, `E2`
 *   can land exactly on zero with `E1` non-zero beside it, and the ratio is
 *   `±Infinity` — which {@link rollingMeanValues} counts as a **missing
 *   cell**, exactly as it counts a `NaN`, so the summation over it is
 *   `undefined` and nothing non-finite ever reaches `withColumn`.
 *   An explicit `d === 0` guard was written here first and **mutation testing
 *   deleted it**: no input can tell it is there. That is the
 *   {@link commodityChannelIndex} finding arriving by a new route — not "the
 *   numerator is forced to zero" but "a rolling kernel downstream of the
 *   division masks non-finite values" — and it is why
 *   {@link choppinessIndex}'s guards, which sit at its *output*, are live
 *   while this one was not. A unit test reaches the crossing case and pins
 *   the `undefined`.
 * - **A leading gap shifts the start**; an **interior** gap blanks that bar
 *   in both EMA stages and then every summation window holding it, after
 *   which the study recovers — the `ema` family skips, so the hole does not
 *   run to the end of the series the way {@link atr}'s does.
 */
export function massIndex<
  S extends SeriesSchema,
  const Output extends string = 'mass',
>(series: TimeSeries<S>, options: MassIndexOptions<S, Output> = {}) {
  const emaPeriod = options.emaPeriod ?? 9;
  const sumPeriod = options.sumPeriod ?? 25;
  assertPeriod(emaPeriod, 'emaPeriod');
  assertPeriod(sumPeriod, 'sumPeriod');

  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const output = (options.output ?? 'mass') as Output;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const range = barRangeValues(
    columnValues(wide, highName),
    columnValues(wide, lowName),
  );

  // Two chained passes of the K2 engine's raw-array EMA — the same recursion
  // `ema()` runs, one definition rather than a private chain. Stage 2 steps
  // over stage 1's NaN head (the array door's rule for derived inputs), which
  // is what makes the warm-up compose to 2·emaPeriod − 2 rather than collapse
  // (the `trix` precedent, one stage shallower).
  const single = movingAverageValues(range, emaPeriod, 'ema');
  const double = movingAverageValues(single, emaPeriod, 'ema');

  const length = range.length;
  const ratio = new Float64Array(length);
  // No zero-denominator guard, and the reason is the summation below rather
  // than the arithmetic here: on real bars `E2 = 0` forces `E1 = 0` (so the
  // ratio is `0/0`, already `NaN`), and on crossing columns it can be
  // `±Infinity` — which `rollingMeanValues` treats as a missing cell, exactly
  // as it treats a `NaN`. Either way the reading is `undefined`. A guard here
  // was written, and mutation testing found no input that could tell it was
  // there. See "Edges".
  for (let i = 0; i < length; i += 1) ratio[i] = single[i]! / double[i]!;

  // Σ ratio is `sumPeriod × mean(ratio)`, so the summation composes on the
  // shipped rolling-mean kernel — whose array door (a window emits only once
  // its last `sumPeriod` values are ALL finite) is the interior-gap rule
  // documented above — rather than on a second sliding accumulator.
  const mean = rollingMeanValues(ratio, sumPeriod);
  const out = new Float64Array(length);
  for (let i = 0; i < length; i += 1) out[i] = mean[i]! * sumPeriod;

  return series.withColumn(output, out);
}
