import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import { highestLowestValues } from '../kernels/highest-lowest.js';
import { rollingMeanValues } from '../kernels/rolling-mean.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';
import { trueRangeValues } from '../kernels/true-range.js';

export interface ChoppinessIndexOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Look-back in **bars**, for both the true-range sum and the HH/LL range.
   *  **Default `14`.** Must be **at least 2** — the reading is normalised by
   *  `log10(period)`, which is zero at 1. */
  period?: number;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Close column. **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'chop'`.** */
  output?: Output;
}

/**
 * **Choppiness Index** (E. W. Dreiss) — **how much path the market walked to
 * get where it got**, on a `0..100` scale: the total distance travelled
 * (summed true range) against the net ground covered (the window's own
 * high-to-low range), normalised so that the scale does not move with
 * `period`.
 *
 * ```
 * ${output} = 100 × log10( ΣTR over period / (HH − LL over period) ) / log10(period)
 * ```
 *
 * Appends one column. Reads **high, low and close**, each named by an option
 * defaulting to its `DEFAULT_OHLCV` name (the {@link atr} precedent) — the
 * close is there because the sum is of *true* range, which reads the previous
 * close.
 *
 * **High is choppy, low is trending — and it says nothing about direction.**
 * A market that ranged sideways covers a lot of true range inside a narrow
 * band, so the ratio is large and the reading is near `100`; a market that
 * went one way covers the same ground once, the ratio approaches `1`, and the
 * reading approaches `0`. Both a crash and a melt-up read *low*. Dreiss'
 * conventional thresholds are 61.8 and 38.2 (Fibonacci), and they are
 * conventions, not properties of the number.
 *
 * ## Why the `log10(period)` denominator
 *
 * It is what makes readings comparable across look-backs. `ΣTR/(HH−LL)` can
 * be as large as roughly `period` (each bar re-walking the whole band), so
 * its logarithm tops out near `log10(period)`; dividing by that maps the
 * ratio onto a fixed `0..100` axis whatever `period` is. That also **rules
 * out `period: 1`**, where `log10(1) = 0` — the study throws rather than
 * dividing by zero, since there is no reading to give (one bar's true range
 * over one bar's range is not a measure of anything).
 *
 * ## Definition — pinned, because no vendor arbitrates it
 *
 * **TA-Lib has no Choppiness Index**, so the oracle is a pandas replication
 * with the analytic first-valid bar asserted and a measured separation from
 * the plausible wrong turn — the same formula on **plain** `high − low`
 * instead of true range, which is the one substitution that leaves the shape
 * of the curve intact while changing every value.
 *
 * - **True range, and it is the package's own** {@link trueRangeValues} —
 *   asserted bit-for-bit equal to `talib.TRANGE` in the oracle through
 *   {@link ultimateOscillator}'s case, so this study and the whole ATR family
 *   measure range identically by construction. (Contrast
 *   {@link chaikinVolatility} and {@link massIndex}, which take *plain* range
 *   because Chaikin and Dorsey defined them that way.)
 * - **`log10`, not `ln` — and it does not matter.** The base cancels:
 *   `log_b(x)/log_b(n)` is `log_n(x)` for every `b`, so the reading is really
 *   "the log, base `period`, of the path-to-range ratio". `log10` is what
 *   every published statement writes and what ships; the generator asserts
 *   the two agree (`1.4e-14`) rather than leaving a reader to work it out.
 *   (The same
 *   cancellation applies to {@link gopalakrishnanRangeIndex}, which is why
 *   neither study takes a base option. **Mixing** the bases would be a real
 *   bug, and that is what the oracle separates against.)
 * - **One `period` for both halves.** Every vendor uses a single length; the
 *   sum and the range are two readings of the same window, and splitting them
 *   would make the `0..100` normalisation meaningless.
 *
 * ## Warm-up
 *
 * `TR[0]` is undefined (no previous close), so a `period`-bar sum of true
 * ranges first exists on bar **`period`** — one row later than the HH/LL
 * range's `period − 1`, and the later of the two is what the column shows.
 * Bar 14 at the default. Length-preserving.
 *
 * ## Edges
 *
 * - **Scale-invariant, and shift-invariant.** Both halves of the ratio are
 *   homogeneous of degree one in price and both are built from differences,
 *   so neither multiplying nor adding a constant moves the reading. Pinned by
 *   property tests.
 * - **Bounded `0..100`, with one honest leak.** `ΣTR ≥ HH − LL` always (the
 *   window's own span is covered by the bars that made it), so the ratio is
 *   at least 1 and the reading is never negative. The upper bound holds
 *   because each bar's true range is contained in the window's range — except
 *   for the **first** bar of the window, whose true range reads a close from
 *   *before* the window and can therefore exceed it. A hard gap into the
 *   window can push the reading a little above 100; nothing clamps it,
 *   because clamping would hide the gap. Measured on the oracle input, whose
 *   bars are gap-free: the reading spans **20.13 … 80.29** at `period 14` and
 *   **22.85 … 87.18** at `period 5`, and the generator asserts `0..100` on
 *   both.
 * - **A flat window → `undefined`, and this is *not* the flat-bar case.**
 *   When `HH === LL` the ratio is `0/0` on consistent bars, and the reading
 *   has no defensible value: unlike a flat bar's close location — whose
 *   numerator `(c−l)−(h−c)` is algebraically forced to zero, so `0` is the
 *   answer rather than a convention ({@link accumulationDistribution}) —
 *   there is nothing here that a limit picks out. Worse, the two candidate
 *   conventions are opposites: `0` would read "perfectly trending" and `100`
 *   "perfectly choppy", for a market that did not move at all. So the guard
 *   is explicit and reports missing (the {@link stochastic} flat-window
 *   precedent, applied on its own merits).
 * - **A zero true-range sum → `undefined` too.** On consistent bars that
 *   implies `HH === LL` and is the same case, but it is reachable
 *   independently when `close` is redirected at a column outside the bar's
 *   own range: bars with `high = low = previous value of that column` have
 *   zero true range while their highs still differ from each other. The
 *   result would be `log10(0) = −Infinity`, so the guard is load-bearing
 *   rather than a formality, and a unit test reaches it.
 * - **A leading gap shifts the start**; an **interior** gap blanks the bar
 *   and the bar after it (true range reads the previous close) and then every
 *   window over those, after which the study recovers. Note the HH/LL half
 *   and the ΣTR half treat a gap **differently**: the extremes come from
 *   core's rolling reducers, which *skip* a missing cell, while the sum comes
 *   from {@link rollingMeanValues}, which does not — so the sum is the half
 *   that blanks, and the study's mask is its mask.
 */
export function choppinessIndex<
  S extends SeriesSchema,
  const Output extends string = 'chop',
>(series: TimeSeries<S>, options: ChoppinessIndexOptions<S, Output> = {}) {
  const period = options.period ?? 14;
  assertPeriod(period);
  if (period < 2) {
    throw new TypeError(
      'choppinessIndex period must be at least 2 (the reading is normalised by log10(period), which is 0 at 1)',
    );
  }

  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const closeName = (options.close ?? DEFAULT_OHLCV.close) as string;
  const output = (options.output ?? 'chop') as Output;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const trueRange = trueRangeValues(
    columnValues(wide, highName),
    columnValues(wide, lowName),
    columnValues(wide, closeName),
  );

  // Σ TR is `period × mean(TR)` over a DERIVED array, so it composes on the
  // rolling-mean kernel — whose array door blanks any window holding a gap,
  // which is the study's own missing-cell rule.
  const meanTrueRange = rollingMeanValues(trueRange, period);
  // The net range comes from one scan of core's reducers (the same pair
  // `stochastic`, `williamsR` and `donchian` read), which SKIP a missing
  // cell — the asymmetry documented above.
  const { highest, lowest } = highestLowestValues(
    wide,
    highName,
    lowName,
    period,
  );

  const logPeriod = Math.log10(period);
  const length = trueRange.length;
  const out = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    const span = highest[i]! - lowest[i]!;
    const sum = meanTrueRange[i]! * period;
    // Both halves of the guard are reachable and neither is cosmetic: a flat
    // window would divide by zero, and a zero true-range sum would take the
    // logarithm of zero. A `NaN` on either side fails both comparisons, so
    // the warm-up and interior gaps fall out of the same expression.
    out[i] =
      span > 0 && sum > 0 ? (100 * Math.log10(sum / span)) / logPeriod : NaN;
  }
  return series.withColumn(output, out);
}
