import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';
import { rollingMeanValues } from '../kernels/rolling-mean.js';
import { symmetricWeightedValues } from '../kernels/weighted-mean.js';

export interface RelativeVigorIndexOptions<
  S extends SeriesSchema,
  Prefix extends string,
> {
  /** Summation look-back in **bars**. **Default `10`.** */
  period?: number;
  /** Open column. **Default `'open'`.** */
  open?: NumericColumnNameForSchema<S>;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Close column. **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Column-family prefix — appends `${prefix}` and `${prefix}Signal`.
   *  **Default `'rvi'`.** Note `rvi` is also the usual abbreviation of the
   *  Relative *Volatility* Index (corpus §6.5, not yet shipped); that study
   *  will take a different default prefix, so the two can coexist on one
   *  series. */
  prefix?: Prefix;
}

/**
 * **Relative Vigor Index** (John Ehlers) — where the bar closed within its
 * own range, aggregated over `period` bars:
 *
 * ```
 * numerator   = SWMA(close − open)          SWMA = (1, 2, 2, 1) / 6, 4 bars
 * denominator = SWMA(high − low)
 * ${prefix}       = Σ numerator over period / Σ denominator over period
 * ${prefix}Signal = SWMA(${prefix})
 * ```
 *
 * Appends **two** columns. The idea is one line long: in an uptrend markets
 * close near the high of the bar, in a downtrend near the low, so the body
 * as a fraction of the range is a measure of conviction — *vigour*. Both
 * legs are smoothed by Ehlers' 4-bar symmetric average
 * ({@link symmetricWeightedValues}) before being summed, which is what keeps
 * a single wide-range bar from swinging the ratio.
 *
 * Read against its **signal line**: the crossover is the signal, the same
 * way {@link macd}'s and {@link trix}'s are, which is why the signal is a
 * column here rather than something the caller re-derives.
 *
 * Reads **open, high, low and close**, each named by an option defaulting to
 * its `DEFAULT_OHLCV` column — the {@link qstick} and {@link atr} shapes
 * combined; this is the first study in the package to read all four.
 *
 * ## Definition source
 *
 * **TradingView's built-in `Relative Vigor Index`**, which is the form the
 * corpus describes ("SWMA-weighted `(C−O)/(H−L)` sums + signal") and the one
 * every charting vendor publishes. **TA-Lib has no RVI**, so the oracle is a
 * pandas replication of the definition above with the analytic first-valid
 * bar asserted **per column**, not a vendor cross-check.
 *
 * Two details that implementations differ on, pinned here:
 *
 * - **The smoothing is symmetric `(1, 2, 2, 1)/6`, not a linear `wma(4)`**
 *   (`1, 2, 3, 4`). They are different filters — the K2 engine's `wma`
 *   leans on the newest bar, and this deliberately does not — so
 *   `movingAverageValues(..., 4, 'wma')` is *not* a drop-in for it. A test
 *   pins the difference.
 * - **The signal line is a fourth SWMA of the index**, not an EMA or an
 *   `sma(4)`. TradingView's is `swma(rvi)`; that is what ships.
 *
 * ## Edges
 *
 * - **Warm-up is per column** (the {@link macd} rule): the SWMA costs 3
 *   rows and the summation `period − 1` more, so `${prefix}` first appears
 *   on bar `period + 2` (bar 12 at the default 10) and `${prefix}Signal`
 *   three bars later, on `period + 5`. Length-preserving.
 * - **Not bounded.** On consistent bars `|close − open| ≤ high − low`, so
 *   the ratio sits inside ±1 and readings cluster far tighter than that —
 *   but it is a ratio of two smoothed sums, not a normalised position, and
 *   nothing clamps it. Do not chart it as if it were `−1..1`.
 * - **A window whose ranges sum to zero** → `undefined` for both columns,
 *   guarded explicitly. On consistent bars that is `0/0` (four bars with no
 *   range have no bodies either); on redirected columns the numerator can
 *   be non-zero over a zero denominator, and `±Infinity` in a chart's
 *   y-domain is worse than a gap.
 * - **Scale- and shift-invariant**: numerator and denominator are both
 *   homogeneous of degree one in price, and both are built from differences
 *   *within* a bar, so adding a constant to every price changes neither.
 *   Pinned by property tests.
 * - **A gap in any of the four inputs** blanks that leg for four bars (a
 *   positional weight cannot skip a cell), and the summation then blanks
 *   every window containing one of those — so an interior gap costs
 *   `period + 3` bars of `${prefix}` and three more of the signal, after
 *   which both recover. Windows and fixed-width filters forget; no
 *   recursion carries the hole to the end of the series.
 */
export function relativeVigorIndex<
  S extends SeriesSchema,
  const Prefix extends string = 'rvi',
>(series: TimeSeries<S>, options: RelativeVigorIndexOptions<S, Prefix> = {}) {
  const period = options.period ?? 10;
  assertPeriod(period);
  const openName = (options.open ?? DEFAULT_OHLCV.open) as string;
  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const closeName = (options.close ?? DEFAULT_OHLCV.close) as string;
  const prefix = (options.prefix ?? 'rvi') as Prefix;
  const signalName = `${prefix}Signal` as const;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, prefix);
  assertNoColumn(wide, signalName);

  const open = columnValues(wide, openName);
  const high = columnValues(wide, highName);
  const low = columnValues(wide, lowName);
  const close = columnValues(wide, closeName);

  // Body and range per bar. A missing cell is `NaN` ([PND-STUDYBOX]) and
  // propagates through both subtractions and then through the symmetric
  // filter, so a half-known bar contributes to neither leg.
  const length = close.length;
  const body = new Float64Array(length);
  const range = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    body[i] = close[i]! - open[i]!;
    range[i] = high[i]! - low[i]!;
  }

  // Σ num / Σ den is mean(num) / mean(den) — the shared 1/period cancels —
  // so the summation composes on the shipped rolling-mean kernel, whose
  // array door is the warm-up and gap rule documented above.
  const meanNumerator = rollingMeanValues(
    symmetricWeightedValues(body),
    period,
  );
  const meanDenominator = rollingMeanValues(
    symmetricWeightedValues(range),
    period,
  );

  const index = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    const d = meanDenominator[i]!;
    // A zero-range window has no vigour to report, and redirected columns
    // can put a non-zero numerator over it — ±Infinity would reach
    // `withColumn`, so this guard is load-bearing rather than decorative.
    index[i] = d === 0 ? NaN : meanNumerator[i]! / d;
  }

  return series
    .withColumn(prefix, index)
    .withColumn(signalName, symmetricWeightedValues(index));
}
