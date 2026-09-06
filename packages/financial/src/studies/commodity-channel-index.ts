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
import {
  rollingMeanAbsDevValues,
  rollingMeanValues,
} from '../kernels/rolling-mean.js';
import { typicalPriceValues } from '../kernels/typical-price.js';

export interface CommodityChannelIndexOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Look-back in **bars**. **Default `20`** (ChartIQ's; Lambert's original
   *  and TA-Lib's own default are both shorter — see the docstring). */
  period?: number;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Close column. **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'cci'`.** */
  output?: Output;
}

/**
 * **Commodity Channel Index** (Donald Lambert, 1980) — how far the typical
 * price sits from its own average, measured in **mean absolute deviations**:
 *
 * ```
 * tp  = (high + low + close) / 3
 * cci = (tp − SMA(tp, period)) / (0.015 · meanAbsDev(tp, period))
 * ```
 *
 * Appends one column; `undefined` for the first `period − 1` rows.
 *
 * It is a z-score with two substitutions, and both are deliberate on
 * Lambert's part: the spread is the **mean absolute** deviation rather than
 * the standard deviation (less sensitive to a single outlier bar), and the
 * whole thing is divided by `0.015` so that a "normal" excursion lands
 * inside ±100 — for a normal distribution the mean absolute deviation is
 * about `0.8σ`, so `0.015` puts roughly 70–80% of readings in that band.
 * The constant is part of the definition, not a knob.
 *
 * Reads **high, low and close**, each named by an option defaulting to its
 * `DEFAULT_OHLCV` column — the {@link atr} shape. For the plain
 * standard-deviation form over a single column, {@link zScore} is the
 * study; CCI is not it.
 *
 * ## Definition source
 *
 * **TA-Lib's `CCI`**, matched bar-for-bar — the oracle asserts identical
 * null masks and agreement to **3.6e-12** at `period 20` and **1.5e-11** at
 * `period 5` (TA-Lib accumulates its deviation sum incrementally; ours
 * re-walks the window, so the two differ only in float summation order).
 *
 * **The `period 20` default is ChartIQ's, and it is not universal**:
 * Lambert's original recommends a length near a third of the instrument's
 * cycle, TA-Lib defaults to 14, StockCharts publishes 20. Nothing here
 * depends on the choice — it is a bar count like every other period in the
 * package — but a comparison against another vendor's chart should check
 * which length that chart used before calling a difference a bug.
 *
 * ## Edges
 *
 * - **Unbounded**, unlike the other momentum oscillators here: ±100 is a
 *   conventional band, not a limit, and readings beyond ±300 happen. Do not
 *   scale a chart axis as if it were `0..100`.
 * - **A window with zero mean absolute deviation** (every typical price in
 *   it identical) → `undefined`. **This is a deliberate delta from TA-Lib**,
 *   which returns `0` there. `0` is also what CCI reports for a price
 *   sitting exactly on its average, so TA-Lib's answer conflates "no
 *   dispersion to measure against" with "no deviation from the average";
 *   the {@link rsi} flat-window precedent applies. No guard is written for
 *   it: the window ends on the bar being reported, so a zero deviation
 *   forces a zero numerator, and the case *is* `0/0` — an explicit branch
 *   would be code no test could distinguish from its absence.
 * - **Scale- and shift-invariant**: numerator and denominator are both
 *   homogeneous of degree one in price, and both are unchanged by adding a
 *   constant to every price. Pinned by property tests.
 * - **A leading gap shifts the start**; **an interior gap costs the `period`
 *   windows containing it**, after which CCI recovers — the window rule,
 *   with no recursion to carry the hole forward. A bar missing any one of
 *   its three prices has no typical price, so one missing `high` costs the
 *   same as a missing `close`.
 * - **Cost.** The mean absolute deviation is the package's one
 *   super-linear kernel — `O(N · period)`; see
 *   {@link rollingMeanAbsDevValues}, which documents both the measurement
 *   and the `O(N log period)` order-statistic form that would replace it if
 *   a very long period ever asked.
 */
export function commodityChannelIndex<
  S extends SeriesSchema,
  const Output extends string = 'cci',
>(
  series: TimeSeries<S>,
  options: CommodityChannelIndexOptions<S, Output> = {},
) {
  const period = options.period ?? 20;
  assertPeriod(period);
  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const closeName = (options.close ?? DEFAULT_OHLCV.close) as string;
  const output = (options.output ?? 'cci') as Output;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const typical = typicalPriceValues(
    columnValues(wide, highName),
    columnValues(wide, lowName),
    columnValues(wide, closeName),
  );
  const mean = rollingMeanValues(typical, period);
  const deviation = rollingMeanAbsDevValues(typical, period);

  const length = typical.length;
  const out = new Float64Array(length);
  // A window with no dispersion has nothing to measure the excursion
  // against, and needs no guard to say so: the window ends on this bar, so a
  // zero deviation means every value in it — including this one — equals the
  // mean, and the reading is `0/0` rather than `x/0`. It arrives as `NaN` and
  // reads back as `undefined` (see *Edges*). An explicit `d === 0` branch was
  // written first and mutation-testing could not tell it was there, which is
  // the `rollingWeightedMeanValues` lesson repeating.
  for (let i = 0; i < length; i += 1) {
    out[i] = (typical[i]! - mean[i]!) / (0.015 * deviation[i]!);
  }
  return series.withColumn(output, out);
}
