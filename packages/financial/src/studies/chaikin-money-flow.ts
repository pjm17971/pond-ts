import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import { clvValues } from '../kernels/close-location.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';
import { rollingWeightedMeanValues } from '../kernels/weighted-mean.js';

export interface ChaikinMoneyFlowOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Window in **bars**. **Default `20`.** */
  period?: number;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Close column. **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Volume column. **Default `'volume'`.** */
  volume?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'cmf'`.** */
  output?: Output;
}

/**
 * **Chaikin Money Flow** — the Accumulation/Distribution term averaged over a
 * window instead of accumulated forever:
 *
 * ```
 * CMF[i] = Σ CLV · volume / Σ volume      over bars i−period+1 .. i
 * CLV    = ((close − low) − (high − close)) / (high − low)
 * ```
 *
 * {@link accumulationDistribution} is a running level; this is the same
 * per-bar pressure expressed as a **bounded, comparable** reading — a
 * volume-weighted mean of {@link clvValues}, so it lies in `[−1, +1]` on any
 * instrument at any price whose closes sit inside their bars (a `close`
 * redirected at a smoothed column can read outside, honestly — the kernel
 * does not clamp). Above zero is net accumulation over the window,
 * below zero net distribution; ±0.25 or so is the conventional strong
 * reading. Appends one column, `undefined` for the first `period − 1` rows.
 *
 * ## It is a weighted mean, and says so
 *
 * The loop is {@link rollingWeightedMeanValues} — the kernel {@link vwap}
 * runs on, with the **close location** for values and volume for weights
 * instead of typical price and volume. That is not a coincidence to be
 * exploited: it is what CMF *is*, `Σ x·w / Σ w`, and reusing the kernel is
 * what makes its edge rules the same rules VWAP already documents (below)
 * rather than a second set to learn.
 *
 * ## Definition, verified
 *
 * TA-Lib has no CMF, so the oracle is a **pandas replication** at
 * `period` 20 and 5, with the analytic first valid bar (`period − 1`)
 * asserted and the result separated from the **unweighted** mean of CLV over
 * the same window — so a study that dropped the volume weighting could not
 * pass. Chaikin's own default is 20 bars (21 in some publications; 20 is
 * ChartIQ's and StockCharts').
 *
 * ## Edges
 *
 * - **Zero volume over the whole window** → `undefined`. There is nothing to
 *   weight by, so there is no answer — not `0`, and not the plain mean of
 *   CLV. (`0 / 0` in the kernel, which needs no guard; see
 *   {@link rollingWeightedMeanValues}.)
 * - **A flat bar (`high === low`) contributes `0` to the numerator and its
 *   volume to the denominator**, pulling the reading toward zero by however
 *   much traded in a bar that reported no direction. That is the
 *   conventional CMF (ChartIQ and StockCharts agree; TA-Lib has none) and it
 *   follows from {@link clvValues}' flat-bar value being exactly `0`, the
 *   same rule {@link accumulationDistribution} runs on.
 * - **A gap in any input** behaves the same way and recovers once it leaves
 *   the window. As with every count-window study the window is emitted once
 *   it spans `period` **rows**, computed from whichever are present.
 * - **Invariant under scaling volume and under any affine change of price**
 *   — it is a weighted mean of a ratio, bounded either way. Both pinned by
 *   property tests, and the bound `|CMF| ≤ 1` with them.
 */
export function chaikinMoneyFlow<
  S extends SeriesSchema,
  const Output extends string = 'cmf',
>(series: TimeSeries<S>, options: ChaikinMoneyFlowOptions<S, Output> = {}) {
  const period = options.period ?? 20;
  assertPeriod(period);
  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const closeName = (options.close ?? DEFAULT_OHLCV.close) as string;
  const volumeName = (options.volume ?? DEFAULT_OHLCV.volume) as string;
  const output = (options.output ?? 'cmf') as Output;
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const clv = clvValues(
    columnValues(wide, highName),
    columnValues(wide, lowName),
    columnValues(wide, closeName),
  );
  return series.withColumn(
    output,
    rollingWeightedMeanValues(clv, columnValues(wide, volumeName), period),
  );
}
