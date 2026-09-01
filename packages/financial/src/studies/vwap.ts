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
import { typicalPriceValues } from '../kernels/typical-price.js';
import { rollingWeightedMeanValues } from '../kernels/weighted-mean.js';

export interface VwapOptions<S extends SeriesSchema, Output extends string> {
  /** Window in **bars**. Required — VWAP has no conventional length. */
  period: number;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Close column. **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Volume column. **Default `'volume'`.** */
  volume?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'vwap'`.** */
  output?: Output;
}

/**
 * **Volume-Weighted Average Price** over a trailing window of `period` bars:
 *
 * ```
 * VWAP[i] = Σ typicalPrice · volume / Σ volume      over bars i−period+1 .. i
 * typicalPrice = (high + low + close) / 3
 * ```
 *
 * The average price actually *traded* over the window, as opposed to the
 * average of the bars' prices: a bar on ten times the volume moves it ten
 * times as far. Appends one column; `undefined` for the first `period − 1`
 * rows, like every other count-window study.
 *
 * Composed from two kernels: {@link typicalPriceValues} (shared with CCI,
 * MFI and Keltner when they land) and {@link rollingWeightedMeanValues}
 * (shared with a volume-weighted moving average, which is the same kernel
 * over close).
 *
 * ## Which VWAP this is — a design decision, not a lookup
 *
 * There is no single VWAP and no TA-Lib reference to defer to. Two
 * definitions are in real use:
 *
 * 1. **Rolling** — the ratio of sums over the last `period` bars. This is
 *    what ships. It has the shape every other study in this package has
 *    (bar-count window, length-preserving warm-up, no state), it is what a
 *    charting package means by "VWAP with a period", and it composes with
 *    the rest of the vocabulary without a new mechanism.
 * 2. **Anchored** — the same ratio accumulated from a fixed bar (the session
 *    open, most often) with no window. That is the VWAP of the intraday
 *    execution desk, and it is *not* a special case of (1): a count window
 *    is emitted only once it spans `period` rows, so `period = length`
 *    yields one value at the last bar, not the running line. An anchored
 *    VWAP is a ratio of two running sums (`cumulativeValues`, already a
 *    kernel) and is only useful with a *reset* — which makes it a
 *    session-anchored study, and it is deferred to that phase rather than
 *    shipped here without one.
 *
 * `period` is **required** for the same reason: a rolling VWAP has no
 * conventional length the way RSI has 14, and a silent default would be an
 * invented one.
 *
 * ## Inputs
 *
 * Four, each named by an option defaulting to its `DEFAULT_OHLCV` name — the
 * rule {@link atr} established, applied once more. The price weighted is the
 * **typical price**, the usual convention. There is deliberately no `price`
 * option to choose another: it would be a knob with one conventional value.
 * A close-weighted VWAP is available without it — point `high` and `low` at
 * the close column too and the typical price *is* the close — and if that
 * shape earns a name it should be a `vwma` study on the same kernel, not a
 * mode here.
 *
 * ## Definition, verified
 *
 * Checked bar-for-bar against a pandas replication
 * (`(tp·v).rolling(n).sum() / v.rolling(n).sum()`) in the oracle fixture, on
 * a volume series with several bars at five to nine times the typical
 * volume — so that a VWAP which dropped the weighting is distinguishable
 * from the mean of typical prices (the generator asserts the two differ by
 * at least a quarter of a price unit somewhere in the fixture). Agreement
 * to `1e-9`.
 *
 * ## Edges
 *
 * - **A window with no volume at all** (`Σ volume = 0`) → `undefined`.
 *   There is nothing to weight by, so there is no answer; not `0`, and not
 *   the plain mean. (It is `0 / 0` in the kernel, which needs no guard — see
 *   {@link rollingWeightedMeanValues} for why one would be dead code.)
 * - **A gap in any input drops that bar from both sums**, numerator and
 *   denominator alike, so the window's answer is the VWAP of the bars it
 *   does have — rather than a number quietly biased toward zero by a volume
 *   whose price went missing. As with `sma`, a window is emitted once it
 *   spans `period` rows, computed from whichever of them are present.
 * - **Scales with price, not with volume:** doubling every price doubles it;
 *   doubling every volume leaves it unchanged. Both are pinned as property
 *   tests.
 */
export function vwap<
  S extends SeriesSchema,
  const Output extends string = 'vwap',
>(series: TimeSeries<S>, options: VwapOptions<S, Output>) {
  const period = options.period;
  assertPeriod(period);
  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const closeName = (options.close ?? DEFAULT_OHLCV.close) as string;
  const volumeName = (options.volume ?? DEFAULT_OHLCV.volume) as string;
  const output = (options.output ?? 'vwap') as Output;
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const typical = typicalPriceValues(
    columnValues(wide, highName),
    columnValues(wide, lowName),
    columnValues(wide, closeName),
  );
  return series.withColumn(
    output,
    rollingWeightedMeanValues(typical, columnValues(wide, volumeName), period),
  );
}
