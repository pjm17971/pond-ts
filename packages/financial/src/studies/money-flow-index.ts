import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import { rollingMeanValues } from '../kernels/rolling-mean.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';
import { typicalPriceValues } from '../kernels/typical-price.js';

export interface MoneyFlowIndexOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Window in **bars**. **Default `14`.** */
  period?: number;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Close column. **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Volume column. **Default `'volume'`.** */
  volume?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'mfi'`.** */
  output?: Output;
}

/**
 * **Money Flow Index** — {@link rsi} computed on *money flow* instead of
 * price:
 *
 * ```
 * typicalPrice = (high + low + close) / 3
 * rawMoneyFlow = typicalPrice · volume
 * positive     = Σ rawMoneyFlow where typicalPrice rose        over `period`
 * negative     = Σ rawMoneyFlow where typicalPrice fell
 * MFI          = 100 · positive / (positive + negative)
 * ```
 *
 * A bounded `0…100` oscillator, read for the same overbought / oversold /
 * divergence signals as RSI, but with each bar's move weighted by the money
 * that changed hands on it — which is why it is sometimes called
 * "volume-weighted RSI". Appends one column.
 *
 * ## Two differences from RSI, both real
 *
 * - **The window is a plain sum, not Wilder's smoothing.** RSI averages its
 *   gains and losses with `(prev·(n−1) + x)/n`, which never forgets; MFI
 *   sums the last `period` flows and drops the rest. So MFI's answer depends
 *   only on the window, and a gap leaves it once the window passes — the
 *   asymmetry `atr`/`rsi` document from the other side.
 * - **Direction comes from the typical price**, not the close. A bar whose
 *   close rose but whose range fell can be a down-flow bar.
 *
 * The sums are two {@link rollingMeanValues} passes over the up-flow and
 * down-flow arrays. Means rather than sums because the kernel exists and the
 * shared `1/period` **cancels in the ratio** — the same trick
 * {@link rollingWeightedMeanValues} uses — so nothing is lost and no second
 * window loop is written.
 *
 * ## Definition, verified
 *
 * **TA-Lib's `MFI`, exactly** on gap-free bars: cross-checked bar-for-bar in
 * the oracle fixture at `period` 14 and 5 (agreement to `2.8e-14`, identical
 * warm-up masks). An **unchanged typical price contributes to neither** sum,
 * which is TA-Lib's rule too (its C code adds to `posSumMF` on `>` and to
 * `negSumMF` on `<`, and to neither on equality).
 *
 * ## Edges
 *
 * - **Warm-up is `period` rows, not `period − 1`** — the first bar has no
 *   previous typical price, so the first window of `period` *changes* closes
 *   on bar `period`. Same off-by-one {@link rsi} and {@link atr} have, and
 *   TA-Lib's own lookback.
 * - **A window with no money flow at all** → `undefined`. Reachable two
 *   ways: every typical price in the window unchanged (nothing rose or
 *   fell), or no volume at all. `0 / 0` has no relative strength, which is
 *   exactly the call {@link rsi} makes on a flat window. **TA-Lib reports
 *   `0`** for this case — measured (0.7.1): a 20-bar flat series at
 *   `period 5` gives `0`, and so does a rising series with zero volume.
 *   `0` is MFI's *most bearish possible reading*, for a window that showed
 *   no direction at all.
 * - **TA-Lib also reports `0` whenever the window's total flow is below
 *   `1.0`**, which is a magnitude guard rather than a definition: measured
 *   on a strictly *rising* 20-bar series with volume `1e-9` per bar, TA-Lib
 *   returns `0` where the answer is `100`. This study has no such threshold
 *   — a market denominated in small units is still a market — so the two
 *   disagree there by the whole range of the indicator. (Real money flow is
 *   a price times a share count; the case does not arise on equity data,
 *   which is presumably why it has survived in TA-Lib.)
 * - **All up** → `100`, **all down** → `0`, both agreeing with TA-Lib.
 * - **A gap in any input costs two windows' worth**: the bar itself and the
 *   bar after it (whose direction reads the missing typical price), and
 *   every window containing either. It **recovers** once they leave.
 * - **Invariant under scaling price and under scaling volume** — a ratio of
 *   flows, so both cancel. Pinned by property tests, with the `0…100` bound.
 */
export function moneyFlowIndex<
  S extends SeriesSchema,
  const Output extends string = 'mfi',
>(series: TimeSeries<S>, options: MoneyFlowIndexOptions<S, Output> = {}) {
  const period = options.period ?? 14;
  assertPeriod(period);
  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const closeName = (options.close ?? DEFAULT_OHLCV.close) as string;
  const volumeName = (options.volume ?? DEFAULT_OHLCV.volume) as string;
  const output = (options.output ?? 'mfi') as Output;
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const typical = typicalPriceValues(
    columnValues(wide, highName),
    columnValues(wide, lowName),
    columnValues(wide, closeName),
  );
  const volume = columnValues(wide, volumeName);
  const length = typical.length;

  // Split each bar's raw money flow by the direction of the typical price.
  // Bar 0 has no predecessor, so both legs are NaN there and the first
  // window that can close is the one ending on bar `period` — the same
  // off-by-one `rsi` gets from `wilderValues(…, start = 1)`. A NaN input
  // makes `d` or `flow` NaN, and the `Number.isNaN` arm keeps the two legs'
  // gaps in the same places (the ratio below reads both).
  const positive = new Float64Array(length).fill(NaN);
  const negative = new Float64Array(length).fill(NaN);
  for (let i = 1; i < length; i += 1) {
    const d = typical[i]! - typical[i - 1]!;
    const flow = typical[i]! * volume[i]!;
    if (Number.isNaN(d + flow)) continue;
    positive[i] = d > 0 ? flow : 0;
    negative[i] = d < 0 ? flow : 0;
  }

  const up = rollingMeanValues(positive, period);
  const down = rollingMeanValues(negative, period);
  const out = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    const total = up[i]! + down[i]!;
    // `total === 0` is a window with no flow in either direction — no
    // relative strength, so no value (the `rsi` flat-window rule). NaN
    // propagates through the division on its own.
    out[i] = total === 0 ? NaN : (100 * up[i]!) / total;
  }
  return series.withColumn(output, out);
}
