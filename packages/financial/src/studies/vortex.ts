import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import { vortexMovementValues } from '../kernels/directional-movement.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';
import { rollingMeanValues } from '../kernels/rolling-mean.js';
import { trueRangeValues } from '../kernels/true-range.js';

export interface VortexOptions<S extends SeriesSchema, Prefix extends string> {
  /** Look-back for the three sums, in **bars**. **Default `14`.** */
  period?: number;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Close column. **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Column-family prefix — appends `${prefix}Plus` / `${prefix}Minus`.
   *  **Default `'vi'`.** */
  prefix?: Prefix;
}

/**
 * **The Vortex Indicator** (Etienne Botes & Douglas Siepman, *Technical
 * Analysis of Stocks & Commodities*, January 2010) — the trend-direction
 * companion to Wilder's `DI` pair, measured **across** consecutive bars
 * instead of along them:
 *
 * ```
 * +VM = |high − prevLow|      −VM = |low − prevHigh|
 * +VI = Σ +VM / Σ TR          ${prefix}Plus
 * −VI = Σ −VM / Σ TR          ${prefix}Minus       (sums over `period` bars)
 * ```
 *
 * Where {@link directionalMovement} asks how far a bar moved *beyond* the
 * previous bar's own high or low — so at most one of its legs is non-zero —
 * the vortex crosses them, and **both legs are always positive**. What
 * carries the signal is their *ratio*: a rising market puts today's high a
 * long way above yesterday's low (`+VM` large) while today's low stays close
 * under yesterday's high (`−VM` small), so `+VI` climbs above `−VI` and the
 * crossings mark the turns. Normalising both by the same true-range total is
 * what makes the pair comparable across instruments.
 *
 * Reads **high, low and close**, each named by an option defaulting to its
 * `DEFAULT_OHLCV` column — the {@link atr} shape. The close is there for the
 * true range only.
 *
 * ## Definition
 *
 * The authors' original, replicated in **pandas** in the oracle — TA-Lib has
 * no vortex function, so there is nothing to be bar-for-bar with. The two
 * choices worth naming, both the published ones: the **absolute values** on
 * the movement legs (they matter only on a bar that gaps clear of its
 * predecessor, where the crossing distance would come out negative), and
 * `TR` as Wilder's true range rather than the plain bar range — the same
 * {@link trueRangeValues} the ATR family uses, which the oracle asserts is
 * `talib.TRANGE` bit-for-bit.
 *
 * Both columns emit from bar `period`: `+VM`, `−VM` and `TR` all read the
 * previous bar, so a `period`-bar sum of them needs `period + 1` bars.
 *
 * The ratio is taken between two **rolling means** rather than two sums —
 * they share the divisor `period`, so it cancels exactly, and going through
 * {@link rollingMeanValues} is what buys the strict window rule (every one
 * of the `period` cells must be finite) instead of a hand-rolled sum that
 * would have to restate it.
 *
 * ## Edges
 *
 * - **`Σ TR = 0` → `undefined`.** Unlike {@link directionalMovement}'s `DX`,
 *   the numerator is *not* forced to zero with it: a window of perfectly
 *   flat bars can still be preceded by a bar at a different level, leaving
 *   `+VM` positive over a zero total range. That is a genuine `x/0`, so the
 *   guard is real (and, unlike the guard `commodityChannelIndex` deleted, it
 *   changes an answer — the alternative is `±Infinity` reaching
 *   `withColumn`).
 * - **Both columns are non-negative** — every term is an absolute value over
 *   a non-negative range — and **scale-invariant**: a ratio of price
 *   differences. Both pinned by property tests.
 * - **Not bounded by 1.** `+VI` and `−VI` typically live around 0.8–1.2 and
 *   sum to roughly 2, but neither is a fraction of anything: a bar can gap
 *   far enough that `|high − prevLow|` exceeds its own true range. Nothing
 *   is clamped.
 * - **A gap costs at most `period + 1` bars and then recovers**, unlike the
 *   Wilder family's carry-to-the-end: these are windows, not recursions. The
 *   two legs lose *different* rows, which is worth knowing when reading them
 *   as a pair. Measured on an 80-bar series with one missing cell at bar 40
 *   and `period 14`: a missing **high** blanks `+VI` over bars 40–53 (it
 *   costs `+VM` on bar 40 and the true range with it) and `−VI` over 40–54
 *   (`−VM` on bar **41** reads that high as `prevHigh`); a missing **low**
 *   is the mirror image; a missing **close** costs only the true range on
 *   bar 41, so both legs blank over 41–54.
 */
export function vortex<
  S extends SeriesSchema,
  const Prefix extends string = 'vi',
>(series: TimeSeries<S>, options: VortexOptions<S, Prefix> = {}) {
  const period = options.period ?? 14;
  assertPeriod(period);
  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const closeName = (options.close ?? DEFAULT_OHLCV.close) as string;
  const prefix = (options.prefix ?? 'vi') as Prefix;
  const plusName = `${prefix}Plus` as const;
  const minusName = `${prefix}Minus` as const;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, plusName);
  assertNoColumn(wide, minusName);

  const high = columnValues(wide, highName);
  const low = columnValues(wide, lowName);
  const close = columnValues(wide, closeName);
  const { plus: vmPlus, minus: vmMinus } = vortexMovementValues(high, low);

  const meanPlus = rollingMeanValues(vmPlus, period);
  const meanMinus = rollingMeanValues(vmMinus, period);
  const meanRange = rollingMeanValues(
    trueRangeValues(high, low, close),
    period,
  );

  const length = high.length;
  const viPlus = new Float64Array(length);
  const viMinus = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    const range = meanRange[i]!;
    // A zero total range is a real division by zero here (see above), not a
    // forced 0/0. NaN is not `=== 0`, so a gap falls through and propagates.
    if (range === 0) {
      viPlus[i] = NaN;
      viMinus[i] = NaN;
      continue;
    }
    viPlus[i] = meanPlus[i]! / range;
    viMinus[i] = meanMinus[i]! / range;
  }

  return series.withColumn(plusName, viPlus).withColumn(minusName, viMinus);
}
