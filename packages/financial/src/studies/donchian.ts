import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import { highestLowestValues } from '../kernels/highest-lowest.js';
import { assertNoColumn, assertPeriod } from '../kernels/rolling.js';

export interface DonchianOptions<
  S extends SeriesSchema,
  Prefix extends string,
> {
  /** Look-back in **bars**. **Default `20`** (the Turtle breakout channel). */
  period?: number;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Column-family prefix — appends `${prefix}Upper` / `${prefix}Lower` /
   *  `${prefix}Middle`. **Default `'dc'`.** */
  prefix?: Prefix;
}

/**
 * **Donchian channel** (Richard Donchian) — the highest high and lowest low
 * of the trailing `period` bars, and their midpoint:
 *
 * ```
 * upper  = max(high, period)      ${prefix}Upper
 * lower  = min(low, period)       ${prefix}Lower
 * middle = (upper + lower) / 2    ${prefix}Middle
 * ```
 *
 * Appends three columns; each is `undefined` for the first `period − 1`
 * rows. Reads **high and low** (no close), each named by an option defaulting
 * to its `DEFAULT_OHLCV` column.
 *
 * ## Definition
 *
 * There is only one: `upper` is exactly what {@link rollingMax} gives over
 * `high` and `lower` what {@link rollingMin} gives over `low` — this study
 * is those two edges in one `rolling` scan (via {@link highestLowestValues},
 * the kernel it shares with {@link stochastic} and {@link williamsR}), plus
 * the midpoint. Verified against pandas `rolling(n).max()` / `.min()` in the
 * oracle fixture; TA-Lib has no Donchian to compare with.
 *
 * ## Edges
 *
 * - **In the units of the price**, like {@link atr}: scaling every price
 *   scales the channel with it. It does not normalise.
 * - **A missing `high` or `low` is skipped**, not propagated: the edge is the
 *   extreme of the cells the window does hold, and only a window with none
 *   reads `undefined`. This is core's reducer policy and the same contract
 *   `rollingMax` / `rollingMin` already ship — including over another
 *   study's output, where the channel starts on that study's first bar with
 *   whatever the window holds by then, rather than `period − 1` bars later.
 * - **`upper` and `lower` bound `close` by construction** only when
 *   `low ≤ close ≤ high` holds on the input bars; the study does not check
 *   that, and a close outside its own bar's range will sit outside the
 *   channel too.
 */
export function donchian<
  S extends SeriesSchema,
  const Prefix extends string = 'dc',
>(series: TimeSeries<S>, options: DonchianOptions<S, Prefix> = {}) {
  const period = options.period ?? 20;
  assertPeriod(period);
  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const prefix = (options.prefix ?? 'dc') as Prefix;
  const upperName = `${prefix}Upper` as const;
  const lowerName = `${prefix}Lower` as const;
  const middleName = `${prefix}Middle` as const;
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, upperName);
  assertNoColumn(wide, lowerName);
  assertNoColumn(wide, middleName);

  const { highest, lowest } = highestLowestValues(
    wide,
    highName,
    lowName,
    period,
  );
  // NaN in either edge (warm-up, empty window) propagates on its own.
  const middle = new Float64Array(highest.length);
  for (let i = 0; i < middle.length; i += 1) {
    middle[i] = (highest[i]! + lowest[i]!) / 2;
  }

  return series
    .withColumn(upperName, highest)
    .withColumn(lowerName, lowest)
    .withColumn(middleName, middle);
}
