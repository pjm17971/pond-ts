import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import {
  highestLowestValues,
  percentOfRangeValues,
} from '../kernels/highest-lowest.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';

export interface WilliamsROptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Look-back for the highest high / lowest low, in **bars**. **Default
   *  `14`.** */
  period?: number;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Close column. **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'williamsR'`.** */
  output?: Output;
}

/**
 * **Williams %R** (Larry Williams) — how far the close sits *below the
 * highest high* of the trailing `period` bars, as a percentage of the range,
 * bounded `−100..0`:
 *
 * ```
 * %R = −100 · (HH − close) / (HH − LL)
 * ```
 *
 * Appends one column; `undefined` for the first `period − 1` rows.
 *
 * ## Definition
 *
 * **TA-Lib's `WILLR`.** Verified bar-for-bar in the oracle fixture with an
 * identical warm-up, agreeing to `1.4e-14`.
 *
 * `%R` is the fast stochastic `%K` shifted down by 100 — `100·(c−LL)/(HH−LL)
 * − 100` rearranges to the formula above — so it is computed on the same
 * kernel as {@link stochastic} rather than owning a second division, and a
 * test pins the identity `williamsR ≡ stochastic({ slowing: 1 }).K − 100`.
 * What differs is convention, not maths: `%R` is quoted negative and
 * unsmoothed, and its "overbought" reads above `−20` where `%K`'s reads
 * above `80`.
 *
 * ## Edges
 *
 * - **A flat window (`HH === LL`) is `undefined`** — the one deliberate delta
 *   from TA-Lib, which reports `0`. `0` is also `%R`'s value for "close at
 *   the very top of a real range", so TA-Lib's answer cannot distinguish the
 *   two; and the same TA-Lib convention gives `%K` `0` — the very *bottom* —
 *   for the same bar. The kernel ({@link percentOfRangeValues}) reports no
 *   value instead, for both studies.
 * - **Bounded `−100..0` whenever `low ≤ close ≤ high`**; redirect `close` at
 *   a column the range does not bound and it reads outside, unclamped.
 * - **Scale-invariant**, like RSI; a leading gap shifts the start; an
 *   interior gap in `close` costs that bar only, and one in `high`/`low` is
 *   skipped by the range (core's reducer policy).
 */
export function williamsR<
  S extends SeriesSchema,
  const Output extends string = 'williamsR',
>(series: TimeSeries<S>, options: WilliamsROptions<S, Output> = {}) {
  const period = options.period ?? 14;
  assertPeriod(period);
  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const closeName = (options.close ?? DEFAULT_OHLCV.close) as string;
  const output = (options.output ?? 'williamsR') as Output;
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const { highest, lowest } = highestLowestValues(
    wide,
    highName,
    lowName,
    period,
  );
  const percent = percentOfRangeValues(
    highest,
    lowest,
    columnValues(wide, closeName),
  );
  // %K − 100: NaN (warm-up, flat window, missing cell) propagates on its own.
  const out = new Float64Array(percent.length);
  for (let i = 0; i < percent.length; i += 1) out[i] = percent[i]! - 100;
  return series.withColumn(output, out);
}
