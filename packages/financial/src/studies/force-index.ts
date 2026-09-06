import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import { movingAverageValues } from '../kernels/moving-average.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';

export interface ForceIndexOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** EMA span in **bars**. **Default `13`** (Elder's). `1` is the raw,
   *  unsmoothed force. */
  period?: number;
  /** Close column. **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Volume column. **Default `'volume'`.** */
  volume?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'force'`.** */
  output?: Output;
}

/**
 * **Elder's Force Index** — how hard a move was pushed, as the bar's price
 * change times the volume behind it, smoothed with an EMA:
 *
 * ```
 * rawForce[i] = (close[i] − close[i−1]) · volume[i]
 * force       = EMA(rawForce, period)                       Elder's 13
 * ```
 *
 * Elder's three elements in one number: **direction** (the sign of the
 * change), **extent** (its size) and **volume**. Read against its zero line
 * and for divergence; the 2-bar version is the short-term trading signal and
 * the 13-bar the trend one, which is why `period` is a knob and not a
 * constant.
 *
 * **`period: 1` is the raw force** — an EMA of span 1 has `α = 1`, so it is
 * its own input, bar for bar. That is the unsmoothed series Elder defines
 * first, available without a second study or a `smooth: false` flag.
 *
 * Appends one column. The EMA is the K2 engine's array door
 * ({@link movingAverageValues} with `'ema'`) — pond's first-sample seed, the
 * same average {@link ema} and {@link elderRay} use — so the study owns only
 * the per-bar derivation.
 *
 * ## Why no `maType`
 *
 * Elder names the EMA, and the {@link elderRay} precedent applies: an option
 * whose other nine settings nobody publishes is a speculative knob. A caller
 * who wants a different smoothing has `forceIndex({ period: 1, output:
 * 'raw' })` followed by `movingAverage({ column: 'raw', … })`, which is the
 * composition the package is built for.
 *
 * ## Definition, verified
 *
 * TA-Lib has no Force Index, so the oracle is a **pandas replication** on
 * pond's EMA seed at `period` 13 and 2, with the analytic first valid bar
 * (`period`, not `period − 1` — see below) asserted, and separated from a
 * version that drops the volume factor so the weighting is pinned.
 *
 * ## Edges
 *
 * - **Warm-up is `period` rows.** The raw force needs a previous close, so
 *   bar 0 has none, and the EMA's array door waits for `period` **finite
 *   values** — one more row than a study whose input is defined from bar 0.
 *   Length-preserving.
 * - **A leading gap shifts the seed** rather than emptying the column
 *   (the array door steps over a non-finite head), so the study runs over
 *   another study's output.
 * - **An interior gap costs two bars, then the EMA carries on** — the bar
 *   itself and the next (whose change reads the missing close). The EMA
 *   recursion *skips* a missing input rather than propagating it, which is
 *   the `ema`-family rule; it is deliberately **not** the running-sum rule
 *   {@link obv} and {@link priceVolumeTrend} follow, because an average has
 *   a local answer where a level does not.
 * - **A missing volume costs one bar** — its own.
 * - **Linear in volume and linear in price** (an absolute quantity in
 *   price × share units, the {@link atr} side of the scale pair, not the
 *   {@link rsi} side), and **shift-invariant in price** — adding a constant
 *   to every close leaves the differences alone. All pinned by property
 *   tests.
 */
export function forceIndex<
  S extends SeriesSchema,
  const Output extends string = 'force',
>(series: TimeSeries<S>, options: ForceIndexOptions<S, Output> = {}) {
  const period = options.period ?? 13;
  assertPeriod(period);
  const closeName = (options.close ?? DEFAULT_OHLCV.close) as string;
  const volumeName = (options.volume ?? DEFAULT_OHLCV.volume) as string;
  const output = (options.output ?? 'force') as Output;
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const close = columnValues(wide, closeName);
  const volume = columnValues(wide, volumeName);
  const raw = new Float64Array(close.length).fill(NaN);
  // Bar 0 has no previous close. Missing cells are NaN and propagate through
  // the arithmetic on their own ([PND-STUDYBOX]).
  for (let i = 1; i < raw.length; i += 1) {
    raw[i] = (close[i]! - close[i - 1]!) * volume[i]!;
  }
  return series.withColumn(output, movingAverageValues(raw, period, 'ema'));
}
