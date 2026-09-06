import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV, DEFAULT_SOURCE } from '../contract/columns.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';
import { atrValues } from '../kernels/true-range.js';

export interface AtrBandsOptions<
  S extends SeriesSchema,
  Prefix extends string,
> {
  /** ATR look-back in **bars**. **Default `14`** (Wilder's own). */
  period?: number;
  /** Band half-width in ATRs. **Default `2`.** */
  multiplier?: number;
  /** The field the bands are drawn around. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** High column (ATR input). **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column (ATR input). **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Close column (ATR input). **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Column-family prefix — appends `${prefix}Upper` / `${prefix}Lower`.
   *  **Default `'atrb'`.** */
  prefix?: Prefix;
}

/**
 * **ATR Bands** — a field with a volatility band either side of it:
 *
 * ```
 * upper = column + multiplier × ATR(period)    ${prefix}Upper
 * lower = column − multiplier × ATR(period)    ${prefix}Lower
 * ```
 *
 * Appends **two** columns, not three. **There is no `${prefix}Middle`: the
 * middle is the field itself**, already on the series as `column` — a third
 * column would be a copy of one the caller passed in, which is a column to
 * keep in step rather than a measurement. (Contrast {@link keltner} and
 * {@link bollinger}, whose centres are *computed* — a moving average the
 * caller does not otherwise have — and are therefore worth appending.)
 *
 * ## Definition
 *
 * The half-width is **Wilder's ATR**, the same `atrValues` kernel call
 * {@link atr} makes, at the same default `period 14`. That is the whole
 * study: `atrBands` is `atr()` plus two additions, and
 * `${prefix}Upper − column === multiplier × atr` holds **bit-for-bit**, not
 * to rounding — pinned by a test against the shipped `atr` study rather than
 * asserted here. TA-Lib has no ATR-band function; the oracle is a pandas
 * replication on the ATR reference it already cross-checks against TA-Lib,
 * so the numbers are TA-Lib's ATR with arithmetic on top.
 *
 * ## Four inputs, and why `column` is separate from `close`
 *
 * `column` is what the bands are drawn *around*; `high` / `low` / `close`
 * are what the ATR is computed *from*. They default to the same `'close'`,
 * and separating them is what lets the bands sit on something the ATR is
 * not measured from — bands around an `sma`, say
 * (`{ column: 'sma' }`) — which is the usual reason to reach for this study
 * rather than {@link keltner}. Redirect all four together to run the study
 * on a second instrument's columns.
 *
 * ## Warm-up and edges
 *
 * - **Both bands start at bar `period`**, the ATR's own first bar (true
 *   range needs a previous close, so a `period`-bar average of it lands one
 *   bar later than a `period`-bar window would — the `atr` off-by-one). If
 *   `column` warms up later than that (bands around another study's output),
 *   the bands start where **it** does; `NaN` propagates through the addition
 *   with no branch.
 * - **In the units of the price**: scaling every bar scales both bands.
 * - **A leading gap shifts the Wilder seed; an interior gap propagates to
 *   the end** — inherent to Wilder smoothing, and the same asymmetry `atr`
 *   documents. A gap in `column` alone costs only that bar.
 * - **No division**, so no zero-denominator case: a flat stretch gives
 *   `ATR = 0` and both bands sit on the field.
 */
export function atrBands<
  S extends SeriesSchema,
  const Prefix extends string = 'atrb',
>(series: TimeSeries<S>, options: AtrBandsOptions<S, Prefix> = {}) {
  const period = options.period ?? 14;
  assertPeriod(period);
  const multiplier = options.multiplier ?? 2;
  if (!Number.isFinite(multiplier) || multiplier <= 0) {
    throw new TypeError('atrBands multiplier must be a positive finite number');
  }

  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const closeName = (options.close ?? DEFAULT_OHLCV.close) as string;
  const prefix = (options.prefix ?? 'atrb') as Prefix;
  const upperName = `${prefix}Upper` as const;
  const lowerName = `${prefix}Lower` as const;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, upperName);
  assertNoColumn(wide, lowerName);

  const field = columnValues(wide, column);
  const atr = atrValues(
    columnValues(wide, highName),
    columnValues(wide, lowName),
    columnValues(wide, closeName),
    period,
  );

  // A missing field cell or a warming-up ATR is `NaN` ([PND-STUDYBOX]) and
  // propagates on its own, so the warm-up is `max(field, atr)` with no
  // branch here.
  const band = (sign: 1 | -1): Float64Array => {
    const out = new Float64Array(field.length);
    for (let i = 0; i < out.length; i += 1) {
      out[i] = field[i]! + sign * multiplier * atr[i]!;
    }
    return out;
  };

  return series.withColumn(upperName, band(1)).withColumn(lowerName, band(-1));
}
