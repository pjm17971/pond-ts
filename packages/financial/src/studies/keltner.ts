import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import {
  assertMaType,
  movingAverageValues,
} from '../kernels/moving-average.js';
import type { MaType } from '../kernels/moving-average.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';
import { atrValues } from '../kernels/true-range.js';
import { typicalPriceValues } from '../kernels/typical-price.js';

export interface KeltnerOptions<S extends SeriesSchema, Prefix extends string> {
  /** Centre-line look-back in **bars**. **Default `20`.** */
  period?: number;
  /** ATR look-back in **bars**. **Default `10`.** */
  atrPeriod?: number;
  /** Band half-width in ATRs. **Default `2`.** */
  multiplier?: number;
  /** Centre-line moving average — any of the shared {@link MaType} menu.
   *  **Default `'ema'`** (the modern variant; see the definition note). */
  maType?: MaType;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Close column. **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Column-family prefix — appends `${prefix}Middle` / `${prefix}Upper` /
   *  `${prefix}Lower`. **Default `'kc'`.** */
  prefix?: Prefix;
}

/**
 * **Keltner Channel** — a moving average of **typical price** with bands a
 * multiple of the **average true range** away:
 *
 * ```
 * middle = MA((high + low + close) / 3, period)     ${prefix}Middle
 * upper  = middle + multiplier × ATR(atrPeriod)     ${prefix}Upper
 * lower  = middle − multiplier × ATR(atrPeriod)     ${prefix}Lower
 * ```
 *
 * Appends three columns. Reads **high, low and close**, each named by an
 * option defaulting to its `DEFAULT_OHLCV` name (the `atr` precedent).
 *
 * ## Which Keltner — the variant is pinned
 *
 * There are two in circulation and they are not close to each other:
 *
 * - **Keltner's own (1960)**: a 10-bar *simple* average of typical price,
 *   bands at ±1 × the 10-bar simple average of the bar's plain
 *   `high − low` range.
 * - **The modern form** (Linda Bradford Raschke's restatement, and what
 *   ChartIQ, StockCharts and TradingView all default to): a **20-bar EMA of
 *   typical price**, bands at **±2 × ATR(10)** — *true* range, Wilder-
 *   smoothed, so an overnight gap widens the channel.
 *
 * **This study ships the modern form** — `period 20`, `atrPeriod 10`,
 * `multiplier 2`, `maType 'ema'` — because it is what every charting package
 * a caller is likely to be comparing against draws. The original is a call
 * away (`{ period: 10, atrPeriod: 10, multiplier: 1, maType: 'sma' }`) apart
 * from one deliberate delta: the half-width is **ATR**, never the plain
 * range, so the original cannot be reproduced exactly. Plain range is not a
 * quantity this package computes anywhere, and adding a `range | trueRange`
 * knob to recover a 1960 convention nobody's chart draws would be a knob
 * with one useful value. Said here rather than left to be discovered from a
 * chart that doesn't line up.
 *
 * `maType` is the shared {@link MaType} menu, so `'sma'`, `'wma'`, `'smma'`
 * and the rest are all available on the centre line; TA-Lib has no Keltner
 * to arbitrate, and the oracle is a pandas replication reusing the same
 * true-range and typical-price definitions `atr` and `vwap` already ship.
 *
 * ## Warm-up: per column, not per study
 *
 * The centre starts at the MA's own first bar (`period − 1` for the window
 * types on gap-free input, later for the composed ones — see
 * `movingAverageValues`); the ATR at bar `atrPeriod`. The **bands therefore
 * start at the later of the two**, `max(centre, atr)`, and the centre is
 * emitted where it is genuinely defined rather than being masked back to the
 * bands — the same per-column warm-up {@link macd} uses for its line.
 * At the defaults that is bar 19 for all three (the EMA's 19 beats the ATR's
 * 10); at `{ period: 5, atrPeriod: 20 }` the centre lands at bar 4 and the
 * bands at bar 20.
 *
 * ## Edges
 *
 * - **In the units of the price**, like {@link atr} and {@link donchian}:
 *   scaling every bar scales the whole channel. It does not normalise.
 * - **A leading gap shifts the start** rather than emptying the study: the
 *   MA steps over its input's warm-up (so Keltner over another study's
 *   output starts late), and so does the Wilder seed.
 * - **An interior gap** is the Wilder asymmetry, and the two halves of this
 *   study genuinely differ: the ATR **propagates it to the end** (a
 *   recursion has no state to carry across a hole), while the centre line
 *   follows its own `maType`'s rule — window types recover, the `ema` family
 *   skips the bar and carries on, `smma` and `kama` propagate. So a hole
 *   past the warm-up leaves the centre drawn and the bands blank at the
 *   defaults. Neither answer is fixable here; a caller who needs continuity
 *   across interior gaps fills before smoothing.
 * - **No division anywhere**, so there is no zero-denominator case: a flat
 *   stretch gives `ATR = 0` and a zero-width channel, which is the honest
 *   reading (contrast {@link bollinger}, where `σ = 0` emits `undefined`
 *   because a zero-width *statistical* band is a degenerate statistic
 *   rather than a real measurement).
 */
export function keltner<
  S extends SeriesSchema,
  const Prefix extends string = 'kc',
>(series: TimeSeries<S>, options: KeltnerOptions<S, Prefix> = {}) {
  const period = options.period ?? 20;
  const atrPeriod = options.atrPeriod ?? 10;
  assertPeriod(period);
  assertPeriod(atrPeriod, 'atrPeriod');
  const maType = options.maType ?? 'ema';
  assertMaType(maType);
  const multiplier = options.multiplier ?? 2;
  if (!Number.isFinite(multiplier) || multiplier <= 0) {
    throw new TypeError('keltner multiplier must be a positive finite number');
  }

  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const closeName = (options.close ?? DEFAULT_OHLCV.close) as string;
  const prefix = (options.prefix ?? 'kc') as Prefix;
  const middleName = `${prefix}Middle` as const;
  const upperName = `${prefix}Upper` as const;
  const lowerName = `${prefix}Lower` as const;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  for (const name of [middleName, upperName, lowerName]) {
    assertNoColumn(wide, name);
  }

  const high = columnValues(wide, highName);
  const low = columnValues(wide, lowName);
  const close = columnValues(wide, closeName);

  // Typical price is a derived array, so the centre goes through the K2
  // engine's ARRAY door — where every window type, `sma` included, emits
  // only once the last `period` rows are ALL finite (a gap blanks every
  // window that contains it). That is the rule for derived inputs (the studies
  // README): averaging a partly-warm typical price as if it were data is how
  // slow %K ended up one bar early.
  const middle = movingAverageValues(
    typicalPriceValues(high, low, close),
    period,
    maType,
  );
  // The same `atrValues` call `atr()` makes, so `kcUpper − kcMiddle` is
  // `multiplier × atr()` bit-for-bit rather than approximately.
  const atr = atrValues(high, low, close, atrPeriod);

  // A missing centre or ATR is `NaN` ([PND-STUDYBOX]) and propagates through
  // the arithmetic on its own, which is exactly the per-column warm-up: the
  // bands appear on the later of the two starts with no branch here.
  const band = (sign: 1 | -1): Float64Array => {
    const out = new Float64Array(middle.length);
    for (let i = 0; i < out.length; i += 1) {
      out[i] = middle[i]! + sign * multiplier * atr[i]!;
    }
    return out;
  };

  return series
    .withColumn(middleName, middle)
    .withColumn(upperName, band(1))
    .withColumn(lowerName, band(-1));
}
