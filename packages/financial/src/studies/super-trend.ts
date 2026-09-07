import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import { foldRows } from '../kernels/fold.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';
import { atrValues } from '../kernels/true-range.js';

export interface SuperTrendOptions<
  S extends SeriesSchema,
  Prefix extends string,
> {
  /** ATR look-back in **bars**. **Default `10`.** */
  period?: number;
  /** Band half-width in ATRs. **Default `3`.** */
  multiplier?: number;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Close column. **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Column-family prefix — appends `${prefix}` (the line) and
   *  `${prefix}Trend` (`+1` up / `−1` down). **Default `'st'`.** */
  prefix?: Prefix;
}

/** SuperTrend's carried state: the two ratcheted bands, the side, and the
 *  previous close the ratchet reads. Mutated in place. */
interface SuperTrendState {
  readonly multiplier: number;
  upper: number;
  lower: number;
  /** `+1` while the line sits **below** price. */
  up: boolean;
  prevClose: number;
}

/**
 * One bar of SuperTrend: ratchet both bands, then decide the side.
 *
 * The ratchet is the whole study. A basic band is `(high+low)/2 ± k·ATR`, and
 * it wanders with volatility; the *final* band only ever moves **towards**
 * price while the side holds, and is released to the basic band the moment
 * the previous close closed through it.
 */
function superTrendStep(
  state: SuperTrendState,
  i: number,
  run: number,
  inputs: readonly Float64Array[],
  outputs: readonly Float64Array[],
): void {
  const high = inputs[0]!;
  const low = inputs[1]!;
  const close = inputs[2]!;
  const atr = inputs[3]!;
  const lineOut = outputs[0]!;
  const trendOut = outputs[1]!;

  const mid = (high[i]! + low[i]!) / 2;
  const halfWidth = state.multiplier * atr[i]!;
  const basicUpper = mid + halfWidth;
  const basicLower = mid - halfWidth;

  if (run === 1) {
    // Seed. Both bands start at their basic values and the line starts on
    // the UPPER band — TradingView's `ta.supertrend` seeds `direction := 1`
    // (its sign convention for a downtrend) on the first bar whose previous
    // ATR is unavailable, which is exactly this bar. Reported here as
    // `trend = −1`, since this package's convention is `+1` = line below
    // price (the same convention as `psarTrend`).
    state.upper = basicUpper;
    state.lower = basicLower;
    state.up = false;
  } else {
    // The ratchet reads the PREVIOUS close against the PREVIOUS final band.
    if (basicUpper < state.upper || state.prevClose > state.upper) {
      state.upper = basicUpper;
    }
    if (basicLower > state.lower || state.prevClose < state.lower) {
      state.lower = basicLower;
    }
    // The flip reads THIS bar's close against the bands just ratcheted.
    state.up = state.up ? !(close[i]! < state.lower) : close[i]! > state.upper;
  }

  lineOut[i] = state.up ? state.lower : state.upper;
  trendOut[i] = state.up ? 1 : -1;
  state.prevClose = close[i]!;
}

/**
 * **SuperTrend** (Olivier Seban) — an ATR band that ratchets towards price
 * and flips side when price closes through it. Kernel **K6** over
 * {@link atrValues}.
 *
 * ```
 * mid        = (high + low) / 2
 * basicUpper = mid + multiplier × ATR(period)
 * basicLower = mid − multiplier × ATR(period)
 *
 * finalUpper = basicUpper < prevFinalUpper or prevClose > prevFinalUpper
 *              ? basicUpper : prevFinalUpper
 * finalLower = basicLower > prevFinalLower or prevClose < prevFinalLower
 *              ? basicLower : prevFinalLower
 *
 * up   = up ? not (close < finalLower) : close > finalUpper
 * line = up ? finalLower : finalUpper
 * ```
 *
 * Appends **two** columns:
 *
 * - `${prefix}` — the line (`st` at the default prefix).
 * - `${prefix}Trend` — `+1` while the line is below price, `−1` above.
 *
 * ## Two columns, not four — the bands are intermediates
 *
 * The obvious fourth and fifth columns are `${prefix}Upper` / `${prefix}Lower`
 * (the two final bands), and they are deliberately **not** shipped. The line
 * already *is* whichever band is live — `${prefix}` equals the final lower
 * band exactly when `${prefix}Trend` is `+1` and the final upper band exactly
 * when it is `−1` — so the drawn band is always present and the extra columns
 * would only ever expose the **inactive** one, which no published SuperTrend
 * chart draws and no consumer has asked for. Publishing it would also make
 * the study's contract wider than its definition: the inactive band's value
 * is an artefact of the ratchet's bookkeeping, and pinning it in the schema
 * would freeze an implementation detail. If a consumer turns up who needs
 * both bands, that is a two-column addition to this file, not a redesign
 * (see the plan write-up).
 *
 * The naming follows {@link parabolicSar}: the bare `${prefix}` is the
 * principal value and `${prefix}Trend` annotates it. The same sign convention
 * holds across both — `+1` means the stop/line sits **below** price.
 *
 * ## Which SuperTrend
 *
 * Seban's definition as implemented by **TradingView's `ta.supertrend`**,
 * which is what a caller's chart draws and what the ChartIQ / Investing.com /
 * TradingView family agree on. TA-Lib has no SuperTrend, so the oracle is a
 * pandas replication with the ratchet spelled out term by term, and a case
 * separating it from the two nearby readings that are easy to write by
 * accident (ratcheting against **this** bar's close instead of the previous
 * one, and flipping on the **previous** final band instead of the ratcheted
 * one).
 *
 * Two conventions worth naming because implementations differ on them:
 *
 * - **The ATR is Wilder's** ({@link atrValues} — true range, Wilder-smoothed,
 *   the same array {@link atr} and {@link keltner} read), not an SMA of true
 *   range. TradingView's `ta.atr` is also Wilder's.
 * - **The seed side is down.** On the first bar with a finite ATR both bands
 *   take their basic values and the line starts on the **upper** band, i.e.
 *   `${prefix}Trend = −1`. This mirrors `ta.supertrend`'s `direction := 1`
 *   branch (its `1` is a downtrend). The side is corrected by the data at the
 *   first close through a band and is not sticky, but it does mean the first
 *   few bars of a series that opens in an uptrend read `−1`.
 * - **The trend sign is inverted relative to Pine.** `ta.supertrend` returns
 *   `-1` for an uptrend; this study returns `+1`, matching
 *   {@link parabolicSar} and the plain reading of "trend".
 *
 * ## Warm-up and gaps
 *
 * Length-preserving. Both columns start on the ATR's first bar — bar `period`
 * on gap-free input, since `atrValues` skips the first bar's undefined true
 * range and then needs `period` values. The bands are seeded there rather
 * than being masked further back.
 *
 * The ATR is the study's gap rule in practice. A missing `high`, `low` or
 * `close` makes the true range `NaN` on that bar and the next, and Wilder's
 * recursion then **propagates to the end** — so the whole study reads
 * `undefined` from an interior hole onwards, which is `atr`'s documented
 * behaviour rather than anything this study adds. The K6 reset rule
 * ([PND-SFOLD], `kernels/fold.ts`) is therefore invisible here except for a
 * gap that lands only in a column the ATR does not read, which for
 * SuperTrend is none of them. A caller who needs continuity across interior
 * gaps fills before smoothing.
 *
 * ## Edges
 *
 * - **In the units of the price**: scaling every bar by `k` scales the line
 *   by `k` and leaves the trend column unchanged; adding `c` shifts it by
 *   `c`. It does not normalise.
 * - **A flat stretch** gives `ATR = 0`, a zero-width band, and a line sitting
 *   exactly on `(high+low)/2`. There is no division anywhere in the rule, so
 *   there is no zero-denominator case (the {@link keltner} precedent).
 * - **A one-bar series, or one shorter than `period + 1`**, gives two
 *   all-`undefined` columns rather than throwing.
 */
export function superTrend<
  S extends SeriesSchema,
  const Prefix extends string = 'st',
>(series: TimeSeries<S>, options: SuperTrendOptions<S, Prefix> = {}) {
  const period = options.period ?? 10;
  assertPeriod(period);
  const multiplier = options.multiplier ?? 3;
  if (!Number.isFinite(multiplier) || multiplier <= 0) {
    throw new TypeError(
      'superTrend multiplier must be a positive finite number',
    );
  }

  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const closeName = (options.close ?? DEFAULT_OHLCV.close) as string;
  const prefix = (options.prefix ?? 'st') as Prefix;
  const lineName = prefix;
  const trendName = `${prefix}Trend` as const;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, lineName);
  assertNoColumn(wide, trendName);

  const high = columnValues(wide, highName);
  const low = columnValues(wide, lowName);
  const close = columnValues(wide, closeName);
  // The same `atrValues` call `atr()` and `keltner()` make, so the band
  // half-width is `multiplier × atr()` bit-for-bit rather than approximately.
  const atr = atrValues(high, low, close, period);

  const state: SuperTrendState = {
    multiplier,
    upper: NaN,
    lower: NaN,
    up: false,
    prevClose: NaN,
  };
  const [line, trend] = foldRows(
    [high, low, close, atr],
    2,
    state,
    superTrendStep,
  );

  return series.withColumn(lineName, line!).withColumn(trendName, trend!);
}
