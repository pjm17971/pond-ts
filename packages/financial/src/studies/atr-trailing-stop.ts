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

export interface AtrTrailingStopOptions<
  S extends SeriesSchema,
  Prefix extends string,
> {
  /** ATR look-back in **bars**. **Default `14`.** */
  period?: number;
  /** Stop distance in ATRs. **Default `3`.** */
  multiplier?: number;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Close column — both the ATR's and the stop's anchor.
   *  **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Column-family prefix — appends `${prefix}` (the stop) and
   *  `${prefix}Trend` (`+1` long / `−1` short). **Default `'ats'`.** */
  prefix?: Prefix;
}

/** The trailing stop's carried state. Mutated in place. */
interface AtsState {
  readonly multiplier: number;
  /** The stop printed on the previous bar. */
  stop: number;
  /** `+1` while the stop trails **below** price. */
  long: boolean;
  prevClose: number;
}

/**
 * One bar of the ATR trailing stop. Four cases, in the published order:
 * two ratchets (price on the same side of the stop as it was) and two
 * flips (price crossed it).
 */
function atsStep(
  state: AtsState,
  i: number,
  run: number,
  inputs: readonly Float64Array[],
  outputs: readonly Float64Array[],
): void {
  const close = inputs[0]!;
  const atr = inputs[1]!;
  const stopOut = outputs[0]!;
  const trendOut = outputs[1]!;

  const c = close[i]!;
  const distance = state.multiplier * atr[i]!;

  if (run === 1) {
    // Seed LONG on the first bar with a finite ATR: the stop is one band
    // below the close. Arbitrary in the same sense as SuperTrend's seed —
    // there is no prior bar to take a side from — and self-correcting at the
    // first close through the stop. Named rather than left implicit.
    state.stop = c - distance;
    state.long = true;
  } else {
    const prev = state.stop;
    const prevClose = state.prevClose;
    if (c > prev && prevClose > prev) {
      // Still long: the stop only ratchets UP.
      state.stop = Math.max(prev, c - distance);
    } else if (c < prev && prevClose < prev) {
      // Still short: the stop only ratchets DOWN.
      state.stop = Math.min(prev, c + distance);
    } else if (c > prev) {
      // Crossed up — flip long and re-anchor a full band below the close.
      state.stop = c - distance;
      state.long = true;
    } else {
      // Crossed down (or landed exactly on the stop) — flip short.
      state.stop = c + distance;
      state.long = false;
    }
  }

  stopOut[i] = state.stop;
  trendOut[i] = state.long ? 1 : -1;
  state.prevClose = c;
}

/**
 * **ATR Trailing Stop** — a stop a fixed number of ATRs from the close that
 * only ever moves in the direction of the trade, and flips side when the
 * close crosses it. Kernel **K6** over {@link atrValues}.
 *
 * ```
 * d = multiplier × ATR(period)          (Wilder's ATR)
 *
 * close > prev and prevClose > prev  ->  stop = max(prev, close − d)   long, ratchet
 * close < prev and prevClose < prev  ->  stop = min(prev, close + d)   short, ratchet
 * close > prev                       ->  stop = close − d              flip long
 * otherwise                          ->  stop = close + d              flip short
 * ```
 *
 * Appends **two** columns:
 *
 * - `${prefix}` — the stop level (`ats` at the default prefix).
 * - `${prefix}Trend` — `+1` while the stop is below price, `−1` above.
 *
 * The two-column shape and the sign convention are {@link parabolicSar}'s,
 * and for the same reason: the side is what a consumer draws, and on a flip
 * bar the stop can print exactly *at* the close (when `ATR = 0`), so
 * `stop < close` is not a safe derivation of it.
 *
 * ## Which trailing stop — the anchor is the close, and the alternative is named
 *
 * Two families circulate under names this close together that picking one has
 * to be explicit:
 *
 * - **This study**: the band is measured from the **close**
 *   (`close ± multiplier × ATR`), ratcheted, with the four-case flip above.
 *   This is the form published as "ATR Trailing Stop" — Sylvain Vervoort's,
 *   and the one TradingView's widely-copied `ATR Trailing Stop` / "UT Bot"
 *   scripts implement.
 * - **The Chandelier Exit** (Chuck LeBeau): the band is measured from the
 *   **rolling extreme** instead (`highestHigh(period) − multiplier × ATR`
 *   while long, `lowestLow(period) + multiplier × ATR` while short). That is
 *   a different study, not a variant of this one, and it is a composition
 *   away from what the package already ships — {@link donchian} supplies both
 *   extremes and {@link atr} the width. It is not implemented here, and this
 *   study deliberately does **not** grow an `anchor: 'close' | 'extreme'`
 *   knob: the two differ in more than one place (the extreme form also
 *   needs its own warm-up, `max(period, period)`, and its flip is
 *   conventionally tested on the close against the *other* side's band), so
 *   a knob would be two studies wearing one name.
 *
 * `high` and `low` are read **only** by the ATR — the stop itself is a
 * close-to-close construction.
 *
 * ## Warm-up and gaps
 *
 * Length-preserving; both columns start on the ATR's first bar, which is bar
 * `period` on gap-free input. As with {@link superTrend}, the ATR is the gap
 * rule in practice: an interior hole leaves Wilder's recursion `NaN` to the
 * end, so the study reads `undefined` from there. The K6 reset
 * ([PND-SFOLD]) is what governs a gap in `close` alone.
 *
 * ## Edges
 *
 * - **In the units of the price**: scaling every bar by `k` scales the stop
 *   by `k`; adding `c` shifts it by `c`. The trend column is unchanged by
 *   either.
 * - **A flat stretch** gives `ATR = 0` and a stop sitting exactly on the
 *   close. The fourth case is written `otherwise` rather than `close < prev`
 *   precisely so that `close === prev` resolves — it flips (or stays) short,
 *   which is the same tie-break the published form uses. No division, so no
 *   zero-denominator case.
 * - **`multiplier` is a width, not a bar count**, so it is validated as a
 *   positive finite number rather than by `assertPeriod`.
 */
export function atrTrailingStop<
  S extends SeriesSchema,
  const Prefix extends string = 'ats',
>(series: TimeSeries<S>, options: AtrTrailingStopOptions<S, Prefix> = {}) {
  const period = options.period ?? 14;
  assertPeriod(period);
  const multiplier = options.multiplier ?? 3;
  if (!Number.isFinite(multiplier) || multiplier <= 0) {
    throw new TypeError(
      'atrTrailingStop multiplier must be a positive finite number',
    );
  }

  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const closeName = (options.close ?? DEFAULT_OHLCV.close) as string;
  const prefix = (options.prefix ?? 'ats') as Prefix;
  const stopName = prefix;
  const trendName = `${prefix}Trend` as const;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, stopName);
  assertNoColumn(wide, trendName);

  const close = columnValues(wide, closeName);
  const atr = atrValues(
    columnValues(wide, highName),
    columnValues(wide, lowName),
    close,
    period,
  );

  const state: AtsState = {
    multiplier,
    stop: NaN,
    long: true,
    prevClose: NaN,
  };
  const [stop, trend] = foldRows([close, atr], 2, state, atsStep);

  return series.withColumn(stopName, stop!).withColumn(trendName, trend!);
}
