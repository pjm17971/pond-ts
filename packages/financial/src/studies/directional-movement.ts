import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import { directionalMovementValues } from '../kernels/directional-movement.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';
import { atrValues } from '../kernels/true-range.js';
import { wilderValues } from '../kernels/wilder.js';

export interface DirectionalMovementOptions<
  S extends SeriesSchema,
  Prefix extends string,
> {
  /** Wilder smoothing length in **bars**, used for all three smooths.
   *  **Default `14`.** */
  period?: number;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Close column. **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Column-family prefix — appends `${prefix}PlusDi` / `${prefix}MinusDi` /
   *  `${prefix}Dx` / `${prefix}Adx` / `${prefix}Adxr`. **Default `'dm'`.** */
  prefix?: Prefix;
}

/**
 * **The Directional Movement System** (J. Welles Wilder Jr., *New Concepts in
 * Technical Trading Systems*, 1978) — the whole family in one study:
 *
 * ```
 * +DM / −DM   this bar's movement outside the previous bar's range
 * +DI = 100 · Wilder(+DM, period) / Wilder(TR, period)     ${prefix}PlusDi
 * −DI = 100 · Wilder(−DM, period) / Wilder(TR, period)     ${prefix}MinusDi
 * DX  = 100 · |+DI − −DI| / (+DI + −DI)                    ${prefix}Dx
 * ADX = Wilder(DX, period)                                 ${prefix}Adx
 * ADXR = (ADX[i] + ADX[i − period + 1]) / 2                ${prefix}Adxr
 * ```
 *
 * The two `DI` lines say **which way** the market is moving, `DX` says how
 * one-sided that movement is, and `ADX` smooths `DX` into the trend-strength
 * reading everyone quotes. `ADXR` is Wilder's own "rating" — the average of
 * today's `ADX` and the one at the other end of a `period`-bar window, which
 * he used to compare instruments rather than to trade.
 *
 * Reads **high, low and close**, each named by an option defaulting to its
 * `DEFAULT_OHLCV` column — the {@link atr} shape.
 *
 * ## One study, five columns, five warm-ups
 *
 * The {@link macd} precedent: each column emits where its own definition
 * makes it defined rather than all five waiting for the slowest. At the
 * default `period 14` that is bar 14 for the `DI` pair and `DX`, bar 27
 * (`2·period − 1`) for `ADX`, and bar 40 (`3·period − 2`) for `ADXR` —
 * **the same warm-ups TA-Lib publishes**, asserted mask-for-mask in the
 * oracle. Splitting `ADXR` into a study of its own was rejected for the
 * reason the family exists: it would recompute the entire pipeline to append
 * one column that is two reads of a column this study already has.
 *
 * ## Definition — Wilder's seeding, not TA-Lib's
 *
 * This is the one place the family's numbers move away from TA-Lib, and it
 * is a **seed** difference, measured rather than assumed.
 *
 * Wilder's published worksheet accumulates the first `period` values of
 * `+DM`, `−DM` and `TR` and then decays: `S ← S − S/period + x`. That is
 * exactly `period ×` the mean-form recursion {@link wilderValues} runs, so
 * this study's `DI` ratio *is* Wilder's — and its denominator is literally
 * {@link atrValues}, so `+DI + −DI` and `atr()` measure range with the same
 * array by construction.
 *
 * **TA-Lib seeds the same three accumulators on the first `period − 1`
 * values and then takes one decayed step**, which is a different quantity —
 * and TA-Lib does not do this in its own `ATR`, which uses the `period`-value
 * mean. Measured on the oracle input, TA-Lib's `ATR(14)` first value is
 * `1.515450` against `1.411787` for the true range its own `ADX` is dividing
 * by on the same bar, so a caller plotting TA-Lib's `+DI` beside TA-Lib's
 * `ATR` is reading two different ranges. Ours cannot diverge that way.
 *
 * The cost is a **decaying transient** against TA-Lib rather than a wrong
 * rate, on the {@link macd} / `ema` precedent. Measured over the oracle's 80
 * bars at `period 14`: `+DI` differs by 0.117 points on the first shared bar,
 * peaks at 0.513, and is 0.0092 by bar 79; `ADX` 0.421 → 0.0046; `ADXR`
 * 0.187 → 0.015. At `period 5` the transient is larger at the start (`+DI`
 * 2.58 at worst) and gone by bar 79 (1.0e-6) — it decays as
 * `(1 − 1/period)^k`, so a longer `period` holds it longer. The oracle
 * asserts the **formula** exactly by replaying our own pipeline on TA-Lib's
 * seed (agreement `≤ 2.9e-14` on all five columns, masks identical) and
 * bounds the pond-seed transient separately, which is the pattern the
 * moving-average engine already uses for its EMA family.
 *
 * ## `ADXR`'s look-back is `period − 1` bars, deliberately
 *
 * Wilder's prose says "the `ADX` `period` days ago", and reading that as a
 * literal `i − period` is what several vendors do. This ships TA-Lib's
 * reading, `i − (period − 1)`, for two reasons: it is what makes the whole
 * family TA-Lib-mask-identical, and it is the package's own **bar-count**
 * convention — a `period`-bar window spans `i − period + 1 … i`, so
 * averaging its two ends is the reading consistent with every other `period`
 * here. The two differ by up to **2.64 points at `period 14`** and 8.19 at
 * `period 5` on the oracle input, so it is a real choice, not a rounding
 * one; a caller who wants the literal reading has `dmAdx` on the series and
 * can shift it.
 *
 * ## Edges
 *
 * - **Bounded `0..100`** — every column. `+DM` and `−DM` are non-negative and
 *   at most one is non-zero per bar, so each `DI` is a non-negative fraction
 *   of the same true-range total, `DX` normalises their difference by their
 *   sum, and `ADX`/`ADXR` are averages of `DX`. (The `DI` pair is bounded by
 *   100 only when the bars are consistent, i.e. movement outside the previous
 *   range is also inside the true range; a redirected `high`/`low` that is not
 *   a real bar can exceed it, honestly, rather than clamped.)
 * - **`+DI + −DI = 0` → `DX = 0`, not `undefined`.** The house test is
 *   whether the numerator is *forced* to zero by the same condition, and here
 *   it is: both legs are non-negative, so a zero sum means both are zero and
 *   `|+DI − −DI|` is exactly zero too — the {@link clvValues} flat-bar case,
 *   not the {@link percentOfRangeValues} flat-window one. It is a real
 *   market state (a run of inside bars: range, but no directional movement),
 *   TA-Lib agrees, and reading it as "no trend strength" is the only
 *   sensible answer. A zero **true range** is different and does read
 *   `undefined`: there the `DI` ratio is a genuine `0/0` and TA-Lib's `0`
 *   would be indistinguishable from a real reading.
 * - **Scale-invariant**: every column is a ratio of price differences, so
 *   multiplying every price leaves all five unchanged (pinned by a property
 *   test).
 * - **A leading gap shifts the start** rather than emptying the study —
 *   {@link wilderValues} steps over it.
 * - **An interior gap propagates to the end**, inherent to Wilder smoothing,
 *   and *which* column dies depends on which input has the hole. Measured on
 *   an 80-bar series with one missing cell at bar 40 and `period 14`: a gap
 *   in `high` or `low` costs both `DI` lines, `DX`, `ADX` and `ADXR` from bar
 *   **40** on (the `DM` split needs both bars); a gap in `close` costs the
 *   same five from bar **41** on (the true range reads only the *previous*
 *   close, so it is the next bar that has no denominator). Callers who need
 *   continuity across a halt must fill before smoothing. TA-Lib's answers
 *   here are worse in both directions, measured: a `NaN` high or low
 *   propagates to the end as ours does, but a `NaN` **close** produces no
 *   missing value at all — its C `max` comparisons are all false against
 *   `NaN`, so the true range silently falls back to the bar's own range and
 *   the study carries on with a wrong number.
 */
export function directionalMovement<
  S extends SeriesSchema,
  const Prefix extends string = 'dm',
>(series: TimeSeries<S>, options: DirectionalMovementOptions<S, Prefix> = {}) {
  const period = options.period ?? 14;
  assertPeriod(period);
  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const closeName = (options.close ?? DEFAULT_OHLCV.close) as string;
  const prefix = (options.prefix ?? 'dm') as Prefix;
  const plusName = `${prefix}PlusDi` as const;
  const minusName = `${prefix}MinusDi` as const;
  const dxName = `${prefix}Dx` as const;
  const adxName = `${prefix}Adx` as const;
  const adxrName = `${prefix}Adxr` as const;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  for (const name of [plusName, minusName, dxName, adxName, adxrName]) {
    assertNoColumn(wide, name);
  }

  const high = columnValues(wide, highName);
  const low = columnValues(wide, lowName);
  const close = columnValues(wide, closeName);
  const { plus: rawPlus, minus: rawMinus } = directionalMovementValues(
    high,
    low,
  );
  // The denominator is `atr()`'s own array, not a second Wilder-over-true-
  // range written to match it — the reason `atrValues` is a kernel.
  const smoothedRange = atrValues(high, low, close, period);
  // `start = 1` for the same reason ATR passes it: `DM[0]` has no previous
  // bar. Redundant (the leading NaN would be stepped over) and kept because
  // it says WHY at the call site.
  const smoothedPlus = wilderValues(rawPlus, period, 1);
  const smoothedMinus = wilderValues(rawMinus, period, 1);

  const length = high.length;
  const diPlus = new Float64Array(length);
  const diMinus = new Float64Array(length);
  const dx = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    const range = smoothedRange[i]!;
    const p = (100 * smoothedPlus[i]!) / range;
    const m = (100 * smoothedMinus[i]!) / range;
    diPlus[i] = p;
    diMinus[i] = m;
    const total = p + m;
    // `total === 0` is the no-directional-movement case (see above): the
    // numerator is exactly zero too, so the value is 0. A NaN `total` is not
    // `=== 0`, so a gap falls through to the division and propagates.
    dx[i] = total === 0 ? 0 : (100 * Math.abs(p - m)) / total;
  }

  const adx = wilderValues(dx, period);
  const adxr = new Float64Array(length);
  const back = period - 1;
  for (let i = 0; i < length; i += 1) {
    adxr[i] = i < back ? NaN : (adx[i]! + adx[i - back]!) / 2;
  }

  return series
    .withColumn(plusName, diPlus)
    .withColumn(minusName, diMinus)
    .withColumn(dxName, dx)
    .withColumn(adxName, adx)
    .withColumn(adxrName, adxr);
}
