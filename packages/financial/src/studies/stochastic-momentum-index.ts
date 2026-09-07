import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import { highestLowestValues } from '../kernels/highest-lowest.js';
import { movingAverageValues } from '../kernels/moving-average.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';

export interface StochasticMomentumIndexOptions<
  S extends SeriesSchema,
  Prefix extends string,
> {
  /** Look-back for the highest high / lowest low, in **bars**.
   *  **Default `13`** (Blau's `q`). */
  period?: number;
  /** Span of the **first** EMA applied to both the numerator and the
   *  denominator, in **bars**. **Default `25`** (Blau's `r`). */
  longPeriod?: number;
  /** Span of the **second** EMA, applied to the first's output, in **bars**.
   *  **Default `2`** (Blau's `s`). */
  shortPeriod?: number;
  /** Signal EMA span in **bars**, taken over the SMI line. **Default `3`.** */
  signalPeriod?: number;
  /** High column (the range's upper input). **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column (the range's lower input). **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** The field measured against the range's midpoint. **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Column-family prefix — appends `${prefix}` and `${prefix}Signal`.
   *  **Default `'smi'`.** */
  prefix?: Prefix;
}

/**
 * **Stochastic Momentum Index** (William Blau, TASC January 1993) — where the
 * close sits relative to the **midpoint** of the recent range rather than to
 * its bottom, double-smoothed above and below the line:
 *
 * ```
 * HH, LL          = highest high / lowest low over `period` bars
 * M               = close − (HH + LL) / 2          the distance from the midpoint
 * H               = (HH − LL) / 2                  half the range
 * ${prefix}       = 100 · EMA(EMA(M, longPeriod), shortPeriod)
 *                       / EMA(EMA(H, longPeriod), shortPeriod)
 * ${prefix}Signal = EMA(${prefix}, signalPeriod)
 * ```
 *
 * Appends two columns, bounded **−100 … +100** (`|M| ≤ H` on every bar, and
 * an EMA has non-negative weights, so the smoothed numerator cannot exceed
 * the smoothed denominator). `+100` is a close at the top of the range,
 * `−100` at the bottom, `0` exactly at the midpoint — where the classic
 * {@link stochastic} reads 50. The midpoint reference is the point of the
 * study: it makes the reading signed, so the zero line means something, and
 * double smoothing makes it far quieter than the raw `%K` it replaces.
 *
 * **The line is named `${prefix}`, not `${prefix}Line`** — the {@link trix}
 * and {@link trueStrengthIndex} shape, with the signal keeping the suffix.
 *
 * ## Option names — Blau's other study's names, deliberately
 *
 * `longPeriod` / `shortPeriod` are the same two knobs {@link
 * trueStrengthIndex} exposes, in the same order (the longer span applied
 * **first**), because they are the same construction by the same author:
 * Blau writes both as `r` and `s`. `period` is the range look-back he writes
 * `q`. So the mapping to the vendor labels is:
 *
 * | this study | Blau | typical chart label |
 * | --- | --- | --- |
 * | `period` | `q` | `%K Length` |
 * | `longPeriod` | `r` | `%D Length` / first smoothing |
 * | `shortPeriod` | `s` | `EMA Length` / second smoothing |
 * | `signalPeriod` | — | `Signal` |
 *
 * ## Which defaults — Blau's `(13, 25, 2)`, and the short fork named
 *
 * Two parameterisations circulate. Blau's own — a 13-bar range, a 25-bar
 * first smoothing and a 2-bar finish — ships here, with a 3-bar signal EMA.
 * The other is a much shorter fork (a 5-bar range with both smoothings at 3)
 * that charting packages ship as a "fast" SMI; it is reachable as
 * `{ period: 5, longPeriod: 3, shortPeriod: 3 }` and is a genuinely
 * different reading rather than a rounding of this one — measured on the
 * package's oracle input, the two lines sit **108.99 apart** on a reading
 * that spans −48.19 … 74.33 (`scripts/oracle/generate.py`). Naming both and
 * shipping one is the F-AMBIG discipline; the number is what makes it a
 * choice rather than a coin flip.
 *
 * The smoothing is not a parameter either, and it is what separates this
 * from a rescaled {@link stochastic}: dropping both EMAs and reading
 * `100 · M / H` straight is **126.95 away** on the same input, measured.
 * Dropping only the **second** stage is a much closer miss — **3.75** at
 * Blau's `shortPeriod: 2`, where the finishing EMA is light, and **23.28**
 * at the oracle's second shape `(8, 10, 4, 5)`. All three separations are
 * asserted by the generator, the small one included: it is the honest size
 * of that particular mistake rather than a number chosen to look large.
 *
 * ## Definition, verified
 *
 * **No TA-Lib function**, so the oracle is a pandas replication built on the
 * same `ewm(adjust=False)` recursion the package's own EMA runs, with the
 * analytic first-valid bar asserted and the three separations above
 * measured.
 *
 * ## Warm-up — per column
 *
 * On gap-free input the range lands at bar `period − 1`, the first EMA
 * `longPeriod − 1` bars later, the second `shortPeriod − 1` after that, and
 * the signal `signalPeriod − 1` after that: bars **37** and **39** at the
 * defaults. Both EMAs go through the K2 engine's array door, which counts
 * finite **values** rather than rows, so each stage steps over the previous
 * one's warm-up instead of averaging it.
 *
 * ## Edges
 *
 * - **Scale-invariant and shift-invariant.** Both `M` and `H` are
 *   differences of prices, so a shift cancels in each; a scale multiplies
 *   both and cancels in the ratio. Pinned as property tests.
 * - **A flat range for the whole smoothed history → `undefined`.** The
 *   denominator is an EMA of an EMA, so it reaches exactly zero only when
 *   every bar it has seen was flat. With the default `close` the numerator
 *   is then zero too (`|M| ≤ H` bar by bar) and the ratio would be a `0/0`;
 *   but `close` can be redirected at a column the range does not bound, and
 *   then the numerator is **not** forced to zero and the division is a real
 *   number over zero. The guard is live for that reason — and for a second:
 *   after a long flat run following live data the denominator underflows to
 *   zero while the numerator is still a denormal, which without the guard
 *   prints `Infinity` rather than a reading. Both are pinned by tests.
 * - **The range skips a missing bar** ({@link highestLowestValues} composes
 *   on core's reducers), so an absent `high` does not blank the extremes; an
 *   absent `close` blanks that bar's `M`, and the EMAs step over it and carry
 *   on. Contrast the Wilder-smoothed studies, where an interior gap
 *   propagates to the end.
 */
export function stochasticMomentumIndex<
  S extends SeriesSchema,
  const Prefix extends string = 'smi',
>(
  series: TimeSeries<S>,
  options: StochasticMomentumIndexOptions<S, Prefix> = {},
) {
  const period = options.period ?? 13;
  const longPeriod = options.longPeriod ?? 25;
  const shortPeriod = options.shortPeriod ?? 2;
  const signalPeriod = options.signalPeriod ?? 3;
  assertPeriod(period);
  assertPeriod(longPeriod, 'longPeriod');
  assertPeriod(shortPeriod, 'shortPeriod');
  assertPeriod(signalPeriod, 'signalPeriod');

  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const closeName = (options.close ?? DEFAULT_OHLCV.close) as string;
  const prefix = (options.prefix ?? 'smi') as Prefix;
  const lineName = `${prefix}` as const;
  const signalName = `${prefix}Signal` as const;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, lineName);
  assertNoColumn(wide, signalName);

  const { highest, lowest } = highestLowestValues(
    wide,
    highName,
    lowName,
    period,
  );
  const close = columnValues(wide, closeName);
  const length = close.length;

  // A warming-up or missing extreme is NaN and propagates through both
  // derivations on its own ([PND-STUDYBOX]), so neither loop needs a branch.
  const distance = new Float64Array(length);
  const halfRange = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    const hh = highest[i]!;
    const ll = lowest[i]!;
    distance[i] = close[i]! - (hh + ll) / 2;
    halfRange[i] = (hh - ll) / 2;
  }

  // Long first, then short — the same order (and the same array door)
  // `trueStrengthIndex` runs, so the two Blau studies share one recursion.
  const smooth = (values: Float64Array) =>
    movingAverageValues(
      movingAverageValues(values, longPeriod, 'ema'),
      shortPeriod,
      'ema',
    );
  const numerator = smooth(distance);
  const denominator = smooth(halfRange);

  const line = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    const den = denominator[i]!;
    // Live, unlike `trueStrengthIndex`'s absent guard: there the numerator is
    // bounded by the denominator unconditionally, here only while `close`
    // sits inside its own range (see the docstring).
    line[i] = den === 0 ? NaN : (100 * numerator[i]!) / den;
  }

  const signal = movingAverageValues(line, signalPeriod, 'ema');

  return series.withColumn(lineName, line).withColumn(signalName, signal);
}
