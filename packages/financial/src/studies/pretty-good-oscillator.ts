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
  rollingValues,
} from '../kernels/rolling.js';
import { trueRangeValues } from '../kernels/true-range.js';

export interface PrettyGoodOscillatorOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Look-back of **both** the simple average and the true-range average, in
   *  **bars**. **Default `14`.** */
  period?: number;
  /** The field measured against its own average. **Default: whatever `close`
   *  resolves to**, so redirecting `close` moves both halves together. */
  column?: NumericColumnNameForSchema<S>;
  /** High column (true-range input). **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column (true-range input). **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Close column (true-range input). **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Appended column name. **Default `'pgo'`.** */
  output?: Output;
}

/**
 * **Pretty Good Oscillator** (Mark Johnson) — how far the close has strayed
 * from its own simple average, measured **in average daily ranges**:
 *
 * ```
 * ${output} = (column − SMA(column, period)) / EMA(TR, period)
 * ```
 *
 * Appends one column, in units of "days' range". A reading of `+2.5` means
 * the close is two and a half average ranges above its average — Johnson's
 * own rule is to enter above `+3.0` (or below `−3.0`) and exit on the return
 * to zero, which is a close back at the average. Expressing the deviation in
 * ranges rather than in points is the whole idea: the same number means the
 * same thing across instruments and across volatility regimes, where
 * {@link movingAverageDeviation}'s raw distance does not.
 *
 * ## F-AMBIG — which denominator, and the measured separation
 *
 * The corpus flags this study as definition-ambiguous, and the fork is the
 * denominator. **Johnson's form ships**: a span **EMA of true range**, which
 * is what "an average true range over a similar period" means when it is
 * written as an exponential average. The common port instead uses Wilder's
 * `ATR` — the `atr()` this package already exports — which is a different
 * smoothing with a different effective memory, not a rounding of the same
 * one. Measured on the package's oracle input at `period 14`, the two
 * readings sit **0.211 apart** on a line that spans −3.71 … 4.23, and
 * **0.084 apart** at `period 5` (`scripts/oracle/generate.py`); the
 * generator asserts both separations so the fixture cannot silently accept
 * the other fork.
 *
 * A caller who wants the Wilder-denominator variant composes it from shipped
 * primitives — `atr()` into a column, then `movingAverageDeviation()` over
 * it — in three lines, and gets a name that says which one it is.
 *
 * ## Two columns of vocabulary: `column` vs `close`
 *
 * `column` is the field the deviation is measured on and `close` is the true
 * range's third input; `column` **defaults to whatever `close` resolves
 * to**, so redirecting `close` alone moves both halves together. That is
 * {@link atrBands}' rule, adopted here for the same reason a Layer-2 review
 * of #696 asked for it there: a default that silently stays on the schema's
 * `close` while the volatility comes from a redirected one is a study
 * measuring two different instruments at once.
 *
 * ## Warm-up
 *
 * Length-preserving, and governed by the denominator: `TR[0]` is undefined
 * (no previous close), so an EMA needing `period` finite true ranges first
 * emits on bar **`period`**, one bar after the simple average's
 * `period − 1`. At the default that is bar 14.
 *
 * The simple average comes from the **column** door (core's count-window
 * `avg`, which counts rows), so it is exactly the average `sma()` gives over
 * the same column; the true-range average comes from the K2 engine's array
 * door, which counts finite values and therefore steps over `TR[0]`.
 *
 * ## Edges
 *
 * - **Scale-invariant and shift-invariant.** The numerator and the
 *   denominator are both first-order in price, so a scale cancels in the
 *   ratio; a shift cancels inside the numerator and does not touch the true
 *   range at all. Both pinned as property tests — and this is the pair that
 *   tells the study apart from a raw deviation, which is linear in scale.
 * - **A zero denominator → `undefined`.** `EMA(TR)` is exactly zero only
 *   when every true range it has seen was zero — a tape that has never
 *   moved. The numerator is not forced to zero with it (`column` can be
 *   redirected at a column the bars do not bound), so it is a real number
 *   over zero rather than a `0/0`, and the guard is live rather than
 *   decorative: a test pins it.
 * - **An interior gap costs exactly two bars**, and which two is worth
 *   naming because the obvious guess is wrong. The simple average comes from
 *   the **column** door, which *skips* a missing cell and averages the rest,
 *   so it costs nothing. A missing close blanks its own bar (the numerator
 *   reads it) and the **next** one (whose true range reads it as
 *   `prevClose`, and the EMA emits nothing on a bar with no input). The bar
 *   after that is back. Contrast the Wilder-denominator port, which would
 *   blank everything after the gap — the recursion has no state to carry
 *   across a hole. A test pins the two-bar footprint.
 */
export function prettyGoodOscillator<
  S extends SeriesSchema,
  const Output extends string = 'pgo',
>(series: TimeSeries<S>, options: PrettyGoodOscillatorOptions<S, Output> = {}) {
  const period = options.period ?? 14;
  assertPeriod(period);

  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const closeName = (options.close ?? DEFAULT_OHLCV.close) as string;
  const column = (options.column ?? closeName) as string;
  const output = (options.output ?? 'pgo') as Output;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const field = columnValues(wide, column);
  const average = rollingValues(wide, column, 'avg', period);
  // Johnson's denominator: a SPAN EMA of true range, not Wilder's ATR (see
  // the docstring for the measured separation from that port).
  const rangeAverage = movingAverageValues(
    trueRangeValues(
      columnValues(wide, highName),
      columnValues(wide, lowName),
      columnValues(wide, closeName),
    ),
    period,
    'ema',
  );

  const values = new Float64Array(field.length);
  for (let i = 0; i < values.length; i += 1) {
    const denominator = rangeAverage[i]!;
    // Live guard: the division is at the OUTPUT and nothing forces the
    // numerator to zero with the denominator.
    values[i] =
      denominator === 0 ? NaN : (field[i]! - average[i]!) / denominator;
  }

  return series.withColumn(output, values);
}
