import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_SOURCE } from '../contract/columns.js';
import {
  assertColumn,
  assertNoColumn,
  columnValues,
} from '../kernels/rolling.js';

export interface PriceRelativeOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Source column. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** The comparison column — **required**, and a column **on this same
   *  series**. Join the benchmark in first. */
  benchmark: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'priceRel'`.** */
  output?: Output;
}

/**
 * **Price Relative** — the ratio of `column` to `benchmark`, bar by bar:
 *
 * ```
 * ${output} = column / benchmark
 * ```
 *
 * Appends one column, with **no period and no warm-up**: it is the only
 * study in the package that reads a single row, so every row emits (bar `0`
 * included) wherever both inputs exist. A rising line means `column` is
 * outperforming, falling means the benchmark is — the *level* is arbitrary
 * (it carries the two instruments' price units), which is why the line is
 * read for its slope and why {@link performanceIndex} exists to normalise it
 * against a starting point.
 *
 * ## "Relative Strength" here is **not** {@link rsi}
 *
 * ChartIQ lists this under two names — *Price Relative* and *Relative
 * Strength (comparative)* — and they are one implementation, which is what
 * ships here under the unambiguous name. Neither is Wilder's **Relative
 * Strength Index**: {@link rsi} is a single-series momentum oscillator
 * bounded `0..100`, this is an unbounded two-series ratio, and the collision
 * of names is the single most common confusion in the corpus. If you want
 * RSI, call {@link rsi}.
 *
 * ## The comparison series is a COLUMN, not a second `TimeSeries`
 *
 * Join the benchmark in first and name its column:
 *
 * ```ts
 * const wide = TimeSeries.joinMany([bars, spy.rename({ close: 'spy' })], {
 *   type: 'inner',
 * });
 * priceRelative(wide, { column: 'close', benchmark: 'spy' });
 * ```
 *
 * See {@link correlation}'s docstring for the reasoning (aligning two bar
 * clocks is `align`/`joinMany`'s job, not a study's). A missing `benchmark`
 * column throws.
 *
 * ## Edges
 *
 * - **No TA-Lib function**, so the oracle is a pandas replication, with the
 *   inverted ratio (`benchmark / column`) measured as the separation — the
 *   one substitution that leaves the curve's shape recognisable and every
 *   value wrong.
 * - **A zero `benchmark` reads `undefined`, and the guard is LIVE.** Unlike
 *   the flat-window cases in {@link correlation} and {@link beta}, the
 *   numerator here is *not* forced to zero with the denominator, so
 *   `5 / 0` is `Infinity` — and `withColumn` throws on a non-finite value
 *   rather than recording it as a gap. The guard sits at the study's
 *   **output**, which is what makes it observable (the #703 rule: a guard
 *   upstream of a window kernel is absorbed and dies; one at the output is
 *   load-bearing). Unreachable on prices, reachable when `benchmark` is
 *   another study's output that crosses zero, and unit-tested there. A
 *   **negative** benchmark still produces a number — `=== 0`, not `<= 0`,
 *   the {@link percentChangeValues} rule, because a ratio against a negative
 *   level is defined if unusual and clamping it would invent a rule.
 * - **Scale-EQUIVARIANT, not invariant.** Multiplying `column` by `k`
 *   multiplies the reading by `k`; multiplying `benchmark` by `k` divides it
 *   by `k`. Both are pinned as property tests, as identities rather than as
 *   "the numbers changed" — this is the one study in the family that is not
 *   scale-invariant, and a test that asserted invariance would have passed
 *   on a study that had normalised the ratio away.
 * - **A missing cell in either column blanks that row and nothing else** —
 *   a window kernel's recovery rule with a window of one.
 * - **`benchmark` must differ from `column`**: the ratio would be `1`
 *   everywhere.
 */
export function priceRelative<
  S extends SeriesSchema,
  const Output extends string = 'priceRel',
>(series: TimeSeries<S>, options: PriceRelativeOptions<S, Output>) {
  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const benchmark = options.benchmark as string;
  const output = (options.output ?? 'priceRel') as Output;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  if (benchmark === column) {
    throw new TypeError(
      `priceRelative benchmark '${benchmark}' is the same column as 'column'; the ratio would be 1 on every bar`,
    );
  }
  assertColumn(wide, benchmark, 'benchmark');
  assertNoColumn(wide, output);

  const value = columnValues(wide, column);
  const base = columnValues(wide, benchmark);
  const length = value.length;
  const out = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    const b = base[i]!;
    // LIVE guard: the division is this study's last step, so an `Infinity`
    // would reach `withColumn` and throw rather than land as a gap.
    out[i] = b === 0 ? NaN : value[i]! / b;
  }
  return series.withColumn(output, out);
}
