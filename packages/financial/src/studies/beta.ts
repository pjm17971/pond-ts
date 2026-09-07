import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_SOURCE } from '../contract/columns.js';
import { rollingBivariateValues } from '../kernels/bivariate.js';
import { percentChangeValues } from '../kernels/rate-of-change.js';
import {
  assertColumn,
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';

export interface BetaOptions<S extends SeriesSchema, Output extends string> {
  /** Window in **bars** — the number of one-bar RETURNS regressed, so the
   *  first reading needs `period + 1` prices. **Default `5`** (TA-Lib
   *  `BETA`'s). Must be at least `2`. */
  period?: number;
  /** Source column — a **price**, not a return; the returns are taken
   *  inside. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** The comparison (market / index) column — **required**, and a column
   *  **on this same series**. Its return variance is the denominator. */
  benchmark: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'beta'`.** */
  output?: Output;
}

/**
 * **Beta** — the slope of `column`'s one-bar returns regressed on
 * `benchmark`'s, over a trailing `period`-bar window:
 *
 * ```
 * r[i]     = column[i] / column[i−1] − 1          (and the same for benchmark)
 * ${output} = cov(r, rBenchmark) / var(rBenchmark)
 * ```
 *
 * Appends one column. `1` means the instrument moved one-for-one with the
 * benchmark over the window, `2` twice as hard, `0` unrelated, negative
 * inverse. `undefined` for the first `period` rows — a bar count of
 * **returns**, so it needs `period + 1` prices and the first reading lands
 * on bar `period`, one later than a plain `period`-bar window study.
 *
 * ## Pass PRICES; the returns are taken inside
 *
 * This is the one study in the two-series family that transforms its inputs
 * before measuring, and it is TA-Lib's `BETA` that decides so: TA-Lib takes
 * two price arrays and differences them internally. A caller who has already
 * differenced would get the beta of the *returns of the returns*, so the
 * option is named `column` (a price) and not `returns`. The one-bar return
 * is {@link percentChangeValues}` (v, 1)` — the same ROC every rate-of-change
 * study in the package composes on, so "a one-bar return" means one thing
 * here. It is expressed as a **percent** (×100), which cancels: scaling both
 * return series by the same constant leaves `cov/var` unchanged.
 *
 * ## Argument order is a trap, and TA-Lib's is the opposite of the reading
 *
 * **Measured**: `talib.BETA(a, b, n)` returns `cov(rA, rB) / var(rA)` — the
 * slope of the **second** argument's returns on the **first**'s. So the
 * TA-Lib call equivalent to `beta(wide, { column: 'close', benchmark: 'spy' })`
 * is `talib.BETA(spy, close, period)`, benchmark first, and that is exactly
 * what the oracle generator asserts against (bar-for-bar at `period` 5 and
 * 20). A caller who passes `(stock, index)` in that order gets the slope of
 * the index on the stock — on an affine pair, `BETA(x, 2x+5) = 0.952` where
 * `BETA(2x+5, x) = 1.050`, measured — which is a different number, not a
 * different sign, so nothing about the output says the arguments were
 * swapped. Pond's option names remove the ambiguity: the **`benchmark`** is
 * always the denominator.
 *
 * ## `benchmark = 2 · column + 5` does NOT give a beta of 1
 *
 * Correlation is invariant to an affine transform, so
 * {@link correlation} reads exactly `+1` on that pair — but beta is a slope
 * of **returns**, and an affine transform is not return-preserving:
 * `r_y = Δx / (x + 2.5)` against `r_x = Δx / x`, so the ratio drifts with the
 * price level. **Measured** on a random walk near 100 at `period 5`:
 * `BETA(x, 2x+5) ≈ 0.976`, not `1`. The exactly-`1` pair is `benchmark =
 * k · column` (a pure scale, no shift), which **is** return-preserving; both
 * cases are pinned by tests.
 *
 * ## The comparison series is a COLUMN, not a second `TimeSeries`
 *
 * Join the benchmark in first and name its column — see
 * {@link correlation}'s docstring for the recipe and the reasoning
 * (alignment is `align`/`joinMany`'s job, and a study that took a second
 * series would have to invent an alignment policy core already expresses).
 * A missing `benchmark` column throws.
 *
 * ## Edges
 *
 * - **The window is strict**: all `period` returns of **both** columns must
 *   exist ({@link rollingBivariateValues}), so an interior gap in either
 *   column blanks `period + 1` rows — the gap bar costs two returns — and
 *   then recovers.
 * - **A flat benchmark window reads `undefined`, and TA-Lib reads `0`.** Its
 *   return variance is zero, and so — exactly — is the covariance, so this is
 *   a genuine `0/0` and needs no guard (see the kernel). **Measured**:
 *   `talib.BETA` over a constant first input returns `0.0`. Same deliberate
 *   delta as {@link correlation}'s, for the same reason: `0` would claim "no
 *   relationship" from data that cannot support the claim.
 * - **A zero price is a missing return here, and `0` in TA-Lib.** TA-Lib
 *   substitutes a return of `0` when the previous price is zero;
 *   {@link percentChangeValues} marks it missing (`x/0` is not a percent
 *   change), and the strict window then blanks the `period` rows that read
 *   it. Note it is exactly **one** return that goes missing, and it is the
 *   one **after** the zero — the return *into* a zero price is a legitimate
 *   `−100%`. Unreachable on prices, reachable when `column` is another
 *   study's output; **measured** on a ramp with one zeroed bar, TA-Lib emits
 *   `4.6e-07`, `-3.0e-05`, `-6.0e-05`, `-1.2e-02` across the four affected
 *   bars where this study emits nothing.
 * - **Invariant to scaling either column, NOT to shifting either.** Scaling
 *   a price series scales its returns by exactly one, so beta does not move;
 *   **adding** a constant changes every return (a `+50` on a 100-price series
 *   halves them) and beta moves with it. Both are pinned, the second as a
 *   deliberate inequality — a test that asserted shift-invariance would pass
 *   only on a study that had normalised by the wrong thing.
 * - **`benchmark` must differ from `column`** (beta against itself is `1` by
 *   construction), and **`period` must be at least 2** — a single return has
 *   zero variance.
 */
export function beta<
  S extends SeriesSchema,
  const Output extends string = 'beta',
>(series: TimeSeries<S>, options: BetaOptions<S, Output>) {
  const period = options.period ?? 5;
  assertPeriod(period);
  if (period < 2) {
    throw new TypeError(
      'beta period must be at least 2 (a single return has no variance, so every reading would be 0/0)',
    );
  }
  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const benchmark = options.benchmark as string;
  const output = (options.output ?? 'beta') as Output;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  if (benchmark === column) {
    throw new TypeError(
      `beta benchmark '${benchmark}' is the same column as 'column'; a column has a beta of exactly 1 against itself`,
    );
  }
  assertColumn(wide, column, 'column');
  assertColumn(wide, benchmark, 'benchmark');
  assertNoColumn(wide, output);

  const { covariance, varianceY } = rollingBivariateValues(
    percentChangeValues(columnValues(wide, column), 1),
    percentChangeValues(columnValues(wide, benchmark), 1),
    period,
  );

  const length = covariance.length;
  const out = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    // No zero-variance guard: a flat benchmark window gives an exactly-zero
    // covariance over an exactly-zero variance, so this is `0/0` → `NaN`
    // already. See the kernel's "A flat window needs no guard".
    out[i] = covariance[i]! / varianceY[i]!;
  }
  return series.withColumn(output, out);
}
