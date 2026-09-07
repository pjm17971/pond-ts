import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_SOURCE } from '../contract/columns.js';
import { rollingBivariateValues } from '../kernels/bivariate.js';
import {
  assertColumn,
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';

export interface CorrelationOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Window in **bars**. **Default `30`** (TA-Lib `CORREL`'s). Must be at
   *  least `2` — a one-bar window has no variance, so every reading would be
   *  `0/0`. */
  period?: number;
  /** Source column. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** The comparison column — **required**, and a column **on this same
   *  series**. Join the benchmark in first; see the study's docstring. */
  benchmark: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'corr'`.** */
  output?: Output;
}

/**
 * **Correlation Coefficient** — Pearson's `r` between `column` and
 * `benchmark` over a trailing `period`-bar window:
 *
 * ```
 * ${output} = cov(column, benchmark) / sqrt(var(column) · var(benchmark))
 * ```
 *
 * Appends one column in `[−1, +1]`: `+1` is a window in which the two moved
 * in exact lock-step (up to an affine transform), `−1` exact opposition, `0`
 * no linear relationship. `undefined` for the first `period − 1` rows.
 *
 * ## The comparison series is a COLUMN, not a second `TimeSeries`
 *
 * Every study in the two-series family (this, {@link beta},
 * {@link priceRelative}, {@link performanceIndex}) names its comparison
 * series with a **column name on the series it is given** — exactly as
 * {@link atr} names `high`/`low`/`close`. None of them takes a second
 * `TimeSeries`, because **aligning and joining is the consumer's job and
 * core already does it**: two instruments have their own bar clocks,
 * holidays and halts, and a study that took a second series would have to
 * invent an alignment policy (hold? interpolate? inner or outer join?) that
 * `align` and `joinMany` already express, better, and once for the whole
 * pipeline.
 *
 * The recipe — join first, then run the study over the wide series:
 *
 * ```ts
 * import { TimeSeries } from 'pond-ts';
 * import { correlation } from '@pond-ts/financial';
 *
 * // `spy` has its own `close`; rename it so both fit on one row.
 * const wide = TimeSeries.joinMany([bars, spy.rename({ close: 'spy' })], {
 *   type: 'inner',
 * });
 *
 * correlation(wide, { column: 'close', benchmark: 'spy', period: 30 });
 * ```
 *
 * Use `align(sequence)` on each side first when the two do not share a bar
 * clock, and `type: 'inner'` (or an outer join, whose missing rows this
 * study's strict window then blanks) to decide what a one-sided bar means.
 * A **missing `benchmark` column throws** rather than reading empty — it is
 * a required option whose only job is to name that joined column.
 *
 * ## It correlates the PRICES, not the returns
 *
 * This is TA-Lib's `CORREL` bar-for-bar (asserted in the oracle at
 * `period` 30 and 5), and TA-Lib correlates the raw inputs. So a strong
 * reading here mostly says "both series trended over the window", which is a
 * weaker statement than the correlation of returns most texts mean by
 * "correlation between two assets". To get the return correlation, run the
 * study over two return columns — `percentChange` each side first, or
 * `correlation(bars, { column: 'ret', benchmark: 'spyRet' })`. That is a
 * one-line composition and deliberately not an option: a `returns: boolean`
 * flag would be two indicators behind a flag, and it would also stop the
 * study from being TA-Lib-comparable. {@link beta}, by contrast, takes
 * returns **inside**, because that is what TA-Lib's `BETA` does.
 *
 * ## Edges
 *
 * - **The window is strict.** A row emits only when all `period` bars carry
 *   a value in **both** columns — see {@link rollingBivariateValues}. A
 *   correlation over three of the last thirty bars is a different statistic
 *   from the one `period` named, so an interior gap in either column blanks
 *   `period` rows and then recovers.
 * - **A flat window on either side reads `undefined`, and TA-Lib reads `0`.**
 *   A constant column has zero variance, so `r` is a genuine `0 / 0` — there
 *   is no correlation to report, and `0` ("uncorrelated") is a claim the data
 *   does not support. **Measured**: `talib.CORREL` over a constant second
 *   input returns `0.0` on every emitted bar (TA-Lib guards its zero
 *   denominator and substitutes zero). This is the package's standing rule —
 *   a genuine `0/0` is `undefined`, as in {@link stochastic}'s flat window —
 *   and it is the one deliberate delta from `CORREL`. No guard is written
 *   here: the kernel's change counter reports a flat column's variance and
 *   covariance as **exactly** `0`, so the division is already `0/0` → `NaN`
 *   → a missing cell — including a column that goes flat mid-window (a
 *   tick-frozen price), which the accumulators alone got wrong (pinned by
 *   tests at both levels).
 * - **Invariant to an independent scale AND shift of either column** — that
 *   is what Pearson's `r` is, and both halves are pinned as property tests.
 *   `benchmark = 2 · column + 5` therefore reads `+1`, and a negative
 *   multiplier `−1`.
 * - **`benchmark` must differ from `column`.** Correlating a column with
 *   itself is `+1` by construction, so it is a mistake rather than a
 *   degenerate reading, and it throws.
 * - **No `±1` clamp, and the overshoot is real.** **Measured**: on
 *   `benchmark = −3 · column + 1000` — a perfectly anti-correlated pair —
 *   one window reads `−1.0000000000000002`, two ulps past the bound, while
 *   the `+1` side of the same test is bit-exact. A clamp would remove 2e-16
 *   that no caller's threshold can see, and it would be one more branch to
 *   keep alive; both halves are pinned by a test instead, so the behaviour
 *   is chosen rather than discovered on a chart.
 */
export function correlation<
  S extends SeriesSchema,
  const Output extends string = 'corr',
>(series: TimeSeries<S>, options: CorrelationOptions<S, Output>) {
  const period = options.period ?? 30;
  assertPeriod(period);
  if (period < 2) {
    throw new TypeError(
      'correlation period must be at least 2 (a one-bar window has no variance, so every reading would be 0/0)',
    );
  }
  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const benchmark = options.benchmark as string;
  const output = (options.output ?? 'corr') as Output;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  if (benchmark === column) {
    throw new TypeError(
      `correlation benchmark '${benchmark}' is the same column as 'column'; a column correlates with itself at exactly 1`,
    );
  }
  assertColumn(wide, benchmark, 'benchmark');
  assertNoColumn(wide, output);

  const { covariance, varianceX, varianceY } = rollingBivariateValues(
    columnValues(wide, column),
    columnValues(wide, benchmark),
    period,
  );

  const length = covariance.length;
  const out = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    // No zero-variance guard: the kernel writes a flat column's variance
    // and covariance as exact `0` (change counter), so this is already
    // `0/0` → `NaN` → a missing cell. See the kernel's "A flat window".
    const r = covariance[i]! / Math.sqrt(varianceX[i]! * varianceY[i]!);
    // |r| ≤ 1 in exact arithmetic; the kernel rebuilds any window whose
    // moments could overshoot materially, so what is left is last-ulp
    // rounding, pinned to the bound rather than reported.
    out[i] = r > 1 ? 1 : r < -1 ? -1 : r;
  }
  return series.withColumn(output, out);
}
