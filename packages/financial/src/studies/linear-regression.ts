import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_SOURCE } from '../contract/columns.js';
import {
  assertRegressionPeriod,
  linearRegressionAt,
  linearRegressionValues,
} from '../kernels/linear-regression.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';

/** Radians → degrees, for `${prefix}Angle`. */
const DEGREES_PER_RADIAN = 180 / Math.PI;

export interface LinearRegressionOptions<
  S extends SeriesSchema,
  Prefix extends string,
> {
  /** Regression window in **bars**. Must be at least 2. **Default `14`.** */
  period?: number;
  /** Source column. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Column-family prefix — appends `${prefix}Value` / `${prefix}Slope` /
   *  `${prefix}Intercept` / `${prefix}Angle` / `${prefix}R2`.
   *  **Default `'linreg'`.** */
  prefix?: Prefix;
}

/**
 * **Rolling linear regression** — the whole family in one study, over a
 * `period`-bar least-squares fit of the column against the bar index
 * (kernel **K7**, corpus §6.7):
 *
 * ```
 * ${prefix}Value      the fit at the window's LAST bar        TA-Lib LINEARREG
 * ${prefix}Slope      change in the fit per bar               TA-Lib LINEARREG_SLOPE
 * ${prefix}Intercept  the fit at the window's FIRST bar       TA-Lib LINEARREG_INTERCEPT
 * ${prefix}Angle      atan(slope) in DEGREES                  TA-Lib LINEARREG_ANGLE
 * ${prefix}R2         the fraction of variance explained      (no TA-Lib function)
 * ```
 *
 * Four of the five are **exact against TA-Lib**, mask and values (measured
 * ≤ 1.3e-12 at `period 14` and ≤ 1.9e-12 at `period 5`, the angle being the
 * loosest of the four; warm-ups identical); the
 * fifth has no vendor implementation and is a pandas replication. One study
 * rather than five because they are five readings of **one** fit: splitting
 * them would run the same O(N) regression up to five times to append
 * columns that are two multiplications of each other — the {@link macd} /
 * {@link directionalMovement} family rule.
 *
 * ## One warm-up, unlike the other families
 *
 * Every column lands on bar `period − 1` and none later: they are all read
 * off the same fit, so there is no column here whose definition needs more
 * bars than another's (contrast `macd`'s signal or the DMS' `ADX`). That is
 * also TA-Lib's lookback for all four of its functions.
 *
 * ## `${prefix}Intercept` is the window's FIRST bar, not its last
 *
 * The regression's `x` runs `0 … period − 1` with `x = 0` the **oldest**
 * bar in the window, so the intercept is the fitted price `period − 1` bars
 * *ago* and `${prefix}Value` is the fitted price now. TA-Lib publishes the
 * same pair, and the identity is asserted in the oracle rather than
 * assumed: `Value = Intercept + Slope·(period − 1)`, and
 * {@link timeSeriesForecast} is the same line one bar further on.
 *
 * A reader who expects "intercept" to mean "the line's value at the current
 * bar" will read it upside down on a trending series — at `period 14` on
 * the oracle input the two differ by up to **13.68 points**, on a series
 * whose entire range is 19.4 — which is why both are appended rather than
 * one.
 *
 * ## `${prefix}Angle` is scale-DEPENDENT, and that is TA-Lib's definition
 *
 * `atan(slope)` treats a slope of "1 price unit per bar" as 45°, so the
 * angle depends on the **units of the price**: the same instrument quoted
 * in cents rather than dollars reads a different angle, and a $400 stock
 * and a $4 stock moving the same *percentage* per bar do not. Nothing
 * normalises it — not by price, not by σ, not by the chart's aspect ratio,
 * which is what the "angle" metaphor is borrowed from.
 *
 * TA-Lib's `LINEARREG_ANGLE` does exactly this and this study matches it
 * bar-for-bar, so the column is here for parity. It is stated this plainly
 * because the property is invisible in the name: the batch's property tests
 * assert that scaling the input **moves** the angle (every other reading in
 * the family is either invariant or equivariant), and a caller who wants a
 * scale-free trend reading wants `${prefix}R2` or a slope divided by price.
 *
 * ## `${prefix}R2` — how well the line fits, `0 … 1`
 *
 * The coefficient of determination, `corr(x, y)²`: `1` is a perfect
 * straight line over the window and `0` is a slope that explains nothing.
 * It is invariant to **both** scale and shift (a correlation is), which
 * makes it the one column in the family a caller can compare across
 * instruments. TA-Lib has no equivalent — its `CORREL` is between two
 * series, not against the bar index — so the oracle is a pandas
 * replication with the analytic first-valid bar asserted and a measured
 * separation from the un-squared `|corr|`.
 *
 * ## Edges
 *
 * - **`period` must be at least 2.** One point does not determine a line —
 *   the regression denominator `n²(n²−1)/12` is exactly `0` at `n = 1` —
 *   so the study throws rather than emitting `0/0` on every bar (the
 *   {@link choppinessIndex} `log10(1) = 0` precedent).
 * - **A flat window: slope `0`, `R2` `undefined`.** The slope's numerator
 *   is *forced* to zero by the same condition that empties it, so `0` is a
 *   real reading (the {@link accumulationDistribution} flat-bar case) and
 *   `Value`, `Intercept` and the forecast all read the flat price back.
 *   `R2` is a genuine `0/0` — a line explaining all of zero variance — and
 *   is `undefined`, the {@link stochastic} flat-window case. Both are
 *   exact, not approximated; see the kernel's note on why that costs a
 *   counter.
 * - **Scale and shift**: `Value` and `Intercept` are *equivariant* to both
 *   (`f(a·y + b) = a·f(y) + b`), `Slope` is equivariant to scale and
 *   invariant to shift, `R2` is invariant to both, and `Angle` is invariant
 *   to shift only. Each is pinned by its own property test rather than by
 *   one loop, because the five answers genuinely differ.
 * - **The strict window**: a bar is emitted only when all `period` cells
 *   are finite, because `x` names a *position* and dropping a cell would
 *   fit the line against the wrong abscissa (the `wma` rule). So a
 *   **leading** gap shifts the start and an **interior** gap blanks
 *   `period` bars and then recovers.
 * - **A misnamed `column` reads all-missing** rather than throwing —
 *   {@link columnValues}' door, the {@link atr} behaviour.
 */
export function linearRegression<
  S extends SeriesSchema,
  const Prefix extends string = 'linreg',
>(series: TimeSeries<S>, options: LinearRegressionOptions<S, Prefix> = {}) {
  const period = options.period ?? 14;
  assertPeriod(period);
  assertRegressionPeriod(period, 'linearRegression');
  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const prefix = (options.prefix ?? 'linreg') as Prefix;
  const valueName = `${prefix}Value` as const;
  const slopeName = `${prefix}Slope` as const;
  const interceptName = `${prefix}Intercept` as const;
  const angleName = `${prefix}Angle` as const;
  const r2Name = `${prefix}R2` as const;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  for (const name of [valueName, slopeName, interceptName, angleName, r2Name]) {
    assertNoColumn(wide, name);
  }

  const fit = linearRegressionValues(columnValues(wide, column), period);
  // The fit read at the window's last bar — TA-Lib's LINEARREG, and the one
  // reading of the five that is not already an array the kernel returns.
  const value = linearRegressionAt(fit, period - 1);
  const angle = new Float64Array(fit.slope.length);
  for (let i = 0; i < angle.length; i += 1) {
    // Degrees, TA-Lib's convention. `atan(NaN)` is `NaN`, so the warm-up
    // needs no separate mask.
    angle[i] = Math.atan(fit.slope[i]!) * DEGREES_PER_RADIAN;
  }

  return series
    .withColumn(valueName, value)
    .withColumn(slopeName, fit.slope)
    .withColumn(interceptName, fit.intercept)
    .withColumn(angleName, angle)
    .withColumn(r2Name, fit.r2);
}
