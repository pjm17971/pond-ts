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

export interface TimeSeriesForecastOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Regression window in **bars**. Must be at least 2. **Default `14`.** */
  period?: number;
  /** Source column. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'tsf'`.** */
  output?: Output;
}

/**
 * **Time Series Forecast** (TA-Lib `TSF`) — the `period`-bar least-squares
 * fit of the column against the bar index, projected **one bar past the
 * window**:
 *
 * ```
 * ${output}[i] = intercept[i] + slope[i] · period
 * ```
 *
 * where `intercept` is the fit at the window's first bar (`x = 0`), so
 * `x = period − 1` is the current bar ({@link linearRegression}'s
 * `linregValue`) and `x = period` is the next one. It is a *forecast* only
 * in the narrow sense that it extrapolates the fitted line by one step;
 * nothing here estimates a distribution, and the value is plotted on the
 * current bar like every other study in the package.
 *
 * **Exact against TA-Lib**, mask and values (measured ≤ 2.1e-13 at
 * `period 14`, identical warm-up at `period − 1`).
 *
 * ## Also called the "time series moving average"
 *
 * It is a smoother — an end-point-projected regression line — and vendors
 * list it in their moving-average menus under that name. It is deliberately
 * **not** a member of the K2 {@link MaType} engine, and the reason is a
 * contract the engine has and a regression cannot keep: every type in that
 * menu is the identity at `period 1`, and a one-bar window has no slope
 * (the regression denominator `n²(n²−1)/12` is `0` at `n = 1`). Adding it
 * would mean a per-type minimum period, which is a change to the engine's
 * shape rather than one more `case` — so this ships as a study, and a
 * caller who wants a regression smooth calls it directly.
 *
 * ## Edges
 *
 * - **`period` must be at least 2** (see above), and the study throws
 *   rather than emitting `0/0`.
 * - **A flat window reads the flat price back**, exactly: the slope is
 *   forced to zero, so the projection is the level itself.
 * - **Equivariant to scale and shift** — `tsf(a·y + b) = a·tsf(y) + b`,
 *   like any linear filter and like every {@link MaType} in the engine.
 * - **The strict window** (all `period` cells finite): a leading gap shifts
 *   the start, an interior gap blanks `period` bars and then recovers. See
 *   {@link linearRegressionValues} for why a positional regressor cannot
 *   skip a cell.
 * - **A misnamed `column` reads all-missing** rather than throwing.
 */
export function timeSeriesForecast<
  S extends SeriesSchema,
  const Output extends string = 'tsf',
>(series: TimeSeries<S>, options: TimeSeriesForecastOptions<S, Output> = {}) {
  const period = options.period ?? 14;
  assertPeriod(period);
  assertRegressionPeriod(period, 'timeSeriesForecast');
  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const output = (options.output ?? 'tsf') as Output;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const fit = linearRegressionValues(columnValues(wide, column), period);
  return series.withColumn(output, linearRegressionAt(fit, period));
}
