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

export interface ChandeForecastOscillatorOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Regression window in **bars**. Must be at least 2. **Default `14`.** */
  period?: number;
  /** Source column. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'cfo'`.** */
  output?: Output;
}

/**
 * **Chande Forecast Oscillator** (Tushar Chande) — how far the price sits
 * from its own {@link timeSeriesForecast}, as a **percentage of the
 * price**:
 *
 * ```
 * ${output}[i] = 100 · (column[i] − TSF[i]) / column[i]
 * ```
 *
 * Positive when the bar closes above the regression line's next step
 * (Chande reads that as the trend running ahead of its own fit) and
 * negative below; it crosses zero where the price meets the forecast.
 * Dividing by the price rather than by the forecast is what makes it
 * comparable across instruments — the {@link disparityIndex} shape, with
 * the regression forecast in place of a moving average.
 *
 * **TA-Lib has no Chande Forecast Oscillator**, so the oracle is a pandas
 * replication built on the TA-Lib-checked `TSF`, with the analytic
 * first-valid bar (`period − 1`) asserted and a measured separation from
 * the plausible wrong turn — subtracting `LINEARREG` (the fit at the
 * window's *last* bar) instead of the one-bar-ahead `TSF`.
 *
 * ## Edges
 *
 * - **`period` must be at least 2** — {@link timeSeriesForecast}'s rule,
 *   inherited: a one-bar window has no slope.
 * - **A zero price reads `undefined`, and the guard is live.** The
 *   division sits at the study's **output**, so an unguarded `x / 0` would
 *   reach `withColumn` as `±Infinity` — which **throws** rather than
 *   mapping to a gap ([PND-WCNAN] covers `NaN`, not infinities). The
 *   numerator is *not* forced to zero with the denominator: a window can
 *   forecast a non-zero level for a bar that prints `0`, so this is the
 *   {@link choppinessIndex} live-guard case rather than the
 *   {@link ulcerIndex} dead one. Unreachable on prices; reachable, and
 *   unit-tested, when `column` is another study's output that crosses zero.
 *   A **negative** price still produces a number, the
 *   {@link percentChange} rule (`=== 0`, not `<= 0`) — a percentage off a
 *   negative base is defined, if unusual, and clamping it would be
 *   inventing a rule.
 * - **A flat window reads exactly `0`** — the forecast is the flat price,
 *   so the numerator is zero and the reading is "the price is exactly on
 *   its forecast", which is information rather than a gap.
 * - **Scale-invariant, not shift-invariant.** Both halves of the numerator
 *   scale with the price and the denominator divides it out, so
 *   multiplying every price leaves the reading unchanged; **adding** a
 *   constant does not — it moves the base of the percentage without moving
 *   the numerator, so every reading shrinks. Both halves are pinned, the
 *   second as a direction rather than merely "different".
 * - **The strict window** (all `period` cells finite) and the all-missing
 *   answer for a misnamed `column` are {@link linearRegressionValues}'.
 */
export function chandeForecastOscillator<
  S extends SeriesSchema,
  const Output extends string = 'cfo',
>(
  series: TimeSeries<S>,
  options: ChandeForecastOscillatorOptions<S, Output> = {},
) {
  const period = options.period ?? 14;
  assertPeriod(period);
  assertRegressionPeriod(period, 'chandeForecastOscillator');
  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const output = (options.output ?? 'cfo') as Output;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const values = columnValues(wide, column);
  const forecast = linearRegressionAt(
    linearRegressionValues(values, period),
    period,
  );

  const length = values.length;
  const out = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    const price = values[i]!;
    // The guard is LIVE — this division is the study's last step, so an
    // Infinity would reach `withColumn` and throw (see "Edges"). `NaN` is
    // not `=== 0`, so a gap falls through and propagates as a gap.
    out[i] = price === 0 ? NaN : (100 * (price - forecast[i]!)) / price;
  }
  return series.withColumn(output, out);
}
