import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_SOURCE } from '../contract/columns.js';
import { percentChangeValues } from '../kernels/rate-of-change.js';
import { assertNoColumn, columnValues } from '../kernels/rolling.js';

export interface PercentChangeOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Look-back in **bars**. **Default `1`** (bar-over-bar). */
  periods?: number;
  /** Source column. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'pctChange'`.** */
  output?: Output;
}

/**
 * **Percent change** (rate of change) — the percent difference from `periods`
 * bars ago: `(value / value[i − periods] − 1) × 100`. Appends one column;
 * `undefined` for the first `periods` rows (no look-back) and where the prior
 * value is `0`/missing. `periods` counts **bars**, so it's gap-correct on a
 * trading axis.
 *
 * **This is ROC.** TA-Lib's `ROC` is the same formula, and the oracle
 * cross-checks this study against it: exact agreement (`0.0`) with identical
 * warm-up masks at every period tested. There is deliberately no separate
 * `roc` study — one primitive rather than two that differ invisibly. The
 * additive form (`value − value[i − periods]`) is {@link momentum}.
 */
export function percentChange<
  S extends SeriesSchema,
  const Output extends string = 'pctChange',
>(series: TimeSeries<S>, options: PercentChangeOptions<S, Output> = {}) {
  const periods = options.periods ?? 1;
  if (!Number.isInteger(periods) || periods < 1) {
    throw new TypeError('percentChange periods must be a positive integer');
  }
  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const output = (options.output ?? 'pctChange') as Output;
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  // The arithmetic and its two edge rules (no predecessor yet, a zero base)
  // live in `percentChangeValues` so the derived-input callers — TRIX's 1-bar
  // rate of change of a triple EMA, Coppock's sum of two — are the same
  // definition rather than three that agree until they don't.
  const pc = percentChangeValues(columnValues(wide, column), periods);
  return series.withColumn(output, pc);
}
