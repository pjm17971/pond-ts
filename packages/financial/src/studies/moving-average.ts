import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  SmoothAppendSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_SOURCE } from '../contract/columns.js';
import {
  assertNoColumn,
  assertPeriod,
  rollingValues,
} from '../kernels/rolling.js';
import {
  assertMaType,
  movingAverageColumn,
} from '../kernels/moving-average.js';
import type { MaType } from '../kernels/moving-average.js';

/** Options shared by the single-line moving averages. `column` is the source
 *  field (default `close`); `output` names the appended column. */
export interface MovingAverageOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Window length in **bars** (a count, not a duration). */
  period: number;
  /** Source column to average. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default the study name** (`'sma'` / `'ema'`);
   *  pass an explicit name to stack several (e.g. `sma20`, `sma50`). */
  output?: Output;
}

/**
 * **Simple moving average** — the mean of the last `period` bars, appended as a
 * new column. A trailing **count** window (correct across session gaps), warmed
 * up length-preservingly (`undefined` for the first `period - 1` bars). Runs
 * over any numeric `column`, including another study's output.
 */
export function sma<
  S extends SeriesSchema,
  const Output extends string = 'sma',
>(series: TimeSeries<S>, options: MovingAverageOptions<S, Output>) {
  assertPeriod(options.period);
  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const output = (options.output ?? 'sma') as Output;
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);
  const values = rollingValues(wide, column, 'avg', options.period);
  return series.withColumn(output, values);
}

/**
 * **Exponential moving average** — a `period`-span EMA (`α = 2/(period+1)`, the
 * financial convention), appended as a new column. Length-preserving warm-up
 * (`undefined` for the first `period - 1` bars) via the ema `minSamples` gate,
 * so it lines up on the source's time axis. Composes on core's `smooth('ema')`.
 */
export function ema<
  S extends SeriesSchema,
  const Output extends string = 'ema',
>(
  series: TimeSeries<S>,
  options: MovingAverageOptions<S, Output>,
): TimeSeries<SmoothAppendSchema<S, Output>> {
  assertPeriod(options.period);
  const column = (options.column ??
    DEFAULT_SOURCE) as NumericColumnNameForSchema<S>;
  const output = (options.output ?? 'ema') as Output;
  assertNoColumn(series as unknown as TimeSeries<SeriesSchema>, output);
  // `output` is always supplied, so smooth resolves to its append branch; the
  // declared return is that branch (a cast past smooth's deferred conditional).
  return series.smooth(column, 'ema', {
    span: options.period,
    minSamples: options.period,
    output,
  }) as unknown as TimeSeries<SmoothAppendSchema<S, Output>>;
}

/** Options for {@link movingAverage} — {@link MovingAverageOptions} plus the
 *  shared **MA type** vocabulary the corpus's ~25 "MA Type" studies expose. */
export interface MovingAverageTypeOptions<
  S extends SeriesSchema,
  Output extends string,
> extends MovingAverageOptions<S, Output> {
  /** Which moving average. **Default `'sma'`.** */
  type?: MaType;
}

/**
 * **Moving average, by type** — the K2 engine as one study: `type` picks from
 * the shared {@link MaType} menu (`sma`, `ema`, `wma`, `smma`, `dema`, `tema`,
 * `trima`, `hull`, `kama`, `zlema`), everything else is the package's uniform
 * shape (bar-count `period`, any numeric `column`, length-preserving warm-up,
 * `output` names the appended column, default `'ma'`).
 *
 * `type: 'sma'` and `type: 'ema'` are the same calls {@link sma} and
 * {@link ema} make, so they produce identical values — this is one more name
 * for them, not a second implementation. `sma()` / `ema()` stay as the
 * shorthand for the two everyone reaches for.
 *
 * Per-type definitions, warm-up lengths, the TA-Lib deltas and the
 * interior-gap rule (window types recover; `smma` and `kama` propagate) are
 * documented on `movingAverageValues`.
 */
export function movingAverage<
  S extends SeriesSchema,
  const Output extends string = 'ma',
>(series: TimeSeries<S>, options: MovingAverageTypeOptions<S, Output>) {
  assertPeriod(options.period);
  const type = options.type ?? 'sma';
  assertMaType(type);
  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const output = (options.output ?? 'ma') as Output;
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);
  const values = movingAverageColumn(wide, column, options.period, type);
  return series.withColumn(output, values);
}
