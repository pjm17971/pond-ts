import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_SOURCE } from '../contract/columns.js';
import { assertNoColumn, assertPeriod } from '../kernels/rolling.js';
import {
  assertMaType,
  movingAverageColumn,
} from '../kernels/moving-average.js';
import type { MaType } from '../kernels/moving-average.js';

export interface EnvelopeOptions<
  S extends SeriesSchema,
  Prefix extends string,
> {
  /** Window length in **bars**. */
  period: number;
  /** Band half-width as a **percent** of the centre line. **Default `2.5`.** */
  percent?: number;
  /** Centre-line moving average — any of the shared {@link MaType} menu.
   *  **Default `'sma'`.** */
  maType?: MaType;
  /** Source column. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Column-family prefix — `${prefix}Middle` / `Upper` / `Lower`. **Default
   *  `'env'`.** */
  prefix?: Prefix;
}

/**
 * **Moving-average envelope** — a moving average (`maType`, any of the shared
 * {@link MaType} menu, default SMA) with
 * upper/lower bands at ±`percent` % of the centre line: `middle × (1 ±
 * percent/100)`. Appends `${prefix}Middle` / `${prefix}Upper` /
 * `${prefix}Lower`; warm-up rows `undefined`. (Bollinger bands scale with
 * volatility, an envelope by a fixed percent.)
 */
export function envelope<
  S extends SeriesSchema,
  const Prefix extends string = 'env',
>(series: TimeSeries<S>, options: EnvelopeOptions<S, Prefix>) {
  assertPeriod(options.period);
  assertMaType(options.maType ?? 'sma');
  const percent = options.percent ?? 2.5;
  if (!Number.isFinite(percent) || percent <= 0) {
    throw new TypeError('envelope percent must be a positive finite number');
  }
  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const prefix = (options.prefix ?? 'env') as Prefix;
  const middleName = `${prefix}Middle` as const;
  const upperName = `${prefix}Upper` as const;
  const lowerName = `${prefix}Lower` as const;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  for (const name of [middleName, upperName, lowerName]) {
    assertNoColumn(wide, name);
  }

  // The centre line comes from the shared K2 engine, which routes `sma` /
  // `ema` back to the same `rollingValues` / `emaValues` calls this study made
  // directly before the engine existed — so widening the menu changed no
  // number on the two types that were already here (pinned bit-for-bit in
  // `moving-average-kernel.test.ts`).
  const middle = movingAverageColumn(
    wide,
    column,
    options.period,
    options.maType ?? 'sma',
  );
  const f = percent / 100;
  // A missing centre is `NaN` and survives the multiply, so no per-cell
  // `undefined` check is needed ([PND-STUDYBOX]).
  const scale = (factor: number): Float64Array => {
    const out = new Float64Array(middle.length);
    for (let i = 0; i < out.length; i += 1) out[i] = middle[i]! * factor;
    return out;
  };

  return series
    .withColumn(middleName, middle)
    .withColumn(upperName, scale(1 + f))
    .withColumn(lowerName, scale(1 - f));
}
