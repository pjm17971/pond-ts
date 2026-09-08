import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_SOURCE } from '../contract/columns.js';
import {
  assertNoColumn,
  assertPeriod,
  rollingColumns,
} from '../kernels/rolling.js';

export interface BollingerOptions<
  S extends SeriesSchema,
  Prefix extends string,
> {
  /** Window length in **bars**. */
  period: number;
  /** Band half-width in standard deviations. **Default `2`.** */
  stdDev?: number;
  /** Source column. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Column-family prefix — appends `${prefix}Middle` / `Upper` / `Lower`.
   *  **Default `'bb'`.** */
  prefix?: Prefix;
}

/**
 * **Bollinger Bands®** (John Bollinger) — a `period`-bar simple moving average
 * (the middle band) with an upper/lower band at ±`stdDev` population standard
 * deviations. Appends three columns (`${prefix}Middle` / `${prefix}Upper` /
 * `${prefix}Lower`); the warm-up rows emit `undefined`. One rolling pass
 * (avg + stdev) over a bar-count window.
 *
 * **A flat window (σ = 0) is a degenerate band, not a missing one:**
 * `upper = lower = middle`. The bands are defined there — `middle ± k·0` —
 * and a chart drawing them over a stale or illiquid stretch wants the
 * ribbon to collapse onto the centre line, not break into segments around
 * an unbroken middle. `undefined` means warm-up only, as it does for
 * {@link keltner}'s zero-range channel. (Until [PND-BBFLAT] the bands were
 * blanked at σ = 0 so that "outside the band" tests would not fire on every
 * bar of a flat stretch; that is the consumer's test to write —
 * `bbUpper > bbLower` — not a hole in the data.) The two derived studies
 * keep their own flat-window answers: {@link bollingerBandwidth} is `0`
 * there and {@link bollingerPercentB} is `undefined` (a genuine 0/0).
 */
export function bollinger<
  S extends SeriesSchema,
  const Prefix extends string = 'bb',
>(series: TimeSeries<S>, options: BollingerOptions<S, Prefix>) {
  assertPeriod(options.period);
  const stdDev = options.stdDev ?? 2;
  if (!Number.isFinite(stdDev) || stdDev <= 0) {
    throw new TypeError('bollinger stdDev must be a positive finite number');
  }
  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const prefix = (options.prefix ?? 'bb') as Prefix;
  const middleName = `${prefix}Middle` as const;
  const upperName = `${prefix}Upper` as const;
  const lowerName = `${prefix}Lower` as const;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  for (const name of [middleName, upperName, lowerName]) {
    assertNoColumn(wide, name);
  }

  // One rolling pass reduces both the middle (avg) and the band width (stdev).
  const rolled = rollingColumns(
    wide,
    {
      middle: { from: column, using: 'avg' },
      sd: { from: column, using: 'stdev' },
    },
    options.period,
  );
  const middle = rolled['middle']!;
  const sd = rolled['sd']!;
  // A missing centre or σ is `NaN` ([PND-STUDYBOX]) and propagates through
  // the arithmetic on its own; a flat window (σ = 0) is `middle ± 0`, the
  // degenerate band the docstring names — no guard here at all.
  const band = (sign: 1 | -1): Float64Array => {
    const out = new Float64Array(middle.length);
    for (let i = 0; i < out.length; i += 1) {
      out[i] = middle[i]! + sign * stdDev * sd[i]!;
    }
    return out;
  };

  return series
    .withColumn(middleName, middle)
    .withColumn(upperName, band(1))
    .withColumn(lowerName, band(-1));
}
