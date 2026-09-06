import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import { cumulativeValues } from '../kernels/cumulative.js';
import { assertNoColumn, columnValues } from '../kernels/rolling.js';
import { signedVolumeValues } from '../kernels/signed-volume.js';

export interface ObvOptions<S extends SeriesSchema, Output extends string> {
  /** Close column. **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Volume column. **Default `'volume'`.** */
  volume?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'obv'`.** */
  output?: Output;
}

/**
 * **On-Balance Volume** — Granville's running total of signed volume:
 *
 * ```
 * OBV[i] = OBV[i−1] + sign(close[i] − close[i−1]) · volume[i]
 * ```
 *
 * A bar that closes up adds its whole volume, one that closes down subtracts
 * it, and an unchanged close adds nothing. The level is arbitrary — it is the
 * *shape* that is read, for divergence against price — so OBV is the first
 * study here with **no `period`**: there is nothing to size a window over.
 *
 * Appends one column, defined from the **first bar**. There is no warm-up:
 * bar 0 has no previous close, but the convention (TA-Lib's, see below) is
 * to seed with its volume rather than wait a bar.
 *
 * The two halves live in kernels: {@link signedVolumeValues} derives the
 * per-bar term and {@link cumulativeValues} accumulates it. The second is
 * shared with every other cumulative volume study (Accumulation/Distribution,
 * Price-Volume Trend), which is why the loop is not here.
 *
 * ## Two inputs
 *
 * OBV reads **close and volume**, each named by an option defaulting to its
 * conventional bar-column name from `DEFAULT_OHLCV` — the same
 * "never hard-code a column" rule {@link atr} applies to its three inputs.
 * Both are columns of one series, so they are aligned by construction.
 *
 * ## Definition
 *
 * **TA-Lib's OBV**, exactly, including its convention for bar 0
 * (`OBV[0] = volume[0]`) and for an unchanged close (adds nothing). Verified
 * against TA-Lib bar-for-bar in the oracle fixture — exact agreement, and an
 * identical (empty) warm-up mask. Some implementations seed at `0` instead;
 * that only shifts the whole line by `volume[0]`, which changes nothing a
 * reader of OBV looks at, and matching TA-Lib lets the oracle assert
 * equality rather than a bound.
 *
 * ## Edges — missing cells in a running sum
 *
 * A gap in a cumulative quantity is a genuinely different problem from a gap
 * in a window: a window recovers once the gap leaves it, but every value
 * after an unknown term in a running sum is itself unknown. The choice made
 * here is the conservative one, and it is the same choice the Wilder
 * recursion makes:
 *
 * - **A leading gap in either input shifts the seed** to the first bar at
 *   which both close and volume are present. This is also what TA-Lib's own
 *   wrapper does (it strips leading `NaN`s before calling the C function),
 *   and it is what lets OBV run over another study's output — pointing
 *   `close` at an `sma` column starts the line at that column's first value
 *   rather than returning nothing.
 * - **An interior gap propagates to the end.** A bar with a missing close or
 *   volume has an unknown contribution, so the level is unknown from that
 *   bar on. Skipping the bar would report a level silently off by the
 *   missing volume for the rest of the series; that is a wrong answer, and
 *   this reports no answer instead. Fill before running OBV if you need
 *   continuity across one. A missing **close** costs two bars (its own, and
 *   the next, whose sign is taken against it), which is moot once the sum
 *   has gone, but is the reason the term kernel is pinned that way.
 *
 * **This is a deliberate delta from TA-Lib on interior gaps**, which has no
 * gap semantics at all: a `NaN` volume propagates through its arithmetic to
 * the end (agreeing with this), but a `NaN` close makes both comparisons
 * false, so TA-Lib silently adds nothing for that bar *and* the one after
 * it, then carries on at a level off by whatever those two bars would have
 * contributed. Measured (TA-Lib 0.7.1) on closes `[10, 11, 11, NaN, 12, 12,
 * 8]` with volumes `[100 … 700]`: TA-Lib reports
 * `[100, 300, 300, 300, 300, 300, −400]` where the gap-free answer (close 9
 * at the gap) is `[100, 300, 300, −100, 400, 400, −300]` — 400 out at the
 * gap bar, 100 out from then on, never recovered, presented as a value.
 * This study reports `undefined` from the gap on.
 */
export function obv<
  S extends SeriesSchema,
  const Output extends string = 'obv',
>(series: TimeSeries<S>, options: ObvOptions<S, Output> = {}) {
  const closeName = (options.close ?? DEFAULT_OHLCV.close) as string;
  const volumeName = (options.volume ?? DEFAULT_OHLCV.volume) as string;
  const output = (options.output ?? 'obv') as Output;
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const signed = signedVolumeValues(
    columnValues(wide, closeName),
    columnValues(wide, volumeName),
  );
  return series.withColumn(output, cumulativeValues(signed));
}
