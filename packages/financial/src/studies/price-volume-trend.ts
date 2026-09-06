import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import { cumulativeValues } from '../kernels/cumulative.js';
import { percentChangeValues } from '../kernels/rate-of-change.js';
import { assertNoColumn, columnValues } from '../kernels/rolling.js';

export interface PriceVolumeTrendOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Close column. **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Volume column. **Default `'volume'`.** */
  volume?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'pvt'`.** */
  output?: Output;
}

/**
 * **Price-Volume Trend** — a running total of each bar's volume scaled by its
 * **fractional** price change:
 *
 * ```
 * PVT[i] = PVT[i−1] + volume[i] · (close[i] − close[i−1]) / close[i−1]
 * ```
 *
 * The third member of the cumulative-volume family, between {@link obv} and
 * {@link accumulationDistribution} in how much of a bar's volume it counts:
 * OBV takes the whole of it on the *sign* of the close change, A/D grades it
 * by where the bar closed in its range, and PVT scales it by *how far* price
 * moved. A 3% up-bar therefore contributes three times what a 1% one does,
 * where OBV would count them the same. As with both, the level is arbitrary
 * and the **slope** is what is read.
 *
 * No `period` — there is nothing to size a window over.
 *
 * ## The fraction, not the percent
 *
 * The rate of change comes from {@link percentChangeValues}, which is
 * pond's `percentChange` / TA-Lib's `ROC` and therefore returns a **percent**
 * (`×100`). PVT's published definition is the **fraction**, so the study
 * divides by 100 rather than re-deriving the ratio: one definition of "price
 * change", shared with the study TA-Lib validates, and a constant that is
 * visible in one place. (The two conventions differ by a factor of 100 in the
 * level, which matters only when comparing against another package's PVT.)
 *
 * ## The first bar has no value
 *
 * `PVT[0]` is **`undefined`**, not `0`. ChartIQ's definition (and every other
 * published one) is a recursion on the *previous* close, so bar 0 has no term
 * — and unlike {@link obv}, whose `volume[0]` seed is TA-Lib's convention and
 * is matched for exactness, PVT has **no TA-Lib function** and so no
 * convention to defer to. Seeding at `0` would print a level for a bar whose
 * term could not be computed; every later level is identical either way (the
 * seed-at-zero line is this one with a `0` painted on bar 0), so nothing is
 * lost by declining to invent it. This is also just what the composition
 * gives: the leading `NaN` shifts {@link cumulativeValues}' seed.
 *
 * ## Definition, verified
 *
 * TA-Lib has no PVT, so the oracle is a **pandas replication** —
 * `(pct_change · volume).cumsum()` — with the analytic first valid bar (1)
 * asserted, and separated from the {@link obv} shape on the same input so a
 * study that dropped the magnitude could not pass.
 *
 * ## Edges
 *
 * - **A leading gap in either input shifts the seed** to the first bar with
 *   a defined term (bar 1 on clean input), so PVT over another study's
 *   output starts at that study's first value rather than coming back empty.
 * - **An interior gap propagates to the end** — the running-sum rule
 *   ({@link cumulativeValues}). A missing **close** costs two terms (its own
 *   bar and the next, which reads it as the base), which is moot once the
 *   sum has gone.
 * - **A zero previous close** → no term, so the line stops there. That is
 *   {@link percentChangeValues}' guard: `x / 0` is `±Infinity`, which is not
 *   a rate of change. Only reachable on a column that can be zero (a
 *   redirected `close`); real prices are positive.
 * - **Linear in volume; invariant under scaling every price** (the fractional
 *   change is scale-free), but **not** under shifting them — a shift changes
 *   the base of every ratio. Pinned by property tests.
 */
export function priceVolumeTrend<
  S extends SeriesSchema,
  const Output extends string = 'pvt',
>(series: TimeSeries<S>, options: PriceVolumeTrendOptions<S, Output> = {}) {
  const closeName = (options.close ?? DEFAULT_OHLCV.close) as string;
  const volumeName = (options.volume ?? DEFAULT_OHLCV.volume) as string;
  const output = (options.output ?? 'pvt') as Output;
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  // `percentChangeValues` is a percent (×100); PVT's definition is the
  // fraction, hence the /100 (see the docstring).
  const terms = percentChangeValues(columnValues(wide, closeName), 1);
  const volume = columnValues(wide, volumeName);
  for (let i = 0; i < terms.length; i += 1) {
    terms[i] = (terms[i]! / 100) * volume[i]!;
  }
  return series.withColumn(output, cumulativeValues(terms));
}
