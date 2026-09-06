import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import { accumulationDistributionValues } from '../kernels/close-location.js';
import { assertNoColumn, columnValues } from '../kernels/rolling.js';

export interface AccumulationDistributionOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Close column. **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Volume column. **Default `'volume'`.** */
  volume?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'ad'`.** */
  output?: Output;
}

/**
 * **Accumulation/Distribution line** (Marc Chaikin) — a running total of each
 * bar's volume signed by *where in its own range it closed*:
 *
 * ```
 * CLV[i] = ((close − low) − (high − close)) / (high − low)      in [−1, +1]
 * AD[i]  = AD[i−1] + CLV[i] · volume[i]
 * ```
 *
 * The {@link obv} idea with a finer signal. OBV counts a bar's whole volume
 * as buying or selling on the sign of the close change alone; A/D grades it
 * by the close's position in the bar, so a bar that rallies and gives it all
 * back contributes little in either direction. Like OBV the **level is
 * arbitrary** — the line is read for its slope and for divergence against
 * price — so this study has **no `period`**: there is nothing to size a
 * window over.
 *
 * Appends one column, defined from the **first bar**: A/D's term needs only
 * the bar itself (no previous close), so unlike OBV there is not even a
 * seeding convention to pick.
 *
 * Both halves live in kernels — {@link clvValues} derives the per-bar
 * location, {@link cumulativeValues} accumulates — and the composed
 * {@link accumulationDistributionValues} is what {@link chaikinOscillator}
 * smooths, so the two studies read one array rather than two derivations.
 *
 * ## Four inputs
 *
 * High, low, close and volume, each named by an option defaulting to its
 * `DEFAULT_OHLCV` name — the {@link atr} rule, applied once more.
 *
 * ## Definition, verified
 *
 * **TA-Lib's `AD`, exactly** on gap-free bars with a range: cross-checked
 * bar-for-bar in the oracle fixture (delta `0`, and an identical — empty —
 * warm-up mask). No `period`, so there is one case rather than two.
 *
 * ## Edges — the running-sum rules, plus the flat bar
 *
 * - **A leading gap in any input shifts the seed** to the first bar where
 *   all four are present, which is what lets the line run over another
 *   study's output rather than come back empty ({@link obv}'s rule).
 * - **An interior gap propagates to the end.** Every level after an unknown
 *   term is unknown; skipping it would report a level silently off by the
 *   missing contribution for the rest of the series. Fill before running if
 *   you need continuity.
 * - **A flat bar (`high === low`) is treated as a gap and stops the line.**
 *   Its close location is `0/0`, and pond reports no answer rather than a
 *   conventional zero. **TA-Lib disagrees** — it contributes `0` for such a
 *   bar and carries on; measured on twelve bars with bar 3 flattened, TA-Lib
 *   reports `[0, 100, 100, 100, 100, 400, …, 1900]`. The delta and the
 *   reasoning are on {@link clvValues} and
 *   {@link accumulationDistributionValues}; the short version is that a bar
 *   with no range is usually a halt, and `0` there reads as "buyers and
 *   sellers exactly balanced", which is a claim the bar did not make.
 * - **Scale behaviour:** linear in volume, and **invariant under any affine
 *   change of price** — scaling or shifting every price leaves the close
 *   location, and so the line, unchanged. Both pinned by property tests.
 */
export function accumulationDistribution<
  S extends SeriesSchema,
  const Output extends string = 'ad',
>(
  series: TimeSeries<S>,
  options: AccumulationDistributionOptions<S, Output> = {},
) {
  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const closeName = (options.close ?? DEFAULT_OHLCV.close) as string;
  const volumeName = (options.volume ?? DEFAULT_OHLCV.volume) as string;
  const output = (options.output ?? 'ad') as Output;
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  return series.withColumn(
    output,
    accumulationDistributionValues(
      columnValues(wide, highName),
      columnValues(wide, lowName),
      columnValues(wide, closeName),
      columnValues(wide, volumeName),
    ),
  );
}
