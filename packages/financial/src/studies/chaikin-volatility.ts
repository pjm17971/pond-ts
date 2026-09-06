import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import { movingAverageValues } from '../kernels/moving-average.js';
import { percentChangeValues } from '../kernels/rate-of-change.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';
import { barRangeValues } from '../kernels/typical-price.js';

export interface ChaikinVolatilityOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** EMA span over the bar range, in **bars**. **Default `10`.** */
  period?: number;
  /** Rate-of-change look-back over that EMA, in **bars**. **Default `10`.** */
  rocPeriod?: number;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'chaikinVol'`.** */
  output?: Output;
}

/**
 * **Chaikin Volatility** (Marc Chaikin) — the **percent rate of change of a
 * smoothed bar range**: how much wider (or narrower) the average bar has
 * become over the last `rocPeriod` bars.
 *
 * ```
 * E = EMA(high − low, period)
 * ${output} = 100 × (E[i] − E[i − rocPeriod]) / E[i − rocPeriod]
 * ```
 *
 * Appends one column. Reads **high and low**, each named by an option
 * defaulting to its `DEFAULT_OHLCV` name (the {@link atr} precedent).
 *
 * Chaikin's reading of it: a *rising* value means the range is expanding,
 * which he associated with tops (panic widens bars), and a slow decline with
 * bottoms (accumulation narrows them). It is a **rate**, not a level — the
 * number says nothing about whether the market is volatile, only whether it
 * is becoming more so.
 *
 * ## Definition — the choices, and who arbitrates them
 *
 * **TA-Lib has no Chaikin Volatility**, so the oracle is a pandas
 * replication of the definition above with the analytic first-valid bar
 * asserted, and the two forks below are pinned rather than left to a vendor:
 *
 * - **Plain range, not true range.** `high − low` per Chaikin, through the
 *   shared {@link barRangeValues} kernel; an overnight gap widens
 *   {@link atr} and does not widen this. The corpus maps it that way and
 *   every published statement of it agrees. There is no `range | trueRange`
 *   knob — that would be two indicators behind a flag (the {@link keltner}
 *   precedent).
 * - **The EMA is pond's**, first-sample seed, `α = 2/(period + 1)` — the same
 *   recursion `ema()` runs, through the K2 engine's array door, so this study
 *   cannot disagree with `ema()` inside the package (the {@link macd} and
 *   {@link trix} precedent). TA-Lib would seed on the SMA of the first
 *   `period` values; nothing here is checked against it because there is
 *   nothing to check against.
 * - **`period` and `rocPeriod` are separate, both defaulting to 10.**
 *   Chaikin's own statement uses 10 for both and most vendors expose one
 *   number; two options is the honest shape, because the smoothing span and
 *   the look-back are different quantities and a caller comparing against a
 *   chart that splits them (ChartIQ does) needs to be able to say so.
 * - **A percent, not a fraction.** `× 100`, matching {@link percentChange}
 *   whose kernel this composes on — so `chaikinVolatility` and
 *   `percentChange` cannot drift on the base case or on the zero-base rule.
 *
 * ## Warm-up
 *
 * The EMA's array door emits once `period` finite ranges have been consumed,
 * so `E` first lands on bar `period − 1`; the rate of change reads a bar
 * `rocPeriod` back, so the column first lands on **`period − 1 +
 * rocPeriod`** — bar 19 at the defaults. Length-preserving.
 *
 * ## Edges
 *
 * - **Scale-invariant, and shift-invariant.** The range is a difference (so
 *   adding a constant to every price leaves it alone) and the reading is a
 *   ratio of two ranges (so scaling every price leaves it alone too). Both
 *   halves are pinned by property tests — an implementation that dropped the
 *   normalisation would keep the shift invariance and lose the scale one.
 * - **Unbounded, and signed.** A range that doubled reads `+100`; one that
 *   halved reads `−50`. The floor is `−100` (a range that fell to zero), the
 *   ceiling is not bounded at all.
 * - **A zero base → `undefined`**, inherited from
 *   {@link percentChangeValues} rather than restated: percent change off a
 *   zero range has no answer, and `x/0` would be an infinity `withColumn`
 *   rejects. `E[i − rocPeriod] = 0` needs every bar from the series' start to
 *   that one to have had no range at all — a halted instrument, reachable and
 *   unit-tested. (A *negative* base still produces a number, per the same
 *   kernel's `=== 0` rule; that only arises if `high` and `low` are
 *   redirected at columns that cross.)
 * - **A leading gap shifts the start**; an **interior** gap blanks the bar
 *   and the bar `rocPeriod` later (the rate of change reads a predecessor),
 *   and the EMA then carries on — the `ema` family's skip rule, not the
 *   Wilder family's carry-to-the-end.
 */
export function chaikinVolatility<
  S extends SeriesSchema,
  const Output extends string = 'chaikinVol',
>(series: TimeSeries<S>, options: ChaikinVolatilityOptions<S, Output> = {}) {
  const period = options.period ?? 10;
  const rocPeriod = options.rocPeriod ?? 10;
  assertPeriod(period);
  assertPeriod(rocPeriod, 'rocPeriod');

  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const output = (options.output ?? 'chaikinVol') as Output;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const range = barRangeValues(
    columnValues(wide, highName),
    columnValues(wide, lowName),
  );

  // The range is a DERIVED array, so the EMA goes through the K2 engine's
  // array door — which steps over a leading run of gaps and waits for
  // `period` finite values rather than `period` rows. That is what makes the
  // warm-up compose when `high`/`low` are themselves another study's output.
  const smoothed = movingAverageValues(range, period, 'ema');

  // The whole of the rest of the study, including the `period`-row look-back
  // rule and the zero-base guard, is `percentChange`'s definition — shared
  // rather than restated, so the two cannot disagree on an edge.
  return series.withColumn(output, percentChangeValues(smoothed, rocPeriod));
}
