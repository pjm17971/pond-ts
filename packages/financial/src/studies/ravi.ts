import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_SOURCE } from '../contract/columns.js';
import {
  assertNoColumn,
  assertPeriod,
  rollingValues,
} from '../kernels/rolling.js';

export interface RaviOptions<S extends SeriesSchema, Output extends string> {
  /** Short simple-average length, in **bars**. **Default `7`.** */
  shortPeriod?: number;
  /** Long simple-average length, in **bars**. **Default `65`.** */
  longPeriod?: number;
  /** Source column. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Appended column name. **Default `'ravi'`.** */
  output?: Output;
}

/**
 * **Range Action Verification Index** (Tushar Chande) — how far apart two
 * simple averages have got, as a percentage of the slower one:
 *
 * ```
 * ${output} = 100 · |SMA(column, shortPeriod) − SMA(column, longPeriod)|
 *                 / SMA(column, longPeriod)
 * ```
 *
 * Appends one column, non-negative. It answers one question — *is this
 * market trending or ranging?* — and deliberately not *which way*: the
 * **absolute value** is Chande's, and it is what makes the reading a single
 * threshold test rather than a two-sided one. Chande's rule is that above
 * **3%** the market is trending and below it is in a trading range, which is
 * why the reading is scaled to a percent rather than left in price units.
 *
 * It is the same two averages {@link priceOscillator} takes the percent
 * difference of; the difference is the absolute value and the intent.
 * `priceOscillator({ mode: 'percent' })` is signed and answers "which
 * direction is the fast average on"; this is unsigned and answers "is there
 * a direction at all".
 *
 * ## F-AMBIG — the periods, and Chande's own
 *
 * The corpus flags the period variants. **Chande's `7 / 65` ship.** The 65
 * is not arbitrary: it is a quarter's worth of trading days (13 weeks), so
 * the long average carries the market's quarterly view, and the short one is
 * about a tenth of it. Vendors ship other pairs — `13 / 65` and short
 * intraday sets are both common — and they are reachable through the
 * options; naming Chande's as the default is the F-AMBIG discipline, not a
 * claim that the others are wrong.
 *
 * The **signed** form (dropping the absolute value) also circulates. It is a
 * genuinely different reading rather than a sign convention, because the
 * threshold rule stops working: measured on the package's long oracle input
 * the two sit **26.98 apart** where the fast average is below the slow, on
 * a reading whose own range is 0.00 … 16.40 (`scripts/oracle/generate.py`)
 * — more than the whole scale, because the signed form goes negative where
 * this one does not. The generator asserts it.
 *
 * That case runs on the oracle's **long** 900-bar input rather than its
 * 80-bar one, and for a reason worth recording: over the sixteen readings a
 * 65-bar average leaves on the short fixture, the 7-bar average never once
 * crosses below it, so the absolute value — the whole point of the study —
 * would have been untested.
 *
 * ## Option names — `shortPeriod` / `longPeriod`
 *
 * Chande writes them as bare lengths and vendors label them "short" and
 * "long", but both bare words are **position** vocabulary in a financial
 * package ("long" reads as a side, not a length), so they carry the
 * `Period` suffix every other length option in this package does — the same
 * call {@link trueStrengthIndex} and {@link coppock} made.
 *
 * ## Warm-up
 *
 * Length-preserving: the reading needs both averages, so it starts at
 * `max(shortPeriod, longPeriod) − 1` — bar 64 at the defaults. Both averages come from the
 * **column** door (core's count-window `avg`), so each is exactly the
 * average `sma()` gives over the same column, and a missing cell is skipped
 * by core's reducer rather than blanking the window.
 *
 * ## Edges
 *
 * - **Scale-invariant, NOT shift-invariant.** The percent normalisation
 *   cancels a multiplicative change; adding a constant to every price moves
 *   the denominator without moving the numerator, so the reading falls. Both
 *   pinned as property tests — a study that quietly dropped the division
 *   would have the opposite pair.
 * - **A zero long average → `undefined`.** Reachable only when `column` is a
 *   study output that averages to exactly zero (an oscillator around zero
 *   does this), never on prices. Nothing forces the numerator to zero with
 *   it, so the division is a real number over zero and the guard is live; a
 *   test pins it.
 * - **`shortPeriod` may equal or exceed `longPeriod`** — that is not
 *   rejected. Unlike {@link macd}, where the ordering flips the sign of the
 *   whole reading, an absolute difference is symmetric, so an inverted pair
 *   gives the same non-negative number with a slower warm-up. There is
 *   nothing to guard against.
 */
export function ravi<
  S extends SeriesSchema,
  const Output extends string = 'ravi',
>(series: TimeSeries<S>, options: RaviOptions<S, Output> = {}) {
  const shortPeriod = options.shortPeriod ?? 7;
  const longPeriod = options.longPeriod ?? 65;
  assertPeriod(shortPeriod, 'shortPeriod');
  assertPeriod(longPeriod, 'longPeriod');

  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const output = (options.output ?? 'ravi') as Output;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const short = rollingValues(wide, column, 'avg', shortPeriod);
  const long = rollingValues(wide, column, 'avg', longPeriod);

  const values = new Float64Array(short.length);
  for (let i = 0; i < values.length; i += 1) {
    const slow = long[i]!;
    // Live guard: the division is at the OUTPUT and nothing forces the
    // numerator to zero with the denominator.
    values[i] = slow === 0 ? NaN : (100 * Math.abs(short[i]! - slow)) / slow;
  }

  return series.withColumn(output, values);
}
