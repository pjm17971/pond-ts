import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import { barsSinceExtremeValues } from '../kernels/highest-lowest.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';

export interface AroonOptions<S extends SeriesSchema, Prefix extends string> {
  /** Look-back in **bars** — the oldest age the oscillator can report.
   *  **Default `25`.** */
  period?: number;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Column-family prefix — appends `${prefix}Up` / `${prefix}Down` /
   *  `${prefix}Osc`. **Default `'aroon'`.** */
  prefix?: Prefix;
}

/**
 * **Aroon** (Tushar Chande, 1995) — how *recently* the window's extremes
 * happened, rather than how far away they are:
 *
 * ```
 * aroonUp   = 100 · (period − barsSinceHighestHigh) / period    ${prefix}Up
 * aroonDown = 100 · (period − barsSinceLowestLow)  / period    ${prefix}Down
 * aroonOsc  = aroonUp − aroonDown                              ${prefix}Osc
 * ```
 *
 * `100` means the extreme is today's bar, `0` means it is the oldest bar
 * still in the window — so a strong uptrend keeps `aroonUp` pinned near 100
 * (fresh highs) while `aroonDown` decays toward 0, and the crossovers are
 * the signal. The name is Sanskrit for "dawn's early light"; Chande's point
 * is that this reads a trend *change* earlier than an average can, because
 * age moves the instant a new extreme prints while a mean has to be dragged.
 *
 * Reads **high and low** (no close), each named by an option defaulting to
 * its `DEFAULT_OHLCV` column.
 *
 * ## The window is `period + 1` bars
 *
 * `period` counts the oldest **age** the study can report, and "`period`
 * bars ago" is itself a reading, so the window has to hold `period + 1` bars
 * — the one place in this package where a `period` is not its window's bar
 * count. The warm-up is therefore `period` rows, which is what TA-Lib's
 * `AROON` publishes too (its look-back is exactly `period`). The rule lives
 * in {@link barsSinceExtremeValues} so a second consumer cannot get it
 * wrong.
 *
 * ## Definition
 *
 * **TA-Lib's `AROON` and `AROONOSC`**, exactly: the oracle asserts bar-for-
 * bar agreement with identical warm-up masks on both columns and the
 * oscillator, at `period 25` and `period 5`. There is no smoothing and no
 * seed here, so "exactly" means exactly — unlike the Wilder family, this
 * study has nothing to diverge on.
 *
 * ## Ties go to the most recent bar
 *
 * A new high that merely **equals** the standing high resets `aroonUp` to
 * 100. That is TA-Lib's rule (measured: on the window `12, 11, 12, 10.5` at
 * `period 4` it reports 75, the newer bar's age, not the older's 25), and it
 * is the reading that matches what the study is for — a market printing an
 * equal high is making a fresh one, not living off an old one.
 *
 * ## Edges
 *
 * - **`aroonUp` and `aroonDown` are bounded `0..100`**, and `aroonOsc`
 *   `−100..100`, by construction: the age is an integer in `0 … period`.
 *   Both are pinned by property tests.
 * - **Both are invariant to *any* monotonic rescaling of price**, not merely
 *   a positive scale factor — the study reads the *position* of the extreme,
 *   never its size, so scaling and shifting the input leave all three
 *   columns bit-identical. That is a stronger invariance than any other
 *   study here has, and the property test asserts it as equality rather than
 *   a tolerance.
 * - **A gap costs `period + 1` bars and then recovers.** The kernel's rule
 *   is strict — every cell in the window must be finite — because an extreme
 *   taken over the cells you *do* have is still an honest extreme but its
 *   **age** is not: the hole could be hiding the very bar being asked about.
 *   Measured: TA-Lib fed a `NaN` high reports `aroonUp = 100` on every bar
 *   after it, forever, because its running extreme silently never updates.
 * - **A leading gap shifts the start**, so running over another study's
 *   output starts that many bars later rather than emptying the column.
 * - **The default `period` is not universal.** 25 is the value StockCharts
 *   and most charting platforms ship; **TA-Lib's own default is 14**
 *   (measured), so a vendor comparison should check the length first — the
 *   same caveat {@link commodityChannelIndex} carries. Aroon is a look-back
 *   over *positions*, so a short window saturates at 100/0 far more often.
 */
export function aroon<
  S extends SeriesSchema,
  const Prefix extends string = 'aroon',
>(series: TimeSeries<S>, options: AroonOptions<S, Prefix> = {}) {
  const period = options.period ?? 25;
  assertPeriod(period);
  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const prefix = (options.prefix ?? 'aroon') as Prefix;
  const upName = `${prefix}Up` as const;
  const downName = `${prefix}Down` as const;
  const oscName = `${prefix}Osc` as const;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  for (const name of [upName, downName, oscName]) assertNoColumn(wide, name);

  // The argmax scan is the kernel's (a monotonic deque, O(N) and independent
  // of `period`); the study only turns an age into a percentage.
  const sinceHigh = barsSinceExtremeValues(
    columnValues(wide, highName),
    period,
    'max',
  );
  const sinceLow = barsSinceExtremeValues(
    columnValues(wide, lowName),
    period,
    'min',
  );

  const length = series.length;
  const up = new Float64Array(length);
  const down = new Float64Array(length);
  const osc = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    // NaN propagates through the arithmetic on its own ([PND-STUDYBOX]) —
    // the kernel has already decided where a value exists.
    const u = (100 * (period - sinceHigh[i]!)) / period;
    const d = (100 * (period - sinceLow[i]!)) / period;
    up[i] = u;
    down[i] = d;
    osc[i] = u - d;
  }

  return series
    .withColumn(upName, up)
    .withColumn(downName, down)
    .withColumn(oscName, osc);
}
