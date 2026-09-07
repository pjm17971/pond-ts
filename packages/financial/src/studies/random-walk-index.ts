import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import { randomWalkValues } from '../kernels/random-walk.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';

export interface RandomWalkIndexOptions<
  S extends SeriesSchema,
  Prefix extends string,
> {
  /** The longest horizon tested, in **bars**; the study takes the maximum
   *  over horizons `2 … period`. **Default `14`.** Must be at least `2`. */
  period?: number;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Close column (true-range input). **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Column-family prefix — appends `${prefix}High` and `${prefix}Low`.
   *  **Default `'rwi'`.** */
  prefix?: Prefix;
}

/**
 * **Random Walk Index** (E. Michael Poulos, TASC 1991) — how far price
 * actually travelled, in units of how far a random walk of the same length
 * would be expected to travel, asked at **every** horizon up to `period` and
 * answered with the largest reading:
 *
 * ```
 * ${prefix}High = max over n = 2 … period of  (high − low[−n])  / (meanTR(n) · √n)
 * ${prefix}Low  = max over n = 2 … period of  (high[−n] − low)  / (meanTR(n) · √n)
 * ```
 *
 * Appends two columns. A symmetric
 * random walk covers a distance proportional to `√n` after `n` steps, and
 * one step is estimated by the mean true range, so a reading above **1**
 * says the move was larger than chance would produce at that horizon and a
 * reading near or below 1 says the market is wandering. `rwiHigh` measures
 * the up-move (today's high against the low `n` bars back) and `rwiLow` the
 * down-move; the conventional reading is a trend when one is above 1 and the
 * other below.
 *
 * Neither column is bounded below by zero, which the "distance travelled"
 * description hides: `high − low[−n]` is **negative** whenever the market
 * fell across every horizon, and the max of negative terms is negative. On
 * the package's oracle input `rwiHigh` runs −0.31 … 2.29 (measured). A
 * negative reading simply says the move went the other way.
 *
 * ## The multi-horizon form is the study — the G2 shape, and its cost
 *
 * The corpus assessment (§5, **G2**) singles this out as the awkward window:
 * the reducer needs a *different* rolling statistic at every horizon inside
 * the window, which no window kernel in the package can express. The
 * alternative — one `ATR(period)` in the denominator for every horizon — is
 * a **different indicator**, not an optimisation: it drops the `n`-specific
 * volatility estimate that makes the `√n` comparison meaningful. It is not
 * what ships, and the generator measures how far away it is rather than
 * asserting a preference.
 *
 * So the study is **O(N · period)** in time — the horizon sweep lives in
 * `kernels/random-walk.ts`, which folds one horizon at a time and keeps
 * memory at O(N) regardless of `period`. `scripts/perf-studies.mjs` carries
 * a `period 14` and a `period 50` entry side by side so the linearity in
 * `period` stays visible; a study that is linear in its look-back is
 * acceptable here for the same reason {@link commodityChannelIndex} is (the
 * published periods are small and the inner pass is cache-local), and it is
 * documented rather than hidden.
 *
 * ## `meanTR(n)` is the **`n`-bar mean** of true range, not Wilder's ATR
 *
 * Poulos writes "the average true range over the last `n` periods", and the
 * structure agrees with the words: `√n` is a statement about `n`
 * independent steps, so the denominator has to be the average step size over
 * exactly those `n` bars. Wilder's recursion has infinite memory and no
 * `n`-bar window at all, so at horizon `n` it would scale by a quantity that
 * partly reflects bars from before the horizon being tested. Measured on the
 * package's oracle input, running the whole study on Wilder denominators
 * puts `rwiHigh` **0.190 apart** at `period 14` on a reading that spans
 * −0.31 … 2.29, and the single-horizon form (only `n = period`) **2.374
 * apart** (`scripts/oracle/generate.py`). Both are asserted. The Wilder
 * separation **shrinks with `period`** — 0.031 at `period 30`, measured —
 * because the recursion and the `n`-bar mean converge as `n` grows; it is
 * the short horizons, which dominate a small `period`, where the choice
 * actually shows.
 *
 * ## Warm-up
 *
 * Length-preserving, and **strict**: a bar reads a value only when every one
 * of the `period − 1` horizons has one, because a "maximum over 2 … 14"
 * taken over the four horizons that happened to be available is not that
 * maximum. `TR[0]` is undefined (no previous close), so the slowest horizon
 * needs `period` true ranges and `low[i − period]`, both of which first
 * exist on bar **`period`** — bar 14 at the default.
 *
 * ## Edges
 *
 * - **Scale- and shift-invariant.** Numerator and denominator are both
 *   first-order in price, so a scale cancels; every term is a difference of
 *   prices, so a shift cancels too. Pinned as property tests.
 * - **A zero denominator → `undefined`.** `meanTR(n) = 0` means every bar in
 *   that horizon was completely flat, but `low[i − n]` is the low of a bar
 *   *outside* that window and is not forced to equal today's high — so it is
 *   a real number over zero rather than a `0/0`, and the guard is live. One
 *   flat horizon blanks the whole bar, which is the strict rule again.
 * - **`period` must be at least 2** — the horizon list `2 … period` is empty
 *   below that, and a "random walk index" with no horizons is not a reading.
 * - **An interior gap** costs the bar plus every horizon window that holds
 *   it — up to `period` bars — and then recovers. Nothing propagates to the
 *   end, because no recursion is involved.
 */
export function randomWalkIndex<
  S extends SeriesSchema,
  const Prefix extends string = 'rwi',
>(series: TimeSeries<S>, options: RandomWalkIndexOptions<S, Prefix> = {}) {
  const period = options.period ?? 14;
  assertPeriod(period);
  if (period < 2) {
    throw new TypeError(
      `randomWalkIndex period must be at least 2 (the horizons are 2 … period); got ${String(period)}`,
    );
  }

  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const closeName = (options.close ?? DEFAULT_OHLCV.close) as string;
  const prefix = (options.prefix ?? 'rwi') as Prefix;
  const highOut = `${prefix}High` as const;
  const lowOut = `${prefix}Low` as const;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, highOut);
  assertNoColumn(wide, lowOut);

  const { rwiHigh, rwiLow } = randomWalkValues(
    columnValues(wide, highName),
    columnValues(wide, lowName),
    columnValues(wide, closeName),
    period,
  );

  return series.withColumn(highOut, rwiHigh).withColumn(lowOut, rwiLow);
}
