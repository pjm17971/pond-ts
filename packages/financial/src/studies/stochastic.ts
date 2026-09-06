import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import {
  highestLowestValues,
  percentOfRangeValues,
} from '../kernels/highest-lowest.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';
import { rollingMeanValues } from '../kernels/rolling-mean.js';

export interface StochasticOptions<
  S extends SeriesSchema,
  Prefix extends string,
> {
  /** Look-back for the highest high / lowest low, in **bars**. **Default
   *  `14`.** */
  kPeriod?: number;
  /** SMA length applied to fast `%K` to make the (slow) `%K`, in **bars**.
   *  **Default `3`.** `1` is the *fast* stochastic — `%K` unsmoothed. */
  slowing?: number;
  /** SMA length applied to `%K` to make `%D`, in **bars**. **Default `3`.** */
  dPeriod?: number;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Close column. **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Column-family prefix — appends `${prefix}K` / `${prefix}D`. **Default
   *  `'stoch'`.** */
  prefix?: Prefix;
}

/**
 * **Stochastic oscillator** (George Lane) — where the close sits in the
 * trailing `kPeriod`-bar range, as a percentage, smoothed twice:
 *
 * ```
 * fast %K = 100 · (close − LL) / (HH − LL)     HH/LL over kPeriod bars
 * %K      = SMA(fast %K, slowing)               ${prefix}K
 * %D      = SMA(%K, dPeriod)                    ${prefix}D
 * ```
 *
 * The defaults `{ kPeriod: 14, slowing: 3, dPeriod: 3 }` are the **slow
 * stochastic** every charting platform ships as "Stochastic". `slowing: 1`
 * gives the *fast* stochastic — the same study, one knob, rather than a
 * second function whose only difference is a `1`.
 *
 * Appends two columns. `%K` is `undefined` for the first
 * `kPeriod + slowing − 2` rows and `%D` for the first
 * `kPeriod + slowing + dPeriod − 3` — at the defaults, bars 0–14 and 0–16.
 *
 * Reads **high, low and close**, each named by an option defaulting to its
 * `DEFAULT_OHLCV` column — the {@link atr} shape.
 *
 * ## Definition
 *
 * **TA-Lib's `STOCH`** with SMA smoothing (`slowk_matype = slowd_matype =
 * 0`), and `STOCHF` at `slowing: 1`. Verified in the oracle fixture: `%D`
 * agrees bar-for-bar with an identical warm-up, and `%K` agrees on every bar
 * TA-Lib emits, both to `5.7e-14` — the ulp of a value near 100.
 *
 * ## Two deliberate deltas from TA-Lib
 *
 * - **`%K` starts when it can.** TA-Lib masks `%K` back to `%D`'s first
 *   valid bar, discarding `dPeriod − 1` real values (bars 15 and 16 at the
 *   defaults). This emits them — the same per-column warm-up {@link macd}
 *   chose over TA-Lib's masking, for the same reason: a value that is
 *   defined by the definition is a value. The oracle generator asserts the
 *   two warm-ups are identical *from TA-Lib's first bar on*, and that ours
 *   starts exactly `dPeriod − 1` bars earlier.
 * - **A flat window is `undefined`.** When `HH === LL` the ratio is `0/0`;
 *   TA-Lib reports `0`, which is also the value for "close at the very
 *   bottom of a real range". pond reports no value, as {@link rsi} does for
 *   its flat window, and the smoothing carries it: a `%K` window containing
 *   such a bar has no average (and so no `%D` for `dPeriod` bars after) —
 *   an average of three values one of which is unknown is unknown. The rule
 *   itself lives in the kernel ({@link percentOfRangeValues}) so
 *   {@link williamsR} makes the same call.
 *
 * ## Edges
 *
 * - **Bounded `0..100`** whenever `low ≤ close ≤ high` — which is a property
 *   of the *input*, not something enforced here. Redirect `close` at a
 *   column the range does not bound (a smoothed close, say) and `%K` reads
 *   outside `0..100`, honestly, rather than clamped.
 * - **Scale-invariant**: a ratio of price differences, so scaling every
 *   price leaves it unchanged (pinned by a property test alongside RSI).
 * - **A leading gap shifts the start.** Running with `close` redirected at
 *   another study's output starts that many bars later.
 * - **An interior gap** in `close` costs that bar's fast `%K`, then
 *   `slowing` bars of `%K` and `dPeriod − 1` more of `%D` — the window
 *   recovers once the gap has left it, unlike a Wilder recursion. A gap in
 *   `high` or `low` is skipped by the range (core's reducer policy: the
 *   extreme over the cells the window does hold), so it costs nothing unless
 *   the window is entirely empty.
 * - **Williams `%R` is `fast %K − 100`**: {@link williamsR} equals
 *   `stochastic({ slowing: 1 })`'s `%K` shifted down by 100, bar for bar,
 *   and a test pins that identity.
 */
export function stochastic<
  S extends SeriesSchema,
  const Prefix extends string = 'stoch',
>(series: TimeSeries<S>, options: StochasticOptions<S, Prefix> = {}) {
  const kPeriod = options.kPeriod ?? 14;
  const slowing = options.slowing ?? 3;
  const dPeriod = options.dPeriod ?? 3;
  assertPeriod(kPeriod, 'kPeriod');
  assertPeriod(slowing, 'slowing');
  assertPeriod(dPeriod, 'dPeriod');
  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const closeName = (options.close ?? DEFAULT_OHLCV.close) as string;
  const prefix = (options.prefix ?? 'stoch') as Prefix;
  const kName = `${prefix}K` as const;
  const dName = `${prefix}D` as const;
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, kName);
  assertNoColumn(wide, dName);

  const { highest, lowest } = highestLowestValues(
    wide,
    highName,
    lowName,
    kPeriod,
  );
  const fastK = percentOfRangeValues(
    highest,
    lowest,
    columnValues(wide, closeName),
  );
  // Both smoothings go through the raw-array kernel rather than a scratch
  // column: core's count-window `avg` counts rows, and would put a "3-bar"
  // %K on the first bar fast %K exists. See `rollingMeanValues` for the
  // measurement.
  const k = rollingMeanValues(fastK, slowing);
  const d = rollingMeanValues(k, dPeriod);

  return series.withColumn(kName, k).withColumn(dName, d);
}
