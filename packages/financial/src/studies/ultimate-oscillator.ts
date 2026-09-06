import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';
import { rollingMeanValues } from '../kernels/rolling-mean.js';
import { trueRangeValues } from '../kernels/true-range.js';

export interface UltimateOscillatorOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Shortest look-back in **bars**, weighted 4. **Default `7`** (Williams'). */
  shortPeriod?: number;
  /** Middle look-back in **bars**, weighted 2. **Default `14`.** */
  mediumPeriod?: number;
  /** Longest look-back in **bars**, weighted 1. **Default `28`.** */
  longPeriod?: number;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Close column. **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'uo'`.** */
  output?: Output;
}

/**
 * **Ultimate Oscillator** (Larry Williams, 1976) — buying pressure as a
 * fraction of true range, averaged over **three** horizons at once and
 * weighted toward the shortest, bounded `0..100`:
 *
 * ```
 * BP  = close − min(low, prevClose)          "buying pressure"
 * TR  = max(high, prevClose) − min(low, prevClose)
 * A_n = Σ BP over n bars / Σ TR over n bars
 * uo  = 100 · (4·A_short + 2·A_medium + A_long) / 7
 * ```
 *
 * Appends one column; `undefined` for the first `longPeriod` rows.
 *
 * Williams built it against the complaint that a single-horizon oscillator
 * gives false divergences whenever the trader's horizon and the indicator's
 * disagree, so it reads three at once. The weights halve as the horizon
 * doubles, which is what keeps the three legs' contributions comparable
 * rather than letting the longest window dominate.
 *
 * Reads **high, low and close**, each named by an option defaulting to its
 * `DEFAULT_OHLCV` column — the {@link atr} shape.
 *
 * ## Three named periods, not a tuple
 *
 * The three look-backs are `shortPeriod` / `mediumPeriod` / `longPeriod`,
 * not a `periods: [7, 14, 28]` array. A tuple would be shorter to type and
 * worse to read: the weights are **positional** (4 / 2 / 1), so
 * `periods: [28, 14, 7]` is a silently different indicator, while
 * `longPeriod: 7` is obviously wrong at the call site. It also matches how
 * every other multi-horizon study here names its lengths
 * ({@link macd}'s `fastPeriod`/`slowPeriod`, {@link coppock}'s
 * `longPeriod`/`shortPeriod`/`wmaPeriod`). The three are validated as
 * **strictly increasing** for the same reason.
 *
 * The weights themselves are **not** options: `4 / 2 / 1` is the definition,
 * and a weight vector nobody publishes an alternative for is a speculative
 * knob.
 *
 * ## Definition source
 *
 * **TA-Lib's `ULTOSC`**, matched bar-for-bar — the oracle asserts identical
 * null masks and agreement to **7.1e-15** at both `(7, 14, 28)` and
 * `(3, 5, 9)`. The true range is the package's own {@link trueRangeValues},
 * which the generator separately confirms is `talib.TRANGE` **exactly**
 * (0.0), so the ATR family and this study measure range the same way by
 * construction rather than by coincidence.
 *
 * ## Edges
 *
 * - **Warm-up is `longPeriod` rows**, not `longPeriod − 1`: both `BP` and
 *   `TR` read the **previous** close, so bar 0 has neither and the first
 *   full window of `longPeriod` defined bars ends on bar `longPeriod`. The
 *   two shorter legs are ready earlier and wait for the longest.
 * - **Bounded `0..100`** on real bars, because `0 ≤ BP ≤ TR` there: `BP` is
 *   non-negative since `close ≥ low`, and `TR ≥ close − min(low, prevClose)`
 *   since `TR`'s upper term is `max(high, prevClose) ≥ close`. Point a
 *   `close` option at a column that is *not* inside its bar (a smoothed
 *   line, say) and both bounds can be left — reported honestly rather than
 *   clamped, as the input is the thing that is inconsistent.
 * - **A window whose true range sums to zero** → `undefined`, guarded
 *   explicitly. On consistent bars it is the `0/0` of a completely flat
 *   window; on a redirected `close` the numerator can be non-zero over a
 *   zero range, and an `±Infinity` in a chart's y-domain is worse than a
 *   gap. Any one of the three legs being undefined makes the reading
 *   undefined — a weighted mean of an unknown is unknown.
 * - **Scale- and shift-invariant**: `BP` and `TR` are both homogeneous of
 *   degree one in price and both unchanged by adding a constant to every
 *   price, so their ratio is invariant to both. Pinned by property tests.
 * - **A leading gap shifts the start**; **an interior gap costs the windows
 *   containing it** — the gap bar and the `longPeriod` bars after it — after
 *   which the study recovers. Note *which* bar a missing close costs: `BP`
 *   and `TR` read the previous close, so a bar with no close removes both its
 *   own reading and the next bar's, which is why the hole reaches one bar
 *   further than the window alone would ({@link trueRangeValues} states the
 *   same asymmetry).
 */
export function ultimateOscillator<
  S extends SeriesSchema,
  const Output extends string = 'uo',
>(series: TimeSeries<S>, options: UltimateOscillatorOptions<S, Output> = {}) {
  const shortPeriod = options.shortPeriod ?? 7;
  const mediumPeriod = options.mediumPeriod ?? 14;
  const longPeriod = options.longPeriod ?? 28;
  assertPeriod(shortPeriod, 'shortPeriod');
  assertPeriod(mediumPeriod, 'mediumPeriod');
  assertPeriod(longPeriod, 'longPeriod');
  if (!(shortPeriod < mediumPeriod && mediumPeriod < longPeriod)) {
    throw new TypeError(
      'ultimateOscillator periods must be strictly increasing: shortPeriod < mediumPeriod < longPeriod (the 4/2/1 weights are positional)',
    );
  }
  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const closeName = (options.close ?? DEFAULT_OHLCV.close) as string;
  const output = (options.output ?? 'uo') as Output;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const high = columnValues(wide, highName);
  const low = columnValues(wide, lowName);
  const close = columnValues(wide, closeName);
  const trueRange = trueRangeValues(high, low, close);

  // Buying pressure: how much of the bar's true range was covered on the way
  // up. Bar 0 has no previous close, so it is `NaN` — the same bar
  // `trueRangeValues` blanks, which keeps the two legs' masks aligned.
  const length = close.length;
  const buyingPressure = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    buyingPressure[i] =
      i === 0 ? NaN : close[i]! - Math.min(low[i]!, close[i - 1]!);
  }

  // Σ BP / Σ TR is mean(BP) / mean(TR) — the shared 1/n cancels — so the
  // three legs compose on the shipped rolling-mean kernel rather than on a
  // second sliding accumulator. Its array door is also the warm-up rule: a
  // window emits only once its last `n` rows are all finite.
  const legs = [shortPeriod, mediumPeriod, longPeriod].map((n) => ({
    bp: rollingMeanValues(buyingPressure, n),
    tr: rollingMeanValues(trueRange, n),
  }));

  const out = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    let weighted = 0;
    for (let k = 0; k < 3; k += 1) {
      const tr = legs[k]!.tr[i]!;
      // A zero-range window has no fraction to report, and a redirected
      // `close` can put a non-zero numerator over it — ±Infinity would
      // reach `withColumn`, so this is a real guard, not a formality.
      const ratio = tr === 0 ? NaN : legs[k]!.bp[i]! / tr;
      weighted += (k === 0 ? 4 : k === 1 ? 2 : 1) * ratio;
    }
    out[i] = (100 * weighted) / 7;
  }
  return series.withColumn(output, out);
}
