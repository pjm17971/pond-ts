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
  emaValues,
} from '../kernels/rolling.js';

export interface ElderRayOptions<
  S extends SeriesSchema,
  Prefix extends string,
> {
  /** EMA span in **bars**. **Default `13`** (Elder's). */
  period?: number;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Close column — the EMA's input. **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Column-family prefix — appends `${prefix}Bull` / `${prefix}Bear`.
   *  **Default `'elder'`.** */
  prefix?: Prefix;
}

/**
 * **Elder Ray Index** (Alexander Elder) — how far the bar's extremes reach
 * beyond the consensus value of price, taken as a 13-bar EMA of the close:
 *
 * ```
 * ${prefix}Bull = high − EMA(close, period)      "bull power"
 * ${prefix}Bear = low  − EMA(close, period)      "bear power"
 * ```
 *
 * Bull power positive means buyers pushed the bar above the average; bear
 * power negative means sellers pushed it below. Elder reads the *pair* — the
 * two are `high − low` apart by construction, so bull ≥ bear on any bar
 * whose range is sane, and it is their signs and slopes that carry the
 * signal.
 *
 * Appends two columns, both warming up together on bar `period − 1`.
 *
 * Reads **high, low and close**, each named by an option defaulting to its
 * `DEFAULT_OHLCV` column — the {@link atr} shape.
 *
 * ## Which EMA, and why there is no `maType`
 *
 * The EMA is pond's — {@link emaValues}, `α = 2/(period+1)` seeded on the
 * **first sample**, the same function the K2 engine's `'ema'` routes to, so
 * `elderRay`'s average is `ema()`'s average bar for bar. TA-Lib seeds on the
 * SMA of the first `n`; the {@link macd} precedent applies and the delta is
 * a decaying transient rather than a different indicator.
 *
 * Unlike {@link disparityIndex} or {@link priceOscillator} this study takes
 * **no `maType`**: Elder's definition names the 13-bar EMA specifically, and
 * an option whose other nine settings nobody publishes is a speculative knob,
 * not a feature. A caller who wants `high − SMA(close, 20)` has
 * `movingAverage()` plus arithmetic.
 *
 * ## Definition source
 *
 * **TA-Lib has no Elder Ray**, so the oracle is a pandas replication of the
 * definition above on pond's EMA seed, with the analytic first-valid bar
 * asserted.
 *
 * ## Edges
 *
 * - **Warm-up**: both columns are `undefined` for the first `period − 1`
 *   rows (bars 0–11 at the default 13) — the EMA's own warm-up, since the
 *   high and low legs are defined from bar 0. Length-preserving.
 * - **Linear in the input, and shift-invariant**: scaling every price by `k`
 *   scales both columns by `k`; adding a constant to every price leaves them
 *   unchanged (it moves the extreme and the average together). Both pinned by
 *   property tests. These are absolute quantities in price units — the
 *   {@link atr} side of the scale pair, not the {@link rsi} side.
 * - **A leading gap in `close`** (running over another study's output) shifts
 *   the EMA's start, so both columns start late rather than coming back
 *   empty.
 * - **An interior gap in `close`** is *skipped* by the EMA recursion, which
 *   carries on — but the bar itself has no average, so both columns are
 *   missing on that bar and defined again on the next. **A gap in `high` or
 *   `low`** costs only its own column on its own bar; the two legs are
 *   independent.
 * - **A range that does not bracket the close** (redirect `close` at a
 *   smoothed column, say) makes bull power negative or bear power positive.
 *   That is reported honestly rather than clamped — it is a property of the
 *   input, not of the study.
 */
export function elderRay<
  S extends SeriesSchema,
  const Prefix extends string = 'elder',
>(series: TimeSeries<S>, options: ElderRayOptions<S, Prefix> = {}) {
  const period = options.period ?? 13;
  assertPeriod(period);
  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const closeName = (options.close ?? DEFAULT_OHLCV.close) as string;
  const prefix = (options.prefix ?? 'elder') as Prefix;
  const bullName = `${prefix}Bull` as const;
  const bearName = `${prefix}Bear` as const;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, bullName);
  assertNoColumn(wide, bearName);

  const ema = emaValues(wide, closeName, period);
  const high = columnValues(wide, highName);
  const low = columnValues(wide, lowName);
  const length = ema.length;
  const bull = new Float64Array(length);
  const bear = new Float64Array(length);
  // Missing cells are NaN and propagate through the subtraction on their own
  // ([PND-STUDYBOX]), so the warm-up needs no explicit mask here.
  for (let i = 0; i < length; i += 1) {
    bull[i] = high[i]! - ema[i]!;
    bear[i] = low[i]! - ema[i]!;
  }

  return series.withColumn(bullName, bull).withColumn(bearName, bear);
}
