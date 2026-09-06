import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import { highestLowestValues } from '../kernels/highest-lowest.js';
import { assertNoColumn, assertPeriod } from '../kernels/rolling.js';

export interface GopalakrishnanRangeIndexOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Look-back in **bars**. **Default `10`.** Must be **at least 2** — the
   *  reading is normalised by `ln(period)`, which is zero at 1. */
  period?: number;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'gapo'`.** */
  output?: Output;
}

/**
 * **Gopalakrishnan Range Index** (GAPO, Jayanthi Gopalakrishnan) — the
 * logarithm of the window's own high-to-low range, normalised by the
 * logarithm of the look-back:
 *
 * ```
 * ${output} = ln( highest(high, period) − lowest(low, period) ) / ln(period)
 * ```
 *
 * Appends one column. Reads **high and low**, each named by an option
 * defaulting to its `DEFAULT_OHLCV` name (the {@link atr} precedent).
 *
 * It is the simplest study in the volatility group and the least normalised:
 * a compressed range reads low, an expanded one reads high, and the `ln`
 * flattens the scale so that a range ten times wider adds a fixed amount
 * rather than multiplying the reading. Gopalakrishnan's use of it is
 * comparative — the *shape* against its own recent history, a squeeze
 * followed by an expansion — not the absolute level.
 *
 * ## Definition — the log base, and why it turns out not to matter
 *
 * **TA-Lib has no GAPO**, so the oracle is a pandas replication with the
 * analytic first-valid bar asserted and a measured separation from the
 * plausible wrong turns.
 *
 * `ln` is **ChartIQ's**, and it is what ships — but the base **cancels**,
 * exactly as it does in {@link choppinessIndex}: `log_b(x)/log_b(n)` is
 * `log_n(x)` for every `b`, so GAPO is simply **the logarithm of the
 * window's range in base `period`** and the `log10` form is the same number.
 * The generator asserts that (`2.2e-16`) rather than leaving it to be
 * rederived, because the first question the formula raises is "which log?"
 * and the honest answer is "either — but not one of each". **Mixing** the
 * bases (`log10` on top, `ln` underneath) is a real bug and is what the
 * separation assert pins, alongside dropping the `ln(period)` normalisation
 * altogether.
 *
 * `period` must be at least **2**: `ln(1) = 0`, and one bar's range over
 * nothing is not a reading. The study throws rather than dividing by zero
 * (the same rule {@link choppinessIndex} takes for the same reason).
 *
 * ## Warm-up
 *
 * One window, so the column first lands on bar **`period − 1`** — bar 9 at
 * the default. Length-preserving.
 *
 * ## Edges
 *
 * - **Shift-invariant, and *not* scale-invariant — it is scale-*additive*.**
 *   Adding a constant to every price leaves the range alone, so the reading
 *   does not move. Multiplying every price by `k` multiplies the range by
 *   `k`, and the logarithm turns that into an **exact constant offset**:
 *   `gapo(k · price) = gapo(price) + ln(k)/ln(period)`. That identity is
 *   pinned by a property test rather than an invariance, because it is the
 *   whole character of the study — GAPO is the one reading in this group that
 *   carries the *units* of the price, and comparing it across instruments
 *   priced differently is comparing their price levels. (Every other study in
 *   the batch — {@link choppinessIndex}, {@link verticalHorizontalFilter},
 *   {@link ulcerIndex}, {@link chaikinVolatility}, {@link massIndex},
 *   {@link relativeVolatilityIndex} — *is* scale-invariant.)
 * - **Negative readings are normal**, not an error: any range below `1` in
 *   the price's units has a negative logarithm. On an instrument quoted in
 *   fractions of a unit the whole series sits below zero.
 * - **A flat window → `undefined`.** `ln(0)` is `−Infinity`, which
 *   `withColumn` rejects, and there is no reading for "no range" on a
 *   logarithmic scale — the honest answer is that the study is not defined
 *   there, not that volatility was minimal. The same guard also catches a
 *   *negative* span, which `high` and `low` redirected at crossing columns
 *   produce and whose logarithm is `NaN` anyway.
 * - **A leading gap shifts the start**; an **interior** gap is *skipped*
 *   rather than blanking the window — this study reads only core's rolling
 *   `max`/`min`, which take the extreme over the cells the window does hold
 *   ({@link highestLowestValues}' documented policy). So GAPO keeps
 *   reporting across a hole, over one bar fewer, where the studies in this
 *   batch built on {@link rollingMeanValues} blank. It is the one study here
 *   with no averaging half to set a stricter mask, and the difference is
 *   pinned by a missing-cell test rather than left to be discovered.
 */
export function gopalakrishnanRangeIndex<
  S extends SeriesSchema,
  const Output extends string = 'gapo',
>(
  series: TimeSeries<S>,
  options: GopalakrishnanRangeIndexOptions<S, Output> = {},
) {
  const period = options.period ?? 10;
  assertPeriod(period);
  if (period < 2) {
    throw new TypeError(
      'gopalakrishnanRangeIndex period must be at least 2 (the reading is normalised by ln(period), which is 0 at 1)',
    );
  }

  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const output = (options.output ?? 'gapo') as Output;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const { highest, lowest } = highestLowestValues(
    wide,
    highName,
    lowName,
    period,
  );

  const logPeriod = Math.log(period);
  const length = highest.length;
  const out = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    const span = highest[i]! - lowest[i]!;
    // `ln(0)` is −Infinity and `ln(negative)` is NaN; both would be a value
    // rather than an absence, and the first would be rejected by
    // `withColumn`. A `NaN` span fails the comparison and propagates.
    out[i] = span > 0 ? Math.log(span) / logPeriod : NaN;
  }
  return series.withColumn(output, out);
}
