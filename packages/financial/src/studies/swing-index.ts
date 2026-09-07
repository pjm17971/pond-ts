import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import { cumulativeValues } from '../kernels/cumulative.js';
import { assertNoColumn, columnValues } from '../kernels/rolling.js';
import { swingIndexValues } from '../kernels/swing-index.js';

export interface SwingIndexOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** **Required.** The instrument's limit move `T` — the largest price
   *  change one bar is allowed to make, in the price's own units. There is
   *  no default; see the study docstring for why. Must be positive and
   *  finite. */
  limit: number;
  /** Open column. **Default `'open'`.** */
  open?: NumericColumnNameForSchema<S>;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Close column. **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Appended column name. **Default `'si'`** (`'asi'` for
   *  {@link accumulativeSwingIndex}). */
  output?: Output;
}

/** Validate and read the four bar columns plus the limit move, shared by the
 *  two studies so they cannot drift apart on either. */
function swingValues<S extends SeriesSchema, Output extends string>(
  series: TimeSeries<S>,
  options: SwingIndexOptions<S, Output>,
  study: string,
): Float64Array {
  const limit = options.limit;
  if (!Number.isFinite(limit) || limit <= 0) {
    throw new TypeError(
      `${study} limit must be a positive finite number (the instrument's limit move); got ${String(limit)}`,
    );
  }
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  return swingIndexValues(
    columnValues(wide, (options.open ?? DEFAULT_OHLCV.open) as string),
    columnValues(wide, (options.high ?? DEFAULT_OHLCV.high) as string),
    columnValues(wide, (options.low ?? DEFAULT_OHLCV.low) as string),
    columnValues(wide, (options.close ?? DEFAULT_OHLCV.close) as string),
    limit,
  );
}

/**
 * **Swing Index** (J. Welles Wilder Jr., *New Concepts in Technical Trading
 * Systems*, 1978) — one bar's "real" price change, once the open, the high,
 * the low and the previous bar are all taken into account:
 *
 * ```
 * A = |high − prevClose|   B = |low − prevClose|   D = |high − low|
 *
 * K = max(A, B)
 * R = A − 0.5·B + 0.25·|prevClose − prevOpen|      if A is the largest of A, B, D
 *   = B − 0.5·A + 0.25·|prevClose − prevOpen|      if B is
 *   = D + 0.25·|prevClose − prevOpen|              if D is
 *
 * ${output} = 50 · ( (close − prevClose)
 *                  + 0.5·(close − open)
 *                  + 0.25·(prevClose − prevOpen) ) / R · (K / limit)
 * ```
 *
 * Every term, in words:
 *
 * - **`close − prevClose`** is the bar's net move, the thing a naive change
 *   would report on its own.
 * - **`0.5·(close − open)`** adds half of today's own body, so a bar that
 *   closed strongly counts for more than one that merely drifted.
 * - **`0.25·(prevClose − prevOpen)`** adds a quarter of yesterday's body,
 *   which is what makes the index a *swing* rather than a per-bar reading:
 *   it carries a memory of the last bar's conviction.
 * - **`R`** normalises by a true-range-like quantity that leans on whichever
 *   of the three spans dominated the bar, so the reading is comparable
 *   across quiet and violent sessions.
 * - **`K / limit`** scales by how large today's gap against yesterday's
 *   close was, relative to the instrument's **limit move** — which is what
 *   bounds the result to **−100 … +100**.
 *
 * Wilder's own reading is that the sign of the swing, and the level a
 * sequence of them accumulates to ({@link accumulativeSwingIndex}), identify
 * the real trend under the noise of individual bars.
 *
 * ## `limit` is REQUIRED, and that is the decision
 *
 * `T` is a fact about the *instrument*, not about the study: it is the
 * exchange's daily limit move for that futures contract. The library cannot
 * guess it, and every possible default is wrong in a way that does not
 * announce itself:
 *
 * - **Defaulting to `1`** (what several charting packages do) silently
 *   rescales the reading by the instrument's price level, so the ±100 bound
 *   the study is *defined* by no longer holds and two symbols' swing indices
 *   are no longer comparable — which is the one thing the parameter exists
 *   to guarantee.
 * - **Defaulting to the bar's own range** would make `K/limit` a ratio of
 *   two quantities that both move with volatility, i.e. a different
 *   indicator wearing this one's name.
 *
 * So it is a required option, the way {@link correlation}'s `benchmark` is:
 * the one number the caller must supply because only they know it. A caller
 * trading an instrument with **no** limit move (equities, FX, crypto) passes
 * the scale they want the reading normalised by — the instrument's typical
 * daily range is the usual choice — and the docstring, rather than a
 * default, is what tells them that is what they are choosing.
 *
 * A `limit` of `0` is a division by zero and is rejected, along with
 * negatives and non-finite values.
 *
 * ## Warm-up and edges
 *
 * - **Bar 0 is `undefined`** — every term reads yesterday. Length-preserving,
 *   with a warm-up of exactly one row.
 * - **`R === 0` → `undefined`.** `R` is zero only when today's high, today's
 *   low, yesterday's close and yesterday's open are the same number — a tape
 *   that has not moved for two bars. `K` is zero on exactly those bars too,
 *   so the expression is `0/0` however it is grouped, and nothing forces the
 *   numerator to zero with it (it reads today's close and open, which a
 *   redirected `close` need not place inside today's range). Pinned by a
 *   test.
 * - **Scale-EQUIVARIANT, and invariant only if `limit` scales too.** This is
 *   the one study in the momentum group that is not scale-invariant, and the
 *   true statement has to name the parameter: every term of the numerator,
 *   of `R` and of `K` is a difference of two prices, so multiplying every
 *   price by `k` multiplies all three by `k` — `N/R` is unchanged, and the
 *   reading therefore scales by `k` through the `K/limit` factor alone.
 *   Scaling `limit` by `k` as well leaves the reading **unchanged**. Both
 *   halves are pinned as property tests, and they are what tells this study
 *   apart from one that dropped the `K/limit` factor: that one would be
 *   scale-invariant.
 * - **Shift-invariant.** Every term being a difference of two prices, adding
 *   a constant to every price changes nothing at all. Pinned.
 * - **No TA-Lib function**, so the oracle is a pandas replication with the
 *   analytic first-valid bar asserted, the ±100 bound checked at a `limit`
 *   at least as large as the largest `K`, and two discriminating separations
 *   measured (dropping the `K/limit` factor, and using the plain-range `D`
 *   branch unconditionally).
 */
export function swingIndex<
  S extends SeriesSchema,
  const Output extends string = 'si',
>(series: TimeSeries<S>, options: SwingIndexOptions<S, Output>) {
  const output = (options.output ?? 'si') as Output;
  assertNoColumn(series as unknown as TimeSeries<SeriesSchema>, output);
  return series.withColumn(output, swingValues(series, options, 'swingIndex'));
}

/**
 * **Accumulative Swing Index** (Wilder, 1978) — the running total of
 * {@link swingIndex}:
 *
 * ```
 * ${output}[i] = Σ SI[0..i]
 * ```
 *
 * Appends one column. Where the swing index is a per-bar reading, the ASI is
 * a *level*: Wilder's point is that it behaves like a smoothed price series
 * whose own trendlines and breakouts are cleaner than the price's, so it is
 * read for support/resistance breaks and for divergence against price.
 *
 * It is the cumulative sum of **that array**, through {@link
 * cumulativeValues} — not a second derivation of the same formula — so every
 * decision on {@link swingIndex} (the required `limit`, the `R === 0` rule,
 * the one-bar warm-up) is inherited by construction rather than restated.
 *
 * ## The running-sum asymmetry, inherited
 *
 * {@link cumulativeValues}' two rules apply unchanged, and the second one
 * matters here:
 *
 * - **A leading run of gaps shifts the start** — the sum begins at the first
 *   bar with a swing, which on clean input is bar 1.
 * - **An interior gap propagates to the end.** Every level after an unknown
 *   swing is a known sum plus an unknown. A halted two-bar stretch
 *   (`R === 0`) is such a gap, so the ASI *ends* there rather than skipping
 *   it. That is the same call OBV and the A/D line make, for the same
 *   reason: a level that is silently short by a missing contribution is
 *   worse than no level. Fill or drop the halted bars first if you need
 *   continuity.
 *
 * The ASI starts at the **first swing's value**, not at zero — Wilder's own
 * arrangement, and the one every cumulative study in this package uses.
 */
export function accumulativeSwingIndex<
  S extends SeriesSchema,
  const Output extends string = 'asi',
>(series: TimeSeries<S>, options: SwingIndexOptions<S, Output>) {
  const output = (options.output ?? 'asi') as Output;
  assertNoColumn(series as unknown as TimeSeries<SeriesSchema>, output);
  return series.withColumn(
    output,
    cumulativeValues(swingValues(series, options, 'accumulativeSwingIndex')),
  );
}
