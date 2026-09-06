import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_SOURCE } from '../contract/columns.js';
import { rollingMeanValues } from '../kernels/rolling-mean.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
  rollingColumns,
} from '../kernels/rolling.js';

export interface VerticalHorizontalFilterOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Look-back in **bars**, for both the range and the path length.
   *  **Default `28`** (Adam White's). */
  period?: number;
  /** Source column. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'vhf'`.** */
  output?: Output;
}

/**
 * **Vertical Horizontal Filter** (Adam White, 1991) — net movement over total
 * movement: how much of the distance the price walked actually took it
 * somewhere.
 *
 * ```
 * ${output} = (highest − lowest of column over period) / Σ |column[i] − column[i−1]| over period
 * ```
 *
 * Appends one column, in `0..1`. **High is trending, low is choppy** — the
 * opposite polarity to {@link choppinessIndex}, which measures the reciprocal
 * idea on a log scale. A market that went one way in a straight line has a
 * net range equal to its path length and reads near `1`; one that shuffled
 * back and forth reads near `period`'s reciprocal. White's use of it was as a
 * *regime filter*: run a trend-following rule when it is high and an
 * oscillator when it is low, rather than reading it directionally.
 *
 * ## Definition — pinned, because no vendor arbitrates it
 *
 * **TA-Lib has no VHF**, so the oracle is a pandas replication with the
 * analytic first-valid bar asserted and a measured separation from the
 * plausible wrong turn — the same ratio over a `period − 1`-long sum of
 * changes, which is the off-by-one this study's window alignment invites (see
 * below).
 *
 * - **Highest and lowest of the *same* column the changes are taken over.**
 *   White's statement is in terms of closes and this study generalises it to
 *   any `column`, one option rather than three, because both halves must read
 *   the same field or the ratio is not a ratio of anything. (Some vendors use
 *   the bar's `high` and `low` for the range and the close for the changes;
 *   that mixes two fields and is a different, larger number. The single
 *   `column` shape is what makes this study composable over another study's
 *   output, which is the {@link zScore} / {@link rollingStdev} shape.)
 * - **A fraction, not a percent.** `0.34`, not `34` — the {@link
 *   historicalVolatility} convention: only {@link percentChange} and the
 *   studies whose *names* say percent multiply by 100.
 * - **28 is White's own default.** Both halves take the one `period`.
 *
 * ## Warm-up: `period` rows, not `period − 1` — the alignment matters
 *
 * The two halves do **not** have the same reach. `period` bars give
 * `period − 1` changes, so a `period`-long sum of changes needs
 * `period + 1` closes: it first exists on bar **`period`**, one row later
 * than the range's `period − 1`. The column therefore first lands on bar
 * `period` — bar 28 at the default — and the range on that bar covers rows
 * `[1 … period]` while the changes cover the transitions `[0 → 1] … [period −
 * 1 → period]`, i.e. **one transition *into* the range's window**. That
 * asymmetry is inherent to the definition (a net range is over bars, a path
 * length is over moves) and is why the oracle separates this study from the
 * version whose sum is one term shorter.
 *
 * ## Edges
 *
 * - **Scale-invariant, and shift-invariant.** Numerator and denominator are
 *   both homogeneous of degree one in price and both are built from
 *   differences. Pinned by property tests.
 * - **Bounded `0 < vhf ≤ 1`**, and the upper bound is the interesting one.
 *   The `period − 1` transitions *inside* the range's window already trace a
 *   path from the window's low to its high, so their absolute values sum to
 *   at least the range; the sum has one further term on top of them, so the
 *   ratio can never exceed `1`, and `1` means "no retracement at all in the
 *   window". There is **no useful lower bound**: the extra term is the move
 *   *into* the window and is not bounded by the window's own range, so a
 *   single large gap on that bar can drive the reading arbitrarily close to
 *   zero. Pinned as `(0, 1]` rather than as the `1/period` floor a
 *   symmetric-looking argument would suggest.
 * - **A window with no movement is `0/0`, and needs no guard.** If the
 *   changes sum to zero then every close in the window is the same one, which
 *   forces the range to zero too — so the ratio is JavaScript's own `NaN` and
 *   the study reports `undefined` on a halted instrument without a branch.
 *   There is no input that puts a non-zero numerator over a zero denominator,
 *   because both halves read the same column: an explicit guard here would be
 *   dead code that no test could kill (the {@link commodityChannelIndex}
 *   finding, applied rather than copied). Contrast {@link choppinessIndex},
 *   whose two halves read *different* columns and which therefore does need
 *   one.
 * - **A leading gap shifts the start**; an **interior** gap blanks the change
 *   on that bar and the next, then every sum over those, after which the
 *   study recovers. As in {@link choppinessIndex}, the two halves treat the
 *   gap differently — core's `max`/`min` skip a missing cell,
 *   {@link rollingMeanValues} does not — so the path-length half is what sets
 *   the mask.
 * - **A misnamed `column` throws** rather than reading empty: `max` / `min`
 *   fall through to core's sweep, which rejects the name. As in
 *   {@link ulcerIndex}, that is a **reducer**-level split — the same
 *   {@link rollingValues} door with `stdev` reads all-missing instead — and
 *   both behaviours are pinned by tests rather than reconciled here.
 */
export function verticalHorizontalFilter<
  S extends SeriesSchema,
  const Output extends string = 'vhf',
>(
  series: TimeSeries<S>,
  options: VerticalHorizontalFilterOptions<S, Output> = {},
) {
  const period = options.period ?? 28;
  assertPeriod(period);
  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const output = (options.output ?? 'vhf') as Output;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  // Both extremes of the SAME column from one scan — `highestLowestValues`
  // is the two-column door (a study's `high` and `low`), and this study has
  // one field, so it calls the shared reducer pass directly.
  const extremes = rollingColumns(
    wide,
    {
      highest: { from: column, using: 'max' },
      lowest: { from: column, using: 'min' },
    },
    period,
  );

  const v = columnValues(wide, column);
  const length = v.length;
  const absoluteChange = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    // Bar 0 has no predecessor and therefore no move — the `NaN` is what
    // makes the sum wait for `period` real changes rather than emitting one
    // term short, which is the whole of the alignment note above.
    absoluteChange[i] = i === 0 ? NaN : Math.abs(v[i]! - v[i - 1]!);
  }

  // Σ |Δ| is `period × mean(|Δ|)` over a DERIVED array, so it rides the
  // rolling-mean kernel's array door: `period` finite changes, not `period`
  // rows.
  const meanChange = rollingMeanValues(absoluteChange, period);

  const highest = extremes['highest']!;
  const lowest = extremes['lowest']!;
  const out = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    // No zero-denominator branch: a zero path length forces a zero range, so
    // the case is `0/0` and already `NaN`. See "Edges".
    out[i] = (highest[i]! - lowest[i]!) / (meanChange[i]! * period);
  }
  return series.withColumn(output, out);
}
