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
  rollingValues,
} from '../kernels/rolling.js';

export interface UlcerIndexOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Look-back in **bars**, for both the running peak and the averaging.
   *  **Default `14`.** */
  period?: number;
  /** Source column. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'ulcer'`.** */
  output?: Output;
}

/**
 * **Ulcer Index** (Peter Martin, 1987) — a volatility measure that counts
 * **only the downside**: the root-mean-square percentage drawdown from the
 * window's own highest close.
 *
 * ```
 * peak[i]     = max(column over period)
 * drawdown[i] = 100 × (column[i] − peak[i]) / peak[i]      ( ≤ 0 )
 * ${output}      = sqrt( mean of drawdown² over period )
 * ```
 *
 * Appends one column; always `≥ 0`, and `0` exactly when the column has made
 * a new high on every bar of the window.
 *
 * The name is the argument: Martin's point was that a standard deviation
 * punishes upside and downside equally, while an investor only loses sleep
 * over the second. Squaring before averaging weights *deep* drawdowns far
 * above shallow ones, so a series that fell 20% once scores worse than one
 * that fell 5% four times, which is the intended ordering.
 *
 * ## Which Ulcer Index — the variant is pinned, and the corpus flags it
 *
 * The corpus marks this **F-AMBIG** on "smoothing variants", and it is right
 * to: three genuinely different things are published under this name.
 *
 * - **This ships the rolling, StockCharts form**: the drawdown is measured
 *   against **the window's own highest close** and both the peak and the mean
 *   run over the same `period`. It is a *local* reading that moves with the
 *   window, which is what makes it chartable beside {@link atr} and
 *   {@link historicalVolatility}.
 * - **Martin's original is cumulative**: the peak is the highest close *of
 *   the whole series so far* and the average runs over the entire history, so
 *   it is one number per portfolio rather than a series. That is a different
 *   deliverable, not a `period` away — `rollingMax` with a period the length
 *   of the series plus a cumulative mean would give it, and it is deliberately
 *   not an option here (a study whose window silently means "everything" is a
 *   footgun beside every other study in the package, all of which are
 *   bar-count).
 * - **Some vendors use a `14`-bar peak with a different averaging length**
 *   (Martin's own book uses 14 for both), and a few average the *absolute*
 *   drawdown rather than its square — that last one is the "Pain Index", a
 *   different statistic with a different name. One `period` for both halves
 *   ships here, because two lengths with no published pairing would be a knob
 *   with no right value.
 *
 * **TA-Lib has no Ulcer Index**, so the oracle is a pandas replication with
 * the analytic first-valid bar asserted and a measured separation from the
 * mean-absolute (Pain Index) form.
 *
 * ## Warm-up: `2·period − 2`, and why it is not `period − 1`
 *
 * The peak needs `period` bars, so the first drawdown lands on bar
 * `period − 1`; the mean of squares then needs `period` **drawdowns**, not
 * `period` rows, so the column first lands on **`2·period − 2`** — bar 26 at
 * the default 14. That is {@link rollingMeanValues}' array door doing its
 * job: averaging a partly-warm drawdown as if it were data would emit a
 * "14-bar" reading built from one number.
 *
 * **The two halves read different doors, and it shows over another study's
 * output.** The peak goes through the *column* door, whose window counts
 * **rows** and skips a missing cell, so over an input with its own warm-up it
 * emits early over however many values it has — {@link rollingMax}'s and
 * {@link donchian}'s documented contract. The mean of squares goes through
 * the *array* door, which waits for `period` finite drawdowns. Measured:
 * `ulcerIndex({ column: 'sma', period: 3 })` over an `sma(3)` first lands on
 * bar **4**, not the `2 + 2·3 − 2 = 6` the array rule alone would give,
 * because the peak started at bar 2 over one close. Pinned by a test.
 *
 * ## Edges
 *
 * - **Scale-invariant, but *not* shift-invariant.** The drawdown is a
 *   percentage, so multiplying every price by a constant leaves the reading
 *   unchanged; **adding** one does not — it moves the base of the percentage
 *   and shrinks every drawdown. Both halves are pinned, the second as a
 *   deliberate inequality: this is the one study in this batch that is not
 *   invariant to both, and a test that asserted the wrong half would pass
 *   vacuously on a study that normalised by the wrong thing.
 * - **Never negative, and `0` is a real reading** — a window whose close is
 *   its own running peak on every bar has no drawdown at all. Contrast the
 *   `0/0` cases elsewhere in this batch: here `0` means "no drawdown", which
 *   is information.
 * - **A window at new highs reads exactly `0`, and that costs a counter.**
 *   This is the first study in the package to take a **square root** of a
 *   rolling mean, and the two do not compose innocently: the rolling
 *   accumulator carries an `O(ε)` residue from the values that have just left
 *   the window, and `sqrt` turns a residue of `2.5e-18` in the *mean of
 *   squares* into `1.6e-9` in the reading. Measured — that is the oracle
 *   input at `period 5`, bar 22, where five consecutive new highs make the
 *   true answer exactly `0`. The residue is negligible against any other
 *   reading (relative `~1e-16`) and glaring against this one, which is also
 *   the reading a caller looks for. So the bars that actually contributed a
 *   drawdown are **counted**, and a window with none reports `0` exactly;
 *   everything else keeps {@link rollingMeanValues}' arithmetic, so ulcer's
 *   mean is that kernel's mean and not a second implementation. The counter
 *   is `O(1)` per bar and the oracle case is what pins it.
 * - **A zero peak → `undefined`, with no guard.** A window whose maximum is
 *   `0` can hold a value of `−5`, so the numerator is *not* forced to zero
 *   and the drawdown is `±Infinity` rather than `0/0` — but
 *   {@link rollingMeanValues} counts a non-finite cell as **missing**, just as
 *   it counts a `NaN`, so the mean of squares over it is `undefined` and
 *   nothing non-finite reaches `withColumn`. An explicit `peak === 0` guard
 *   was written here and **mutation testing deleted it**: no input can tell it
 *   is there, which is the {@link commodityChannelIndex} finding by a new
 *   route (a rolling kernel downstream of the division absorbs the guard —
 *   contrast {@link choppinessIndex}, whose guards sit at its *output* and are
 *   live). Unreachable on prices; reachable, and unit-tested, when `column` is
 *   another study's output that crosses zero. A **negative** peak produces a
 *   number, which is the `percentChange` rule (`=== 0`, not `<= 0`) applied
 *   here for the same reason — a percentage off a negative base is defined, if
 *   unusual, and clamping it would be inventing a rule.
 * - **A leading gap shifts the start**; an **interior** gap blanks the
 *   drawdown on that bar and then every averaging window over it, after which
 *   the study recovers. The peak itself does **not** blank — core's rolling
 *   `max` skips a missing cell — so the mask you see is the averaging half's.
 * - **A misnamed `column` throws**, rather than reading empty: the rolling
 *   `max` falls through to core's sweep, which rejects the name. Note this is
 *   a **reducer**-level split, not a study-level one — the same
 *   {@link rollingValues} call with `stdev` takes the range-exact path and
 *   reads all-missing instead, which is why {@link relativeVolatilityIndex}
 *   in this batch answers empty where this one throws. (The multi-input
 *   studies, {@link choppinessIndex} and {@link gopalakrishnanRangeIndex},
 *   read theirs through {@link highestLowestValues}, which answers
 *   all-missing by design — the {@link atr} precedent.) All three are pinned
 *   by tests; the inconsistency is the kernel's to resolve, not a study's.
 */
export function ulcerIndex<
  S extends SeriesSchema,
  const Output extends string = 'ulcer',
>(series: TimeSeries<S>, options: UlcerIndexOptions<S, Output> = {}) {
  const period = options.period ?? 14;
  assertPeriod(period);
  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const output = (options.output ?? 'ulcer') as Output;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const v = columnValues(wide, column);
  // The running peak is core's count-window `max` — the same reducer
  // `rollingMax` and `donchian` read, so "the highest close of the last
  // `period` bars" means one thing in the package.
  const peak = rollingValues(wide, column, 'max', period);

  const length = v.length;
  const squared = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    // No zero-peak guard. A zero peak with a non-zero value under it gives
    // `±Infinity` rather than `0/0`, but the mean below counts a non-finite
    // cell as missing exactly as it counts a `NaN`, so the reading is
    // `undefined` either way and nothing non-finite reaches `withColumn`. A
    // guard was written here and mutation testing deleted it — see "Edges".
    const drawdown = (100 * (v[i]! - peak[i]!)) / peak[i]!;
    squared[i] = drawdown * drawdown;
  }

  // The mean of squares goes through the rolling-mean kernel's ARRAY door, so
  // it waits for `period` finite drawdowns rather than `period` rows — the
  // 2·period − 2 warm-up documented above, and the interior-gap rule.
  const mean = rollingMeanValues(squared, period);

  // A window with no drawdown at all reads EXACTLY zero, and the rolling
  // accumulator cannot give that on its own — see "A window at new highs" in
  // the docstring. A running count of the bars that actually contributed a
  // drawdown is O(1) per bar and makes that one reading exact while every
  // other value keeps the shared kernel's arithmetic. `NaN !== 0`, so a
  // missing drawdown counts as a contributor and its window still reads
  // `sqrt(NaN)`; the `isFinite` test is what keeps the warm-up rows missing
  // rather than snapping them to zero.
  const out = new Float64Array(length);
  let contributing = 0;
  for (let i = 0; i < length; i += 1) {
    if (squared[i]! !== 0) contributing += 1;
    if (i >= period && squared[i - period]! !== 0) contributing -= 1;
    out[i] =
      contributing === 0 && Number.isFinite(mean[i]!) ? 0 : Math.sqrt(mean[i]!);
  }

  return series.withColumn(output, out);
}
