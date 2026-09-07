import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_SOURCE } from '../contract/columns.js';
import { movingAverageValues } from '../kernels/moving-average.js';
import { rollingMeanValues } from '../kernels/rolling-mean.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';

export interface CenterOfGravityOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Look-back in **bars**. **Default `10`** (Ehlers'). */
  period?: number;
  /** Source column. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'cog'`.** */
  output?: Output;
}

/**
 * **Center of Gravity** (John F. Ehlers, *Stocks & Commodities*, May 2002 —
 * "The CG Oscillator") — where the window's price mass balances, measured
 * in **bars back from the current one**:
 *
 * ```
 * ${output}[i] = − Σ_{k=0}^{period−1} (k + 1) · column[i − k]
 *                 ────────────────────────────────────────────
 *                     Σ_{k=0}^{period−1} column[i − k]
 * ```
 *
 * The current bar carries weight `1` and the oldest carries `period`, so
 * the reading is a **negative** number between `−period` and `−1`: a flat
 * window balances in the middle at exactly `−(period + 1)/2`, and it moves
 * *up* (toward `−1`) as recent bars get heavier, i.e. as the price rises.
 * Ehlers' point was that this is a moment rather than a momentum — it has
 * essentially no lag, because a change in the newest bar moves the balance
 * point immediately.
 *
 * It is **not** a regression, despite arriving in the same batch: it takes
 * one position-weighted moment of the price rather than fitting a line, so
 * nothing here has a slope or a residual.
 *
 * ## The sign and the zero line — TradingView's convention, not Ehlers'
 *
 * Ehlers' published EasyLanguage adds `(Length + 1)/2` at the end, which
 * re-centres a flat window on `0`; TradingView's built-in `ta.cog` leaves
 * it off, so a flat window reads `−(period + 1)/2` (`−5.5` at the default
 * `period 10`). **This ships the uncentred form**, the one more consumers
 * plot. The two differ by a constant that depends only on `period`, so the
 * *shape* is identical and a caller who wants Ehlers' zero line adds
 * `(period + 1)/2`. The oracle pins the choice with a separation assert
 * rather than leaving it to be discovered from a chart, and the sign
 * convention is pinned the same way (the ascending-weight reading — oldest
 * bar lightest — is **0.17** away at `period 10`, a full window's worth of
 * the reading's own range on the fixture).
 *
 * **TA-Lib has no Center of Gravity**, so the oracle is a pandas
 * replication with the analytic first-valid bar (`period − 1`) asserted.
 *
 * ## No kernel: it is `wma` and `sma`, exactly
 *
 * The weights run *down* from the oldest bar, which neither
 * {@link rollingWeightedMeanValues} (per-**row** weights, not positional)
 * nor {@link symmetricWeightedValues} (a fixed 4-bar `1,2,2,1`) can
 * express — but no new kernel is needed either, because the descending
 * weights are the ascending ones subtracted from a constant. Writing `u`
 * for the position in the window (`0` oldest, `n − 1` newest), the numerator
 * weight `k + 1` is `n − u`, so
 *
 * ```
 * Σ (n − u)·p = (n + 1)·Σp − Σ (u + 1)·p = (n + 1)·n·SMA − WMA·n(n+1)/2
 * ```
 *
 * and dividing by `Σp = n·SMA` collapses the whole study to
 *
 * ```
 * ${output} = (period + 1) · ( WMA / (2 · SMA) − 1 )
 * ```
 *
 * — the K2 engine's `wma` over {@link rollingMeanValues}' `sma`, both O(N)
 * and flat in `period`, both already carrying the package's rebuild-every-
 * `period` numerics and its strict-window mask. The identity is exact
 * algebra rather than an approximation, and it is **pinned by a test**
 * against the naive `O(N·period)` definition (agreement ≤ 1.1e-14 at
 * `period 20` on the oracle input, ≤ 8.0e-15 over 50k bars at a price of
 * 1e12), so a future editor can check the shortcut rather than trust it.
 *
 * ## Edges
 *
 * - **Bounded `−period … −1`**, and both ends are reachable only in the
 *   limit (all the weight on the oldest bar, or on the newest).
 * - **A zero-sum window reads `undefined`, and the guard is live.** The
 *   division is the study's **output**, and the numerator is *not* forced
 *   to zero with the denominator — `[1, −1]` at `period 2` sums to `0`
 *   with a weighted sum of `−1` — so an unguarded `x / 0` would reach
 *   `withColumn` as `−Infinity` and **throw**. This is the
 *   {@link choppinessIndex} live-guard case, not the {@link ulcerIndex}
 *   dead one. Unreachable on prices (they are positive, so a zero sum
 *   means an all-zero window and `0/0` is already `NaN`); reachable, and
 *   unit-tested, over another study's output that crosses zero.
 * - **Scale-invariant, not shift-invariant.** Every term of both sums
 *   scales with the price and the ratio divides it out; **adding** a
 *   constant does not cancel — it adds `(n+1)/2·c` to the numerator and
 *   `c` to the mean, dragging the balance point toward the middle. Both
 *   halves are pinned, the second as a direction.
 * - **The strict window** — a bar is emitted only when all `period` cells
 *   are finite, because a positional weight cannot skip a cell without
 *   silently re-weighting the rest (`wma`'s rule, and `rollingMeanValues`
 *   masks the same way, so the two halves agree on where a reading
 *   exists). A **leading** gap shifts the start; an **interior** gap
 *   blanks `period` bars and then recovers.
 * - **A misnamed `column` reads all-missing** rather than throwing.
 */
export function centerOfGravity<
  S extends SeriesSchema,
  const Output extends string = 'cog',
>(series: TimeSeries<S>, options: CenterOfGravityOptions<S, Output> = {}) {
  const period = options.period ?? 10;
  assertPeriod(period);
  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const output = (options.output ?? 'cog') as Output;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const values = columnValues(wide, column);
  // The two halves of the identity derived above. Both are strict-window
  // array doors, so their masks agree bar for bar by construction.
  const weighted = movingAverageValues(values, period, 'wma');
  const mean = rollingMeanValues(values, period);

  const length = values.length;
  const out = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    const m = mean[i]!;
    // LIVE guard: this division is the study's last step, so a zero-sum
    // window would otherwise reach `withColumn` as ±Infinity and throw.
    // `NaN` is not `=== 0`, so a gap falls through and propagates.
    out[i] = m === 0 ? NaN : (period + 1) * (weighted[i]! / (2 * m) - 1);
  }
  return series.withColumn(output, out);
}
