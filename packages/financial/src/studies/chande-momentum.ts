import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_SOURCE } from '../contract/columns.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';
import { rollingMeanValues } from '../kernels/rolling-mean.js';
import { upDownLegValues } from '../kernels/up-down.js';

export interface ChandeMomentumOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Look-back in **bars**. **Default `14`** (Chande's own). */
  period?: number;
  /** Source column. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'cmo'`.** */
  output?: Output;
}

/**
 * **Chande Momentum Oscillator** (Tushar Chande, 1994) — the normalised
 * difference between the up moves and the down moves of the last `period`
 * bars, bounded `−100..100`:
 *
 * ```
 * cmo = 100 · (Σ up − Σ down) / (Σ up + Σ down)
 * ```
 *
 * where `up` and `down` are the positive and negative parts of the bar-over-bar
 * change ({@link upDownLegValues}) and both sums are **plain, unsmoothed**
 * window sums.
 *
 * Appends one column; `undefined` for the first `period` rows. Note that is
 * `period`, not `period − 1`: CMO is computed from **differences**, so a
 * `period`-bar sum of them needs `period + 1` bars of input — the {@link rsi}
 * warm-up, for the same reason.
 *
 * ## Definition — Chande's unsmoothed sums, deliberately not TA-Lib's `CMO`
 *
 * **TA-Lib's `CMO` is a different indicator, and it is one this package
 * already ships.** TA-Lib smooths the two legs with Wilder's recursion before
 * taking the ratio, which makes its `CMO` exactly `2 · RSI − 100` — measured
 * on the oracle's own input, `talib.CMO` and `2 · talib.RSI − 100` agree to
 * **2.8e-14** at both `period 14` and `period 5`. So shipping TA-Lib's
 * definition would have added a study that is an affine re-scaling of
 * {@link rsi}: a second name for a shipped column, differing from it
 * invisibly. Step 0 of the studies README exists to catch precisely that.
 *
 * What ships is therefore **Chande's original**: unsmoothed sums over the
 * window, which is also the form the corpus names (ChartIQ: "unsmoothed
 * up/down sums") and the one VIDYA's adaptive smoothing constant is defined
 * on. It is a genuinely different series from the Wilder-smoothed one, not a
 * warm-up transient: measured against `talib.CMO` on the oracle input it
 * differs by up to **68.28 points** at `period 14` and **131.55** at
 * `period 5`, on a scale that only spans 200. The two agree on where the
 * warm-up ends (both first emit on bar `period`), which is what makes the
 * difference a definition rather than an alignment.
 *
 * A caller who wants TA-Lib's number has it exactly, today, as
 * `2 · rsi(...) − 100` — a fact the oracle asserts rather than a claim this
 * docstring makes.
 *
 * ## Edges
 *
 * - **Bounded `−100..100`** by construction: `|Σup − Σdown| ≤ Σup + Σdown`.
 *   `+100` is a window with no down bars, `−100` one with no up bars; both
 *   are real readings, not saturation.
 * - **A perfectly flat window** (`Σup + Σdown = 0`) → `undefined`. The ratio
 *   is `0/0`: there is no momentum to normalise, and `0` is the value CMO
 *   gives for a *balanced* window, so emitting it here would conflate "no
 *   movement" with "movement that cancelled". The {@link rsi} precedent. No
 *   guard is written for it — with non-negative legs a zero denominator
 *   forces a zero numerator, so the case *is* `0/0` and arrives as `NaN` on
 *   its own; a guard would be code no test could distinguish from its
 *   absence.
 * - **An unchanged bar counts as neither up nor down** — it contributes `0`
 *   to both sums, which shrinks nothing but the ratio's sensitivity.
 * - **Scale- and shift-invariant**: the legs are differences (so a constant
 *   added to every price cancels) and the ratio is homogeneous of degree
 *   zero (so a scale factor cancels). Both pinned by property tests — CMO is
 *   on the {@link rsi} side of the scale pair, not the {@link atr} side.
 * - **A leading gap shifts the start** (running over another study's output
 *   begins that many bars later) — the sums are windowed, not recursive, so
 *   nothing is poisoned.
 * - **An interior gap costs the windows containing it**, and CMO recovers
 *   once it leaves: the bar with the gap and the `period` bars after it are
 *   `undefined`, then values resume. This is the window rule, and it is the
 *   deliberate asymmetry against {@link rsi}, whose Wilder recursion carries
 *   an interior gap to the end of the series.
 */
export function chandeMomentum<
  S extends SeriesSchema,
  const Output extends string = 'cmo',
>(series: TimeSeries<S>, options: ChandeMomentumOptions<S, Output> = {}) {
  const period = options.period ?? 14;
  assertPeriod(period);
  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const output = (options.output ?? 'cmo') as Output;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const v = columnValues(wide, column);
  const length = v.length;
  // Bar-over-bar change. Bar 0 has no predecessor, so it is `NaN` rather
  // than 0 — the array door then starts the sums on bar 1, which is what
  // puts the first reading on bar `period` rather than `period − 1`.
  const deltas = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    deltas[i] = i === 0 ? NaN : v[i]! - v[i - 1]!;
  }

  const { up, down } = upDownLegValues(deltas);
  // Means, not sums: the shared `1/period` cancels in the ratio, so this is
  // Chande's formula exactly while composing on the shipped kernel (whose
  // array door — a window emits only once its last `period` rows are all
  // finite — is also the warm-up and interior-gap rule documented above).
  const meanUp = rollingMeanValues(up, period);
  const meanDown = rollingMeanValues(down, period);

  const out = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    const u = meanUp[i]!;
    const d = meanDown[i]!;
    out[i] = (100 * (u - d)) / (u + d);
  }
  return series.withColumn(output, out);
}
