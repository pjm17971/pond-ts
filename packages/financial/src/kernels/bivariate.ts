import { assertPeriod } from './rolling.js';
/** The three row-aligned moment arrays {@link rollingBivariateValues}
 *  returns, each `NaN` wherever the window has no reading. */
export interface RollingBivariateMoments {
  /** Population covariance of the two columns over the window. */
  covariance: Float64Array;
  /** Population variance of the **first** column (`x`) over the window. */
  varianceX: Float64Array;
  /** Population variance of the **second** column (`y`) over the window. */
  varianceY: Float64Array;
}

/**
 * **Rolling bivariate moments** — kernel **K8** of the corpus assessment
 * (`docs/notes/financial-indicators-assessment-2026-07.md` §3, gap note G7):
 * the population covariance of two row-aligned columns over a trailing
 * `period`-bar window, with each column's own variance beside it.
 *
 * Every two-series study in the package is a closed form over this triple:
 *
 * ```
 * correlation = cov / sqrt(varX · varY)          (Pearson's r)
 * beta        = cov / varY                       (slope of x on y)
 * ```
 *
 * so they share one pass, one warm-up rule and one set of numerics rather
 * than three near-identical accumulators.
 *
 * ## Population moments, `ddof = 0`
 *
 * Divided by `period`, not `period − 1` — the package's convention
 * everywhere (`bollinger`'s σ, `rollingStdev`, `zScore`, and TA-Lib's own
 * `STDDEV`/`CORREL`/`BETA`). It does not matter for `correlation` or `beta`,
 * whose `n`s cancel; it matters for a caller reading `covariance` directly,
 * so it is stated rather than left to be inferred.
 *
 * ## The window is STRICT — all `period` rows of BOTH columns, or nothing
 *
 * A bivariate moment is a statement about **pairs**. A row where one column
 * is missing contributes no pair, so "average over whatever is present"
 * would silently compute a `period`-bar correlation from fewer than `period`
 * pairs — and, worse, from a *different* set of pairs than the caller's other
 * column-pair study saw. So a window emits only when all `period` rows carry
 * a finite value in **both** columns; anything else is `NaN`.
 *
 * This is {@link rollingMeanValues}' array-door rule ("`period` finite
 * values, not `period` rows") extended to the pair, and the same argument:
 * a correlation over three of the last thirty bars is a different statistic
 * from the one the option named. It is deliberately *not* the count-window
 * rule core's reducers use (rows, skipping gaps), which is right for an
 * extreme (`highestLowestValues`) and wrong for a moment.
 *
 * ## Numerics — shifted frame, Welford, rebuild every `period` rows
 *
 * The textbook `Σxy − Σx·Σy/n` is not used. It is the arrangement that
 * cancels catastrophically at price magnitudes: `Σxy` at 1e12-scale prices
 * is ~1e24 per pair, and a covariance of order 1 is then the difference of
 * two numbers agreeing in their first 24 digits — of which a double has 16.
 * Three things instead, all of them the pattern
 * [PND-SHIFTFRAME]/[PND-PROCKERN] established on `rollingDeviationSd` and
 * `ranged.ts`:
 *
 * - **Shifted frame.** Every accumulator sees `x − anchorX`, `y − anchorY`,
 *   where the anchors are values taken from *inside* the current window. The
 *   moments are translation-invariant, so this changes no answer and fixes
 *   the conditioning: the accumulated quantities are the size of the
 *   window's spread rather than the size of the price.
 * - **Welford / co-moment updates, not raw sums of products.** `m2x`, `m2y`
 *   and `cxy` are carried already centred, so there is no cancelling
 *   subtraction at the end at all — the emitted value is a division, not a
 *   difference.
 * - **Rebuild every `period` rows**, aligned to the absolute index
 *   (`i % period === 0`). The reverse-Welford removal below is the one step
 *   that accumulates drift; a full window turnover is the only rebuild
 *   interval that is scale-free (a fixed interval is simultaneously too long
 *   for a short `period` and too dear for a long one — the Codex finding on
 *   `rollingDeviationSd`). One extra accumulation per row at any `period`.
 * - **Rebuild on demand when a moment is ill-conditioned.** The change
 *   counter (below) settles mathematical flatness; a window whose values
 *   differ by ulps, or a plateau the anchor has gone stale across, is not
 *   flat, yet its moments are then residues: `m2` driven to 0 (a *false*
 *   missing cell), or a tiny positive `m2` beside a co-moment residue
 *   (|corr| = 20.5, reviewed 2026-09-07). So the kernel also carries the
 *   **gross** shifted squares that have passed through the moments since
 *   the last rebuild — added and removed alike, the scale their residue
 *   is measured against — and rebuilds the window fresh on its own first
 *   pair whenever a changing column's `m2` is below `1e-3` of that, or
 *   `cxy² > m2x · m2y` past rounding slack. O(period) per such window;
 *   the property test checks the emitted correlation against an exact
 *   BigInt-rational reference over plateau-stepped, ulp-jittered input.
 *   Below |x| ≈ 1e-154 the squares underflow and the window reads flat.
 *
 * Measured over 200k rows at `period 30`, worst **absolute error in the
 * resulting correlation coefficient** (the scale that means something when
 * the covariance itself passes through zero) against a per-window two-pass
 * reference that shifts by the window's own first value:
 *
 * | input                          | naive `Σxy − ΣxΣy/n` | this    |
 * | ------------------------------ | -------------------- | ------- |
 * | random walk ≈100               | 7.4e-7               | 1.7e-14 |
 * | prices ≈1e6 with ±3 structure  | 1.2e-2               | 2.1e-15 |
 * | prices ≈1e12 with ±3 structure | **`Infinity`**       | 2.4e-15 |
 *
 * The last row is the point: at 1e12 the naive form's variance comes back
 * *negative*, so its correlation is not merely wrong but non-finite, while
 * this one has not moved. The 1e6 and 1e12 rows are pinned by
 * `test/bivariate-kernel.test.ts`; the full comparison is
 * `scratchpad/two-series-numerics2.mjs`.
 *
 * ## A flat window reads exact zeros — from a change counter, not the sums
 *
 * If one column is constant across the window, its variance is `0` and so
 * is its covariance with anything, and every consumer's division is a
 * genuine `0 / 0` → `NaN` → a missing cell. The accumulators get that
 * *exactly* right only on a rebuild row or on a column that has been flat
 * since the last rebuild: a column that goes flat **mid-window** (a
 * tick-frozen price, a forward-filled benchmark) leaves the reverse-Welford
 * removal with a ~2e-16 residue in `cxy` beside an `m2x` clamped to exact
 * `0`, and `residue / sqrt(0)` is `±Infinity` — which `withColumn` throws
 * on. Measured (Layer-2 review of #706): `close = 100 + ⌊i/97⌋` against a
 * moving benchmark threw at `period 30` on 416 of 4 971 rows.
 *
 * So the kernel keeps a per-column **change counter** — how many adjacent
 * pairs inside the window differ, maintained in O(1) per row — and when it
 * reads `0` the column's variance and the covariance are written as exact
 * `0` regardless of what the accumulators hold. This is the same device
 * `linearRegressionValues` uses for its `r²`, and for the same reason: a
 * flat window is a *fact about the input*, not a value the sums converge
 * to. (Contrast `priceRelative`, whose zero denominator sits at the output
 * with a live numerator and *does* need a guard of its own.) Pinned by
 * kernel tests on a globally flat column and on a column that goes flat
 * mid-window between rebuilds.
 *
 * O(N) time — one add, one remove and one amortised rebuild step per row —
 * three allocations.
 */
/** See {@link linearRegressionValues}' `RELATIVE_SPREAD_FLOOR`: a moment
 *  below this fraction of the gross shifted-squares magnitude that has
 *  passed through it since the last rebuild is residue, not variance. */
const RELATIVE_MOMENT_FLOOR = 1e-3;
/** `cxy² ≤ m2x·m2y` exactly; a rebuilt window honours it to `O(period·ε)`,
 *  so a violation past this slack means the co-moment has drifted. */
const CAUCHY_SCHWARZ_SLACK = 1 + 1e-6;

export function rollingBivariateValues(
  x: Float64Array,
  y: Float64Array,
  period: number,
): RollingBivariateMoments {
  assertPeriod(period);
  if (period < 2) {
    throw new TypeError(
      'rollingBivariateValues period must be at least 2 (a one-bar window has no variance)',
    );
  }
  const length = x.length;
  if (y.length !== length) {
    throw new TypeError(
      'rollingBivariateValues needs two row-aligned columns of equal length',
    );
  }
  const covariance = new Float64Array(length).fill(NaN);
  const varianceX = new Float64Array(length).fill(NaN);
  const varianceY = new Float64Array(length).fill(NaN);

  // Every accumulator below lives in the SHIFTED frame — it sees
  // `x - anchorX` and `y - anchorY`, never the raw values.
  let anchorX = 0;
  let anchorY = 0;
  let count = 0;
  let meanX = 0;
  let meanY = 0;
  let m2x = 0;
  let m2y = 0;
  let cxy = 0;
  // Change counters: how many adjacent pairs inside the window differ, per
  // column. `0` means the column is flat across the whole window, and that
  // is the one fact the accumulators cannot be trusted to report exactly
  // between rebuilds — see "A flat window" above.
  let changesX = 0;
  let changesY = 0;
  // GROSS shifted squares that have passed through the moments since the
  // last rebuild — added and removed alike, never decreasing — the scale
  // their residue is measured against (the current sums can themselves be
  // residue after a plateau step).
  let grossX = 0;
  let grossY = 0;
  let windowStart = 0;
  let windowEnd = 0;
  // Rows in the window with a missing cell in EITHER column. The window is
  // strict, so this is the whole mask: one incomplete pair blanks the row.
  let incomplete = 0;

  // Recompute every moment from the current window [windowStart, windowEnd),
  // re-anchoring on its first complete pair so the offsets are bounded by
  // the window's own spread whatever the magnitude. Called on the aligned
  // schedule (`i % period === 0`) and on demand when a removal has driven a
  // changing column's `m2` to zero or below.
  const rebuild = (): void => {
    anchorX = 0;
    anchorY = 0;
    for (let k = windowStart; k < windowEnd; k += 1) {
      const u = x[k]!;
      const v = y[k]!;
      if (Number.isFinite(u) && Number.isFinite(v)) {
        anchorX = u;
        anchorY = v;
        break;
      }
    }
    count = 0;
    meanX = 0;
    meanY = 0;
    m2x = 0;
    m2y = 0;
    cxy = 0;
    grossX = 0;
    grossY = 0;
    for (let k = windowStart; k < windowEnd; k += 1) {
      const u = x[k]!;
      const v = y[k]!;
      if (!Number.isFinite(u) || !Number.isFinite(v)) continue;
      const su = u - anchorX;
      const sv = v - anchorY;
      grossX += su * su;
      grossY += sv * sv;
      count += 1;
      const dx = su - meanX;
      const dy = sv - meanY;
      meanX += dx / count;
      meanY += dy / count;
      m2x += dx * (su - meanX);
      m2y += dy * (sv - meanY);
      cxy += dx * (sv - meanY);
    }
  };

  for (let i = 0; i < length; i += 1) {
    const lo = i - period + 1 > 0 ? i - period + 1 : 0;

    while (windowEnd <= i) {
      const u = x[windowEnd]!;
      const v = y[windowEnd]!;
      if (windowEnd > windowStart) {
        if (x[windowEnd - 1] !== u) changesX += 1;
        if (y[windowEnd - 1] !== v) changesY += 1;
      }
      if (Number.isFinite(u) && Number.isFinite(v)) {
        const su = u - anchorX;
        const sv = v - anchorY;
        grossX += su * su;
        grossY += sv * sv;
        count += 1;
        const dx = su - meanX;
        const dy = sv - meanY;
        meanX += dx / count;
        meanY += dy / count;
        m2x += dx * (su - meanX);
        m2y += dy * (sv - meanY);
        // The co-moment update pairs the OLD x-deviation with the NEW
        // y-mean; a flat column makes `sv - meanY` exactly zero, which is
        // what gives the exact `0` covariance documented above.
        cxy += dx * (sv - meanY);
      } else {
        incomplete += 1;
      }
      windowEnd += 1;
    }

    while (windowStart < lo) {
      const u = x[windowStart]!;
      const v = y[windowStart]!;
      if (windowStart + 1 < windowEnd) {
        if (x[windowStart + 1] !== u) changesX -= 1;
        if (y[windowStart + 1] !== v) changesY -= 1;
      }
      if (Number.isFinite(u) && Number.isFinite(v)) {
        const su = u - anchorX;
        const sv = v - anchorY;
        grossX += su * su;
        grossY += sv * sv;
        if (count <= 1) {
          count = 0;
          meanX = 0;
          meanY = 0;
          m2x = 0;
          m2y = 0;
          cxy = 0;
        } else if (count === 2) {
          // Down to one pair: the moments are exactly zero and the mean is
          // exactly the survivor, so say so rather than letting the general
          // recurrence leave a residue at the one size where the answer is
          // known in closed form (the `ranged.ts` special case).
          count = 1;
          meanX = meanX * 2 - su;
          meanY = meanY * 2 - sv;
          m2x = 0;
          m2y = 0;
          cxy = 0;
        } else {
          const oldMeanX = meanX;
          const oldMeanY = meanY;
          count -= 1;
          meanX = oldMeanX - (su - oldMeanX) / count;
          meanY = oldMeanY - (sv - oldMeanY) / count;
          m2x -= (su - meanX) * (su - oldMeanX);
          m2y -= (sv - meanY) * (sv - oldMeanY);
          cxy -= (su - meanX) * (sv - oldMeanY);
          if (m2x < 0) m2x = 0;
          if (m2y < 0) m2y = 0;
        }
      } else {
        incomplete -= 1;
      }
      windowStart += 1;
    }

    // The aligned rebuild — `i % period`, not a counter, so the state at a
    // given row does not depend on where the sweep began. It does two jobs:
    // it re-anchors the shifted frame before the series has trended away
    // from it, and it discards whatever drift the removals above have
    // accumulated, neither of which can then survive one window turnover.
    if (i % period === 0) rebuild();

    if (windowEnd - windowStart < period || incomplete > 0) continue;
    // A flat column has variance exactly 0 and covariance exactly 0 by
    // definition; the counters say so where the accumulators only nearly do.
    const flatX = changesX === 0;
    const flatY = changesY === 0;
    // The converse: a column that DOES change must have a positive variance.
    // The reverse-Welford removal can drive `m2` to zero or below on a
    // window whose values differ by ulps (the clamp above then reads 0 and
    // the study reports a false missing cell — reviewed 2026-09-07). That is
    // numerical, not mathematical, degeneracy, so the answer is a fresh
    // O(period) rebuild of this window: Welford's `m2` is a sum of
    // non-negative terms and is positive for any non-flat window.
    if (
      (!flatX && m2x < RELATIVE_MOMENT_FLOOR * grossX) ||
      (!flatY && m2y < RELATIVE_MOMENT_FLOOR * grossY) ||
      cxy * cxy > m2x * m2y * CAUCHY_SCHWARZ_SLACK
    ) {
      // Ill-conditioned, not flat: the moment is a residue beside the
      // gross magnitude that has passed through it since the last rebuild
      // (the anchor went stale across a plateau step), or the co-moment
      // has drifted past what Cauchy–Schwarz allows. Rebuild this window
      // fresh on its own first pair. With `gross = 0` the squares
      // themselves underflowed (|x| ≲ 1e-154): nothing to rebuild from,
      // and the window reads as flat.
      rebuild();
    }
    covariance[i] = flatX || flatY ? 0 : cxy / count;
    varianceX[i] = flatX ? 0 : m2x / count;
    varianceY[i] = flatY ? 0 : m2y / count;
  }

  return { covariance, varianceX, varianceY };
}
