import { assertPeriod } from './rolling.js';

/**
 * One bar's rolling ordinary-least-squares fit — the three arrays
 * {@link linearRegressionValues} returns, row-aligned with its input.
 *
 * `NaN` marks a bar the fit does not cover ([PND-STUDYBOX]).
 */
export interface RollingRegression {
  /** Change in the fitted line **per bar** — the units are the column's own
   *  per bar, so it scales with the price and is not a percentage. */
  readonly slope: Float64Array;
  /** The fitted line at the window's **first** bar (`x = 0`) — TA-Lib's
   *  `LINEARREG_INTERCEPT`, not the value at the window's last bar. */
  readonly intercept: Float64Array;
  /** Coefficient of determination, `0 … 1` — the fraction of the window's
   *  variance the line explains. A **flat** window is `NaN`, not `0`. */
  readonly r2: Float64Array;
}

/**
 * **Rolling linear regression** — kernel **K7** of the corpus assessment
 * (`docs/notes/financial-indicators-assessment-2026-07.md` §4, §6.7).
 *
 * For every bar, the least-squares fit of the last `period` values against
 * the bar index **`x = 0 … period − 1`, with `x = 0` the window's OLDEST
 * bar**:
 *
 * ```
 * slope[i]     = (n·Σxy − Σx·Σy) / (n·Σx² − (Σx)²)
 * intercept[i] = (Σy − slope·Σx) / n          the fit at x = 0
 * r2[i]        = (n·Σxy − Σx·Σy)² / ((n·Σx² − (Σx)²)·(n·Σy² − (Σy)²))
 * ```
 *
 * Everything the regression family reports is one of these three or a
 * **projection** of the first two — the fit read at some `x`:
 *
 * | reading | `x` | TA-Lib |
 * | --- | --- | --- |
 * | the window's last bar | `period − 1` | `LINEARREG` |
 * | one bar past the window | `period` | `TSF` |
 *
 * so {@link linearRegressionAt} is how a study asks for one, and no study
 * re-derives the arithmetic.
 *
 * ## `x` is deterministic, which is the whole reason this is O(N)
 *
 * The regressor is the bar index, so `Σx = n(n−1)/2` and
 * `Σx² = (n−1)n(2n−1)/6` are **constants of `period`** — and so is the
 * denominator, `n²(n²−1)/12`, which is strictly positive for every
 * `period ≥ 2`. Only `Σy`, `Σxy` and `Σy²` move, and each has an O(1)
 * update, so the whole kernel is **O(N) and flat in `period`**: no window
 * is ever rescanned except by the amortised rebuild below.
 *
 * `Σxy`'s update is the one that has to be written for it. The window's
 * origin moves with the window, so every retained term loses one unit of
 * `x`:
 *
 * ```
 * Σxy(i) = Σxy(i−1) − Σy(i−1) + y[i−period] + (period−1)·y[i]
 * ```
 *
 * — the same running-weighted-sum shape `wma` runs on, with the extra
 * `y[i−period]` term because the leaving bar's weight goes to `−1` rather
 * than to `0` under this origin.
 *
 * ## Numerics: the shifted frame, and why `Σxy − ΣxΣy/n` is not enough
 *
 * Every accumulator here works on `z = y − anchor`, and the state is
 * **rebuilt from the window every `period` rows**, aligned to `i % period
 * === 0` — the {@link rollingMeanSdInto} pattern ([PND-SHIFTFRAME] /
 * [PND-PROCKERN]), for the same reason and with the same amortised cost of
 * one extra accumulation per row.
 *
 * The reason is sharper here than for a mean. `n·Σxy − Σx·Σy` is a
 * difference of two quantities that are each `O(n²·ȳ)` and whose
 * difference is `O(n²·σ)`, so at a price of 1e15 with a window spanning ±3
 * the subtraction cancels away everything the answer is made of. Shifting
 * by a value **inside the window** makes both operands `O(n²·σ)` instead,
 * and the aligned rebuild is what keeps the anchor at most one window
 * turnover stale (the anchor row is always inside the current window: it
 * is `⌊i/period⌋·period`, and `i − ⌊i/period⌋·period ≤ period − 1`).
 *
 * Every quantity this kernel reports is invariant to that shift —
 * `slope`, the two moment differences in `r2`, and `intercept` once
 * `anchor` is added back — so shifting costs nothing but the subtraction.
 *
 * Measured over 200k rows of `base + 0.01·i + 3·sin(i/7)` at `period 20`,
 * worst absolute error against a two-pass (centred) reference at the same
 * magnitude — the same kernel with the anchor pinned at 0 is the control
 * (`scratchpad/regression-check.mjs`):
 *
 * | base | reading | raw frame | shifted |
 * | --- | --- | --- | --- |
 * | `1e6` | slope | 2.97e-5 | **0** |
 * | `1e6` | intercept | 2.82e-4 | **5.8e-10** (≈1 ulp of 1e6) |
 * | `1e6` | r² | 1.56e-2 | **5.1e-15** |
 * | `1e12` | slope | 27.8 | **0** |
 * | `1e12` | intercept | 264 | **4.9e-4** (≈1 ulp of 1e12) |
 * | `1e12` | r² | `Infinity` | **1.0e-7** |
 *
 * The right-hand column is the gap to a two-pass **reference**, which is
 * itself ~1 ulp from the true value at this scale, so the intercept and r²
 * rows read as an order of magnitude (a second reference measured 6.1e-4 and
 * 1.3e-7), not as exact figures; the slope row and the raw-frame column are
 * the load-bearing ones.
 *
 * The raw frame does not merely lose precision at 1e12 — its `r²` leaves
 * `[0, 1]` and reaches `Infinity`, because the two cancelling moment
 * differences round to different signs. Above ~1e13 the *input* stops
 * carrying the answer (a ±3 window at 1e15 spans ~48 ulps, so `y − anchor`
 * is quantised to ~2% of the spread) and no arrangement of the arithmetic
 * recovers it; that is a representation limit, not this kernel's.
 *
 * ## A flat window: slope `0` exactly, `r2` **undefined**
 *
 * The two halves of the answer go opposite ways, and the #699 test (is the
 * numerator *forced* to zero?) is what splits them.
 *
 * - **`slope` is forced to zero** — every `y` equal makes `n·Σxy − Σx·Σy`
 *   algebraically zero — so a flat window's slope is `0`, its `intercept`
 *   is the value itself, and both projections read that value back. That
 *   is the {@link clvValues} flat-bar case: a real reading, not a gap.
 * - **`r2` is a genuine `0/0`** — the line explains all of zero variance —
 *   so it is `undefined`, the {@link percentOfRangeValues} flat-window
 *   case. `0` would be a lie in the readable direction ("no fit"), which
 *   is exactly why it is not what ships.
 *
 * Both need the flat window to be **recognised** rather than computed, and
 * the second one badly. The accumulators carry an `O(ε)` residue from the
 * bars that have just left, so the `slope` numerator and the `r2`
 * denominator are both residues rather than zeros — and `r2` is their
 * *ratio*, which is not small, not bounded, and not even signed the right
 * way. Measured: on `[186.6, 154.81, 103.74, 193.5, 193.5, 193.5, 193.5]`
 * at `period 3`, the flat window at bar 5 reads `slope = −2.1e-14` and
 * **`r2 = −13.5`** without the count, against `0` and `undefined` with it.
 * A running count of the **changes** inside the window (rows `j` in
 * `(lo, i]` with `y[j] !== y[j−1]`) is O(1) per bar, exact — it is integer
 * arithmetic, so it needs no rebuild — and decides both. It is the
 * {@link ulcerIndex} counter by the same argument, one step worse: there
 * the residue was a small wrong number, here it is a wrong number outside
 * the statistic's own range.
 *
 * The counter settles *mathematical* degeneracy only. A window that changes
 * by a few ulps is not flat, but the rolling `n·Σz² − (Σz)²` can still
 * cancel to zero or below (reviewed 2026-09-07: a 3-bar window at
 * `3.002998998997 ± 1e-15` read `spread = −1.0e-24` and a **negative r²**).
 * When `spread ≤ 0` on a changing window the kernel recomputes that one
 * window two-pass and centred — `Σ(dx·dz)² / (Σdx²·Σdz²)`, no cancelling
 * subtraction — and every emitted `r²` is pinned to its bound of 1, so the
 * column's `0 … 1` contract holds on every finite cell (property-tested over
 * ulp-jittered input at four magnitudes and four periods). O(period) per
 * such window, and only such windows.
 *
 * ## Missing cells: the **strict** window, like `wma`
 *
 * A row is emitted once the window spans `period` rows **and every one of
 * them is finite**. That is the positional-weight rule
 * ({@link movingAverageValues}' `wma` / `trima` / `hull`), and it is
 * forced here: `x` names a *position*, so dropping a cell would silently
 * re-index every bar after it and fit a line against the wrong abscissa.
 * A **leading** run of gaps therefore steps the warm-up over (a study run
 * over another study's output starts late rather than coming back empty),
 * and an **interior** gap blanks the gap bar and the `period − 1` bars
 * after it, after which the fit recovers.
 *
 * O(N), one pass, three allocations.
 */
export function linearRegressionValues(
  values: Float64Array,
  period: number,
): RollingRegression {
  // Validated here as well as in each study, because this is a public
  // export: `period 1` would divide by a zero `denominator` rather than
  // throw, and `period 0` would not even have that.
  assertPeriod(period);
  assertRegressionPeriod(period, 'linearRegressionValues');

  const length = values.length;
  const slope = new Float64Array(length).fill(NaN);
  const intercept = new Float64Array(length).fill(NaN);
  const r2 = new Float64Array(length).fill(NaN);

  const sumX = (period * (period - 1)) / 2;
  const sumXX = ((period - 1) * period * (2 * period - 1)) / 6;
  // n·Σx² − (Σx)² = n²(n²−1)/12 — a positive constant for every period ≥ 2,
  // which is why this kernel has no zero-denominator guard on the slope.
  const denominator = period * sumXX - sumX * sumX;

  // Every accumulator below is in the SHIFTED frame: it sees `y − anchor`,
  // never `y` (see the numerics note above).
  let anchor = 0;
  let sumZ = 0;
  let sumXZ = 0;
  let sumZZ = 0;
  let missing = 0;
  // Changes strictly inside the window. Zero means flat, which is the one
  // state the accumulators cannot report exactly (see above). `NaN !== NaN`,
  // so a gap counts as a change — a window holding one is masked anyway.
  let changes = 0;

  for (let i = 0; i < length; i += 1) {
    const low = i - period + 1; // the window is [low, i]; rows < 0 read 0
    const leavingIndex = low - 1;
    const entering = values[i]!;
    const enteringFinite = Number.isFinite(entering);
    const zIn = enteringFinite ? entering - anchor : 0;

    let zOut = 0;
    if (leavingIndex >= 0) {
      const leaving = values[leavingIndex]!;
      if (Number.isFinite(leaving)) zOut = leaving - anchor;
      else missing -= 1;
    }
    if (!enteringFinite) missing += 1;

    // Σxy first: shifting the origin costs the OLD Σy, so this must read it
    // before the line below moves it.
    sumXZ = sumXZ - sumZ + zOut + (period - 1) * zIn;
    sumZ = sumZ - zOut + zIn;
    sumZZ = sumZZ - zOut * zOut + zIn * zIn;

    // The window [low, i] holds the changes at indices low+1 … i, so one
    // enters at `i` and one leaves at `low`.
    if (i >= 1 && values[i] !== values[i - 1]) changes += 1;
    if (low >= 1 && values[low] !== values[low - 1]) changes -= 1;

    // The aligned rebuild — `i % period`, not a counter, so a caller reading
    // the same column twice reconstructs the same state (`ranged.ts`).
    if (i % period === 0) {
      const at = values[i]!;
      anchor = Number.isFinite(at) ? at : 0;
      sumZ = 0;
      sumXZ = 0;
      sumZZ = 0;
      missing = 0;
      for (let k = low > 0 ? low : 0; k <= i; k += 1) {
        const y = values[k]!;
        if (!Number.isFinite(y)) {
          missing += 1;
          continue;
        }
        const z = y - anchor;
        sumZ += z;
        sumXZ += (k - low) * z;
        sumZZ += z * z;
      }
    }

    if (i < period - 1 || missing > 0) continue;
    if (changes === 0) {
      // A flat window. `slope` is forced to zero and `intercept` is the
      // value itself; `r2` stays `NaN` — the 0/0 the line cannot answer.
      slope[i] = 0;
      intercept[i] = entering;
      continue;
    }
    const numerator = period * sumXZ - sumX * sumZ;
    const m = numerator / denominator;
    slope[i] = m;
    // Back into the unshifted frame: the fit at x = 0 is `mean(y) − m·mean(x)`,
    // and `mean(y) = anchor + Σz/n`.
    intercept[i] = anchor + (sumZ - m * sumX) / period;
    // n·Σy² − (Σy)² is the window's own variance times n², translation-
    // invariant, so the shifted accumulators give it unchanged.
    const spread = period * sumZZ - sumZ * sumZ;
    if (spread <= 0) {
      // Numerically degenerate, not flat: the window changes (the counter
      // says so) but its values differ by so little that the rolling
      // `n·Σz² − (Σz)²` cancelled to zero or below. A negative `spread`
      // here put a negative r² into a column whose contract is 0 … 1
      // (reviewed 2026-09-07, a 3-bar window at 3.002998998997 ± 1e-15).
      // Recompute BOTH moments for this window two-pass and centred — no
      // cancellation — so the ratio is a real r², not a ratio of residues.
      // O(period), and only on a window this degenerate.
      let meanZ = 0;
      for (let k = low; k <= i; k += 1) meanZ += values[k]! - anchor;
      meanZ /= period;
      const meanXc = (period - 1) / 2;
      let sxx = 0;
      let sxz = 0;
      let szz = 0;
      for (let k = low; k <= i; k += 1) {
        const dx = k - low - meanXc;
        const dz = values[k]! - anchor - meanZ;
        sxx += dx * dx;
        sxz += dx * dz;
        szz += dz * dz;
      }
      if (szz <= 0) {
        // Still nothing to explain — treat as the flat case's 0/0.
        continue;
      }
      // r² = Σ(dx·dz)² / (Σdx² · Σdz²) — the same ratio as below, on the
      // centred sums (n·Σz² − (Σz)² is n·Σdz², and likewise for the others).
      const r = (sxz * sxz) / (sxx * szz);
      r2[i] = r > 1 ? 1 : r;
      continue;
    }
    const r = (numerator * numerator) / (denominator * spread);
    // r² is bounded by 1 in exact arithmetic; a last-ulp overshoot is not a
    // reading, so it is pinned to the bound rather than reported.
    r2[i] = r > 1 ? 1 : r;
  }

  return { slope, intercept, r2 };
}

/**
 * **The fitted line read at one `x`** — the projection every regression
 * study reports.
 *
 * `x` is in the window's own coordinates, the ones
 * {@link linearRegressionValues} documents: `period − 1` is the window's
 * last bar (TA-Lib `LINEARREG`), `period` is one bar past it (TA-Lib
 * `TSF`), and `0` would be the intercept itself.
 *
 * Row-aligned and length-preserving; a bar with no fit stays `NaN`.
 */
export function linearRegressionAt(
  fit: RollingRegression,
  x: number,
): Float64Array {
  const length = fit.slope.length;
  const out = new Float64Array(length);
  for (let i = 0; i < length; i += 1)
    out[i] = fit.intercept[i]! + fit.slope[i]! * x;
  return out;
}

/**
 * Throw unless `period` is at least 2 — a one-bar window has no slope.
 *
 * The regression denominator is `n²(n²−1)/12`, which is **zero at `n = 1`**:
 * one point does not determine a line, so the answer would be `0/0` on
 * every bar rather than a degenerate-but-defined reading. Named here so the
 * kernel and its three studies give the same message with their own name in
 * it — the {@link choppinessIndex} `log10(1) = 0` precedent.
 */
export function assertRegressionPeriod(period: number, name: string): void {
  if (period < 2) {
    throw new TypeError(
      `${name} period must be at least 2 (a one-bar window has no slope: the regression denominator n²(n²−1)/12 is 0 at n = 1)`,
    );
  }
}
