import { cumulativeValues } from './cumulative.js';

/**
 * **Close Location Value** — where the close sits inside the bar's own range,
 * as a number in `[−1, +1]`:
 *
 * ```
 * clv = ((close − low) − (high − close)) / (high − low)
 * ```
 *
 * `+1` is a close on the high, `−1` a close on the low, `0` the midpoint.
 * Chaikin's reading of it is buying-vs-selling pressure: the fraction of the
 * bar's volume that "belongs" to the buyers minus the fraction that belongs
 * to the sellers. It is the per-bar term of both the Accumulation/
 * Distribution line (which sums `clv · volume`) and Chaikin Money Flow
 * (which ratios the same product against volume over a window), which is why
 * the loop lives here rather than in either.
 *
 * ## The same quantity the stochastic measures, on a different range
 *
 * `clv = 2·%K/100 − 1` when `%K` is taken over the **bar's own** high and low
 * rather than a rolling `period`-bar extreme
 * ({@link percentOfRangeValues}) — the same "position in a range", rescaled
 * to `[−1, +1]` and anchored to one bar. It is written out here as a single
 * pass rather than composed on that kernel (which would cost a second array
 * and a second pass for an affine map), but the **flat-range rule is
 * deliberately the same one**, and for the same reason:
 *
 * - **`high === low` → `0`, not missing.** This is the one place the
 *   flat-range rule is NOT `percentOfRangeValues`' `undefined`, and the
 *   reason is algebraic rather than conventional: the numerator
 *   `(c − l) − (h − c)` is exactly zero on any flat bar (the close is at the
 *   high and at the low), so `0` is the value the ratio takes in the limit
 *   from either side and the only contribution the bar can make to a money
 *   flow — a stochastic's flat *window*, by contrast, has a numerator that
 *   is not forced to zero. It is also TA-Lib's `AD` (`if (high − low > 0)`
 *   … else add nothing) and every conventional CMF, so a running sum built
 *   on it carries on through a halt instead of ending there. (The first cut
 *   reported `NaN` here for consistency with the stochastic rule; the
 *   Layer-2 review of #699 made the algebraic case, and it is the better
 *   one.) A close outside `[low, high]` on a flat bar is only reachable by
 *   redirecting `close`, and reads `0` too — the bar has no range to place
 *   it in.
 *
 * `NaN` marks a gap ([PND-STUDYBOX]) and propagates through the arithmetic on
 * its own, so a bar missing any of its three prices has no close location.
 * Nothing is clamped: a `close` outside `[low, high]` — reachable when a
 * caller redirects `close` at a smoothed column — reads outside `[−1, +1]`,
 * honestly.
 *
 * O(N), one pass, one allocation.
 */
export function clvValues(
  high: Float64Array,
  low: Float64Array,
  close: Float64Array,
): Float64Array {
  const length = high.length;
  const out = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    const h = high[i]!;
    const l = low[i]!;
    const range = h - l;
    // `range === 0` is the flat bar (see above): the numerator is exactly
    // zero, so the value is 0 — but only when the close is present, so a
    // missing close still reads as a gap. A NaN input makes `range` NaN,
    // which is not `=== 0`, so it falls to the division and propagates.
    const c = close[i]!;
    out[i] = range === 0 ? (c === c ? 0 : NaN) : (2 * c - h - l) / range;
  }
  return out;
}

/**
 * **The Accumulation/Distribution line** — the running total of
 * {@link clvValues} weighted by volume:
 *
 * ```
 * AD[i] = AD[i−1] + clv[i] · volume[i]
 * ```
 *
 * Shared by two studies rather than owned by either: `accumulationDistribution`
 * appends it, and `chaikinOscillator` is the difference of two EMAs **of this
 * array** (so it must be the same array, bar for bar, not a second
 * derivation that could drift).
 *
 * ## Edges — a running sum's asymmetry, again
 *
 * The accumulation half is {@link cumulativeValues}, so its rules apply
 * unchanged and are the ones OBV already documents:
 *
 * - **A leading run of gaps shifts the seed** to the first bar with a
 *   defined term, rather than emptying the column. That is what lets the
 *   line run over another study's output.
 * - **An interior gap propagates to the end.** Every level after an unknown
 *   term is a known sum plus an unknown.
 *
 * A **flat bar** (`high === low`) is an interior gap by that rule, because
 * {@link clvValues} reports no close location for one — so the line stops
 * there. **This is a deliberate delta from TA-Lib's `AD`**, which contributes
 * `0` for a flat bar and carries on. Measured (TA-Lib 0.7.1) on twelve rising
 * bars with bar 3 flattened to `h = l = c = 14` and volumes `100…1200`,
 * TA-Lib reports `[0, 100, 100, 100, 100, 400, 400, 800, 800, 1300, 1300,
 * 1900]` — bar 3 folded in as a zero contribution. Two things make that worth
 * diverging from: `0` is a *reading* (equal buying and selling pressure) for a
 * bar that reported none, and a bar with no range at all is usually a halt or
 * an untraded bar, where "no level" is the more honest answer. A caller who
 * wants TA-Lib's answer can drop or fill the flat bars before running it.
 *
 * O(N), two passes, two allocations.
 */
export function accumulationDistributionValues(
  high: Float64Array,
  low: Float64Array,
  close: Float64Array,
  volume: Float64Array,
): Float64Array {
  const clv = clvValues(high, low, close);
  // Derive in place — `clv` is this function's own buffer, and nothing else
  // reads it. A missing volume makes the term NaN on its own.
  for (let i = 0; i < clv.length; i += 1) clv[i] = clv[i]! * volume[i]!;
  return cumulativeValues(clv);
}
