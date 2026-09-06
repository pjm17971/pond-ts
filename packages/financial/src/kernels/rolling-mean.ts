import { rollingMeanSdInto } from './ranged.js';

/**
 * **Simple moving average of a raw array**, `NaN` unless the window holds
 * `period` finite values — the smoothing step of a *slow* stochastic (`%K`
 * is an SMA of fast `%K`; `%D` an SMA of that).
 *
 * ## Why this is not `rollingValues(scratchColumn, 'avg', period)`
 *
 * Core's count-window `avg` counts **rows**, not values: a window that spans
 * `period` rows but holds only one defined cell averages that one cell.
 * That is the documented contract for `sma()` over another study's output
 * (the outer warm-up does not add to the inner one), and it is the wrong
 * contract for a named indicator's own smoothing. A "3-bar SMA of `%K`"
 * with one `%K` in it is not a 3-bar SMA; TA-Lib and pandas both emit
 * nothing until three exist, and so does this. Measured on the oracle input
 * at `(14, 3, 3)`, the row-counting version would put slow `%K` on bar 13
 * with fast `%K`'s own value; the definition puts it on bar 15.
 *
 * The same rule covers the other source of a `NaN` in the window: a fast
 * `%K` that has no value because its range was flat. An average of three
 * values one of which is unknown is unknown, so the gap is carried through
 * the smoothing rather than averaged around — the conservative answer, and
 * the one pandas gives. (This is a deliberate contrast with
 * `highestLowestValues`, which composes on core's reducers and skips: an
 * extreme over the cells you *do* have is still an honest extreme; a mean
 * over fewer cells than the period names is a different statistic.)
 *
 * ## Shape
 *
 * The arithmetic is {@link rollingMeanSdInto}'s — the range-exact shifted
 * frame `sma()` itself runs on — so this is the same SMA `sma()` would give
 * over the same values, not a second implementation that could drift from
 * it. This kernel only adds the mask: a running count of non-finite cells in
 * the window, and `NaN` wherever it is non-zero or the window is short.
 *
 * O(N), one pass over the mask plus the kernel's own, two allocations.
 */
export function rollingMeanValues(
  values: Float64Array,
  period: number,
): Float64Array {
  const length = values.length;
  const out = new Float64Array(length);
  rollingMeanSdInto(values, period, 0, length, out, undefined);

  let missing = 0;
  for (let i = 0; i < length; i += 1) {
    if (!Number.isFinite(values[i]!)) missing += 1;
    if (i >= period && !Number.isFinite(values[i - period]!)) missing -= 1;
    if (i < period - 1 || missing > 0) out[i] = NaN;
  }
  return out;
}
