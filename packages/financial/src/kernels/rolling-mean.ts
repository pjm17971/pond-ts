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

/**
 * **Rolling mean absolute deviation of a raw array** — the average distance
 * of the window's values from the window's own mean, `NaN` unless the window
 * holds `period` finite values:
 *
 * ```
 * mad[i] = (1/period) · Σ_{j=i−period+1}^{i} |x[j] − mean[i]|
 * ```
 *
 * This is the denominator of the Commodity Channel Index, and it is the
 * reason CCI needs a kernel of its own rather than composing on
 * {@link rollingMeanValues}: mean **absolute** deviation is not a moment, so
 * none of core's reducers produce it and there is no incremental update that
 * would let it ride the same accumulator the mean and the standard deviation
 * share.
 *
 * ## Cost — O(N·period), stated rather than hidden
 *
 * The mean comes from {@link rollingMeanValues} (one O(N) pass, and its mask
 * is what this inherits, so the two agree on where a window is missing by
 * construction). The deviations do not: each emitted window is re-walked to
 * sum `|x − mean|`, so the whole kernel is **O(N·period)** — the only kernel
 * in the package that is not linear in the input alone.
 *
 * There is no O(1) sliding update for it. `Σ|x − m|` is a function of how the
 * window's values sit **relative to** `m`, and both the values and `m` move
 * each step, so one removal and one addition can flip the sign of every
 * remaining term. A **sub-linear** form does exist: keep the window in an
 * order-statistic index (a Fenwick tree over ranks, or an indexed skip list)
 * carrying prefix sums, find `m`'s rank, and read
 * `Σ|x − m| = (S_hi − k_hi·m) + (k_lo·m − S_lo)` in `O(log period)` — an
 * `O(N log period)` kernel. It is not what ships, because at the periods CCI
 * is published on (14, 20) the naive walk is a short, cache-local inner loop
 * and costs little: measured at 1M bars, `commodityChannelIndex` runs in
 * **128 ms at `period 20`** and **229 ms at `period 100`**, against
 * `awesomeOscillator`'s 85 ms and the range studies' 250–330 ms. The
 * difference across those two periods is the deviation walk and nothing else,
 * so it costs about **1.3 ms per unit of `period` per 1M bars** — roughly
 * 25 ms of the 128 at the default. Linear in `period`, as advertised: the
 * tree form is the answer if a caller ever wants CCI at a `period` in the
 * thousands, and it is written down here so that is a lookup rather than a
 * rediscovery.
 *
 * O(N·period) time, two passes, two allocations.
 */
export function rollingMeanAbsDevValues(
  values: Float64Array,
  period: number,
): Float64Array {
  const length = values.length;
  const mean = rollingMeanValues(values, period);
  const out = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    const m = mean[i]!;
    // `mean` is already `NaN` for a short window or one holding a gap, so it
    // carries the whole missing-cell rule and this loop needs no second copy
    // of it — the two kernels cannot disagree about where a value exists.
    if (Number.isNaN(m)) {
      out[i] = NaN;
      continue;
    }
    let sum = 0;
    for (let j = i - period + 1; j <= i; j += 1)
      sum += Math.abs(values[j]! - m);
    out[i] = sum / period;
  }
  return out;
}
