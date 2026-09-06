import type { SeriesSchema, TimeSeries } from 'pond-ts';
import { rollingColumns } from './rolling.js';

/**
 * **Highest high / lowest low** over a trailing `period`-bar window — the
 * range every range-position study reads: stochastics (`%K`), Williams `%R`,
 * Donchian channels, and later Aroon and Keltner-style breakout rules.
 *
 * ## Why a kernel and not two `rollingValues` calls in each study
 *
 * The three studies that ship with it all want the same pair, so the contract
 * lives in one place: the same `period` rows, the same warm-up, the same
 * treatment of a missing cell, for every consumer. Both extremes come from
 * **one** `rolling` call (via {@link rollingColumns}) — though, measured,
 * that is worth less than it sounds: at 1M bars core's count-window `max`
 * alone is 69 ms and `max` + `min` together 144 ms, against 74 + 81 ms as
 * two `rollingMax` / `rollingMin` studies. The per-reducer min/max state
 * dominates, not the sweep, so the single call saves ~7%, and the studies on
 * this kernel cost what that substrate costs (`donchian` 164 ms, `williamsR`
 * 198 ms, `stochastic` 233 ms at 1M, against `bollinger`'s 104 ms). A
 * monotonic-deque fast path for core's rolling min/max would lift all three
 * at once; it is not built here.
 *
 * ## Missing cells
 *
 * This composes on core's reducers, so it inherits core's policy: a missing
 * (or non-finite) cell is **skipped**, and the extreme is taken over the
 * cells the window does hold. A window with nothing in it reads `NaN`. That
 * is the same behaviour `rollingMax` / `rollingMin` already ship, and it is
 * what makes a study over another study's output *start late* rather than
 * come back empty. (`rollingMeanValues` deliberately does **not** skip —
 * see there for why the two differ.)
 *
 * A `high` or `low` that names no numeric column reads as **all-`NaN`**,
 * the same answer `columnValues` gives a study pointed at a column that is
 * not there — so a multi-input study has one rule for all of its inputs,
 * rather than a missing `close` reading empty while a missing `high` throws
 * from inside core's `rolling`. (That is the {@link atr} precedent; the
 * single-column studies on `rollingValues` do throw, and this does not
 * change them.)
 *
 * The first `period − 1` rows are `NaN`: length-preserving warm-up.
 */
export function highestLowestValues(
  series: TimeSeries<SeriesSchema>,
  high: string,
  low: string,
  period: number,
): { highest: Float64Array; lowest: Float64Array } {
  if (!hasNumericColumn(series, high) || !hasNumericColumn(series, low)) {
    const missing = new Float64Array(series.length).fill(NaN);
    return { highest: missing, lowest: missing.slice() };
  }
  const out = rollingColumns(
    series,
    {
      highest: { from: high, using: 'max' },
      lowest: { from: low, using: 'min' },
    },
    period,
  );
  return { highest: out['highest']!, lowest: out['lowest']! };
}

function hasNumericColumn(
  series: TimeSeries<SeriesSchema>,
  name: string,
): boolean {
  return series.schema
    .slice(1)
    .some((c) => c.name === name && c.kind === 'number');
}

/**
 * **Where a value sits in a range, as a percentage** — `100 · (value −
 * lowest) / (highest − lowest)`. This *is* the fast stochastic `%K`;
 * Williams `%R` is the same number minus 100, which is why both studies are
 * built on it rather than each owning the division.
 *
 * ## The flat window
 *
 * When `highest === lowest` the ratio is `0/0` and the answer is **`NaN`
 * (missing)**, not a conventional value. TA-Lib returns `0` here — which for
 * `%K` means "close at the very bottom of the range" and for `%R` means
 * "close at the very top", the two opposite extremes for the one situation in
 * which the close is at neither. pond's studies distinguish "no answer" from
 * "an answer that happens to be zero" everywhere else (RSI's flat window,
 * Bollinger's σ = 0), and do here too. The rule lives in this kernel so that
 * every study on it makes the same call.
 *
 * `NaN` in any input propagates on its own ([PND-STUDYBOX]). Nothing is
 * clamped: a `value` outside `[lowest, highest]` — possible when the caller
 * redirects `close` at a column the range does not bound, like a smoothed
 * close — reads outside `0..100`, honestly.
 */
export function percentOfRangeValues(
  highest: Float64Array,
  lowest: Float64Array,
  value: Float64Array,
): Float64Array {
  const length = value.length;
  const out = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    const hh = highest[i]!;
    const ll = lowest[i]!;
    const range = hh - ll;
    out[i] = range === 0 ? NaN : (100 * (value[i]! - ll)) / range;
  }
  return out;
}

/**
 * **Bars since the window's extreme** — how many bars ago, counting back from
 * the bar being reported, the highest (or lowest) value in a window of
 * `period + 1` bars occurred. `0` means the extreme is today's value,
 * `period` means it is the oldest bar still in the window.
 *
 * This is the **argmax** the range studies never needed: stochastics, `%R`
 * and Donchian all want the extreme's *value*, and Aroon wants its *age*.
 * That one difference is why it could not be another
 * {@link highestLowestValues} reducer — core's `max` returns the value and
 * has nowhere to carry the index — and it is the G3 gap the corpus
 * assessment flagged (§5) as the one small reducer the ChartIQ set needs and
 * pond does not ship.
 *
 * ## The window is `period + 1` bars, not `period`
 *
 * Aroon's `period` counts the **age** the oscillator can report, and an age
 * of `period` bars ago is still a reading, so the window has to hold
 * `period + 1` bars for the count `0 … period` to be reachable. That is
 * TA-Lib's `AROON` too — its lookback is exactly `period`, so the first bar
 * with a value is bar `period` — and it is the one place in this package
 * where a `period` does not equal its window's bar count. It is stated here
 * rather than in the study so a second consumer cannot get it wrong.
 *
 * ## Ties go to the most recent bar
 *
 * When several bars in the window share the extreme, the **newest** wins, so
 * a fresh high that merely equals the old one still resets Aroon to 100.
 * Measured against TA-Lib on a window carrying a repeated high (`12, 11, 12,
 * 10.5` at `period 4`): TA-Lib reports `aroonUp = 75`, which is the newest
 * bar's age, not the oldest's `25`. The monotonic deque below gets this from
 * its **non-strict** eviction (`<=` for a max): an incoming equal value
 * evicts the older one rather than queueing behind it.
 *
 * ## Missing cells
 *
 * `NaN` marks a gap ([PND-STUDYBOX]), and this is the **strict** rule —
 * every one of the `period + 1` cells must be finite or the bar reads `NaN`,
 * so an interior gap costs `period + 1` bars and then recovers. That is
 * {@link rollingMeanValues}' rule rather than {@link highestLowestValues}'
 * skip-and-carry-on, and deliberately: an extreme taken over the cells you
 * *do* have is still an honest extreme, but its **age** is not — a hole
 * could be hiding the very bar the answer is asking about, and a
 * confidently-reported "12 bars ago" that is really "unknown" is worse than
 * no reading. (TA-Lib's own answer here is the argument: fed a `NaN` high it
 * silently skips that bar — a comparison against `NaN` is false — and its
 * output is bit-identical to the clean run, measured, so the hole leaves no
 * trace and every age counted across it is confidently wrong.)
 *
 * ## Cost — O(N), one pass, via a monotonic deque
 *
 * The naive form re-scans the window per bar, O(N·period). This keeps a
 * deque of candidate indices whose values are strictly decreasing (for a
 * max), so the front is always the current extreme: each index is pushed and
 * popped at most once, giving **O(N) amortised, independent of `period`**.
 * The ring buffer is `period + 1` entries, so the whole kernel is two
 * allocations regardless of input length. Measured at 1M bars (see
 * `scripts/perf-studies.mjs`), that is the difference between a flat cost
 * and one that grows with the look-back — the same structure a monotonic-
 * deque fast path for core's rolling min/max would use, which is why it is
 * written here as a general kernel rather than inside `aroon`.
 */
export function barsSinceExtremeValues(
  values: Float64Array,
  period: number,
  mode: 'max' | 'min',
): Float64Array {
  const length = values.length;
  const out = new Float64Array(length).fill(NaN);
  const capacity = period + 1;
  const ring = new Int32Array(capacity);
  const wantMax = mode === 'max';
  let head = 0;
  let count = 0;
  let missing = 0;

  for (let i = 0; i < length; i += 1) {
    const value = values[i]!;
    // Maintain the count of non-finite cells inside the window [i-period, i].
    const leaving = i - capacity;
    if (leaving >= 0 && !Number.isFinite(values[leaving]!)) missing -= 1;
    if (!Number.isFinite(value)) missing += 1;

    // Drop candidates that have aged out of the window.
    while (count > 0 && ring[head]! < i - period) {
      head = (head + 1) % capacity;
      count -= 1;
    }
    // A non-finite cell is never a candidate; the `missing` count above is
    // what makes the window it sits in report nothing at all.
    if (Number.isFinite(value)) {
      // Non-strict eviction: an equal value displaces the older candidate,
      // which is what puts a tie on the most recent bar (see above).
      while (count > 0) {
        const back = values[ring[(head + count - 1) % capacity]!]!;
        if (wantMax ? back <= value : back >= value) count -= 1;
        else break;
      }
      ring[(head + count) % capacity] = i;
      count += 1;
    }

    if (i >= period && missing === 0) out[i] = i - ring[head]!;
  }
  return out;
}
