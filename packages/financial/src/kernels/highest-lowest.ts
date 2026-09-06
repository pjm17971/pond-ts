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
