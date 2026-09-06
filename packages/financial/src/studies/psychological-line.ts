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

export interface PsychologicalLineOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Look-back in **bars**. **Default `12`** (the Japanese convention). */
  period?: number;
  /** Source column. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'psy'`.** */
  output?: Output;
}

/**
 * **Psychological Line** — the percentage of the last `period` bars that
 * closed **up**, bounded `0..100`:
 *
 * ```
 * psy = 100 · count(close > prevClose) / period
 * ```
 *
 * Appends one column; `undefined` for the first `period` rows.
 *
 * The simplest sentiment measure there is: it throws away the *size* of every
 * move and counts only its sign, so it says how persistent a move has been
 * rather than how big. Above 75 is conventionally read as over-optimistic and
 * below 25 as over-pessimistic. It is the count-only sibling of
 * {@link chandeMomentum}, which weights the same up/down split by magnitude —
 * a run of twelve tiny gains reads 100 here and modestly on CMO, which is
 * exactly the difference the study is for.
 *
 * ## Definition
 *
 * Standard, and short enough that the only choices are the ones named below.
 * **TA-Lib has no Psychological Line**, so the oracle is a pandas replication
 * with the analytic first-valid bar asserted, not a vendor cross-check. The
 * `period 12` default is the usual Japanese convention (the study is a
 * Japanese-charting staple); 10, 12 and 24 all appear in print.
 *
 * ## Edges
 *
 * - **An unchanged close is not an up bar.** The test is strictly `>`, so a
 *   flat bar counts toward the denominator and not the numerator, and a
 *   perfectly flat window reads `0` rather than `50` or `undefined`. That is
 *   a real reading here, not a `0/0` — the denominator is `period`, a
 *   positive integer, never the data — which is the one place this study
 *   differs from every other ratio in the momentum family. (Some vendors
 *   count an unchanged bar as half. That is a different study, and this one
 *   states which it is rather than leaving it to be discovered.)
 * - **Warm-up is `period` rows**, not `period − 1`: a bar's direction needs
 *   its predecessor, so bar 0 has none and the first full window of directed
 *   bars ends on bar `period`.
 * - **A bar whose close is missing has no direction**, and neither does the
 *   bar after it (whose predecessor is the gap) — so both drop out and every
 *   window containing either is `undefined`. The alternative, counting an
 *   unknown bar as *not up*, would report a definite percentage computed from
 *   a bar nobody knows the direction of; the array door's rule is the honest
 *   one. So the gap bar and the `period` bars after it are `undefined` and the
 *   study then **recovers** — exactly the rows {@link chandeMomentum} loses on
 *   the same input, since the two read the same per-bar directions and differ
 *   only in what they do with them.
 * - **Bounded `0..100`, and quantised**: with `period 12` the only values it
 *   can take are the thirteen multiples of `100/12`. A chart of it is a
 *   staircase, and that is the study, not an artefact.
 * - **Scale- and shift-invariant** — it reads only the *sign* of each change,
 *   so any positive scaling or constant shift of every price leaves it
 *   identical. The strongest invariance in the momentum family, and pinned by
 *   property tests.
 * - **A leading gap shifts the start** rather than emptying the study.
 */
export function psychologicalLine<
  S extends SeriesSchema,
  const Output extends string = 'psy',
>(series: TimeSeries<S>, options: PsychologicalLineOptions<S, Output> = {}) {
  const period = options.period ?? 12;
  assertPeriod(period);
  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const output = (options.output ?? 'psy') as Output;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const v = columnValues(wide, column);
  const length = v.length;
  // One indicator per bar: 1 for an up close, 0 for flat or down, `NaN` for
  // a bar whose direction is unknown. The `NaN` test is explicit because
  // `d > 0` is false for a `NaN` difference — an unknown bar would otherwise
  // be silently counted as "not up" rather than as no answer.
  const upBar = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    const d = i === 0 ? NaN : v[i]! - v[i - 1]!;
    upBar[i] = Number.isNaN(d) ? NaN : d > 0 ? 1 : 0;
  }

  // The mean of a 0/1 indicator IS the fraction, so the percentage is the
  // shipped rolling-mean kernel times 100 — and its array door (a window
  // emits only once its last `period` rows are all finite) is the warm-up
  // and missing-cell rule documented above.
  const fraction = rollingMeanValues(upBar, period);
  const out = new Float64Array(length);
  for (let i = 0; i < length; i += 1) out[i] = 100 * fraction[i]!;

  return series.withColumn(output, out);
}
