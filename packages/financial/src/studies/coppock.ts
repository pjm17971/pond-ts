import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_SOURCE } from '../contract/columns.js';
import { movingAverageValues } from '../kernels/moving-average.js';
import { percentChangeValues } from '../kernels/rate-of-change.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';

export interface CoppockOptions<S extends SeriesSchema, Output extends string> {
  /** Longer rate-of-change look-back in **bars**. **Default `14`.** */
  longPeriod?: number;
  /** Shorter rate-of-change look-back in **bars**. **Default `11`.** */
  shortPeriod?: number;
  /** Weighted-average length in **bars**. **Default `10`.** */
  wmaPeriod?: number;
  /** Source column. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'coppock'`.** */
  output?: Output;
}

/**
 * **Coppock Curve** (Edwin Sedgwick Coppock, *Barron's*, 1962) — a linearly
 * weighted average of two rates of change added together:
 *
 * ```
 * coppock = WMA(ROC(longPeriod) + ROC(shortPeriod), wmaPeriod)
 * ```
 *
 * where `ROC(n)` is the **percent** change over `n` bars,
 * `(x[i]/x[i−n] − 1) × 100` — the same {@link percentChange} the package
 * already ships (and the same TA-Lib `ROC`). Appends one column.
 *
 * ## The monthly convention — and why the periods are still bar counts
 *
 * Coppock's defaults are **months**: 14- and 11-*month* rates of change,
 * smoothed by a 10-*month* weighted average, on a **monthly** index chart.
 * The lengths are not arbitrary — he took 11 and 14 from an estimate of the
 * average bereavement period, the analogy being how long a market takes to
 * recover from a loss — and the curve is read one way only: a **cross up
 * through zero from below** as a long-term buy signal, roughly once a
 * market cycle.
 *
 * The study is nonetheless **bar-count** like every other in this package,
 * so `coppock()` on daily bars is a 14-*day* / 11-*day* / 10-*day* curve —
 * a defensible short-horizon oscillator, but **not** the indicator Coppock
 * defined and not one his thresholds apply to. Run it on monthly bars (or
 * on `aggregate` output at a monthly grain) to get his. Stated here because
 * the defaults look innocuous on any chart and quietly mean something else
 * on a daily one.
 *
 * ## Definition notes
 *
 * - **No TA-Lib function exists**, so the oracle is a pandas replication —
 *   on the same `pct_change` and linear-weight helpers used for the
 *   TA-Lib-verified `percentChange` and `wma`, with the analytic first valid
 *   bar asserted.
 * - **The average is a WMA, and that is part of the definition** (linear
 *   weights `1…wmaPeriod`, newest heaviest), not a `maType` knob. Published
 *   Coppock is the weighted average; an EMA-smoothed variant is a different
 *   curve. If a caller wants one, `movingAverage` over this study's inputs
 *   composes it — a knob here would let "the Coppock Curve" name two
 *   different lines.
 * - **`longPeriod` and `shortPeriod` are symmetric** — the two rates of
 *   change are added, so swapping them changes nothing, and no ordering is
 *   enforced. (Contrast {@link macd}, which rejects `fastPeriod >=
 *   slowPeriod` because its two spans are *subtracted* and the sign of the
 *   result depends on which is which.)
 *
 * ## Warm-up and edges
 *
 * - **First value at `max(longPeriod, shortPeriod) + wmaPeriod − 1`** — bar
 *   23 at the defaults. The sum is defined from bar `longPeriod` (the later
 *   of the two look-backs), and the WMA then waits for `wmaPeriod` finite
 *   values: a positional weight cannot skip a cell without reweighting the
 *   rest, so it masks a short window rather than averaging what it has.
 * - **Scale-invariant.** Both terms are ratios, so multiplying every bar by
 *   a positive constant leaves the curve unchanged — pinned as a property
 *   test. The output is in **percent**, and is unbounded either way.
 * - **A zero base → `undefined`** for that rate of change, and the sum and
 *   the windows containing it with it. Unreachable on prices; reachable
 *   when `column` is a study output that crosses zero.
 * - **A leading gap shifts the start** (the WMA waits for finite values); an
 *   **interior** gap blanks the bar, the bar `longPeriod`/`shortPeriod`
 *   later, and every window containing either — then recovers, the `wma`
 *   rule.
 */
export function coppock<
  S extends SeriesSchema,
  const Output extends string = 'coppock',
>(series: TimeSeries<S>, options: CoppockOptions<S, Output> = {}) {
  const longPeriod = options.longPeriod ?? 14;
  const shortPeriod = options.shortPeriod ?? 11;
  const wmaPeriod = options.wmaPeriod ?? 10;
  assertPeriod(longPeriod, 'longPeriod');
  assertPeriod(shortPeriod, 'shortPeriod');
  assertPeriod(wmaPeriod, 'wmaPeriod');

  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const output = (options.output ?? 'coppock') as Output;
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  // Both rates of change share `percentChange`'s definition — the same
  // warm-up and the same zero-base guard — rather than restating it twice
  // more here.
  const values = columnValues(wide, column);
  const long = percentChangeValues(values, longPeriod);
  const short = percentChangeValues(values, shortPeriod);

  // A missing or still-warming term is `NaN` ([PND-STUDYBOX]) and survives
  // the addition, so the sum is defined only where both are — which is what
  // makes the warm-up the later look-back's, with no branch here.
  const sum = new Float64Array(values.length);
  for (let i = 0; i < sum.length; i += 1) sum[i] = long[i]! + short[i]!;

  // The WMA goes through the K2 engine's array door, so it is the same
  // linear-weight average `movingAverage({ type: 'wma' })` gives — and,
  // being the array door, it emits only once the last `wmaPeriod` rows are
  // all finite (a missing ROC blanks the windows holding it), which is the
  // rule for a derived input.
  return series.withColumn(output, movingAverageValues(sum, wmaPeriod, 'wma'));
}
