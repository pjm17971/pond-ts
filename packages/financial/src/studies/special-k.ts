import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_SOURCE } from '../contract/columns.js';
import { percentChangeValues } from '../kernels/rate-of-change.js';
import { rollingMeanValues } from '../kernels/rolling-mean.js';
import { assertNoColumn, columnValues } from '../kernels/rolling.js';

/** Pring's twelve rate-of-change look-backs, in **bars** (his daily set). */
const SPECIAL_K_ROC = [
  10, 15, 20, 30, 40, 65, 75, 100, 195, 265, 390, 530,
] as const;
/** The simple average applied to each rate of change, in **bars**. */
const SPECIAL_K_SMOOTHING = [
  10, 10, 10, 15, 50, 65, 75, 100, 130, 130, 130, 195,
] as const;
/** The weight each smoothed term carries — `1 … 4` within each of the three
 *  groups (short, intermediate, long). */
const SPECIAL_K_WEIGHTS = [1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4] as const;

export interface SpecialKOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Source column. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Appended column name. **Default `'specialK'`.** */
  output?: Output;
}

/**
 * **Pring's Special K** (Martin Pring) — the {@link kst} construction
 * extended from four terms to **twelve**, so that one line carries the
 * short-, intermediate- and long-term momentum of the whole business cycle
 * at once:
 *
 * ```
 * term_i    = SMA(ROC(column, roc_i), smooth_i)     ROC in PERCENT
 * ${output} = Σ weight_i · term_i
 *
 *   group        short-term            intermediate            long-term
 *   roc_i      10   15   20   30     40   65   75  100     195  265  390  530
 *   smooth_i   10   10   10   15     50   65   75  100     130  130  130  195
 *   weight_i    1    2    3    4      1    2    3    4       1    2    3    4
 * ```
 *
 * Appends one column, in percent-times-weight units (the weights sum to 30).
 * Pring's claim for it is that the primary trend turns — the ones a
 * four-term KST is too fast to call — show up as the Special K's own peaks
 * and troughs, so it is read as a **primary-trend** indicator rather than as
 * a trading oscillator.
 *
 * It composes on the same two primitives {@link kst} does — {@link
 * percentChangeValues} for each rate of change and {@link rollingMeanValues}
 * for each smoothing — so the two studies share one definition of both, and
 * the only difference between them is the table above.
 *
 * ## The thirty-six numbers are the study — there are no options
 *
 * `column` and `output` are the whole option list, and that is the same
 * decision {@link kst} records: Pring published *several* Special Ks (this
 * daily set, and weekly and monthly ones), and they are different indicators
 * with different readings rather than one indicator with parameters. Arrays
 * of periods on this function would make `specialK()` name all of them at
 * once. Unlike `kst`, there is not even a `signalPeriod`: Pring's Special K
 * is read against its **own** moving average when a signal is wanted, and
 * that is `sma()` over the output column — a shipped primitive, not a
 * second column this study has to name.
 *
 * ## The warm-up is 724 bars — read this before pointing it at a chart
 *
 * The slowest term is a 530-bar rate of change smoothed over 195 bars, so
 * the line first exists at bar `530 + 195 − 1 = 724` and a series needs
 * **725 bars** to produce a single value. That is not a defect: Pring's
 * design deliberately reaches back about three years of daily data so that
 * the long group can see a whole cycle. A study run on a year of bars comes
 * back entirely `undefined`, with the row count preserved.
 *
 * The line is defined exactly where all twelve terms are, with no branch in
 * the code — a missing term is `NaN` and survives the addition.
 *
 * ## Definition, verified
 *
 * **No TA-Lib function.** The oracle is a pandas replication over a longer
 * input than the rest of the fixture uses (900 bars, generated the same
 * deterministic way), with the analytic first-valid bar (**724**) asserted
 * and two discriminating separations measured — **156.73** from an
 * equal-weight sum and **281.23** from a sum of unsmoothed rates of change,
 * on a line spanning −260.73 … 159.45. Both survive the shape of the study,
 * which is why they are the probes.
 *
 * The long input exists because of this study: a 530-bar look-back cannot
 * warm up inside the 80-bar fixture, and a case that came back entirely
 * `undefined` would pass **vacuously**. It carries a much slighter drift
 * than the short one, because a steadily rising series makes all twelve
 * rates of change positive at once and the line never crosses zero
 * (measured: 151.5 … 379.3 at the first drift tried).
 *
 * The rates of change are **percent** (`(x/x[−n] − 1)·100`), matching
 * {@link percentChange} and TA-Lib's `ROC`; a ratio form would shift each
 * term by 100 and the whole line by 3000.
 *
 * ## Edges
 *
 * - **Scale-invariant, not shift-invariant** — every term is a ratio, so
 *   multiplying every price leaves the line alone while adding a constant
 *   changes each ratio. The same pair {@link kst} has, and pinned the same
 *   way.
 * - **A zero look-back base → `undefined`** for that term, and the sum with
 *   it, inherited from {@link percentChangeValues}. Unreachable on prices.
 * - **An interior gap** blanks the bar, the twelve bars that read it as a
 *   look-back base, and every smoothing window holding one of those — then
 *   recovers. With smoothings up to 195 bars that is a wide hole, but it is
 *   a hole and not a tail: no recursion is involved.
 */
export function specialK<
  S extends SeriesSchema,
  const Output extends string = 'specialK',
>(series: TimeSeries<S>, options: SpecialKOptions<S, Output> = {}) {
  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const output = (options.output ?? 'specialK') as Output;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const values = columnValues(wide, column);
  const line = new Float64Array(values.length);
  for (let term = 0; term < SPECIAL_K_ROC.length; term += 1) {
    // Each term shares `percentChange`'s definition (percent, and its
    // zero-base guard) and `rollingMeanValues`' derived-input rule, exactly
    // as `kst`'s four do.
    const smoothed = rollingMeanValues(
      percentChangeValues(values, SPECIAL_K_ROC[term]!),
      SPECIAL_K_SMOOTHING[term]!,
    );
    const weight = SPECIAL_K_WEIGHTS[term]!;
    // A still-warming or missing term is NaN and survives the addition, so
    // the line is defined exactly where all twelve are, with no branch here.
    for (let i = 0; i < line.length; i += 1) {
      line[i]! += weight * smoothed[i]!;
    }
  }

  return series.withColumn(output, line);
}
