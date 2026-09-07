import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_SOURCE } from '../contract/columns.js';
import { percentChangeValues } from '../kernels/rate-of-change.js';
import { rollingMeanValues } from '../kernels/rolling-mean.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';

/** Pring's four rate-of-change look-backs, in **bars** (his daily set). */
const KST_ROC = [10, 15, 20, 30] as const;
/** The simple average applied to each rate of change, in **bars**. */
const KST_SMOOTHING = [10, 10, 10, 15] as const;
/** The weight each smoothed term carries in the sum — slower terms heavier. */
const KST_WEIGHTS = [1, 2, 3, 4] as const;

export interface KstOptions<S extends SeriesSchema, Prefix extends string> {
  /** Source column. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Simple-average length of the signal line, in **bars**. **Default `9`.** */
  signalPeriod?: number;
  /** Column-family prefix — appends `${prefix}` and `${prefix}Signal`.
   *  **Default `'kst'`.** */
  prefix?: Prefix;
}

/**
 * **Know Sure Thing** (Martin Pring) — four smoothed rates of change at four
 * horizons, added together with the slower ones weighted more heavily, plus a
 * simple-average signal line:
 *
 * ```
 * term_i        = SMA(ROC(column, roc_i), smooth_i)     ROC in PERCENT
 * ${prefix}       = 1·term₁ + 2·term₂ + 3·term₃ + 4·term₄
 * ${prefix}Signal = SMA(${prefix}, signalPeriod)
 *
 *          i =      1    2    3    4
 *   roc_i      =   10   15   20   30
 *   smooth_i   =   10   10   10   15
 *   weight_i   =    1    2    3    4
 * ```
 *
 * Appends two columns. The point of the construction is that a single rate of
 * change reads one cycle length; summing four of them, each smoothed and the
 * longest weighted heaviest, gives a momentum line dominated by the primary
 * trend but still able to turn early. It is read on zero crossings and on
 * crosses of its own signal.
 *
 * **The line is named `${prefix}`, not `${prefix}Line`** — the {@link trix}
 * shape: "the KST" *is* the line, and the signal keeps the family suffix.
 *
 * ## The twelve numbers are the study — there are no period options
 *
 * `column`, `prefix` and `signalPeriod` are the whole option list. The four
 * look-backs, the four smoothings and the four weights are **not** exposed,
 * and that is a decision rather than an omission: Pring published *several*
 * KSTs — a short-term daily set, this intermediate daily one, a weekly and a
 * monthly — and they are different indicators with different readings, not
 * one indicator with parameters. Arrays of periods on this function would
 * make `kst()` name all of them at once, so a chart legend reading "KST"
 * would mean nothing without the call site beside it.
 *
 * A caller who wants one of the others composes it from shipped primitives —
 * `percentChange` at each look-back, `sma` over each, then the weighted sum —
 * which is four lines of arithmetic and is honest about not being this study.
 * Pring's **Special K** (the extended, more-terms version listed separately
 * in the corpus) is likewise its own study, not a mode here.
 *
 * `signalPeriod` *is* exposed because it is the one number vendors genuinely
 * differ on and it does not change what the KST line is.
 *
 * ## Definition, verified
 *
 * **No TA-Lib function**, so the oracle is a pandas replication built on the
 * same `pct_change` that the TA-Lib-verified {@link percentChange} case uses,
 * with the analytic first-valid bar asserted and two discriminating
 * separations measured — from an **equal-weight** sum (80.6 apart on the
 * oracle input) and from a sum of **unsmoothed** rates of change (68.5
 * apart), on a line that spans −56.9…125.4.
 *
 * The rates of change are **percent** (`(x/x[−n] − 1)·100`), matching
 * {@link percentChange} and TA-Lib's `ROC`; a *ratio* form (`x/x[−n]·100`,
 * TA-Lib's `ROCP`-vs-`ROCR` distinction) would shift each term by a constant
 * 100 and therefore the whole line by 1000, which is why it is named here.
 *
 * ## Warm-up
 *
 * The four terms warm up at `roc_i + smooth_i − 1` (bars 19, 24, 29 and
 * **44** at the defaults), and the sum is defined only where all four are, so
 * `${prefix}` starts at bar 44 — the fourth term's, with no branch in the
 * code: a missing term is `NaN` and survives the addition. The signal then
 * starts `signalPeriod − 1` bars later, at **52**. Each column is emitted
 * where it is defined rather than both waiting for the slower.
 *
 * Both smoothings go through the raw-array kernel ({@link rollingMeanValues}),
 * which waits for `smooth_i` finite **values** rather than rows — the rule for
 * a derived input, and the same reason {@link stochastic}'s slow `%K` is not
 * one bar early.
 *
 * ## Edges
 *
 * - **Scale-invariant**: every term is a ratio, so multiplying every price by
 *   a positive constant leaves both columns unchanged. It is **not**
 *   shift-invariant — adding a constant changes each ratio. Both pinned as
 *   property tests.
 * - **A zero look-back base → `undefined`** for that term, and the sum with
 *   it, inherited from {@link percentChangeValues} rather than restated.
 *   Unreachable on prices; reachable when `column` is a study output that
 *   crosses zero.
 * - **An interior gap** blanks the bar, the four bars that read it as a
 *   look-back base, and every smoothing window holding one of those — then
 *   recovers. No recursion is involved, so nothing propagates to the end.
 * - **Unbounded**, in percent-times-weight units; the sum of the weights is
 *   10, so a KST of 30 is roughly "the four horizons averaged 3% each".
 */
export function kst<
  S extends SeriesSchema,
  const Prefix extends string = 'kst',
>(series: TimeSeries<S>, options: KstOptions<S, Prefix> = {}) {
  const signalPeriod = options.signalPeriod ?? 9;
  assertPeriod(signalPeriod, 'signalPeriod');

  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const prefix = (options.prefix ?? 'kst') as Prefix;
  const lineName = `${prefix}` as const;
  const signalName = `${prefix}Signal` as const;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, lineName);
  assertNoColumn(wide, signalName);

  const values = columnValues(wide, column);
  const line = new Float64Array(values.length);
  for (let term = 0; term < KST_ROC.length; term += 1) {
    // Each term shares `percentChange`'s definition (percent, and its
    // zero-base guard) and `rollingMeanValues`' derived-input rule: the
    // window waits for `smooth_i` finite VALUES, so a term's own warm-up
    // shifts its average rather than being averaged as if it were data.
    const smoothed = rollingMeanValues(
      percentChangeValues(values, KST_ROC[term]!),
      KST_SMOOTHING[term]!,
    );
    const weight = KST_WEIGHTS[term]!;
    // A still-warming or missing term is NaN and survives the addition, so
    // the line is defined exactly where all four are — the warm-up is the
    // slowest term's with no branch here.
    for (let i = 0; i < line.length; i += 1) {
      line[i]! += weight * smoothed[i]!;
    }
  }

  // The signal is a simple average OF THE LINE, through the same array door,
  // so it steps over the line's own warm-up with no arithmetic here.
  const signal = rollingMeanValues(line, signalPeriod);

  return series.withColumn(lineName, line).withColumn(signalName, signal);
}
