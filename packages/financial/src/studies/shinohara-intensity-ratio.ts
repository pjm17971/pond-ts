import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import { shinoharaTermsValues } from '../kernels/intensity-ratio.js';
import { rollingMeanValues } from '../kernels/rolling-mean.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';

export interface ShinoharaIntensityRatioOptions<
  S extends SeriesSchema,
  Prefix extends string,
> {
  /** Window in **bars**. **Default `26`** (Shinohara's). */
  period?: number;
  /** Open column. **Default `'open'`.** */
  open?: NumericColumnNameForSchema<S>;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Close column. **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Column-family prefix — appends `${prefix}Strong` and `${prefix}Weak`.
   *  **Default `'sir'`.** */
  prefix?: Prefix;
}

/** The ratio of two rolling sums over the same window, as a percent. Taking
 *  the ratio of the two MEANS is the same number (the counts cancel) and
 *  reuses the kernel `sma()` runs on rather than a second accumulator. */
function ratio(
  up: Float64Array,
  down: Float64Array,
  period: number,
): Float64Array {
  const numerator = rollingMeanValues(up, period);
  const denominator = rollingMeanValues(down, period);
  const out = new Float64Array(numerator.length);
  for (let i = 0; i < out.length; i += 1) {
    const d = denominator[i]!;
    // Live guard: the division is at the OUTPUT and nothing forces the
    // numerator to zero with the denominator (see the study docstring).
    out[i] = d === 0 ? NaN : (100 * numerator[i]!) / d;
  }
  return out;
}

/**
 * **Shinohara Intensity Ratio** (篠原レシオ) — the pair of ratios Shinohara
 * built to read how hard a market is being pushed, each a ratio of two sums
 * over the same `period`-bar window, as a percent:
 *
 * ```
 * ${prefix}Strong = 100 · Σ(high − open)      / Σ(open − low)        ← the A ratio
 * ${prefix}Weak   = 100 · Σ(high − prevClose) / Σ(prevClose − low)   ← the B ratio
 * ```
 *
 * Appends **two** columns. `100` is the neutral reading in both: above it the
 * window travelled further up than down, below it the reverse. The two
 * measure the same push against different reference points — `Strong`
 * against each bar's **own open**, so it reads the session's conviction;
 * `Weak` against the **previous close**, so it reads how the session
 * behaved relative to where the last one left it, gaps included.
 *
 * ## Neither ratio is bounded, and the B ratio can invert
 *
 * `${prefix}Strong`'s terms are non-negative on any well-formed bar
 * (`low ≤ open ≤ high`), so it runs from `0` upwards and is unbounded above.
 *
 * `${prefix}Weak`'s are not: on a bar that **gapped up**, `low > prevClose`,
 * so `prevClose − low` is negative and the bar subtracts from the
 * denominator. On an instrument that gaps more than it ranges, the
 * denominator can therefore approach zero from either side and the reading
 * swings violently or changes sign. That is the definition rather than a
 * defect — the B ratio is asking "how far did the market travel above the
 * previous close, per unit of travel below it", and on a market that never
 * traded below the previous close the answer is genuinely unbounded — but it
 * is worth stating plainly, because it is exactly what the package's own
 * oracle input does: 33 of its 80 bars gap, and `${prefix}Weak` there spans
 * **−22,761.08 … 7,620.00** against a `${prefix}Strong` of 45.51 … 584.50
 * (`scripts/oracle/generate.py`, `period 26`). Read the B ratio on bars
 * whose ranges are wide relative to their gaps, or read the A ratio.
 *
 * ## F-AMBIG — which ratio is "strong", and the source followed
 *
 * The corpus flags this study as **F-AMBIG on the A/B conventions**, and it
 * is a naming fork rather than an arithmetic one. What ships:
 *
 * - The **arithmetic** is the standard Shinohara A and B ratios as they are
 *   published in the Japanese technical-analysis literature — A against the
 *   open, B against the previous close, both over 26 bars, both ×100.
 * - The **column names** come from the corpus' own study list (`strong` /
 *   `weak`, `docs/notes/financial-indicators-assessment-2026-07.md` §6.6),
 *   mapped A → `${prefix}Strong` and B → `${prefix}Weak`.
 *
 * The alternative convention charts them the other way round (B as the
 * "strong" line, on the argument that a gap-relative reading is the stronger
 * signal), and some vendors simply label them `A` and `B` and leave the
 * reading to the reader. That matters because the two lines are genuinely
 * different: measured on the package's oracle input at `period 26` they sit
 * **23,312.47** apart at their widest (`scripts/oracle/generate.py`), so a
 * build that swapped the labels would not be wrong by a rounding. A caller
 * who wants the other convention renames the columns; the study does not
 * offer a flag, because a flag would make one study into two.
 *
 * The wrong turn worth naming is a different one: reading the B ratio
 * against the bar's **own** close rather than the previous one. That is the
 * mistake that leaves the shape intact, and it sits **22,862.88** away
 * (measured); the generator asserts the separation.
 *
 * ## Warm-up — per column, and they differ by one bar
 *
 * `${prefix}Strong` reads one bar at a time, so it first prints at
 * `period − 1` (bar 25 at the default). `${prefix}Weak` reads the previous
 * close, so bar 0 has no term and it first prints at `period` (bar 26). Each
 * column warms up when it can rather than both waiting for the slower, which
 * is what {@link macd} established.
 *
 * Both sums come from {@link rollingMeanValues}, so a window holding a gap
 * emits nothing rather than averaging around it — the rule for a **derived**
 * array, and the one that keeps "a 26-bar sum" meaning 26 bars.
 *
 * ## Edges
 *
 * - **Invariant under scaling AND shifting every price.** Every term of both
 *   sums is a difference of two prices from the same bar (or the same
 *   adjacent pair), so a shift cancels inside each term and a scale factor
 *   cancels between numerator and denominator. That is a stronger statement
 *   than most ratio studies get, and both halves are pinned as property
 *   tests — a build that dropped the `100 ·` factor would still pass both, so
 *   the unit tests pin the scale.
 * - **A zero denominator → `undefined`, not `0` or `Infinity`.** `Σ(open −
 *   low) = 0` on real bars means every open in the window was exactly its
 *   low, which does **not** force `Σ(high − open)` to zero — a market that
 *   opened on its low every day and rallied every day is a real (if
 *   extraordinary) tape, and its A ratio is `x/0`. Applying the package's
 *   flat-window test (#699: is the numerator *forced* to zero, and is the
 *   answer determined?) both answers are no, so the reading is missing. The
 *   guard is live, and without it the value would be `±Infinity`, which
 *   `withColumn` rejects outright.
 * - **A negative sum is possible** only by redirecting a column (an `open`
 *   pointed at a smoothed price can sit outside the bar), and reads
 *   honestly: a negative ratio means the two sums disagreed in sign. Nothing
 *   clamps it.
 */
export function shinoharaIntensityRatio<
  S extends SeriesSchema,
  const Prefix extends string = 'sir',
>(
  series: TimeSeries<S>,
  options: ShinoharaIntensityRatioOptions<S, Prefix> = {},
) {
  const period = options.period ?? 26;
  assertPeriod(period);
  const prefix = (options.prefix ?? 'sir') as Prefix;
  const strongName = `${prefix}Strong` as const;
  const weakName = `${prefix}Weak` as const;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, strongName);
  assertNoColumn(wide, weakName);

  const { strongUp, strongDown, weakUp, weakDown } = shinoharaTermsValues(
    columnValues(wide, (options.open ?? DEFAULT_OHLCV.open) as string),
    columnValues(wide, (options.high ?? DEFAULT_OHLCV.high) as string),
    columnValues(wide, (options.low ?? DEFAULT_OHLCV.low) as string),
    columnValues(wide, (options.close ?? DEFAULT_OHLCV.close) as string),
  );

  return series
    .withColumn(strongName, ratio(strongUp, strongDown, period))
    .withColumn(weakName, ratio(weakUp, weakDown, period));
}
