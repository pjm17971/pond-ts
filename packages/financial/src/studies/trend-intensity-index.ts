import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_SOURCE } from '../contract/columns.js';
import { rollingMeanValues } from '../kernels/rolling-mean.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
  rollingValues,
} from '../kernels/rolling.js';

export interface TrendIntensityIndexOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** How many bars of deviations are summed, in **bars** (Pee's *minor*
   *  period). **Default `30`.** */
  period?: number;
  /** Length of the simple average the deviations are taken from, in **bars**
   *  (Pee's *major* period). **Default `60`.** */
  maPeriod?: number;
  /** Source column. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Appended column name. **Default `'tii'`.** */
  output?: Output;
}

/**
 * **Trend Intensity Index** (M. H. Pee, TASC June 2002) — what share of the
 * recent distance from a long average was spent **above** it:
 *
 * ```
 * dev       = column − SMA(column, maPeriod)
 * SDpos     = Σ max(dev, 0)  over the last `period` bars
 * SDneg     = Σ max(−dev, 0) over the last `period` bars
 * ${output} = 100 · SDpos / (SDpos + SDneg)
 * ```
 *
 * Appends one column, bounded **0 … 100** and centred on 50. Above 50 the
 * price has spent more of its recent distance above its long average than
 * below; the further from 50, the more one-sided — and Pee's reading is that
 * an intensity persistently far from 50 is a trend worth trading, while one
 * hovering near it is a range.
 *
 * Pee's own periods ship: a **60**-bar average with the last **30**
 * deviations summed, so the summing window is half the average's length.
 *
 * ## F-AMBIG — sums of deviations, not a count of them
 *
 * Two definitions circulate under this name and they are not close. Pee's
 * original sums the **magnitudes** of the deviations, as above. The common
 * simplification counts them instead — the percentage of the last `period`
 * closes that were above the average, which is
 * {@link psychologicalLine} applied to a moving-average crossing rather than
 * to up-closes.
 *
 * The difference is that the sum form is **weighted by how far** price
 * strayed: one large excursion above the average outweighs several small
 * dips below it, which is exactly the "intensity" the name refers to. The
 * count form gives every bar the same vote and saturates at 0 or 100 as soon
 * as the price sits on one side, so it is a much blunter instrument.
 * Measured, the two sit **28.85 apart** at Pee's own periods on a reading
 * that spans the full 0 … 100 (`scripts/oracle/generate.py`), and **20.43
 * apart** at the oracle's shorter `(10, 20)` shape; the generator asserts
 * both, so the fixture cannot accept the count form. The count form is one line of shipped primitives if a caller wants
 * it, and would deserve its own name.
 *
 * ## Warm-up
 *
 * Length-preserving, and it stacks: the average lands at `maPeriod − 1`, the
 * deviations with it, and the sums need `period` finite deviations, so the
 * reading starts at `maPeriod + period − 2` — bar **88** at the defaults.
 * That is a long warm-up for a study whose look-backs read as "30", and it
 * is worth knowing before pointing it at three months of daily bars — it is
 * also why the default oracle case runs on the package's **long** 900-bar
 * input: at Pee's periods the 80-bar fixture produces no value at all.
 *
 * The two sums go through the raw-array kernel ({@link rollingMeanValues},
 * which is the same sum scaled by `period` and cancels in the ratio), so
 * each waits for `period` finite **values** rather than rows — the
 * derived-input rule, and what keeps the study from emitting a
 * "30-deviation" reading built from two deviations during the average's own
 * warm-up.
 *
 * ## Edges
 *
 * - **Scale-invariant and shift-invariant.** Both sums are homogeneous of
 *   degree one in the deviations, so a scale cancels in the ratio; a shift
 *   moves the price and its average together and leaves the deviations
 *   alone. Pinned as property tests.
 * - **All deviations exactly zero → `undefined`, not `0`.** The denominator
 *   is `Σ|dev|`, so it is zero only when every deviation in the window is
 *   exactly zero — price sitting precisely on its own average for `period`
 *   bars. The numerator *is* forced to zero with it (`SDpos ≤ Σ|dev|` by
 *   construction, unconditionally — there is no redirected-column escape
 *   here), so this is a genuine `0/0`, and the flat-window rule turns on
 *   what the ratio approaches: it approaches **100** from a run of tiny
 *   positive deviations and **0** from tiny negative ones. The limit is
 *   direction-dependent, so there is no value to return and `undefined` is
 *   the honest answer — where `0` would read "every deviation was negative",
 *   which is exactly what did not happen. (Contrast {@link clvValues}' flat
 *   bar, where `0` *is* the limit from either side.) A test pins it.
 * - **An interior gap** blanks that bar, the `maPeriod` average windows that
 *   hold it and the `period` sums that hold one of those — then recovers.
 *   No recursion is involved.
 */
export function trendIntensityIndex<
  S extends SeriesSchema,
  const Output extends string = 'tii',
>(series: TimeSeries<S>, options: TrendIntensityIndexOptions<S, Output> = {}) {
  const period = options.period ?? 30;
  const maPeriod = options.maPeriod ?? 60;
  assertPeriod(period);
  assertPeriod(maPeriod, 'maPeriod');

  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const output = (options.output ?? 'tii') as Output;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const values = columnValues(wide, column);
  const average = rollingValues(wide, column, 'avg', maPeriod);
  const length = values.length;

  // `Math.max` propagates NaN, so a missing value or a still-warming average
  // leaves both legs missing on that bar and the rolling sums step over it.
  const up = new Float64Array(length);
  const down = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    const deviation = values[i]! - average[i]!;
    up[i] = Math.max(deviation, 0);
    down[i] = Math.max(-deviation, 0);
  }

  // MEANS rather than sums: the `period` divisor is in both and cancels in
  // the ratio, and this is the door that waits for `period` finite VALUES.
  const upMean = rollingMeanValues(up, period);
  const downMean = rollingMeanValues(down, period);

  const out = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    const total = upMean[i]! + downMean[i]!;
    // A genuine 0/0 whose limit depends on the direction of approach — see
    // the docstring. `NaN` is not `=== 0`, so a gap propagates instead.
    out[i] = total === 0 ? NaN : (100 * upMean[i]!) / total;
  }

  return series.withColumn(output, out);
}
