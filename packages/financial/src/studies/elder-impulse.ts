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
  emaValues,
} from '../kernels/rolling.js';
import { macd } from './macd.js';

export interface ElderImpulseOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Span of the trend EMA, in **bars**. **Default `13`** (Elder's). */
  emaPeriod?: number;
  /** Fast MACD EMA span in **bars**. **Default `12`.** */
  fastPeriod?: number;
  /** Slow MACD EMA span in **bars**. **Default `26`.** */
  slowPeriod?: number;
  /** MACD signal EMA span in **bars**. **Default `9`.** */
  signalPeriod?: number;
  /** Source column. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'impulse'`.** */
  output?: Output;
}

/** Scratch prefix the MACD columns are computed under. Never returned. */
const ELDER_SCRATCH = '__elderMacd__';

/**
 * **Elder Impulse System** (Alexander Elder, *Come Into My Trading Room*) —
 * a per-bar verdict on whether trend and momentum **agree**, as `+1`, `0` or
 * `−1`:
 *
 * ```
 * ${output}[i] = +1   if EMA(emaPeriod) rose AND the MACD histogram rose
 *              = −1   if EMA(emaPeriod) fell AND the MACD histogram fell
 *              =  0   otherwise
 * ```
 *
 * Elder's reading is that `+1` (his green bar) means both the trend and its
 * momentum are pushing up, so short positions are forbidden; `−1` (red)
 * likewise forbids longs; and `0` (blue) is the common case where the two
 * disagree and neither side is barred. It is a **censoring** rule rather
 * than an entry signal — that is why the three-valued output is the whole
 * study.
 *
 * Appends **one** column. The EMA and the MACD themselves are
 * {@link ema} and {@link macd}'s business and are not emitted here; a caller
 * who wants to chart them calls those studies too, and gets exactly the same
 * numbers because this study calls them.
 *
 * ## The column is NUMERIC, and that is forced
 *
 * Elder's own presentation is a **colour** — green / blue / red — and the
 * corpus describes the output as a "categorical color column (string col
 * OK)". It is not OK here, and the reason is mechanical rather than
 * stylistic: `TimeSeries.withColumn` takes a `Float64Array` or a
 * `(number | undefined)[]` and appends a **number** column. There is no door
 * on it for a string column, so a study cannot append one (verified against
 * `packages/core/src/batch/time-series.ts`). `+1 / 0 / −1` is the natural
 * numeric encoding — it is signed, so it sorts and sums the way the reading
 * does, and a chart maps the sign to Elder's colours in one expression.
 *
 * ## Composition — it does not re-implement MACD
 *
 * The histogram comes from calling {@link macd} under a scratch prefix and
 * reading its `Hist` column back; the trend EMA comes from
 * {@link emaValues}, which is what `ema()` runs on. Both matter: the study is
 * then *by construction* the same MACD and the same EMA a caller charts
 * beside it, rather than a second derivation that could drift by a seed
 * convention. It inherits `macd`'s EMA seed (pond's first-sample seed, not
 * TA-Lib's SMA seed — see that study's note) and, through it, `macd`'s
 * per-column warm-up.
 *
 * ## Ties are `0`, and the comparison is strict on both sides
 *
 * "Rose" means **strictly** greater than the previous bar and "fell"
 * strictly less. A flat EMA or a flat histogram therefore reads `0`, not the
 * previous verdict: the rule is a statement about *this* bar, and a machine
 * that carried the last verdict across a flat bar would be a different
 * study (contrast {@link tradeVolumeIndex}, where persistence across an
 * undecided bar IS the definition). It also means the study has no carried
 * state at all — every bar is a function of two adjacent bars of two arrays
 * — so it needs no {@link foldRows} and has no gap-reset rule of its own.
 *
 * ## Warm-up
 *
 * The verdict needs bar `i` and bar `i − 1` of **both** inputs, so it first
 * prints one bar after the slower of the two has two values: at the defaults
 * the histogram starts at bar 33 (`slowPeriod + signalPeriod − 2`) and the
 * EMA at bar 12, so the impulse starts at bar **34**. Length-preserving, as
 * everywhere else.
 *
 * A missing cell in either input blanks the bar that reads it **and the next
 * one** — the ordinary consequence of comparing adjacent bars, not a
 * propagation rule. The EMA recursion skips a gap and recovers; the MACD's
 * does too, so the impulse comes back rather than dying at the hole.
 *
 * ## Edges
 *
 * - **Invariant under any positive scale and any shift of the price.** Every
 *   average here is affine-equivariant (`EMA(k·x + a) = k·EMA(x) + a` for
 *   `k > 0`), the histogram is a difference of such averages so the shift
 *   cancels outright, and only the **sign** of each change is read — so the
 *   verdicts are identical. Both are pinned as property tests. A *negative*
 *   scale would flip every comparison, and is not claimed.
 * - **Only three values ever appear**, and a test asserts the column is a
 *   subset of `{−1, 0, +1}` with all three present on a real fixture.
 * - **No division anywhere**, so there is no zero-denominator case.
 */
export function elderImpulse<
  S extends SeriesSchema,
  const Output extends string = 'impulse',
>(series: TimeSeries<S>, options: ElderImpulseOptions<S, Output> = {}) {
  const emaPeriod = options.emaPeriod ?? 13;
  const fastPeriod = options.fastPeriod ?? 12;
  const slowPeriod = options.slowPeriod ?? 26;
  const signalPeriod = options.signalPeriod ?? 9;
  assertPeriod(emaPeriod, 'emaPeriod');
  assertPeriod(fastPeriod, 'fastPeriod');
  assertPeriod(slowPeriod, 'slowPeriod');
  assertPeriod(signalPeriod, 'signalPeriod');
  if (fastPeriod >= slowPeriod) {
    // Checked here as well as inside `macd`, so the message names the study
    // the caller actually called.
    throw new TypeError(
      `elderImpulse fastPeriod (${fastPeriod}) must be shorter than slowPeriod (${slowPeriod})`,
    );
  }

  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const output = (options.output ?? 'impulse') as Output;
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const trend = emaValues(wide, column, emaPeriod);
  // The histogram, from `macd` itself rather than a second derivation of it.
  // The scratch prefix is guarded by `macd`'s own `assertNoColumn`, so a
  // caller who somehow holds a column by that name gets a clear collision
  // error rather than a confusing failure.
  const scratch = macd(wide, {
    column: column as never,
    fastPeriod,
    slowPeriod,
    signalPeriod,
    prefix: ELDER_SCRATCH,
  }) as unknown as TimeSeries<SeriesSchema>;
  const hist = columnValues(scratch, `${ELDER_SCRATCH}Hist`);

  const values = new Float64Array(trend.length);
  if (values.length > 0) values[0] = NaN;
  for (let i = 1; i < values.length; i += 1) {
    const e = trend[i]!;
    const ePrev = trend[i - 1]!;
    const h = hist[i]!;
    const hPrev = hist[i - 1]!;
    if (
      Number.isNaN(e) ||
      Number.isNaN(ePrev) ||
      Number.isNaN(h) ||
      Number.isNaN(hPrev)
    ) {
      values[i] = NaN;
      continue;
    }
    // Strict on both sides: a flat EMA or a flat histogram is `0`.
    values[i] = e > ePrev && h > hPrev ? 1 : e < ePrev && h < hPrev ? -1 : 0;
  }
  return series.withColumn(output, values);
}
