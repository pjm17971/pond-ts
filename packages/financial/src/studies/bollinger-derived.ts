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
  rollingColumns,
} from '../kernels/rolling.js';

/** Options shared by the two studies derived from the Bollinger bands. */
export interface BollingerDerivedOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Window length in **bars**. **Default `20`** (Bollinger's own). */
  period?: number;
  /** Band half-width in standard deviations. **Default `2`.** */
  stdDev?: number;
  /** Source column. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default the study's own name.** */
  output?: Output;
}

/** One rolling pass — the *same* `rollingColumns` call {@link bollinger}
 *  makes, so the σ underneath these two studies is that study's σ rather
 *  than a second derivation of it. Returns the centre and the deviation. */
function meanAndSd(
  wide: TimeSeries<SeriesSchema>,
  column: string,
  period: number,
): { middle: Float64Array; sd: Float64Array } {
  const rolled = rollingColumns(
    wide,
    {
      middle: { from: column, using: 'avg' },
      sd: { from: column, using: 'stdev' },
    },
    period,
  );
  return { middle: rolled['middle']!, sd: rolled['sd']! };
}

/**
 * **Bollinger BandWidth** — how wide the {@link bollinger} channel is,
 * relative to its own centre:
 *
 * ```
 * bbWidth = 100 × (upper − lower) / middle
 *         = 100 × 2·stdDev·σ / SMA(period)
 * ```
 *
 * A volatility reading in **percent of price**, and the quantity Bollinger's
 * "squeeze" is defined on: a bbWidth at a multi-month low means the bands
 * have contracted, which is the setup the pattern names. Appends one column,
 * default `'bbWidth'`.
 *
 * ## ×100, and why
 *
 * John Bollinger's own statement of BandWidth is the bare ratio
 * `(upper − lower)/middle`; StockCharts, ChartIQ and TradingView all draw it
 * **×100** so it reads as a percentage. **The ×100 form ships**, because it
 * is the one a caller comparing against a chart will see, and dividing by
 * 100 to recover the ratio is a great deal more discoverable than
 * multiplying by it after wondering why the numbers are a hundredth of the
 * chart's. Stated here rather than left to be found.
 *
 * ## It is `bollinger`'s σ, not a second one — with one deliberate delta
 *
 * The rolling pass is the *same* `rollingColumns` call {@link bollinger}
 * makes (`avg` + `stdev`, population `ddof=0`, one window scan), so on every
 * bar where `bollinger` emits bands, `bbWidth` is exactly
 * `100·(bbUpper − bbLower)/bbMiddle` — pinned by a test against the shipped
 * study rather than asserted here.
 *
 * **The exception is a flat window**, and it is worth reading. `bollinger`
 * emits `undefined` bands when `σ = 0`, on the grounds that a zero-width
 * *statistical band* is a degenerate statistic rather than a measurement.
 * BandWidth answers `0` there instead, because the flat-window test comes
 * out the other way: the numerator is `2·stdDev·σ`, which is **forced** to
 * zero, and the denominator is the flat window's own price level, which is
 * not zero — so the value is `0`, a real reading ("the bands have no width",
 * which is what a squeeze is). That is the #699 rule applied rather than
 * copied. The consequence is that on a flat stretch `bbWidth` is **not**
 * recoverable from the `bollinger` columns (they are missing there and this
 * is `0`); everywhere else it is, bit-for-bit.
 *
 * The one genuine `0/0` is a window that is flat **at zero** — every value
 * exactly `0`, so the centre is `0` too. That reads `undefined`, and the
 * guard is at the output and therefore live.
 *
 * ## Edges
 *
 * - **Warm-up is `period − 1` rows**, the rolling window's own, and it does
 *   **not** compose with the source's. Over another study's output the first
 *   value still lands at bar `period − 1`, computed from however many
 *   contributors that window holds — core's count-window counts *rows* for
 *   `minSamples` and its reducers skip a missing cell. That is inherited
 *   from {@link bollinger}, which does the same, and matching it is what
 *   keeps this study arithmetic on that one's columns. Length-preserving.
 * - **Scale-INVARIANT and NOT shift-invariant.** Scaling every price scales
 *   the numerator and the denominator together, so the reading does not
 *   move; adding a constant moves the denominator only, so it does. Both
 *   pinned by property tests — this is the {@link rsi} side of the scale
 *   pair, and the second half is the one that would be missed.
 * - **A negative centre flips the sign.** Reachable only over a column that
 *   can go negative (another study's output). Nothing takes an absolute
 *   value; the sign is the centre's, honestly.
 * - **A gap costs it nothing at all.** BandWidth reads only the window's
 *   `avg` and `stdev`, and core's count-window reducers compute those over
 *   the contributors that are present — `minSamples` counts rows, a missing
 *   cell is skipped — so the line is drawn straight through a hole, over one
 *   fewer sample on the windows containing it. That is the opposite of
 *   {@link bollingerPercentB}, which additionally reads the bar's own price
 *   and therefore loses exactly that bar. Measured and pinned in
 *   `study-missing-cells.test.ts`; a caller who wants a gap to blank the
 *   reading has to fill or drop the row.
 */
export function bollingerBandwidth<
  S extends SeriesSchema,
  const Output extends string = 'bbWidth',
>(series: TimeSeries<S>, options: BollingerDerivedOptions<S, Output> = {}) {
  const period = options.period ?? 20;
  assertPeriod(period);
  const stdDev = options.stdDev ?? 2;
  if (!Number.isFinite(stdDev) || stdDev <= 0) {
    throw new TypeError(
      'bollingerBandwidth stdDev must be a positive finite number',
    );
  }
  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const output = (options.output ?? 'bbWidth') as Output;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const { middle, sd } = meanAndSd(wide, column, period);
  const out = new Float64Array(middle.length);
  for (let i = 0; i < out.length; i += 1) {
    const m = middle[i]!;
    const d = sd[i]!;
    // The bands are re-formed with `bollinger`'s OWN expression
    // (`middle + sign * stdDev * d`) rather than simplified to `2·k·d`:
    // the two are equal in real arithmetic and not in IEEE754, and the
    // identity `bbWidth === 100·(bbUpper − bbLower)/bbMiddle` is asserted
    // bit-for-bit against the shipped study.
    const upper = m + stdDev * d;
    const lower = m + -1 * stdDev * d;
    // A centre of exactly zero is the only real zero denominator, and it is
    // checked FIRST so an all-zero (flat AND zero) window reads as the 0/0
    // it is rather than as the forced-zero numerator case. A warm-up bar has
    // `m` NaN, which is not `=== 0`, so it falls through to the division and
    // stays NaN ([PND-STUDYBOX]).
    out[i] = m === 0 ? NaN : (100 * (upper - lower)) / m;
  }
  return series.withColumn(output, out);
}

/**
 * **Bollinger %B** — where the price sits inside its own {@link bollinger}
 * channel, as a fraction of the channel's width:
 *
 * ```
 * percentB = (price − lower) / (upper − lower)
 * ```
 *
 * `1` is a price on the upper band, `0` on the lower, `0.5` on the centre;
 * above `1` or below `0` is a price outside its bands. Appends one column,
 * default `'percentB'`.
 *
 * **Not ×100.** StockCharts and TradingView both draw %B as a *decimal* with
 * `0` and `1` as the reference lines, and the whole reading is "is it above
 * 1 / below 0", which the decimal form states directly. The `%` in the name
 * is the indicator's, not a unit. (This is the opposite choice from
 * {@link bollingerBandwidth}'s ×100, and for the same reason: match the
 * chart a caller is comparing against, per study.)
 *
 * ## One `column`, read twice — deliberately
 *
 * The price in the numerator and the price the bands are built from are the
 * **same** `column`. That is the definition (%B places a series inside its
 * *own* bands) and it is what makes the flat-window case a genuine `0/0`
 * rather than something over zero. A caller who wants one series placed
 * inside another's bands has {@link bollinger} plus arithmetic; an option
 * for it here would be a second indicator wearing this one's name.
 *
 * ## The flat window is `undefined`, and here that IS a 0/0
 *
 * When `σ = 0` the denominator `2·stdDev·σ` is zero — and so is the
 * numerator, because a flat window's price *is* its own mean. A genuine
 * `0/0`: the price could be said to sit anywhere in a channel of no width,
 * so it sits nowhere. `undefined`.
 *
 * This is the **opposite** answer from {@link bollingerBandwidth}'s `0` on
 * the same window, and the pair is the clearest illustration of the rule the
 * package applies: ask whether the numerator is *forced* to zero
 * independently of the denominator. BandWidth's is (it is `2·stdDev·σ`
 * itself); %B's is only zero *because* the window is flat, which is the same
 * fact as the denominator being zero. It also agrees with {@link bollinger},
 * whose bands are `undefined` there.
 *
 * ## Edges
 *
 * - **Warm-up is `period − 1` rows**, and — as with
 *   {@link bollingerBandwidth} — it does not compose with the source's when
 *   run over another study's output. Length-preserving.
 * - **Scale- AND shift-INVARIANT**: numerator and denominator are both
 *   differences of prices in the same units, so a common multiplier cancels
 *   in the ratio and a common offset cancels in the numerator. Unlike
 *   {@link bollingerBandwidth}, which is only the first of the two — pinned
 *   by property tests on both studies, because the difference between them
 *   is exactly the mistake a reader would make.
 * - **Unbounded.** There is no clamp: a price far outside its bands reads
 *   well above `1` or below `0`, which is the signal.
 * - **A gap costs exactly its own bar.** The window statistics skip a
 *   missing cell (see {@link bollingerBandwidth}, which therefore loses
 *   nothing), but the numerator reads the bar's **own** price, so the gap
 *   bar has no position and the next one does. Measured and pinned in
 *   `study-missing-cells.test.ts`.
 * - **On every bar where `bollinger` emits bands**, `percentB` equals
 *   `(price − bbLower)/(bbUpper − bbLower)`; pinned by a test against the
 *   shipped study. Unlike BandWidth there is no flat-window exception,
 *   because both studies answer `undefined` there.
 */
export function bollingerPercentB<
  S extends SeriesSchema,
  const Output extends string = 'percentB',
>(series: TimeSeries<S>, options: BollingerDerivedOptions<S, Output> = {}) {
  const period = options.period ?? 20;
  assertPeriod(period);
  const stdDev = options.stdDev ?? 2;
  if (!Number.isFinite(stdDev) || stdDev <= 0) {
    throw new TypeError(
      'bollingerPercentB stdDev must be a positive finite number',
    );
  }
  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const output = (options.output ?? 'percentB') as Output;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const { middle, sd } = meanAndSd(wide, column, period);
  const price = columnValues(wide, column);
  const out = new Float64Array(middle.length);
  for (let i = 0; i < out.length; i += 1) {
    const d = sd[i]!;
    const m = middle[i]!;
    // Re-formed with `bollinger`'s OWN band expression rather than
    // simplified to `(price − middle)/(2·k·σ) + 0.5`: the two agree in real
    // arithmetic and not in IEEE754, and the identity
    // `percentB === (price − bbLower)/(bbUpper − bbLower)` is asserted
    // bit-for-bit against the shipped study.
    const upper = m + stdDev * d;
    const lower = m + -1 * stdDev * d;
    // A flat window is a genuine 0/0 (see the docstring) — the numerator is
    // zero only because the denominator is. `d === 0` is false when `d` is
    // NaN, so a warm-up bar falls through to the arithmetic and stays NaN.
    out[i] = d === 0 ? NaN : (price[i]! - lower) / (upper - lower);
  }
  return series.withColumn(output, out);
}
