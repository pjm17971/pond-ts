import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import { rollingBarExtremesValues } from '../kernels/highest-lowest.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';

export interface IchimokuOptions<
  S extends SeriesSchema,
  Prefix extends string,
> {
  /** Tenkan-sen (conversion line) window, in **bars**. **Default `9`.** */
  conversionPeriod?: number;
  /** Kijun-sen (base line) window, in **bars**. **Default `26`.** */
  basePeriod?: number;
  /** Senkou Span B window, in **bars**. **Default `52`.** */
  spanBPeriod?: number;
  /** How many bars a chart plots the two spans **forward** and the Chikou
   *  span **back**. **Default `26`.**
   *
   *  **This option changes no value in any appended column** — it is
   *  *metadata*, validated here and echoed by {@link ichimokuOffsets}, and
   *  the study deliberately does not displace its own data. See "Displacement
   *  is data-only" on the study docstring for why. */
  displacement?: number;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Close column — the Chikou span. **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Column-family prefix — appends `${prefix}Tenkan`, `${prefix}Kijun`,
   *  `${prefix}SenkouA`, `${prefix}SenkouB` and `${prefix}Chikou`.
   *  **Default `'ichi'`.** */
  prefix?: Prefix;
}

/** The five column-name suffixes {@link ichimoku} appends, in order. */
const SUFFIXES = ['Tenkan', 'Kijun', 'SenkouA', 'SenkouB', 'Chikou'] as const;

/**
 * The midpoint of the highest high and the lowest low of the last `period`
 * bars — the one shape all three of Ichimoku's lines have.
 *
 * Both extremes come from {@link rollingBarExtremesValues}, the **strict**
 * deque door: every one of the `period` cells of `high` AND of `low` must be
 * finite or the bar reads `NaN`. That is why the warm-up is exactly
 * `period − 1` bars and why an interior hole in either column blanks the line
 * for `period` bars rather than quietly averaging over a shorter window
 * (contrast `highestLowestValues`, whose skip-and-carry-on rule `donchian`
 * uses — the two lines are otherwise the same arithmetic).
 */
function midpointValues(
  highs: Float64Array,
  lows: Float64Array,
  period: number,
): Float64Array {
  // ONE walk per window, via the paired door. The single-array
  // `rollingExtremesValues` would take two calls here and discard half of
  // each answer, which measured as this study'''s whole cost at 1M bars: six
  // passes at 414 ms against 213 ms for the three below.
  const { highest, lowest } = rollingBarExtremesValues(highs, lows, period);
  const out = new Float64Array(highs.length);
  for (let i = 0; i < out.length; i += 1) {
    // NaN propagates: a bar missing either extreme has no midpoint, which is
    // what puts BOTH bar columns' gaps on the same rows of every line.
    out[i] = (highest[i]! + lowest[i]!) / 2;
  }
  return out;
}

/**
 * **Ichimoku Kinko Hyo** (Goichi Hosoda, 1969) — the five-line "one-glance
 * equilibrium chart", appended as five columns:
 *
 * ```
 * ${prefix}Tenkan   (HH + LL) / 2 over conversionPeriod   (9)   conversion line
 * ${prefix}Kijun    (HH + LL) / 2 over basePeriod         (26)  base line
 * ${prefix}SenkouA  (Tenkan + Kijun) / 2                        leading span A
 * ${prefix}SenkouB  (HH + LL) / 2 over spanBPeriod        (52)  leading span B
 * ${prefix}Chikou   the close itself                            lagging span
 * ```
 *
 * `HH`/`LL` are the highest `high` and lowest `low` of the window — **not**
 * the closes. The cloud (kumo) is the band between the two Senkou spans, and
 * its colour flips with which span is on top; that is a chart concern (the
 * corpus' charts ask **C3**), not a column.
 *
 * ## Displacement is data-only — this study does NOT shift rows
 *
 * On a chart, Senkou A and B are drawn `displacement` bars **into the
 * future** and Chikou `displacement` bars **into the past**. This study
 * keys every column to **the bar it is computed from** and shifts nothing.
 *
 * The forward half has no honest alternative: `TimeSeries.shift(col, −n)`
 * moves values across rows that already exist, and there are no rows past
 * the last bar for the spans to land on (the corpus assessment's gap
 * **G5**). Inventing them means inventing bar times, which for anything but
 * a daily series is trading-calendar arithmetic — the same blocker as
 * [PND-TCAL]. The recommendation there, and here, is the **chart-side
 * lever**: a per-layer x-offset in bars (the charts ask **C2**), which is
 * where ChartIQ itself puts it and what keeps pond's data/marks separation
 * intact. {@link ichimokuOffsets} hands a chart exactly that map, so the
 * sign and the column names cannot be got wrong at the call site.
 *
 * The backward half — Chikou — **could** have been pre-shifted, since
 * `shift(col, −displacement)` lands entirely on rows that exist, and that is
 * what TradingView plots (its Chikou point at bar `i` is `close[i + 26]`).
 * It ships **raw** anyway, and this is the deliberate part:
 *
 * - **A pre-shifted Chikou is a look-ahead column.** Its value at row `i`
 *   is not knowable at row `i`, and every other column in this package is
 *   causal. Joined into a feature matrix or a backtest it leaks the future
 *   silently — there is nothing in the column's name or type to warn a
 *   reader.
 * - **It would make the study's own rule non-uniform.** With Chikou raw,
 *   the rule is one sentence — no column is displaced, the chart offsets
 *   `+displacement` on the spans and `−displacement` on Chikou. Pre-shift
 *   one of the three and a consumer has to remember which.
 *
 * The plotted form is one call away, and the docstring is where the
 * warning belongs rather than the default:
 *
 * ```ts
 * // For DRAWING or visual inspection only — the result is non-causal.
 * const plotted = ichimoku(bars).shift('ichiChikou', -26);
 * ```
 *
 * Measured on the oracle's 80 bars, the displacement is not a rounding
 * detail: `SenkouA` against itself displaced 26 bars differs by up to
 * **11.81**, `SenkouB` by **5.11**, and the close against the shifted Chikou
 * by **17.60**, on a series spanning 98.65…118.01. Getting the offset wrong
 * is a whole different indicator, which is why the number is exposed rather
 * than left to the caller's memory.
 *
 * ## Warm-up, per column
 *
 * Length-preserving, and each column warms up on its **own** window:
 * `Tenkan` at bar `conversionPeriod − 1` (8), `Kijun` and `SenkouA` at
 * `basePeriod − 1` (25 — `SenkouA` is the average of the two and inherits
 * the later), `SenkouB` at `spanBPeriod − 1` (51), and `Chikou` at bar
 * **0**, since it is the close. Those five are asserted against the pandas
 * oracle, not recalled.
 *
 * ## Missing cells
 *
 * The three windowed lines take the **strict** rule (see
 * {@link rollingBarExtremesValues}): a hole in `high` or `low` blanks a line
 * for its whole window and it then recovers — `period` bars for `Tenkan`
 * from a hole at bar `k`, and so on. Both bar columns blank the same rows of
 * every line, because the midpoint is `(HH + LL) / 2` and `NaN` propagates
 * through it. A hole in `close` blanks `Chikou` on that bar only, and
 * nothing else: Ichimoku's other four lines never read the close, which is
 * the fact a build that computed the ranges over closes would hide (measured
 * separation below).
 *
 * ## Definition, verified
 *
 * TA-Lib has no Ichimoku. The oracle is a pandas replication at the
 * published parameters (9 / 26 / 52 / 26), asserting the five first-valid
 * bars above and separating the study from **the common slip — taking the
 * ranges over the close instead of the bar's high and low**: measured at up
 * to **0.2281** (mean 0.0654) on `Tenkan`, 0.2383 on `Kijun`, 0.2226 on
 * `SenkouA` and 0.2328 on `SenkouB` over the oracle's deliberately narrow
 * bars. That is small in absolute terms *because* the fixture's bars are
 * narrow by construction; on a real tape the gap is the bar range itself.
 *
 * The parameter set is a knob (`conversionPeriod` / `basePeriod` /
 * `spanBPeriod` / `displacement`) rather than the fixed nine-twenty-six of
 * Hosoda's daily-with-a-Saturday-session origin, because every charting
 * platform exposes it and a crypto or intraday user genuinely retunes it.
 * No relation between the three windows is enforced: `spanBPeriod` shorter
 * than `basePeriod` is unusual but not wrong, and the study is not the place
 * to have an opinion.
 *
 * ## Edges
 *
 * - **In the units of the price.** Scaling every price by `k` scales all
 *   five lines by `k`; adding `c` shifts all five by `c`. Nothing here
 *   normalises.
 * - **`displacement` changes no value** (see above). It is validated as a
 *   bar count so that a nonsense value fails at the call rather than
 *   silently reaching a chart, and an oracle case pins that a different
 *   `displacement` produces a bit-identical set of columns.
 * - **A flat window** is ordinary input: `HH === LL` makes the midpoint the
 *   price itself, which is a fact, not a `0/0` (contrast a stochastic's flat
 *   window — there the range is a *denominator*).
 */
export function ichimoku<
  S extends SeriesSchema,
  const Prefix extends string = 'ichi',
>(series: TimeSeries<S>, options: IchimokuOptions<S, Prefix> = {}) {
  const conversionPeriod = options.conversionPeriod ?? 9;
  const basePeriod = options.basePeriod ?? 26;
  const spanBPeriod = options.spanBPeriod ?? 52;
  const displacement = options.displacement ?? 26;
  assertPeriod(conversionPeriod, 'ichimoku conversionPeriod');
  assertPeriod(basePeriod, 'ichimoku basePeriod');
  assertPeriod(spanBPeriod, 'ichimoku spanBPeriod');
  assertPeriod(displacement, 'ichimoku displacement');

  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const closeName = (options.close ?? DEFAULT_OHLCV.close) as string;
  const prefix = (options.prefix ?? 'ichi') as Prefix;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  const names = SUFFIXES.map((s) => `${prefix}${s}`);
  for (const name of names) assertNoColumn(wide, name);

  const highs = columnValues(wide, highName);
  const lows = columnValues(wide, lowName);
  const tenkan = midpointValues(highs, lows, conversionPeriod);
  const kijun = midpointValues(highs, lows, basePeriod);
  const senkouB = midpointValues(highs, lows, spanBPeriod);
  const senkouA = new Float64Array(tenkan.length);
  for (let i = 0; i < senkouA.length; i += 1) {
    senkouA[i] = (tenkan[i]! + kijun[i]!) / 2;
  }
  const chikou = columnValues(wide, closeName);

  return series
    .withColumn(`${prefix}Tenkan` as const, tenkan)
    .withColumn(`${prefix}Kijun` as const, kijun)
    .withColumn(`${prefix}SenkouA` as const, senkouA)
    .withColumn(`${prefix}SenkouB` as const, senkouB)
    .withColumn(`${prefix}Chikou` as const, chikou);
}

export interface IchimokuOffsetOptions<Prefix extends string> {
  /** The same `displacement` passed to {@link ichimoku}. **Default `26`.** */
  displacement?: number;
  /** The same `prefix` passed to {@link ichimoku}. **Default `'ichi'`.** */
  prefix?: Prefix;
}

/** The five Ichimoku column names mapped to their plotting offset in bars. */
export type IchimokuOffsets<Prefix extends string> = Readonly<
  Record<`${Prefix}${(typeof SUFFIXES)[number]}`, number>
>;

/**
 * **The x-offset, in bars, a chart applies to each {@link ichimoku} column** —
 * `+displacement` for the two Senkou spans (drawn into the future),
 * `−displacement` for the Chikou span (drawn into the past), and `0` for
 * Tenkan and Kijun.
 *
 * The study keys every column to the bar it is computed from and displaces
 * nothing (see {@link ichimoku}), so the offset has to live *somewhere*. It
 * lives here rather than in the caller's head:
 *
 * ```ts
 * const opts = { displacement: 26 } as const;
 * const withCloud = ichimoku(bars, opts);
 * const offsets = ichimokuOffsets(opts);   // { ichiSenkouA: 26, … }
 * // <LineChart column="ichiSenkouA" xOffsetBars={offsets.ichiSenkouA} />
 * ```
 *
 * Pass the **same options object** you passed to `ichimoku` — the two read
 * the same two fields and default them the same way, so the names and the
 * signs cannot drift apart. This is the data-side half of the charts ask
 * **C2** (per-layer `xOffsetBars` plus forward projection space); until that
 * lands, the map is still what a consumer needs to shift the series itself.
 *
 * The precedent for exporting it at all is `GUPPY_SHORT_PERIODS` — a number
 * a chart has to restate is a number a chart gets wrong.
 */
export function ichimokuOffsets<const Prefix extends string = 'ichi'>(
  options: IchimokuOffsetOptions<Prefix> = {},
): IchimokuOffsets<Prefix> {
  const displacement = options.displacement ?? 26;
  assertPeriod(displacement, 'ichimokuOffsets displacement');
  const prefix = (options.prefix ?? 'ichi') as Prefix;
  return {
    [`${prefix}Tenkan`]: 0,
    [`${prefix}Kijun`]: 0,
    [`${prefix}SenkouA`]: displacement,
    [`${prefix}SenkouB`]: displacement,
    [`${prefix}Chikou`]: -displacement,
  } as IchimokuOffsets<Prefix>;
}
