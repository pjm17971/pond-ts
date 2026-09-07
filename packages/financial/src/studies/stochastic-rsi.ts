import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_SOURCE } from '../contract/columns.js';
import {
  percentOfRangeValues,
  rollingExtremesValues,
} from '../kernels/highest-lowest.js';
import { rollingMeanValues } from '../kernels/rolling-mean.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';
import { rsi } from './rsi.js';

/** The scratch column the RSI is computed into before the stochastic reads
 *  it. Never reaches the caller — the result is appended to the ORIGINAL
 *  series. (`emaValues`' `'__ema__'` is the same trick one layer down.) */
const RSI_SCRATCH = '__stochRsiRsi__';

export interface StochasticRsiOptions<
  S extends SeriesSchema,
  Prefix extends string,
> {
  /** Source column the RSI is taken over. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Wilder look-back of the underlying RSI, in **bars**. **Default `14`.** */
  rsiPeriod?: number;
  /** Look-back for the highest/lowest **of the RSI**, in **bars**.
   *  **Default `14`.** TA-Lib calls this `fastk_period`. */
  stochPeriod?: number;
  /** Simple-average length applied to the raw range position to make `%K`,
   *  in **bars**. **Default `3`.** `1` leaves `%K` unsmoothed. TA-Lib calls
   *  this `fastd_period`. */
  kPeriod?: number;
  /** Simple-average length applied to `%K` to make `%D`, in **bars**.
   *  **Default `3`.** No TA-Lib equivalent. */
  dPeriod?: number;
  /** Column-family prefix — appends `${prefix}K` and `${prefix}D`.
   *  **Default `'stochRsi'`.** */
  prefix?: Prefix;
}

/**
 * **Stochastic RSI** (Tushar Chande & Stanley Kroll) — the
 * {@link stochastic} construction applied to {@link rsi} instead of to
 * price:
 *
 * ```
 * r        = rsi(column, rsiPeriod)
 * raw      = 100 · (r − LL(r, stochPeriod)) / (HH(r, stochPeriod) − LL(…))
 * ${prefix}K = SMA(raw, kPeriod)
 * ${prefix}D = SMA(${prefix}K, dPeriod)
 * ```
 *
 * Appends two columns, both bounded `0..100`. RSI spends most of its life
 * between 40 and 60, so its own overbought/oversold thresholds fire rarely;
 * normalising it against its **own** recent range makes it reach both
 * extremes constantly. It is a much faster indicator than the RSI under it,
 * and that is the point rather than a defect.
 *
 * It composes on the shipped {@link rsi} — the study, not a private copy —
 * so Wilder's seed, the flat-window rule and every other RSI decision are
 * inherited by construction rather than restated.
 *
 * ## The option names are TradingView's, and `kPeriod` does NOT mean what it
 * means on {@link stochastic}
 *
 * Read this before reaching for the analogy. The two studies use their own
 * vendor's vocabulary, and the same word lands on different knobs:
 *
 * | this study | {@link stochastic} | TA-Lib `STOCHRSI` | what it is |
 * | --- | --- | --- | --- |
 * | `stochPeriod` | `kPeriod` | `fastk_period` | the HH/LL look-back |
 * | `kPeriod` | `slowing` | `fastd_period` | the smoothing of the raw position |
 * | `dPeriod` | `dPeriod` | — | the smoothing of `%K` |
 *
 * `rsiPeriod` and `stochPeriod` are separate knobs even though both default
 * to 14: they answer different questions (how much history the RSI averages,
 * and how much of the RSI's own history the range covers), and vendors ship
 * them independently.
 *
 * ## Definition — TA-Lib's `STOCHRSI`, with the mapping measured
 *
 * TA-Lib's `STOCHRSI` returns **`fastk` and `fastd`**, not a slowed `%K` and
 * `%D`, so the correspondence is not the obvious one and the generator
 * measures which column equals which. On the oracle input at the defaults:
 *
 * - **`${prefix}K` == `talib.STOCHRSI(…, fastk_period = stochPeriod,
 *   fastd_period = kPeriod).fastd`** — bar for bar, to **9.9e-14**, with
 *   **identical null masks** (both first valid at bar 29).
 * - The raw, unsmoothed range position — which this study does **not** emit
 *   — equals TA-Lib's `fastk` to **1.3e-13**.
 * - **`${prefix}D` has no TA-Lib counterpart.** `STOCHRSI` stops at `fastd`;
 *   the second smoothing is the conventional `%D` that every charting
 *   platform draws beside it, and it is a pandas replication in the oracle.
 *
 * Crossing the columns is a real error, not a rounding one: `${prefix}K`
 * against TA-Lib's `fastk` differs by **45 points** on this fixture, which
 * is why the generator asserts both the match and the mismatch.
 *
 * ## Two deliberate deltas, both inherited
 *
 * - **A flat RSI window is `undefined`.** When `HH === LL` the ratio is
 *   `0/0`; TA-Lib reports `0`, which is also the value for "the RSI is at the
 *   very bottom of its range". The rule lives in
 *   {@link percentOfRangeValues} so {@link stochastic}, {@link williamsR} and
 *   this study all make the same call. It is genuinely reachable here — an
 *   RSI pinned at 100 through a run of unbroken gains gives a flat window on
 *   real data, where a flat *price* range mostly does not — and a unit test
 *   pins it.
 * - **A flat RSI window inside the underlying RSI** (no gains and no losses
 *   at all) is `undefined` too, from {@link rsi}'s own rule.
 *
 * ## Warm-up — per column
 *
 * On gap-free input: the RSI at bar `rsiPeriod` (differences need one extra
 * bar), the range at `+ stochPeriod − 1`, `%K` at `+ kPeriod − 1` and `%D` at
 * `+ dPeriod − 1`. At the defaults that is bars **29** and **31**. Both
 * smoothings go through {@link rollingMeanValues}, which waits for the
 * window's worth of finite **values** — the derived-input rule, and the same
 * reason {@link stochastic}'s slow `%K` is not one bar early.
 *
 * ## Edges
 *
 * - **Scale- and shift-invariant**, because the RSI under it is: multiplying
 *   or offsetting every price leaves both columns unchanged (pinned).
 * - **An interior gap propagates to the end**, and that is the RSI's Wilder
 *   recursion rather than anything here — a recursion has no state to carry
 *   across a hole. Fill before smoothing if you need continuity.
 * - **`kPeriod: 1`** leaves `%K` as the raw range position — the "fast"
 *   Stochastic RSI, one knob rather than a second function, exactly as
 *   {@link stochastic}'s `slowing: 1` gives the fast stochastic.
 */
export function stochasticRsi<
  S extends SeriesSchema,
  const Prefix extends string = 'stochRsi',
>(series: TimeSeries<S>, options: StochasticRsiOptions<S, Prefix> = {}) {
  const rsiPeriod = options.rsiPeriod ?? 14;
  const stochPeriod = options.stochPeriod ?? 14;
  const kPeriod = options.kPeriod ?? 3;
  const dPeriod = options.dPeriod ?? 3;
  assertPeriod(rsiPeriod, 'rsiPeriod');
  assertPeriod(stochPeriod, 'stochPeriod');
  assertPeriod(kPeriod, 'kPeriod');
  assertPeriod(dPeriod, 'dPeriod');

  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const prefix = (options.prefix ?? 'stochRsi') as Prefix;
  const kName = `${prefix}K` as const;
  const dName = `${prefix}D` as const;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, kName);
  assertNoColumn(wide, dName);

  // The RSI comes from the shipped STUDY, into a scratch column that never
  // reaches the caller — one RSI definition in the package, not two. The
  // result below is appended to the ORIGINAL series, so the scratch column
  // is dropped with the intermediate.
  const withRsi = rsi(wide, {
    column: column as NumericColumnNameForSchema<SeriesSchema>,
    period: rsiPeriod,
    output: RSI_SCRATCH,
  }) as unknown as TimeSeries<SeriesSchema>;

  const rsiValues = columnValues(withRsi, RSI_SCRATCH);
  // The range is over the RSI, and it must be STRICT — every one of the
  // `stochPeriod` cells finite. The skipping door (`highestLowestValues`,
  // which is right for a bar's high and low) would emit a "14-bar range of
  // the RSI" as soon as TWO values existed, putting %K on bar 17 instead of
  // 29 and disagreeing with `talib.STOCHRSI`. Measured; see the kernel.
  const { highest, lowest } = rollingExtremesValues(rsiValues, stochPeriod);
  // 100·(r − LL)/(HH − LL), with the flat-window rule the kernel owns.
  const fastK = percentOfRangeValues(highest, lowest, rsiValues);
  // Both smoothings through the raw-array kernel: core's count-window `avg`
  // counts ROWS and would emit a "3-bar" %K on the first bar the raw
  // position exists.
  const k = rollingMeanValues(fastK, kPeriod);
  const d = rollingMeanValues(k, dPeriod);

  return series.withColumn(kName, k).withColumn(dName, d);
}
