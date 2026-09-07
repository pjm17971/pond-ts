import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_SOURCE } from '../contract/columns.js';
import type { MaType } from '../kernels/moving-average.js';
import {
  assertMaType,
  movingAverageColumn,
} from '../kernels/moving-average.js';
import { assertNoColumn } from '../kernels/rolling.js';

/** The short ("trader") half of Daryl Guppy's stack, in **bars**. */
export const GUPPY_SHORT_PERIODS = [3, 5, 8, 10, 12, 15] as const;
/** The long ("investor") half of Daryl Guppy's stack, in **bars**. */
export const GUPPY_LONG_PERIODS = [30, 35, 40, 45, 50, 60] as const;

export interface GuppyOptions<S extends SeriesSchema, Prefix extends string> {
  /** Source column, smoothed twelve times. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Which moving average — any of the shared {@link MaType} menu.
   *  **Default `'ema'`**, which is Guppy's own. */
  type?: MaType;
  /** Column-family prefix — appends `${prefix}S3 … ${prefix}S15` and
   *  `${prefix}L30 … ${prefix}L60`. **Default `'gmma'`.** */
  prefix?: Prefix;
}

/**
 * **Guppy Multiple Moving Average** (Daryl Guppy, *Trend Trading*) — two
 * ribbons of six exponential averages each, plotted together:
 *
 * ```
 * short ("traders")    3, 5, 8, 10, 12, 15    → ${prefix}S3  … ${prefix}S15
 * long  ("investors")  30, 35, 40, 45, 50, 60 → ${prefix}L30 … ${prefix}L60
 * ```
 *
 * Appends **twelve** columns. Nothing is combined: the study *is* the twelve
 * averages, and the reading is visual — the short ribbon compressing and
 * expanding is short-term agreement and disagreement, the long ribbon is the
 * investor view, and the two crossing while the long ribbon stays spread is
 * Guppy's trend-change signal. A chart draws them as a ribbon; the data
 * layer's job is the twelve columns.
 *
 * ## The twelve periods are the study — there is no "which periods" option
 *
 * They are named, published constants (exported as
 * {@link GUPPY_SHORT_PERIODS} / {@link GUPPY_LONG_PERIODS} so a chart can
 * label the ribbon without restating them), and a `periods` option would make
 * `guppy()` a generic "stack of moving averages" wearing Guppy's name. A
 * caller who wants their own stack has {@link movingAverage} called as many
 * times as they like with their own `output` names — clearer at the call site
 * and honest about not being GMMA. The one knob that *is* Guppy-compatible is
 * which average (chart platforms ship the stack on simple averages too), so
 * `type` takes the whole shared {@link MaType} menu and defaults to `'ema'`.
 *
 * `type`, not `maType`: the appended columns **are** the moving averages, as
 * {@link movingAverage}'s are. The `maType` spelling is for the studies where
 * an average is an *ingredient* of something else (`keltner`,
 * `disparityIndex`, `envelope`, `movingAverageDeviation`).
 *
 * ## Definition, verified
 *
 * TA-Lib has no GMMA, but it has **`EMA` at every one of the twelve periods**,
 * so the oracle checks each column against `talib.MA` rather than only the
 * assembly. As everywhere else in the package the EMAs are pond's — seeded on
 * the first sample, not on the SMA of the first `n` (the {@link macd}
 * precedent) — so the generator splits the question the way the K2 engine's
 * own case does: the **formula** is rebuilt on TA-Lib's SMA seed and required
 * to agree bit-exactly, and pond's seed transient is then required to be
 * **exactly geometric** at `(1 − α)`, which is what the difference of two
 * EMAs sharing a rate must be.
 *
 * The usual "decayed to under 0.5% of scale over the last 20 bars" bound
 * cannot be used at `period 60`: only 21 bars of the 80-bar fixture are
 * shared, so the transient has not finished decaying and is still **0.367%**
 * at the last bar (measured). The geometric check is the stronger statement
 * anyway — a wrong `α` leaves a residue **1e12× larger** (measured: 3.15
 * relative against 1e-12 for the correct rate).
 *
 * ## Warm-up — per column, and that is the point
 *
 * Each column starts where its own average does: on gap-free input and
 * `type: 'ema'`, `${prefix}S3` at bar 2 and `${prefix}L60` at bar 59, with
 * the ten others in between. Masking the ribbon back to its slowest member
 * would discard 57 real values of the fastest one, and the ribbon's whole
 * reading is how the members sit *relative to each other* as they arrive.
 * Other `type`s move every column together, per the table on
 * {@link movingAverageValues} (`dema` twice as late, `hull` and `kama` later
 * again).
 *
 * ## Edges
 *
 * - **Linear in price**, not scale-invariant: every column is a moving
 *   average, so scaling the input scales all twelve and shifting it shifts
 *   all twelve (both pinned as property tests).
 * - **A leading gap shifts each column's start** for every `type` except
 *   `'sma'`, which keeps `sma()`'s row-counting window — the column door's
 *   documented asymmetry.
 * - **An interior gap** costs whatever the chosen `type` costs, per column:
 *   `ema` skips the bar, the window types recover once it leaves the window,
 *   `smma` and `kama` propagate to the end.
 * - **Twelve columns is twelve passes.** At 1M bars `guppy()` costs roughly
 *   twelve `ema()` calls (measured — see `scripts/perf-studies.mjs`); there is
 *   no shared work between periods to save.
 */
export function guppy<
  S extends SeriesSchema,
  const Prefix extends string = 'gmma',
>(series: TimeSeries<S>, options: GuppyOptions<S, Prefix> = {}) {
  const type = options.type ?? 'ema';
  assertMaType(type);
  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const prefix = (options.prefix ?? 'gmma') as Prefix;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  // Guard every name before doing any work, so a collision on the twelfth
  // column does not cost eleven moving averages first.
  for (const period of GUPPY_SHORT_PERIODS) {
    assertNoColumn(wide, `${prefix}S${period}`);
  }
  for (const period of GUPPY_LONG_PERIODS) {
    assertNoColumn(wide, `${prefix}L${period}`);
  }

  // The K2 engine's COLUMN door, once per period: it keeps `'sma'` on
  // `sma()`'s own call and `'ema'` on `smooth('ema')`'s columnar fast path,
  // so a twelve-deep stack is twelve accelerated passes rather than twelve
  // array copies. Written out rather than folded into a loop because each
  // `withColumn` widens the schema and a loop would erase that.
  const ma = (period: number) =>
    movingAverageColumn(wide, column, period, type);

  return series
    .withColumn(`${prefix}S3` as const, ma(3))
    .withColumn(`${prefix}S5` as const, ma(5))
    .withColumn(`${prefix}S8` as const, ma(8))
    .withColumn(`${prefix}S10` as const, ma(10))
    .withColumn(`${prefix}S12` as const, ma(12))
    .withColumn(`${prefix}S15` as const, ma(15))
    .withColumn(`${prefix}L30` as const, ma(30))
    .withColumn(`${prefix}L35` as const, ma(35))
    .withColumn(`${prefix}L40` as const, ma(40))
    .withColumn(`${prefix}L45` as const, ma(45))
    .withColumn(`${prefix}L50` as const, ma(50))
    .withColumn(`${prefix}L60` as const, ma(60));
}
