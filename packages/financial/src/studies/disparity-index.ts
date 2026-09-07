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
} from '../kernels/rolling.js';
import {
  assertMaType,
  movingAverageColumn,
} from '../kernels/moving-average.js';
import type { MaType } from '../kernels/moving-average.js';

export interface DisparityIndexOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Moving-average length in **bars**. **Default `14`.** */
  period?: number;
  /** Which moving average — any of the shared {@link MaType} menu.
   *  **Default `'sma'`.** */
  maType?: MaType;
  /** Source column, read both as the price and as the average's input.
   *  **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Appended column name. **Default `'disparity'`.** */
  output?: Output;
}

/**
 * **Disparity Index** (Steve Nison) — how far the price sits from its own
 * moving average, as a percentage of that average:
 *
 * ```
 * 100 · (price − MA(period)) / MA(period)
 * ```
 *
 * Zero means price is exactly on the average; `+3` means 3% above it. The
 * average comes from the shared K2 engine, so `maType` is the whole
 * {@link MaType} menu.
 *
 * ## Definition
 *
 * **TA-Lib has no Disparity Index**, so this is a pandas replication of the
 * definition above (assessment §6.1: `100·(C−MA)/MA`, MA-type), with the
 * analytic first-valid bar asserted in the oracle generator rather than a
 * vendor mask copied. The formula is unambiguous across sources — the only
 * convention worth naming is that it is a **percent** (×100), not a ratio,
 * which is what every published version plots.
 *
 * It is the **percent sibling** of {@link movingAverageDeviation}, which is
 * the same numerator in price units (`price − MA`). The two ship as separate
 * studies rather than one with a `mode` flag — that would be two indicators
 * behind an option, the {@link keltner} precedent this package avoids — and
 * the identity `100·maDev/MA === disparity` is asserted in the oracle so
 * they cannot drift. (An earlier version of this note declined the absolute
 * form as "`momentum`-shaped arithmetic anyone can write"; that was wrong on
 * the detail — {@link momentum} subtracts a *lagged price*, not a *smoothed*
 * one — and the corpus names it as a study of its own.)
 *
 * ## Relationship to {@link priceOscillator}
 *
 * Same shape, different fast leg: the price oscillator's numerator is
 * `MA(fast) − MA(slow)`, this one's is `price − MA`. Setting a price
 * oscillator's `fastPeriod` to 1 does **not** reproduce it in general (an
 * `MA(1)` is the price only for the window types), so the two stay separate
 * studies rather than one with a magic period.
 *
 * ## Edges
 *
 * - **Warm-up is the average's**, per the K2 engine's table — bar
 *   `period − 1` for the window types and `ema`, later for `dema` / `tema` /
 *   `hull` / `kama` / `zlema`. Length-preserving; earlier rows `undefined`.
 * - **Scale-invariant**: multiplying every price by `k` multiplies both the
 *   numerator and the denominator, so the reading is unchanged (pinned by a
 *   property test). It is *not* shift-invariant — adding a constant moves
 *   the denominator without moving the numerator.
 * - **A leading gap shifts the start** for every `maType` except `'sma'`,
 *   which keeps `sma()`'s row-counting window (the column door's documented
 *   asymmetry).
 * - **An interior gap** costs whatever the chosen `maType` costs — window
 *   types recover once it leaves the window, the `ema` family skips the bar,
 *   `smma` and `kama` propagate to the end (stated per type on
 *   {@link movingAverageValues}). The bar's own missing price also costs that
 *   bar's numerator, so the reading is missing there regardless of the type.
 * - **A zero moving average** (reachable only on a column that can be zero or
 *   negative — a return series, another oscillator) reports **no value**, not
 *   `Infinity` and not `0`: the ratio is genuinely undefined there.
 */
export function disparityIndex<
  S extends SeriesSchema,
  const Output extends string = 'disparity',
>(series: TimeSeries<S>, options: DisparityIndexOptions<S, Output> = {}) {
  const period = options.period ?? 14;
  assertPeriod(period);
  const maType = options.maType ?? 'sma';
  assertMaType(maType);

  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const output = (options.output ?? 'disparity') as Output;
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const price = columnValues(wide, column);
  const ma = movingAverageColumn(wide, column, period, maType);
  const out = new Float64Array(price.length);
  for (let i = 0; i < out.length; i += 1) {
    const m = ma[i]!;
    // A missing price or average is NaN and propagates ([PND-STUDYBOX]);
    // `m === 0` would instead give ±Infinity, which is reported as no value.
    out[i] = m === 0 ? NaN : (100 * (price[i]! - m)) / m;
  }

  return series.withColumn(output, out);
}
