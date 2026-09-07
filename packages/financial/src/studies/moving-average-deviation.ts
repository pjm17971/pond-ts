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
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';

export interface MovingAverageDeviationOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Moving-average length in **bars**. **Default `20`.** */
  period?: number;
  /** Which moving average — any of the shared {@link MaType} menu.
   *  **Default `'sma'`.** */
  maType?: MaType;
  /** Source column, read both as the price and as the average's input.
   *  **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Appended column name. **Default `'maDev'`.** */
  output?: Output;
}

/**
 * **Moving Average Deviation** — how far the price sits from its own moving
 * average, **in price units**:
 *
 * ```
 * maDev = price − MA(period)
 * ```
 *
 * Appends one column. Positive means price is above its average, and the
 * magnitude is a number of points on the instrument's own scale — which is
 * the whole reason to want this form rather than a percentage: it is
 * directly comparable with an ATR, a Keltner half-width or a stop distance.
 *
 * ## There is no `mode: 'percent'`, and that is a step-0 finding
 *
 * The corpus lists this study as "price − MA, points **or** percent"
 * (assessment §6.1). The percent form is **already shipped**, exactly, as
 * {@link disparityIndex}: `100·(price − MA)/MA` over the same `maType` menu
 * and the same defaults-shape. Measured over the oracle's closes at
 * `(20, sma)` and `(14, ema)`, `100·maDev/MA` and `disparity` agree to
 * **0.0** — bit for bit, not to rounding — and the generator asserts that
 * identity so the two cannot drift.
 *
 * So a `mode` option here would be two indicators behind a flag, one of them
 * a duplicate of an existing export: the {@link keltner} precedent this
 * package avoids. What ships is the **points** form only, which is the half
 * that had no name.
 *
 * (This reverses a line in {@link disparityIndex}'s own docstring, which
 * declined the absolute form as "`momentum`-shaped arithmetic anyone can
 * write". It is not quite: {@link momentum} is `price − price[−period]`, a
 * *lagged price*, where this is `price − MA(period)`, a *smoothed* one, and
 * the corpus names it as a study of its own. The two studies are now the
 * two halves of one pair and each points at the other.)
 *
 * ## Definition, verified
 *
 * **TA-Lib has no Moving Average Deviation**, so the oracle is a pandas
 * replication reusing the TA-Lib-verified K2 engine replication for the
 * average, with the analytic first-valid bar asserted and two discriminating
 * checks: the identity with {@link disparityIndex} above, and a separation
 * from {@link momentum} (`price − price[−period]`), which is the wrong turn
 * that leaves the shape intact.
 *
 * ## Edges
 *
 * - **Warm-up is the average's**, per the K2 engine's table — bar
 *   `period − 1` for the window types and `ema`, later for `dema` / `tema` /
 *   `hull` / `kama` / `zlema`. Length-preserving.
 * - **Linear in price**: scaling every price scales the reading, and — unlike
 *   {@link disparityIndex} — **shifting every price leaves it unchanged**,
 *   because the constant cancels between the price and its own average. Both
 *   pinned as property tests, and the shift half is exactly what the percent
 *   form does *not* satisfy.
 * - **No zero-denominator case at all.** There is no division, which is the
 *   other half of why the two forms are separate studies rather than a flag:
 *   {@link disparityIndex} has to answer "what does a zero average mean" and
 *   this never asks.
 * - **A leading gap shifts the start** for every `maType` except `'sma'`,
 *   which keeps `sma()`'s row-counting window (the column door's documented
 *   asymmetry). **An interior gap** costs whatever the chosen `maType` costs
 *   — window types recover, `ema` skips, `smma` and `kama` propagate — plus
 *   the bar's own missing price.
 */
export function movingAverageDeviation<
  S extends SeriesSchema,
  const Output extends string = 'maDev',
>(
  series: TimeSeries<S>,
  options: MovingAverageDeviationOptions<S, Output> = {},
) {
  const period = options.period ?? 20;
  assertPeriod(period);
  const maType = options.maType ?? 'sma';
  assertMaType(maType);

  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const output = (options.output ?? 'maDev') as Output;
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const price = columnValues(wide, column);
  const ma = movingAverageColumn(wide, column, period, maType);
  const out = new Float64Array(price.length);
  // A missing price or average is NaN and propagates on its own
  // ([PND-STUDYBOX]); there is no division, so there is nothing else to
  // guard.
  for (let i = 0; i < out.length; i += 1) {
    out[i] = price[i]! - ma[i]!;
  }

  return series.withColumn(output, out);
}
