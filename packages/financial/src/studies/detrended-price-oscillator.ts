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

export interface DetrendedPriceOscillatorOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Moving-average length in **bars**; also sets the displacement
   *  (`⌊period/2⌋ + 1`). **Default `20`.** */
  period?: number;
  /** Which moving average — any of the shared {@link MaType} menu.
   *  **Default `'sma'`.** */
  maType?: MaType;
  /** Source column. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Appended column name. **Default `'dpo'`.** */
  output?: Output;
}

/**
 * **Detrended Price Oscillator (DPO)** — the price less a moving average
 * displaced `shift = ⌊period/2⌋ + 1` bars **back**:
 *
 * ```
 * DPO[i] = price[i] − MA(period)[i − shift]        shift = ⌊period/2⌋ + 1
 * ```
 *
 * The displacement is the whole point: subtracting the *current* average
 * leaves a series that still lags the trend, while subtracting an average
 * from half a window ago removes the trend the average was measuring and
 * leaves the cycle. DPO is used to read cycle **length**, not direction, and
 * it deliberately does not extend to the last bar of a trend the way a plain
 * `price − MA` does.
 *
 * ## The integer rule on odd periods
 *
 * `period/2 + 1` is not an integer for odd `period`, and the published
 * sources write it without saying which way it goes. We **floor**:
 * `shift = ⌊period/2⌋ + 1`, so `period 20 → 11` and `period 15 → 8`. That is
 * StockCharts' `{X/2 + 1}` read as the number of whole bars back, and it is
 * the only rule that keeps the study a pure re-indexing of the average (the
 * alternative is interpolating between two bars of it, which no
 * implementation does). The kernel makes the same call for `zlema`'s lag, for
 * the same reason.
 *
 * ## Which alignment (a real fork, named)
 *
 * There are two published DPOs and they are **different series**, not one
 * series plotted two ways:
 *
 * - **Ours (TradingView's default, `isCentered = false`)**:
 *   `price[i] − MA[i − shift]` — the current bar's price against a displaced
 *   average.
 * - **The centered form (StockCharts, TradingView's `isCentered = true`)**:
 *   `price[i − shift] − MA[i]` — a *past* price against the current average.
 *
 * Both are causal at bar `i`; they differ in which bar the value describes.
 * We ship the first because a study here appends a column **aligned to the
 * source's own time axis**: the value on bar `i` has to be about bar `i`'s
 * price, or it silently misaligns with everything else on that row (the
 * bar's close, another study's output, a chart's crosshair). The centered
 * form's value on bar `i` is about a price `shift` bars earlier, which is a
 * plotting convention, not a column. A caller who wants it can shift ours —
 * the numbers are not the same series, so we do not claim they are.
 *
 * `maType` is offered because the average is the only free choice in the
 * definition and the K2 engine already names the menu; the classic DPO is
 * the SMA one, which is the default.
 *
 * ## Definition source
 *
 * **TA-Lib has no DPO**, so the oracle is a pandas replication with the
 * analytic first-valid bar asserted (below), not a vendor mask copied.
 *
 * ## Edges
 *
 * - **Warm-up is the average's, plus the displacement**: first value on bar
 *   `period − 1 + shift` for the window types and `ema` (bar 30 at the
 *   default `period 20`), later for `dema` / `tema` / `hull` / `kama` /
 *   `zlema`, whose own first valid bar is later. Length-preserving; earlier
 *   rows `undefined`.
 * - **Linear in the input, and shift-invariant** — a difference of prices, so
 *   scaling every price by `k` scales the reading by `k` (the {@link macd}
 *   side of the scale pair, not the {@link disparityIndex} side), while
 *   *adding* a constant adds it to both the price and the displaced average
 *   and cancels. Both pinned by a property test, over every `maType`, since
 *   every type in the menu satisfies `MA(a·x + b) = a·MA(x) + b`.
 * - **An interior gap** costs the bar itself (the price is missing) *and*
 *   the bar `shift` later (the displaced average is missing there), plus
 *   whatever the `maType`'s own gap rule costs — window types recover, the
 *   `ema` family skips, `smma` and `kama` propagate to the end.
 * - **A series shorter than `period − 1 + shift`** reads entirely
 *   `undefined`, with the row count kept.
 */
export function detrendedPriceOscillator<
  S extends SeriesSchema,
  const Output extends string = 'dpo',
>(
  series: TimeSeries<S>,
  options: DetrendedPriceOscillatorOptions<S, Output> = {},
) {
  const period = options.period ?? 20;
  assertPeriod(period);
  const maType = options.maType ?? 'sma';
  assertMaType(maType);

  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const output = (options.output ?? 'dpo') as Output;
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const shift = Math.floor(period / 2) + 1;
  const price = columnValues(wide, column);
  const ma = movingAverageColumn(wide, column, period, maType);
  const out = new Float64Array(price.length).fill(NaN);
  // Rows before `shift` have no displaced average at all and stay NaN; from
  // there on a missing price or average is NaN and propagates on its own
  // ([PND-STUDYBOX]).
  //
  // Starting the loop at `shift` rather than at 0 is deliberate even though
  // it changes no output: `ma[i - shift]` at a negative index reads
  // `undefined` (the `!` is a type assertion, not a bounds check) and the
  // subtraction would give NaN anyway — so the two forms are equivalent, and
  // a mutation between them survives the suite. Depending on an
  // out-of-range read to produce the right answer is not something to leave
  // to luck, so the bound is explicit.
  for (let i = shift; i < out.length; i += 1) {
    out[i] = price[i]! - ma[i - shift]!;
  }

  return series.withColumn(output, out);
}
