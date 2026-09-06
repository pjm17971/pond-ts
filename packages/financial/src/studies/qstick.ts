import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import {
  assertMaType,
  movingAverageValues,
} from '../kernels/moving-average.js';
import type { MaType } from '../kernels/moving-average.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';

export interface QstickOptions<S extends SeriesSchema, Output extends string> {
  /** Smoothing look-back in **bars**. **Default `8`.** */
  period?: number;
  /** Which moving average smooths the bodies — any of the shared
   *  {@link MaType} menu. **Default `'sma'`.** */
  maType?: MaType;
  /** Open column. **Default `'open'`.** */
  open?: NumericColumnNameForSchema<S>;
  /** Close column. **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'qstick'`.** */
  output?: Output;
}

/**
 * **QStick** (Tushar Chande) — a moving average of the **candle body**:
 *
 * ```
 * qstick = MA(close − open, period)
 * ```
 *
 * Appends one column. It is the candlestick chart's colour, quantified: a
 * positive reading means the last `period` bars closed above their opens on
 * average (white/hollow candles dominating), negative the reverse, and the
 * zero line is the crossover everyone reads it for. Chande's own default is
 * an 8-bar simple average, which is what ships.
 *
 * ## Definition
 *
 * There is only one, and it is short — the only free choices are the
 * smoothing length and type, both options here. **No TA-Lib function
 * exists**, so the oracle is a pandas replication (`(close −
 * open).rolling(n).mean()` for the default, the corresponding recursion for
 * the other types) with the analytic first valid bar asserted, not a vendor
 * cross-check.
 *
 * `maType` is the shared {@link MaType} menu — ChartIQ exposes an MA-type
 * input on this study, and the menu is the package's one answer to that
 * (`movingAverageValues`), never a private smoother.
 *
 * ## Two inputs, both named
 *
 * QStick is the package's **first study to read `open`**, so `open` joins
 * `high` / `low` / `close` / `volume` as a column a study names by option
 * with a `DEFAULT_OHLCV` default (the `atr` precedent). `close − open` is
 * the whole derivation, and both sides are redirectable — pointing `close`
 * at a study's output and `open` at the raw open gives "how far the smoothed
 * close sits above the open", which is a different (and unnamed) study, so
 * the flexibility is deliberate rather than incidental.
 *
 * ## Warm-up and edges
 *
 * - **In the units of the price**, not a percent or an oscillator: a body of
 *   0.5 on a $10 stock and on a $1000 stock read the same. It is a
 *   **linear** quantity — scaling every bar scales it — so it is not
 *   comparable across instruments at different price levels. Chande's own
 *   reading is relative to its own recent range.
 * - **The body is a derived array**, so the MA runs through the K2 engine's
 *   array door, where every type — `sma` included — waits for `period`
 *   **finite** values. First value at bar `period − 1` for the window types
 *   on gap-free input; the composed types (`dema`, `hull`, …) land later,
 *   per `movingAverageValues`.
 * - **A bar missing either open or close has no body**, and the gap then
 *   follows the chosen `maType`'s interior-gap rule: window types blank the
 *   windows containing it and recover, the `ema` family skips it, `smma`
 *   and `kama` propagate to the end.
 * - **No division**, so no zero-denominator case: a doji (`close === open`)
 *   contributes an honest 0, not a gap.
 */
export function qstick<
  S extends SeriesSchema,
  const Output extends string = 'qstick',
>(series: TimeSeries<S>, options: QstickOptions<S, Output> = {}) {
  const period = options.period ?? 8;
  assertPeriod(period);
  const maType = options.maType ?? 'sma';
  assertMaType(maType);
  const openName = (options.open ?? DEFAULT_OHLCV.open) as string;
  const closeName = (options.close ?? DEFAULT_OHLCV.close) as string;
  const output = (options.output ?? 'qstick') as Output;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const open = columnValues(wide, openName);
  const close = columnValues(wide, closeName);
  // A missing open or close is `NaN` ([PND-STUDYBOX]) and survives the
  // subtraction, so the body of a half-known bar is unknown rather than
  // being read as if the missing side were the other one.
  const body = new Float64Array(close.length);
  for (let i = 0; i < body.length; i += 1) body[i] = close[i]! - open[i]!;

  return series.withColumn(output, movingAverageValues(body, period, maType));
}
