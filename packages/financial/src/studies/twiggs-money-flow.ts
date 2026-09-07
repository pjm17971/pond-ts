import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import { moneyFlowVolumeValues } from '../kernels/close-location.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';
import { trueRangeBoundsValues } from '../kernels/true-range.js';
import { wilderValues } from '../kernels/wilder.js';

export interface TwiggsMoneyFlowOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Smoothing length in **bars**. **Default `21`** (Twiggs'). */
  period?: number;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Close column. **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Volume column. **Default `'volume'`.** */
  volume?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'tmf'`.** */
  output?: Output;
}

/**
 * **Twiggs Money Flow** (Colin Twiggs) — Chaikin Money Flow rebuilt on the
 * bar's **true** range and on Wilder's exponential smoothing:
 *
 * ```
 * trueHigh = max(high, prevClose)      trueLow = min(low, prevClose)
 *
 * flow  = volume · ((close − trueLow) − (trueHigh − close))
 *                / (trueHigh − trueLow)
 *
 * ${output} = wilder(flow, period) / wilder(volume, period)
 * ```
 *
 * Appends one column, bounded **−1 … +1** on real bars (the per-bar close
 * location is, and this is a ratio of two smoothings of it against its own
 * weight), `undefined` until the smoothing is seeded. Above zero is net
 * accumulation, below zero net distribution — the same reading
 * {@link chaikinMoneyFlow} gives, on a different measurement of the bar.
 *
 * ## What it changes about Chaikin Money Flow — the whole point of the study
 *
 * Both studies are `Σ(close-location · volume) / Σ volume`. Twiggs' two
 * corrections are exactly the two terms this package can name:
 *
 * 1. **The range is the TRUE range**, `max(high, prevClose)` to
 *    `min(low, prevClose)` ({@link trueRangeBoundsValues}), not the bar's own
 *    `high − low`. A bar that gapped away from the previous close covers
 *    ground its own span does not show, and CMF scores such a bar as though
 *    the gap never happened — a gap-down bar that then closes at its own high
 *    reads `+1` (maximum accumulation) on Chaikin's range and something much
 *    closer to neutral on Twiggs'. This is the same argument
 *    {@link trueRangeValues} makes for ATR over plain range, applied to a
 *    money flow.
 * 2. **The averaging is exponential, not a flat window.** CMF weights the
 *    twentieth bar back exactly as much as today's and then drops it off a
 *    cliff; Wilder's recursion decays it. That also means TMF has no window
 *    to fall out of: an interior gap ends the reading (below), where CMF
 *    recovers `period` bars later.
 *
 * The two are therefore **different readings, not a reparametrisation**.
 * Measured on the package's oracle input at `period 21`, TMF and
 * `chaikinMoneyFlow({ period: 21 })` sit **0.1494** apart at their widest on
 * a TMF spanning −0.0279 … 0.1493 (`scripts/oracle/generate.py`) — wider than
 * the whole reading, on a fixture whose bars gap frequently. At `period 5`
 * the gap is 0.3514 on a range of −0.1629 … 0.3555.
 *
 * ## F-AMBIG — the smoothing, and the source followed
 *
 * The corpus flags Twiggs Money Flow as **F-AMBIG** on the smoothing, and it
 * is the real fork. **This ships Wilder's exponential form** —
 * {@link wilderValues}, `α = 1/period`, seeded on the mean of the first
 * `period` terms — which is the algorithm published on Twiggs' own site
 * (Incredible Charts, *Twiggs Money Flow*), where the smoothing is described
 * as an exponential moving average with the `1/n` constant Wilder's
 * indicators use. Two other forms circulate:
 *
 * - **A window sum**, `Σ flow / Σ volume` over `period` bars. That is
 *   {@link chaikinMoneyFlow} on the true range rather than Twiggs' study;
 *   measured on the oracle input at `period 21` it sits **0.0706** away from
 *   what ships (on a range of −0.0279 … 0.1493), so the choice is visible
 *   rather than cosmetic, and the generator asserts the separation.
 * - **A span EMA**, `α = 2/(period+1)`. Same family, faster decay; a caller
 *   who wants it can smooth `flow` and `volume` themselves. It is not offered
 *   as an option here because "which exponential average" is not a knob this
 *   study has — it is which definition you are computing, and a `maType`
 *   option would quietly make one study into three.
 *
 * ## Warm-up, and why numerator and denominator blank the same bars
 *
 * Bar 0 has no previous close, so it has no true range and no `flow`. The
 * smoothing therefore starts at bar 1 and the first reading lands on bar
 * **`period`** (one later than a plain window study's `period − 1`).
 *
 * The volume that goes into the denominator is **blanked wherever `flow` is**
 * before it is smoothed. That is not tidiness: `wilderValues` steps its seed
 * over a leading run of gaps, so a missing `high` on bar 1 would shift the
 * numerator's seed window without shifting the denominator's, and the ratio
 * would then be a numerator over `period` bars divided by a denominator over
 * a *different* `period` bars. It is the #710 lesson (two smoothings of two
 * columns must blank the same bars) and it is pinned by a test.
 *
 * ## Edges
 *
 * - **A zero smoothed volume → `undefined`.** The division is at the output,
 *   so the guard is live: a window of genuinely zero-volume bars has nothing
 *   to weight by, and nothing forces the numerator to zero with it (a
 *   redirected `volume` can be negative on some bars and cancel). Without the
 *   guard the reading would be `±Infinity`, which `withColumn` rejects
 *   outright.
 * - **A flat true range** (`trueHigh === trueLow` — a halted bar that also
 *   did not move from the previous close) contributes `0` to the numerator
 *   and its volume to the denominator, exactly as {@link clvValues}' flat-bar
 *   rule says, pulling the reading toward zero.
 * - **An interior gap ends the reading**, because Wilder's recursion has no
 *   state to carry across a hole — the same asymmetry `atr` and `rsi` have
 *   and {@link chaikinMoneyFlow} does not. A leading run of gaps only shifts
 *   the seed.
 * - **Invariant under scaling volume, and under any affine change of price**
 *   — it is a ratio of a weighted mean of a bounded ratio; the price terms
 *   are differences, so a constant added to every price cancels in numerator
 *   and denominator alike (the previous close shifts with the bar). All three
 *   are pinned as property tests.
 */
export function twiggsMoneyFlow<
  S extends SeriesSchema,
  const Output extends string = 'tmf',
>(series: TimeSeries<S>, options: TwiggsMoneyFlowOptions<S, Output> = {}) {
  const period = options.period ?? 21;
  assertPeriod(period);
  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const closeName = (options.close ?? DEFAULT_OHLCV.close) as string;
  const volumeName = (options.volume ?? DEFAULT_OHLCV.volume) as string;
  const output = (options.output ?? 'tmf') as Output;
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const closes = columnValues(wide, closeName);
  const volume = columnValues(wide, volumeName);
  const { trueHigh, trueLow } = trueRangeBoundsValues(
    columnValues(wide, highName),
    columnValues(wide, lowName),
    closes,
  );
  const flow = moneyFlowVolumeValues(trueHigh, trueLow, closes, volume);

  // Blank the denominator wherever the numerator is blank, so the two
  // smoothings seed on and consume exactly the same bars (see the docstring).
  // `flow` already carries every gap in high / low / close / volume.
  const weight = new Float64Array(volume.length);
  for (let i = 0; i < weight.length; i += 1) {
    weight[i] = Number.isNaN(flow[i]!) ? NaN : volume[i]!;
  }

  // `start = 1` states why bar 0 is skipped: it has no previous close, so it
  // has no true range (strictly redundant — `wilderValues` steps over the
  // leading NaN anyway — and kept for the reason `atrValues` keeps it).
  const numerator = wilderValues(flow, period, 1);
  const denominator = wilderValues(weight, period, 1);

  const values = new Float64Array(flow.length);
  for (let i = 0; i < values.length; i += 1) {
    const d = denominator[i]!;
    // Live guard: the division is at the OUTPUT, and nothing forces the
    // numerator to zero with the denominator.
    values[i] = d === 0 ? NaN : numerator[i]! / d;
  }
  return series.withColumn(output, values);
}
