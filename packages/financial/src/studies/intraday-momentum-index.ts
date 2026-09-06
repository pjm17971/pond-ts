import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';
import { rollingMeanValues } from '../kernels/rolling-mean.js';
import { upDownLegValues } from '../kernels/up-down.js';

export interface IntradayMomentumIndexOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Look-back in **bars**. **Default `14`** (Chande's own). */
  period?: number;
  /** Open column. **Default `'open'`.** */
  open?: NumericColumnNameForSchema<S>;
  /** Close column. **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'imi'`.** */
  output?: Output;
}

/**
 * **Intraday Momentum Index** (Tushar Chande) — RSI's question asked of the
 * candle **body** instead of the close-to-close change, bounded `0..100`:
 *
 * ```
 * gain[i] = max(close − open, 0)          an up (white/hollow) candle
 * loss[i] = max(open − close, 0)          a down (black/filled) candle
 * imi     = 100 · Σ gain / (Σ gain + Σ loss)
 * ```
 *
 * over the last `period` bars, both sums **plain and unsmoothed**.
 *
 * Appends one column; `undefined` for the first `period − 1` rows. Note that
 * is `period − 1`, not `period`: a body needs no previous bar, so unlike
 * {@link rsi} and {@link chandeMomentum} this study loses no row to the
 * difference and warms up on its own window alone.
 *
 * Where RSI measures the market's move *between* bars, IMI measures it
 * *within* them: a market that gaps up every morning and then sells off all
 * day is strong on RSI and weak here, which is the divergence Chande built
 * it to show. It reads on RSI's scale — above 70 overbought, below 30
 * oversold — and it is the {@link qstick} question normalised: QStick
 * averages the body in price units, IMI reports the fraction of the total
 * body movement that was upward.
 *
 * Reads **open and close**, each named by an option defaulting to its
 * `DEFAULT_OHLCV` column — the {@link qstick} shape.
 *
 * ## Definition — plain sums, not Wilder smoothing
 *
 * The corpus maps IMI as "RSI form on `(C − O)`", and *RSI form* is exactly
 * the ambiguity worth pinning: RSI's own averages are **Wilder-smoothed**,
 * while every published statement of IMI (Chande's, and the vendor
 * descriptions that follow him) sums the gains and losses over the window
 * **unsmoothed**. What ships is the unsmoothed form. **TA-Lib has no IMI**,
 * so there is no vendor bar-for-bar reference; the oracle is a pandas
 * replication of the definition above with its analytic first-valid bar
 * asserted.
 *
 * The consequence of the choice is the interior-gap and recovery behaviour
 * below — a window sum forgets, a Wilder recursion does not — so it is worth
 * knowing which one you have. A caller who wants the smoothed variant has it
 * as `rsi()` over a column holding `close − open`.
 *
 * **Same form, different input, as {@link chandeMomentum}.** Over identical
 * legs the two are affine: `imi = (cmo + 100) / 2`. They are different
 * studies because their *inputs* differ — the candle body here, the
 * close-to-close change there — and a test pins the identity on a series
 * where those coincide (`open[i] = close[i−1]`, a tape with no gaps).
 *
 * ## Edges
 *
 * - **Bounded `0..100`**: `100` is a window of nothing but up candles, `0`
 *   nothing but down candles.
 * - **A doji** (`close === open`) contributes `0` to both sums — no
 *   direction, not a gap.
 * - **An all-doji window** (`Σ gain + Σ loss = 0`) → `undefined`: the ratio
 *   is `0/0`, and `0` is what IMI reports for an all-*down* window, so
 *   emitting it would conflate the weakest possible reading with no
 *   movement at all (the {@link rsi} precedent). No guard is written — with
 *   non-negative legs, a zero denominator forces a zero numerator, so the
 *   case *is* `0/0` and arrives as `NaN` unaided.
 * - **Scale- and shift-invariant**: the legs are differences within a bar
 *   (so a constant added to every price cancels) and the ratio is
 *   homogeneous of degree zero (so a scale factor cancels). Pinned by
 *   property tests.
 * - **A bar missing either its open or its close has no body**, and every
 *   window containing it is `undefined` — the array door. That is the gap bar
 *   and the `period − 1` bars after it, and then IMI **recovers**: the window
 *   rule, rather than {@link rsi}'s carry-forward. Note it costs one bar of
 *   input where {@link rsi} and {@link chandeMomentum} lose two, since a body
 *   does not read the previous close.
 * - **A leading gap shifts the start** rather than emptying the study.
 */
export function intradayMomentumIndex<
  S extends SeriesSchema,
  const Output extends string = 'imi',
>(
  series: TimeSeries<S>,
  options: IntradayMomentumIndexOptions<S, Output> = {},
) {
  const period = options.period ?? 14;
  assertPeriod(period);
  const openName = (options.open ?? DEFAULT_OHLCV.open) as string;
  const closeName = (options.close ?? DEFAULT_OHLCV.close) as string;
  const output = (options.output ?? 'imi') as Output;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const open = columnValues(wide, openName);
  const close = columnValues(wide, closeName);
  // The candle body. A missing open or close is `NaN` ([PND-STUDYBOX]) and
  // survives the subtraction, so a half-known bar has an unknown direction
  // rather than being read as a doji.
  const length = close.length;
  const bodies = new Float64Array(length);
  for (let i = 0; i < length; i += 1) bodies[i] = close[i]! - open[i]!;

  const { up, down } = upDownLegValues(bodies);
  // Means, not sums: the shared `1/period` cancels in the ratio, so this is
  // the definition exactly while composing on the shipped kernel (whose
  // array door is also the warm-up and interior-gap rule above).
  const meanUp = rollingMeanValues(up, period);
  const meanDown = rollingMeanValues(down, period);

  const out = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    const u = meanUp[i]!;
    out[i] = (100 * u) / (u + meanDown[i]!);
  }
  return series.withColumn(output, out);
}
