import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_SOURCE } from '../contract/columns.js';
import {
  alphaEmaValues,
  movingAverageValues,
} from '../kernels/moving-average.js';
import { percentChangeValues } from '../kernels/rate-of-change.js';
import { assertNoColumn, columnValues } from '../kernels/rolling.js';

/** DecisionPoint's first custom-smoothing length, in **bars**. */
const PMO_FIRST = 35;
/** DecisionPoint's second custom-smoothing length, in **bars**. */
const PMO_SECOND = 20;
/** The signal line's **span** EMA length, in **bars**. */
const PMO_SIGNAL = 10;
/** The scaling DecisionPoint applies between the two smoothing stages. */
const PMO_SCALE = 10;

export interface PriceMomentumOscillatorOptions<
  S extends SeriesSchema,
  Prefix extends string,
> {
  /** Source column. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Column-family prefix — appends `${prefix}` and `${prefix}Signal`.
   *  **Default `'pmo'`.** */
  prefix?: Prefix;
}

/**
 * **Price Momentum Oscillator** (Carl Swenlin, DecisionPoint) — a one-bar
 * percent rate of change put through two stages of DecisionPoint's *custom*
 * smoothing, scaled by ten, with a span-EMA signal line:
 *
 * ```
 * roc             = 100 · (price[i]/price[i−1] − 1)
 * stage₁          = customEMA(roc, 35)          α = 2/35
 * ${prefix}       = customEMA(10 · stage₁, 20)  α = 2/20
 * ${prefix}Signal = EMA(${prefix}, 10)          α = 2/11
 * ```
 *
 * Appends two columns. Double-smoothing a one-bar rate of change is what
 * makes this readable at all — the raw series is noise — and the result is a
 * slow momentum line read on zero crossings and on crosses of its own signal.
 *
 * **The line is named `${prefix}`, not `${prefix}Line`** — the {@link trix}
 * shape, with the signal keeping the family suffix.
 *
 * ## "Custom smoothing" is `α = 2/n`, and it is NOT the K2 engine's `ema`
 *
 * This is the one definition in the package that is not a span EMA.
 * DecisionPoint defines its smoothing multiplier as **`2/n`** where every
 * other EMA in this package (and in TA-Lib, and in pandas' `ewm(span=n)`)
 * uses **`2/(n+1)`**. At `n = 20` that is `0.1` against `0.0952` — a
 * different rate, not a different seed, so it cannot be expressed as a span
 * and rounding it to one would ship a differently-named indicator.
 *
 * Measured on the oracle input: building both stages on the span EMA instead
 * puts the line **0.106** away on a reading whose scale is **3.906** — about
 * 2.7%, which is visible on a chart and invisible in a spot check. So the
 * study composes on `alphaEmaValues`, the kernel door that takes `α`
 * directly. That door exists **only** for this study; every other consumer
 * takes a span, because a span is the vocabulary the whole package and every
 * vendor speaks.
 *
 * **The signal line is a plain span EMA** (`α = 2/11`), not a custom-smoothed
 * one — DecisionPoint specifies "a 10-period EMA of the PMO Line", and the
 * asymmetry is theirs rather than an oversight here. Measured, a
 * custom-smoothed signal would sit **0.088** away, the same order as the
 * stage difference above, so this is stated rather than left to chance.
 *
 * ## Where the ×10 goes — nowhere, measurably
 *
 * DecisionPoint writes the scaling between the two stages (`customEMA(10 ×
 * stage₁, 20)`) and that is what the code does, but **every stage is
 * homogeneous**, so multiplying before the first stage, between them, or
 * after the second gives the same numbers: measured agreement to **8.9e-16**
 * on the oracle input, i.e. floating-point noise. Worth knowing before
 * someone "fixes" the placement.
 *
 * ## No period options — the four numbers are the study
 *
 * `column` and `prefix` are the whole option list. 35 / 20 / 10 with the
 * ×10 are DecisionPoint's published PMO; a version with other lengths is a
 * double-smoothed ROC, which is `percentChange` plus two `movingAverage`
 * calls and should say so at the call site. (This is the {@link kst}
 * decision applied to a second fixed-parameter study — with one asymmetry
 * worth naming: `kst` *does* expose `signalPeriod`, because vendors differ
 * on it there. DecisionPoint's 10 is not similarly contested, so it stays
 * fixed here.)
 *
 * ## Definition, verified
 *
 * **No TA-Lib function**, so the oracle is a pandas replication with the
 * analytic first-valid bars asserted and three measured separations — the
 * span-EMA stages, the custom-smoothed signal, and the ×10 placement.
 *
 * ## Warm-up
 *
 * The rate of change costs bar 0; each custom stage then emits once it has
 * consumed its own `n` finite samples, stepping over the previous stage's
 * warm-up rather than seeding on it. So on gap-free input the line first
 * lands on bar **54** (`1 + 35 − 1 + 20 − 1`) and the signal on bar **63**.
 * Length-preserving; each column emitted where it is defined.
 *
 * ## Edges
 *
 * - **Scale-invariant, not shift-invariant.** The rate of change is a ratio,
 *   so scaling every price leaves both columns unchanged; adding a constant
 *   changes the ratio and moves them. Both pinned as property tests.
 * - **A zero previous price → `undefined`** for that bar's rate of change,
 *   inherited from {@link percentChangeValues}. Unreachable on prices;
 *   reachable when `column` is a study output that crosses zero.
 * - **A leading gap shifts the start**; an **interior** gap costs the bar and
 *   the bar after it (the rate of change reads a predecessor), and the two
 *   recursions then skip and carry on — the `ema` family's rule, not
 *   Wilder's, so nothing propagates to the end.
 * - **Not bounded.** It is a percent rate of change times ten, smoothed, so
 *   its scale depends entirely on the instrument's daily moves.
 */
export function priceMomentumOscillator<
  S extends SeriesSchema,
  const Prefix extends string = 'pmo',
>(
  series: TimeSeries<S>,
  options: PriceMomentumOscillatorOptions<S, Prefix> = {},
) {
  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const prefix = (options.prefix ?? 'pmo') as Prefix;
  const lineName = `${prefix}` as const;
  const signalName = `${prefix}Signal` as const;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, lineName);
  assertNoColumn(wide, signalName);

  // A one-bar PERCENT rate of change, sharing `percentChange`'s definition
  // (including its zero-base guard) rather than restating it.
  const roc = percentChangeValues(columnValues(wide, column), 1);

  // Stage one: DecisionPoint's custom smoothing, α = 2/n rather than the
  // span EMA's 2/(n+1). See the docstring for the measured separation.
  const first = alphaEmaValues(roc, 2 / PMO_FIRST, PMO_FIRST);

  // The ×10 sits between the stages, as DecisionPoint writes it. Every stage
  // is homogeneous, so the placement is immaterial (measured to 8.9e-16);
  // it is written where the definition puts it.
  const scaled = new Float64Array(first.length);
  for (let i = 0; i < scaled.length; i += 1) {
    scaled[i] = PMO_SCALE * first[i]!;
  }
  const line = alphaEmaValues(scaled, 2 / PMO_SECOND, PMO_SECOND);

  // The signal is a plain SPAN EMA of the line — DecisionPoint's own
  // asymmetry, not a slip. Through the K2 engine's array door, so it steps
  // over the line's warm-up with no arithmetic here.
  const signal = movingAverageValues(line, PMO_SIGNAL, 'ema');

  return series.withColumn(lineName, line).withColumn(signalName, signal);
}
