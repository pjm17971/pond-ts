import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import { foldRows } from '../kernels/fold.js';
import {
  percentOfRangeValues,
  rollingExtremesValues,
} from '../kernels/highest-lowest.js';
import { assertNoColumn, assertPeriod } from '../kernels/rolling.js';
import { columnValues } from '../kernels/rolling.js';
import { medianPriceValues } from '../kernels/typical-price.js';

export interface FisherTransformOptions<
  S extends SeriesSchema,
  Prefix extends string,
> {
  /** Look-back for the highest/lowest **median price**, in **bars**.
   *  **Default `10`** (Ehlers' `Len`). */
  period?: number;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Column-family prefix — appends `${prefix}` and `${prefix}Signal`.
   *  **Default `'fisher'`.** */
  prefix?: Prefix;
}

/** The machine's carried state. Mutated in place by {@link fisherStep};
 *  allocated once per call. */
interface FisherState {
  /** The smoothed, clamped normalised price — Ehlers' `Value1`. */
  value: number;
  /** The previous bar's transformed value — Ehlers' `Fish[1]`. */
  fish: number;
}

/**
 * One bar of Ehlers' recursion, in his published order (`Using The Fisher
 * Transform`, TASC November 2002). Both constants are Ehlers', not options —
 * see the study's docstring.
 */
function fisherStep(
  state: FisherState,
  i: number,
  run: number,
  inputs: readonly Float64Array[],
  outputs: readonly Float64Array[],
): void {
  if (run === 1) {
    // A fresh run starts from Ehlers' implicit zeros (an EasyLanguage
    // variable is 0 before its first assignment), which is what makes the
    // first bars of a run carry a decaying transient rather than a jump.
    state.value = 0;
    state.fish = 0;
  }

  let value = 0.33 * inputs[0]![i]! + 0.67 * state.value;
  // Ehlers' clamp, asymmetric on purpose: the TEST is at ±0.99 and the
  // REPLACEMENT is ±0.999, so a value just inside the test is pushed
  // outwards. Kept exactly as published — the point is to stop `ln` blowing
  // up at ±1, and rounding the two thresholds together would be a different
  // indicator (see the docstring).
  if (value > 0.99) value = 0.999;
  else if (value < -0.99) value = -0.999;
  state.value = value;

  const fish = 0.5 * Math.log((1 + value) / (1 - value)) + 0.5 * state.fish;
  outputs[0]![i] = fish;
  // The trigger is the PREVIOUS bar's transform, so it exists only from the
  // second bar of a run — on the seed bar `state.fish` is the cold-start
  // zero, which is not a reading.
  if (run >= 2) outputs[1]![i] = state.fish;
  state.fish = fish;
}

/**
 * **Ehlers Fisher Transform** (John F. Ehlers, TASC November 2002) — the
 * median price normalised into `(−1, 1)` over its own recent range and then
 * pushed through the Fisher transform, which turns a roughly uniform
 * distribution into a roughly Gaussian one so that turning points become
 * sharp spikes rather than gentle rolls:
 *
 * ```
 * price           = (high + low) / 2
 * x               = 2 · (price − LL) / (HH − LL) − 1          over `period` bars
 * value           = 0.33 · x + 0.67 · value[−1]     clamped to ±0.999
 * ${prefix}       = 0.5 · ln((1 + value) / (1 − value)) + 0.5 · ${prefix}[−1]
 * ${prefix}Signal = ${prefix}[−1]
 * ```
 *
 * Appends two columns. The line is **unbounded** — that is the whole point:
 * the transform's tails run to infinity, so an extreme in the normalised
 * price that a stochastic would flatten against 0 or 100 becomes a spike
 * here. The signal is not a smoothing but the line **delayed one bar**
 * (Ehlers calls it the trigger), so a crossing is simply a turn.
 *
 * **The line is named `${prefix}`, not `${prefix}Line`** — the {@link trix}
 * shape, with the signal keeping the family suffix.
 *
 * ## The constants are Ehlers', not options
 *
 * `0.33 / 0.67`, the `±0.99` clamp test with its `±0.999` replacement, and
 * the `0.5 / 0.5` second smoothing are **fixed**. They are not tuning knobs
 * that happen to have defaults: the first pair is the exponential smoothing
 * Ehlers specified so that the normalised input is not jittering across the
 * transform's steep centre, and the clamp exists solely because `ln` of
 * `(1+x)/(1−x)` diverges at `x = ±1` — a value that has spent several bars
 * pinned at an extreme of its range reaches the test, and without the clamp
 * the column would carry an infinity that `withColumn` rejects outright.
 * Exposing them would invite a caller to set the clamp to `1.0` and get a
 * study that throws on real data. `period` is the one number Ehlers
 * parameterises, and it is the only one here.
 *
 * The clamp's asymmetry (`> 0.99` becomes `0.999`, so a value of `0.995` is
 * pushed **up**) is published, not a transcription slip, and is kept.
 *
 * ## Which range — the **median price's** own extremes, not high/low
 *
 * Ehlers takes `Highest(price, Len)` and `Lowest(price, Len)` of the *median
 * price series*, not the highest `high` and lowest `low` of the bars. Ports
 * differ on this and the difference is real: measured on the package's
 * oracle input, running the same machine on `HH(high)`/`LL(low)` puts the
 * line **5.95 apart** on a reading that spans −4.69 … 7.60
 * (`scripts/oracle/generate.py`) — most of the scale. Ehlers' reading ships;
 * the port is named here so a caller comparing against another platform
 * knows which fork they are looking at. Dropping the second (`0.5/0.5`)
 * smoothing, the other common transcription slip, is **3.80** away; both
 * separations are asserted by the generator.
 *
 * Because the extremes are taken over a **derived** array, they go through
 * the strict door ({@link rollingExtremesValues}) — every one of the
 * `period` median prices must exist — rather than the skipping one the raw
 * bar studies use.
 *
 * ## A state machine, so a gap RESETS it
 *
 * The two recursions carry state, so this is a {@link foldRows} step
 * ([PND-SFOLD]) rather than a private loop, and the kernel's rule applies: a
 * bar the machine did not see leaves `value` and `fish` unknown — a
 * recursion never washes the error out — so the machine **restarts** from
 * Ehlers' zeros on the next complete bar. That costs the real state before
 * the gap and buys a reading that is exactly what the definition says on
 * every bar it prints.
 *
 * A **flat window** (`HH === LL`) is such a bar: the normalised price is a
 * `0/0` and reads `undefined` ({@link percentOfRangeValues} owns that rule
 * for every range study), which resets the machine. It is reachable — a
 * halted instrument prints one within `period` bars.
 *
 * ## Warm-up and the seed transient
 *
 * Length-preserving. On gap-free input the line lands at bar `period − 1`
 * (the range's own first bar) and the signal one bar later — bars **9** and
 * **10** at the default. Ehlers' zeros
 * mean the first bars of a run carry a **transient**: the `value` recursion
 * forgets its seed at `0.67ᵏ` and the `fish` recursion at `0.5ᵏ`, so the
 * error is under a thousandth of the seed after ~17 and ~10 bars
 * respectively. The oracle transcribes the same seed and therefore agrees
 * exactly — which is worth saying plainly: for a state machine the pandas
 * side is a **transcription** of this step, not an independent derivation,
 * so what carries the verification is the analytic first-valid bar and the
 * two measured separations above, both asserted in the generator.
 *
 * ## Edges
 *
 * - **Scale- and shift-invariant.** The normalised price is a position
 *   within a range, so multiplying or offsetting every bar leaves both
 *   columns unchanged. Pinned as property tests.
 * - **`period: 1`** makes every window flat (`HH === LL === price`), so the
 *   whole column is `undefined` — honest rather than special-cased.
 */
export function fisherTransform<
  S extends SeriesSchema,
  const Prefix extends string = 'fisher',
>(series: TimeSeries<S>, options: FisherTransformOptions<S, Prefix> = {}) {
  const period = options.period ?? 10;
  assertPeriod(period);

  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const prefix = (options.prefix ?? 'fisher') as Prefix;
  const lineName = `${prefix}` as const;
  const signalName = `${prefix}Signal` as const;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, lineName);
  assertNoColumn(wide, signalName);

  const price = medianPriceValues(
    columnValues(wide, highName),
    columnValues(wide, lowName),
  );
  const { highest, lowest } = rollingExtremesValues(price, period);
  // `percentOfRangeValues` is `100·(p − LL)/(HH − LL)` with the flat-window
  // rule the package shares; Ehlers' `x` is that rescaled to [−1, 1].
  const percent = percentOfRangeValues(highest, lowest, price);
  const normalised = new Float64Array(percent.length);
  for (let i = 0; i < percent.length; i += 1) {
    normalised[i] = percent[i]! / 50 - 1;
  }

  const [line, signal] = foldRows(
    [normalised],
    2,
    { value: 0, fish: 0 },
    fisherStep,
  );

  return series.withColumn(lineName, line!).withColumn(signalName, signal!);
}
