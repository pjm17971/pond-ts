import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_SOURCE } from '../contract/columns.js';
import { foldRows } from '../kernels/fold.js';
import {
  percentOfRangeValues,
  rollingExtremesValues,
} from '../kernels/highest-lowest.js';
import { assertNoColumn, assertPeriod, emaValues } from '../kernels/rolling.js';

/** The fixed smoothing rate of both recursions — see the docstring. */
const SCHAFF_FACTOR = 0.5;

export interface SchaffTrendCycleOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Fast EMA span of the underlying MACD, in **bars**. **Default `23`.** */
  fastPeriod?: number;
  /** Slow EMA span of the underlying MACD, in **bars**. **Default `50`.** */
  slowPeriod?: number;
  /** Look-back of **both** stochastic passes, in **bars**. **Default `10`.** */
  cyclePeriod?: number;
  /** Source column. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Appended column name. **Default `'stc'`.** */
  output?: Output;
}

/** The carried state of one `0.5` recursion. */
interface SmoothState {
  previous: number;
}

/**
 * One bar of the `x += 0.5·(raw − x)` recursion, seeded on the first bar of a
 * run. Both stages run the same step over their own state, which is what
 * makes them one definition rather than two copies.
 */
function halfSmoothStep(
  state: SmoothState,
  i: number,
  run: number,
  inputs: readonly Float64Array[],
  outputs: readonly Float64Array[],
): void {
  const raw = inputs[0]![i]!;
  state.previous =
    run === 1 ? raw : state.previous + SCHAFF_FACTOR * (raw - state.previous);
  outputs[0]![i] = state.previous;
}

/**
 * **Schaff Trend Cycle** (Doug Schaff, ~1999) — a MACD read as a *cycle*
 * rather than as a level, by running a stochastic over it twice:
 *
 * ```
 * macd  = EMA(column, fastPeriod) − EMA(column, slowPeriod)
 * k1    = 100 · (macd − LL(macd)) / (HH(macd) − LL(macd))    over `cyclePeriod`
 * d1    = d1[−1] + 0.5 · (k1 − d1[−1])                       seeded on k1
 * k2    = 100 · (d1 − LL(d1)) / (HH(d1) − LL(d1))            over `cyclePeriod`
 * ${output} = stc[−1] + 0.5 · (k2 − stc[−1])                 seeded on k2
 * ```
 *
 * Appends one column, bounded **0 … 100**. A MACD says how far the fast
 * average is from the slow one in price units, which is not comparable
 * across instruments or across time; normalising it against its own recent
 * range twice turns it into a cycle oscillator that reaches both extremes on
 * every swing and turns earlier than the MACD it is built on. It is read on
 * the 25 and 75 lines rather than on a zero crossing.
 *
 * ## What the parameters are, and the shape of the port
 *
 * `23 / 50 / 10` are Schaff's, and the arrangement above is the one every
 * widely-copied port implements (the TradingView `STC` scripts and the
 * charting packages that follow them). Two details of it are worth naming
 * because prose descriptions leave them out:
 *
 * - **Both stochastic passes use the same `cyclePeriod`.** There is no
 *   separate `%D` length; the second pass is the same window over the
 *   smoothed output of the first.
 * - **The smoothing is a fixed `0.5`, not a span EMA.** `x += 0.5·(raw − x)`
 *   is an exponential average with `α = 0.5` — a *rate*, like
 *   DecisionPoint's `2/n` in {@link priceMomentumOscillator}, not a period —
 *   so it is a constant of the definition rather than an option. Dropping
 *   both recursions (reading the raw double stochastic straight) is a real
 *   difference and not a subtle one: measured, the two sit **98.30 apart**
 *   on a 0…100 reading (`scripts/oracle/generate.py`), which is why the
 *   generator asserts it.
 *
 * Because both recursions carry state, the pandas oracle is a
 * **transcription** of this step rather than an independent derivation; the
 * separation above and the analytic warm-up bound below are what actually
 * verify it.
 *
 * ## A state machine, so a gap RESETS it — the delta from the common port
 *
 * The two recursions carry state, so each is a {@link foldRows} step
 * ([PND-SFOLD]) rather than a private loop, and the kernel's reset rule
 * applies: a bar with no input re-seeds the recursion from the next raw
 * value.
 *
 * That is a **deliberate delta from the usual port**, which on a flat
 * window writes `nz(frac[1])` — it repeats the previous stochastic reading
 * as if it had been observed. This package does not do that anywhere: a flat
 * window is a `0/0` and reads `undefined` ({@link percentOfRangeValues}
 * owns the rule for every range study), and holding the previous value would
 * report a position in a range that has no positions in it. The bar reads
 * missing and the recursion re-seeds on the next one.
 *
 * **A flat window is not an exotic case here, and it is the sharp edge of
 * this study.** A sustained trend pins the *first* stochastic at 100 (or 0)
 * for `cyclePeriod` bars; the `0.5` smoothing of a constant is that
 * constant, so the *second* window is then exactly flat and the STC reads
 * `undefined` — during the strongest part of the move. It is not
 * hypothetical: on this package's own 80-bar oracle input at the defaults
 * the MACD rises monotonically for sixteen bars and the line starts at bar
 * **74** instead of the analytic **67**, seven bars later than the warm-up
 * alone accounts for (measured; the generator asserts that every such null
 * really is a flat second window). The usual port hides this by repeating
 * the previous reading, which reports a position in a range that has no
 * positions in it. If you need a value there, the honest fix is a longer
 * `cyclePeriod`, not a held one.
 *
 * ## Warm-up
 *
 * Length-preserving, and it stacks: the MACD lands at `slowPeriod − 1` (both
 * EMAs through the K2 door, so the slower governs), each stochastic pass
 * costs `cyclePeriod − 1` more, and each recursion costs nothing because it
 * seeds on its first input. So the column starts at
 * `slowPeriod + 2·cyclePeriod − 3` — bar **67** at the defaults, which is
 * why a chart needs a good deal more history than the 50-bar slow EMA
 * suggests. That is the **earliest** bar, not necessarily the first: a flat
 * second window pushes it later, as described above.
 *
 * ## Edges
 *
 * - **Scale- and shift-invariant.** A shift cancels in the MACD (a
 *   difference of two averages of the same column) and a scale cancels in
 *   the stochastic that follows. Pinned as property tests.
 * - **`fastPeriod` must be shorter than `slowPeriod`** — the same guard
 *   {@link macd} carries, and for the same reason: the difference of two
 *   EMAs with the fast one slower is the same study with its sign flipped,
 *   which is a mistake rather than a mode.
 * - **An interior gap** in `column` costs the bar plus every window and
 *   recursion run that reads it, and then recovers — the EMAs skip a missing
 *   cell and the folds re-seed. Nothing propagates to the end.
 */
export function schaffTrendCycle<
  S extends SeriesSchema,
  const Output extends string = 'stc',
>(series: TimeSeries<S>, options: SchaffTrendCycleOptions<S, Output> = {}) {
  const fastPeriod = options.fastPeriod ?? 23;
  const slowPeriod = options.slowPeriod ?? 50;
  const cyclePeriod = options.cyclePeriod ?? 10;
  assertPeriod(fastPeriod, 'fastPeriod');
  assertPeriod(slowPeriod, 'slowPeriod');
  assertPeriod(cyclePeriod, 'cyclePeriod');
  if (fastPeriod >= slowPeriod) {
    throw new TypeError(
      `schaffTrendCycle fastPeriod (${fastPeriod}) must be shorter than slowPeriod (${slowPeriod})`,
    );
  }

  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const output = (options.output ?? 'stc') as Output;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const fast = emaValues(wide, column, fastPeriod);
  const slow = emaValues(wide, column, slowPeriod);
  const length = fast.length;
  // A warming-up EMA is NaN and propagates through the subtraction on its
  // own ([PND-STUDYBOX]), so the MACD is defined exactly where both are.
  const macd = new Float64Array(length);
  for (let i = 0; i < length; i += 1) macd[i] = fast[i]! - slow[i]!;

  // Both stochastic passes run over a DERIVED array, so both take the strict
  // door: a "10-bar range of the MACD" computed from the two values that
  // exist during the MACD's own warm-up is not that range.
  const stochasticOf = (values: Float64Array): Float64Array => {
    const { highest, lowest } = rollingExtremesValues(values, cyclePeriod);
    return percentOfRangeValues(highest, lowest, values);
  };

  const [smoothedFirst] = foldRows(
    [stochasticOf(macd)],
    1,
    { previous: NaN },
    halfSmoothStep,
  );
  const [smoothedSecond] = foldRows(
    [stochasticOf(smoothedFirst!)],
    1,
    { previous: NaN },
    halfSmoothStep,
  );

  return series.withColumn(output, smoothedSecond!);
}
