import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import { foldRows } from '../kernels/fold.js';
import { assertNoColumn, columnValues } from '../kernels/rolling.js';

export interface ParabolicSarOptions<
  S extends SeriesSchema,
  Prefix extends string,
> {
  /** Acceleration increment — the step the factor grows by on each new
   *  extreme, and its starting value. **Default `0.02`.** */
  step?: number;
  /** Acceleration ceiling. **Default `0.2`.** */
  maxStep?: number;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Column-family prefix — appends `${prefix}` (the stop value) and
   *  `${prefix}Trend` (`+1` long / `−1` short). **Default `'psar'`.** */
  prefix?: Prefix;
}

/** The machine's carried state plus its two constants. Mutated in place by
 *  {@link sarStep}; allocated once per call. */
interface SarState {
  readonly step: number;
  readonly maxStep: number;
  /** `true` while the stop trails **below** price (a long position). */
  long: boolean;
  /** The stop to print on the next bar — computed one bar ahead, which is
   *  what "stop and reverse" means and what TA-Lib does. */
  sar: number;
  /** Extreme point: the highest high of the current up-leg (or the lowest
   *  low of the current down-leg). */
  ep: number;
  /** Acceleration factor. */
  af: number;
  /** The previous bar's high/low — the clamp reads yesterday as well as
   *  today, so the machine carries them rather than indexing backwards
   *  (which would read across a gap the fold has already reset over). */
  prevHigh: number;
  prevLow: number;
}

/**
 * One bar of Wilder's SAR, in TA-Lib's exact arrangement (`ta_SAR.c`).
 *
 * The order matters and is not the order the prose descriptions imply: the
 * value **printed** on bar `i` was computed at the end of bar `i−1`, and what
 * bar `i` computes is the value bar `i+1` prints. The clamp
 * (`min`/`max` against the last two bars' extremes) is applied both to a
 * reversal's override and to the ordinary advance.
 */
function sarStep(
  state: SarState,
  i: number,
  run: number,
  inputs: readonly Float64Array[],
  outputs: readonly Float64Array[],
): void {
  const high = inputs[0]!;
  const low = inputs[1]!;
  const sarOut = outputs[0]!;
  const trendOut = outputs[1]!;
  const newHigh = high[i]!;
  const newLow = low[i]!;

  if (run === 1) {
    // No predecessor: nothing to print, and nothing to seed from. The output
    // stays NaN and the next bar seeds. (The bar is still remembered — the
    // seed reads it.)
    state.prevHigh = newHigh;
    state.prevLow = newLow;
    return;
  }

  if (run === 2) {
    // The seed. TA-Lib picks the initial side from Wilder's −DM over the
    // one-bar move: if the low fell further than the high rose, the tape
    // opened short. Measured against `talib.SAR`, not recalled — the oracle
    // generator asserts this replication bar-for-bar.
    const diffPlus = newHigh - state.prevHigh;
    const diffMinus = state.prevLow - newLow;
    state.long = !(diffMinus > 0 && diffMinus > diffPlus);
    state.af = state.step;
    if (state.long) {
      state.ep = newHigh;
      state.sar = state.prevLow;
    } else {
      state.ep = newLow;
      state.sar = state.prevHigh;
    }
    // TA-Lib's "cheat" for the first printed bar: yesterday's extremes are
    // taken to be today's, so the clamp below has something to bite on.
    // Dropping it moves the first bar only, which is exactly the kind of
    // difference the oracle's mask-then-values assertion is there to catch.
    state.prevHigh = newHigh;
    state.prevLow = newLow;
  }

  const prevHigh = state.prevHigh;
  const prevLow = state.prevLow;

  if (state.long) {
    if (newLow <= state.sar) {
      // Reverse: the stop is overridden with the up-leg's extreme, clamped
      // so it cannot print inside the last two bars' ranges.
      state.long = false;
      let sar = state.ep;
      if (sar < prevHigh) sar = prevHigh;
      if (sar < newHigh) sar = newHigh;
      sarOut[i] = sar;
      state.af = state.step;
      state.ep = newLow;
      sar = sar + state.af * (state.ep - sar);
      if (sar < prevHigh) sar = prevHigh;
      if (sar < newHigh) sar = newHigh;
      state.sar = sar;
    } else {
      sarOut[i] = state.sar;
      if (newHigh > state.ep) {
        state.ep = newHigh;
        state.af = Math.min(state.af + state.step, state.maxStep);
      }
      let sar = state.sar + state.af * (state.ep - state.sar);
      if (sar > prevLow) sar = prevLow;
      if (sar > newLow) sar = newLow;
      state.sar = sar;
    }
  } else {
    if (newHigh >= state.sar) {
      state.long = true;
      let sar = state.ep;
      if (sar > prevLow) sar = prevLow;
      if (sar > newLow) sar = newLow;
      sarOut[i] = sar;
      state.af = state.step;
      state.ep = newHigh;
      sar = sar + state.af * (state.ep - sar);
      if (sar > prevLow) sar = prevLow;
      if (sar > newLow) sar = newLow;
      state.sar = sar;
    } else {
      sarOut[i] = state.sar;
      if (newLow < state.ep) {
        state.ep = newLow;
        state.af = Math.min(state.af + state.step, state.maxStep);
      }
      let sar = state.sar + state.af * (state.ep - state.sar);
      if (sar < prevHigh) sar = prevHigh;
      if (sar < newHigh) sar = newHigh;
      state.sar = sar;
    }
  }

  trendOut[i] = state.long ? 1 : -1;
  state.prevHigh = newHigh;
  state.prevLow = newLow;
}

/**
 * **Parabolic SAR** (J. Welles Wilder Jr., 1978) — the canonical multi-column
 * state machine, and kernel **K6**'s first consumer.
 *
 * A stop that trails the trend and accelerates towards it, reversing side
 * whenever price crosses it:
 *
 * ```
 * SAR[i+1] = SAR[i] + AF × (EP − SAR[i])
 * ```
 *
 * where `EP` is the extreme point of the current leg (the highest high while
 * long, the lowest low while short) and `AF` starts at `step`, grows by
 * `step` on each **new** extreme, and is capped at `maxStep`. When price
 * penetrates the stop the side reverses, `SAR` is overridden with the leg's
 * `EP`, and `AF` resets. The advance is clamped so a printed stop never sits
 * inside the last two bars' ranges.
 *
 * Reads **high and low only** — the close plays no part in Wilder's rule.
 *
 * ## Two columns, and why the value one is bare `${prefix}`
 *
 * - `${prefix}` — the stop value (`psar` at the default prefix).
 * - `${prefix}Trend` — `+1` while the stop trails below price, `−1` above.
 *
 * The side is the half consumers need and cannot reliably recover: a chart
 * draws the dot below the bar when long and above it when short, and
 * `psar < low` is **not** a safe derivation because the clamp can print a
 * stop exactly *on* an extreme (the advance is clamped to `min(sar, prevLow,
 * low)`, so equality is reachable, and then a strict comparison flips the
 * dot to the wrong side). So the flag is carried out of the machine rather
 * than re-derived from its output.
 *
 * Two columns means a `prefix`, per the studies README's uniform shape. The
 * value column is the **bare** prefix rather than `${prefix}Line`, which is a
 * departure from {@link macd}'s `${prefix}Line`/`Signal`/`Hist`, and
 * deliberate: MACD's three columns are three peers with no principal among
 * them, whereas Parabolic SAR *is* one number and the trend is an annotation
 * on it. Naming the value `psarLine` would make this the one study whose
 * principal output is not reachable under the name the caller passed, and
 * `psar()` would leave no column called `psar`. The precedent it follows is
 * the single-output studies' bare `output` ({@link atr}, {@link obv}), not
 * MACD's.
 *
 * ## Which SAR — TA-Lib, bar for bar
 *
 * TA-Lib's `SAR(high, low, acceleration, maximum)` is the reference and this
 * matches it **exactly** (`0.0` maximum absolute difference across
 * `(0.02, 0.2)`, `(0.05, 0.5)` and `(0.01, 0.1)` on the oracle's 80 bars,
 * asserted in the generator). Three details of Wilder's rule are ambiguous in
 * prose and are pinned to TA-Lib's reading:
 *
 * - **The initial side** comes from Wilder's `−DM` over the first one-bar
 *   move: short if the low fell further than the high rose, long otherwise
 *   (including on a tie and on an inside bar). Measured, not recalled.
 * - **The first printed bar** is bar 1, and TA-Lib treats "yesterday" as
 *   "today" for its clamp on that bar only.
 * - **The clamp uses the last two bars**, both on a reversal override and on
 *   an ordinary advance.
 *
 * `SAREXT` (which adds offset-on-reverse and independent long/short
 * acceleration parameters) is a different function with a different output
 * convention — it signs the short-side output negative — and is out of scope.
 *
 * ## Warm-up and gaps
 *
 * Length-preserving: bar 0 is `undefined` on both columns (the seed needs a
 * predecessor) and every bar from 1 on carries a value, matching TA-Lib's
 * look-back of 1.
 *
 * A missing `high` or `low` **resets the machine** ([PND-SFOLD]): that bar is
 * `undefined` on both columns, and the next two complete bars re-seed the
 * side from scratch. That is a deliberate delta from TA-Lib, which does not
 * check for `NaN` at all and whose comparisons against one are all false —
 * measured on an interior hole, TA-Lib emits a full column of numbers with
 * no gap in it. The reasoning for resetting is on `kernels/fold.ts`: a SAR
 * that did not see a bar cannot know whether it flipped, and a wrong side
 * never washes out.
 *
 * ## Edges
 *
 * - **In the units of the price**: scaling every bar by `k` scales the stop
 *   by `k` and leaves the trend unchanged; adding `c` shifts it by `c`. It
 *   does not normalise (contrast {@link rsi}).
 * - **`step` and `maxStep` are rates, not bar counts**, so they are validated
 *   as positive finite numbers rather than by `assertPeriod`. `maxStep` below
 *   `step` would cap the factor before its first increment; that is rejected.
 * - **A flat bar** (`high === low`) is ordinary input — the machine reads
 *   both, and neither a division nor a degenerate window arises anywhere in
 *   the rule, so there is no zero-denominator case to guard.
 */
export function parabolicSar<
  S extends SeriesSchema,
  const Prefix extends string = 'psar',
>(series: TimeSeries<S>, options: ParabolicSarOptions<S, Prefix> = {}) {
  const step = options.step ?? 0.02;
  const maxStep = options.maxStep ?? 0.2;
  if (!Number.isFinite(step) || step <= 0) {
    throw new TypeError('parabolicSar step must be a positive finite number');
  }
  if (!Number.isFinite(maxStep) || maxStep <= 0) {
    throw new TypeError(
      'parabolicSar maxStep must be a positive finite number',
    );
  }
  if (maxStep < step) {
    throw new TypeError(
      `parabolicSar maxStep (${maxStep}) must be at least step (${step})`,
    );
  }

  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const prefix = (options.prefix ?? 'psar') as Prefix;
  const sarName = prefix;
  const trendName = `${prefix}Trend` as const;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, sarName);
  assertNoColumn(wide, trendName);

  const state: SarState = {
    step,
    maxStep,
    long: true,
    sar: NaN,
    ep: NaN,
    af: step,
    prevHigh: NaN,
    prevLow: NaN,
  };
  const [sar, trend] = foldRows(
    [columnValues(wide, highName), columnValues(wide, lowName)],
    2,
    state,
    sarStep,
  );

  return series.withColumn(sarName, sar!).withColumn(trendName, trend!);
}
