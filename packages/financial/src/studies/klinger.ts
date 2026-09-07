import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import { foldRows } from '../kernels/fold.js';
import { movingAverageValues } from '../kernels/moving-average.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';

export interface KlingerOptions<S extends SeriesSchema, Prefix extends string> {
  /** Fast EMA span over the volume force, in **bars**. **Default `34`.** */
  fastPeriod?: number;
  /** Slow EMA span over the volume force, in **bars**. **Default `55`.** */
  slowPeriod?: number;
  /** Signal EMA span over the oscillator, in **bars**. **Default `13`.** */
  signalPeriod?: number;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Close column. **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Volume column. **Default `'volume'`.** */
  volume?: NumericColumnNameForSchema<S>;
  /** Column-family prefix — appends `${prefix}` (the oscillator) and
   *  `${prefix}Signal`. **Default `'kvo'`.** */
  prefix?: Prefix;
}

/** The volume-force machine's state: the trend flag, the cumulative
 *  measurement it accumulates into, and the previous bar's two derived
 *  quantities. Mutated in place. */
interface KlingerState {
  /** `+1` / `−1`, or `0` before the first comparison. */
  trend: number;
  /** Cumulative measurement — the running sum of `dm` over the current
   *  trend leg. */
  cm: number;
  /** The previous bar's `high + low + close`. */
  prevHlc: number;
  /** The previous bar's daily measurement `high − low`. */
  prevDm: number;
}

/**
 * One bar of Klinger's volume force.
 *
 * `trend` is the direction of the bar's **HLC sum**, `dm` is the bar's plain
 * range, and `cm` accumulates `dm` for as long as the trend holds — so `cm`
 * is "how much ground this leg has covered" and `dm/cm` is "how much of it
 * this bar covered".
 */
function klingerStep(
  state: KlingerState,
  i: number,
  run: number,
  inputs: readonly Float64Array[],
  outputs: readonly Float64Array[],
): void {
  const high = inputs[0]!;
  const low = inputs[1]!;
  const close = inputs[2]!;
  const volume = inputs[3]!;
  const out = outputs[0]!;

  const hlc = high[i]! + low[i]! + close[i]!;
  const dm = high[i]! - low[i]!;

  if (run === 1) {
    state.trend = 0;
    state.cm = 0;
    state.prevHlc = hlc;
    state.prevDm = dm;
    return;
  }

  const trend = hlc > state.prevHlc ? 1 : -1;
  // A trend CHANGE re-bases the cumulative measurement on the two bars that
  // straddle it (`dm[i−1] + dm[i]`), which is also the seed: on the first
  // comparable bar there is no prior trend, so the same branch runs.
  state.cm = trend === state.trend ? state.cm + dm : state.prevDm + dm;
  state.trend = trend;

  // No `cm === 0` guard, deliberately. `cm` is a sum of `dm = high - low`
  // terms, which are non-negative on a well-formed bar, so `cm === 0` forces
  // `dm === 0` with it and the ratio is a genuine `0/0` — `NaN` on its own,
  // which is the stochastic's flat-window answer rather than the
  // close-location bar's forced zero. A guard here would be dead code (the
  // mutation matrix could not kill one), and the studies README's rule is to
  // delete a dead guard rather than test it.
  out[i] = volume[i]! * Math.abs(2 * (dm / state.cm) - 2) * trend * 100;

  state.prevHlc = hlc;
  state.prevDm = dm;
}

/**
 * **Klinger Volume Oscillator** (Stephen Klinger, 1997) — the difference of
 * two EMAs of a *volume force* that carries its own trend state. Kernel
 * **K6** feeding the **K2** engine.
 *
 * ```
 * trend[i] = (high+low+close)[i] > (high+low+close)[i−1] ? +1 : −1
 * dm[i]    = high[i] − low[i]
 * cm[i]    = trend[i] === trend[i−1] ? cm[i−1] + dm[i] : dm[i−1] + dm[i]
 * vf[i]    = volume[i] × |2 × (dm[i]/cm[i] − 1)| × trend[i] × 100
 *
 * ${prefix}       = EMA(vf, fastPeriod) − EMA(vf, slowPeriod)
 * ${prefix}Signal = EMA(${prefix}, signalPeriod)
 * ```
 *
 * Appends **two** columns (`kvo` / `kvoSignal` at the default prefix). Reads
 * **four** — high, low, close and volume — each named by an option
 * defaulting to its `DEFAULT_OHLCV` name.
 *
 * ## Which Klinger — F-AMBIG, and the fork is named
 *
 * The corpus assessment flags this study `F-AMBIG` and it earns it: two
 * different formulas ship under the name.
 *
 * - **This study is the original**, as Klinger published it and as
 *   StockCharts documents it: the volume force above, with the trend state
 *   and the cumulative measurement. The `|2 × (dm/cm − 1)|` factor is the
 *   part every restatement garbles; the reading taken here is
 *   `2 × ((dm/cm) − 1)` under the absolute value, *not* `2 × (dm/cm) − 1`.
 *   The two differ by a constant inside the modulus and therefore by a
 *   different amount on every bar; the oracle carries a case that separates
 *   them.
 * - **The named alternative is TradingView's `ta.kvo`**, which drops the
 *   `dm/cm` factor entirely: its volume force is just `±volume × 100` on the
 *   sign of the HLC change, so it is an EMA-pair oscillator of signed volume
 *   and no state machine at all. It is not implemented here, and it is not
 *   an option on this study — the two are different indicators that share a
 *   name, and a `variant` knob would hide that. Measured on the oracle's 80
 *   bars, the two disagree by more than the oscillator's own range: the
 *   simplified form's `kvo` spans a different order of magnitude entirely
 *   (the generator prints both).
 *
 * The EMAs are **pond's** — first-sample seed, `α = 2/(span+1)`, the
 * {@link movingAverageValues} array door — matching {@link macd} and for the
 * same reason (making them TA-Lib-seeded here would make this study's EMAs
 * disagree with `ema()` inside the package). TA-Lib has no Klinger, so
 * nothing arbitrates it.
 *
 * ## Warm-up — per column
 *
 * The volume force starts at **bar 1** (it needs a predecessor for the trend
 * comparison and for `dm[i−1]`), so on gap-free input the oscillator starts
 * at bar `slowPeriod` and the signal at `slowPeriod + signalPeriod − 1`. Each
 * column is emitted where it is genuinely defined rather than both waiting
 * for the slower — the same per-column warm-up {@link macd} uses.
 *
 * ## Edges
 *
 * - **Scale behaviour is mixed, and the mixture is the point.** The volume
 *   force is `volume × (a ratio of ranges) × ±1 × 100`, so the study is
 *   **linear in volume** (double every volume and both columns double) but
 *   **invariant to a price scale *and* a price shift**: `dm/cm` is a ratio of
 *   ranges, so a scale cancels and a shift never reaches it, and the trend
 *   flag compares two `high + low + close` sums, which a positive scale and
 *   a `+3c` on both sides both preserve. Adding a constant to every *volume*
 *   is **not** a no-op, unlike {@link negativeVolumeIndex}, which only
 *   compares volumes — the force multiplies by one. All four are asserted as
 *   property tests.
 * - **A zero-range leg** (`cm = 0`, which needs every bar in the leg to have
 *   `high === low`) is a genuine `0/0` and reads `undefined` for that bar;
 *   the EMAs then propagate it, since a recursion has no state to carry
 *   across a hole. There is **no guard** for it: `cm` is a sum of
 *   non-negative ranges, so a zero `cm` forces a zero `dm` with it and the
 *   ratio is `NaN` on its own — a guard would be dead code.
 * - **A missing cell in any of the four inputs** resets the K6 machine
 *   ([PND-SFOLD]): the volume force is `undefined` for that bar and the
 *   trend/`cm` state re-seeds from the next complete pair. The EMAs
 *   downstream nonetheless carry the `undefined` forward — pond's EMA skips
 *   a missing bar in its recursion, so the oscillator resumes, but the two
 *   spans resume at different points, which is why an interior gap is best
 *   filled before running this.
 */
export function klinger<
  S extends SeriesSchema,
  const Prefix extends string = 'kvo',
>(series: TimeSeries<S>, options: KlingerOptions<S, Prefix> = {}) {
  const fastPeriod = options.fastPeriod ?? 34;
  const slowPeriod = options.slowPeriod ?? 55;
  const signalPeriod = options.signalPeriod ?? 13;
  assertPeriod(fastPeriod, 'fastPeriod');
  assertPeriod(slowPeriod, 'slowPeriod');
  assertPeriod(signalPeriod, 'signalPeriod');
  if (fastPeriod >= slowPeriod) {
    throw new TypeError(
      `klinger fastPeriod (${fastPeriod}) must be shorter than slowPeriod (${slowPeriod})`,
    );
  }

  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const closeName = (options.close ?? DEFAULT_OHLCV.close) as string;
  const volumeName = (options.volume ?? DEFAULT_OHLCV.volume) as string;
  const prefix = (options.prefix ?? 'kvo') as Prefix;
  const lineName = prefix;
  const signalName = `${prefix}Signal` as const;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, lineName);
  assertNoColumn(wide, signalName);

  const state: KlingerState = { trend: 0, cm: 0, prevHlc: NaN, prevDm: NaN };
  const [force] = foldRows(
    [
      columnValues(wide, highName),
      columnValues(wide, lowName),
      columnValues(wide, closeName),
      columnValues(wide, volumeName),
    ],
    1,
    state,
    klingerStep,
  );

  // The volume force is a DERIVED array, so both EMAs go through the K2
  // engine's array door rather than a scratch column — the `keltner`
  // precedent. Same recursion and same seed as `ema()`.
  const fast = movingAverageValues(force!, fastPeriod, 'ema');
  const slow = movingAverageValues(force!, slowPeriod, 'ema');
  const length = force!.length;
  const line = new Float64Array(length);
  for (let i = 0; i < length; i += 1) line[i] = fast[i]! - slow[i]!;
  const signal = movingAverageValues(line, signalPeriod, 'ema');

  return series.withColumn(lineName, line).withColumn(signalName, signal);
}
