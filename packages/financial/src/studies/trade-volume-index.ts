import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV, DEFAULT_SOURCE } from '../contract/columns.js';
import { foldRows } from '../kernels/fold.js';
import { assertNoColumn, columnValues } from '../kernels/rolling.js';

export interface TradeVolumeIndexOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Price column the tick direction is read from. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Volume column. **Default `'volume'`.** */
  volume?: NumericColumnNameForSchema<S>;
  /** **Required.** The instrument's minimum tick — how far the price must
   *  move for the trade to count as a new direction, in the price's own
   *  units. There is no default; see the study docstring for why. Must be
   *  positive and finite. */
  minTick: number;
  /** Name of the appended column. **Default `'tvi'`.** */
  output?: Output;
}

/** The carried state of one Trade Volume Index. Mutated in place. */
interface TradeVolumeIndexState {
  readonly minTick: number;
  /** `true` once a run has started, so a LATER `run === 1` is an interior
   *  gap rather than the first bar of the series (see the gap rule). */
  started: boolean;
  /** The running level. `NaN` once an interior gap has killed it — the
   *  arithmetic then carries that forward on its own. */
  value: number;
  /** `+1`, `−1`, or `0` for "no direction established yet". */
  direction: number;
  prevClose: number;
}

/** One bar of the tick-direction accumulation. */
function tradeVolumeIndexStep(
  state: TradeVolumeIndexState,
  i: number,
  run: number,
  inputs: readonly Float64Array[],
  outputs: readonly Float64Array[],
): void {
  const close = inputs[0]!;
  const volume = inputs[1]!;
  const out = outputs[0]!;
  const c = close[i]!;

  if (run === 1) {
    // A LEADING run of gaps only shifts the start; an INTERIOR one ends the
    // index, because the level is a volume total and the missed bar's volume
    // is unknown. That is `cumulativeValues`' asymmetry, reproduced inside
    // the fold — see the docstring for why this machine does not re-seed the
    // way `negativeVolumeIndex` does.
    state.value = state.started ? NaN : 0;
    state.started = true;
    state.direction = 0;
  } else {
    const move = c - state.prevClose;
    // A move of exactly `minTick` is NOT decisive: the comparison is strict
    // on both sides, so the dead band is closed and the direction persists
    // across it.
    if (move > state.minTick) state.direction = 1;
    else if (move < -state.minTick) state.direction = -1;
    // else: hold the previous direction (which is 0 until the first
    // decisive move, so the level does not move either).
    state.value += state.direction * volume[i]!;
  }

  out[i] = state.value;
  state.prevClose = c;
}

/**
 * **Trade Volume Index** — a cumulative volume level driven by the **tick
 * direction**, where an *undecided* bar keeps trading in the last decided
 * direction rather than counting for nothing:
 *
 * ```
 * direction[i] = +1                if close[i] − close[i−1] >  minTick
 *              = −1                if close[i] − close[i−1] < −minTick
 *              = direction[i−1]    otherwise            ← the whole study
 *
 * ${output}[0] = 0
 * ${output}[i] = ${output}[i−1] + direction[i] · volume[i]
 * ```
 *
 * Appends one column, defined from **bar 0** — like {@link obv} there is no
 * `period` and no warm-up, because there is no window to size.
 *
 * ## What it is for, and how it differs from OBV
 *
 * {@link obv} reads the same tape and makes the opposite call on the bars
 * that did not move: it adds nothing for an unchanged close, which treats a
 * quiet bar as though no trading had a side. The Trade Volume Index is built
 * on the tick rule an exchange tape actually uses — a trade at the same
 * price as the last one is classified with the last *directed* trade — so a
 * run of unchanged bars accumulates in whichever direction the tape was last
 * going. On an instrument that trades in small ticks against a large price
 * that is most of the volume, which is the whole reason the study exists.
 *
 * The second difference is the **dead band**. OBV compares closes exactly;
 * this compares them against `minTick`, so a one-tick wobble inside the
 * spread does not flip the direction. Set `minTick` to the instrument's own
 * tick and the study reads what the tape did; set it larger and it becomes a
 * noise filter.
 *
 * ## `minTick` is REQUIRED, and that is the decision
 *
 * It is a fact about the **instrument** — the exchange's minimum price
 * increment — not about the study, exactly as {@link swingIndex}'s `limit`
 * is, and it is the third required option in the package after that and
 * `benchmark`. Every candidate default is wrong in a way that does not
 * announce itself:
 *
 * - **`0`** collapses the dead band and makes the direction change on any
 *   move at all, so the persistence rule — the only thing that distinguishes
 *   this study from OBV with a carried sign — never fires on real (float)
 *   prices.
 * - **A fixed `0.01`** is right for a US equity, wrong by two orders of
 *   magnitude for a JPY cross and by six for a crypto pair, and the reading
 *   it produces is a *number*, not an error.
 *
 * So the caller names it. A caller who wants no dead band at all passes a
 * value below the smallest move their data can make and gets exactly that.
 *
 * ## The seed: `0`, and no invented first direction
 *
 * Bar 0 has no previous close, so it has no direction; the level starts at
 * `0` and the first bar contributes nothing. Bars before the first
 * **decisive** move contribute nothing either — the direction is `0` until
 * one happens — rather than being scored as up-volume. Several vendor ports
 * seed the direction to `+1` instead. That is a real fork, and it is pinned
 * by a **unit test** rather than by the oracle: the oracle input's bar 1
 * carries the largest close-to-close move in the whole series (1.412), so it
 * is decisive at any `minTick` worth testing and an up-seeded build would
 * agree with this one bar for bar there — the generator asserts that
 * agreement, so the day the input changes the omission surfaces. On closes
 * `100, 100.2, 100.3, 101.5` at `minTick 0.5` with volumes `10, 20, 30, 40`,
 * this study reads `0, 0, 0, 40` and the up-seeded fork reads
 * `0, 20, 50, 90`.
 *
 * The level is a total of volume, so `0` is a base and only *differences*
 * along the line are the reading — the same thing OBV's arbitrary seed
 * means. There is deliberately no `start` option: unlike
 * {@link negativeVolumeIndex}, which is multiplicative and where the base
 * scales the whole line, this one is additive, so a base is a constant
 * offset and adding a knob for it would be adding a knob for `+ k`.
 *
 * ## The gap rule: an interior gap ENDS the index
 *
 * This is a K6 machine ({@link foldRows}), and the kernel's rule is that a
 * missing cell resets it. This machine resets its *direction* — it must; a
 * bar it did not see cannot tell it which way the tape went — but it does
 * **not** re-seed the level. It marks it `NaN`, so the index ends there.
 *
 * That is {@link obv}'s rule and {@link cumulativeValues}', and the reason is
 * what the level *means*: it is a total of volume, a quantity with units,
 * where the distance between two points on the line is the whole reading. A
 * hole makes every later level wrong by whatever the missing bar traded, and
 * a re-based line silently claims a distance that was never travelled.
 * Contrast {@link negativeVolumeIndex}, which re-seeds after a gap and is
 * right to: it accumulates *returns* from an arbitrary base, so re-basing
 * loses only the base. A **leading** run of gaps, by the same argument, only
 * shifts the start — the level begins at `0` on the first complete bar.
 *
 * ## Edges
 *
 * - **Equivariant in volume, invariant in a price shift, and invariant in a
 *   price scale only if `minTick` scales too.** The true statement has to
 *   name the parameter, as `swingIndex`'s does: doubling every volume
 *   doubles the line (it is a sum of volumes); adding a constant to every
 *   price leaves every difference alone and so changes nothing; multiplying
 *   every price by `k` moves the differences against a **fixed** dead band
 *   and can change the classification of a bar, so invariance needs
 *   `minTick · k`. All three are pinned as property tests.
 * - **A zero volume bar** moves nothing, whichever way it was ticking. There
 *   is no division anywhere in the study, so there is no zero-denominator
 *   case to guard.
 * - **A `minTick` of `0`, negative, or non-finite is rejected**, the same
 *   validation `swingIndex` applies to `limit`.
 */
export function tradeVolumeIndex<
  S extends SeriesSchema,
  const Output extends string = 'tvi',
>(series: TimeSeries<S>, options: TradeVolumeIndexOptions<S, Output>) {
  const minTick = options.minTick;
  if (!Number.isFinite(minTick) || minTick <= 0) {
    throw new TypeError(
      `tradeVolumeIndex minTick must be a positive finite number (the instrument's minimum tick); got ${String(minTick)}`,
    );
  }
  const columnName = (options.column ?? DEFAULT_SOURCE) as string;
  const volumeName = (options.volume ?? DEFAULT_OHLCV.volume) as string;
  const output = (options.output ?? 'tvi') as Output;
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const state: TradeVolumeIndexState = {
    minTick,
    started: false,
    value: NaN,
    direction: 0,
    prevClose: NaN,
  };
  const [values] = foldRows(
    [columnValues(wide, columnName), columnValues(wide, volumeName)],
    1,
    state,
    tradeVolumeIndexStep,
  );
  return series.withColumn(output, values!);
}
