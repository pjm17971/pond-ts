import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_SOURCE } from '../contract/columns.js';
import { foldRows } from '../kernels/fold.js';
import {
  assertMaType,
  movingAverageColumn,
} from '../kernels/moving-average.js';
import type { MaType } from '../kernels/moving-average.js';
import { assertNoColumn, assertPeriod } from '../kernels/rolling.js';

export interface MovingAverageCrossOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Fast average length in **bars**. **Default `10`.** */
  fastPeriod?: number;
  /** Slow average length in **bars**. **Default `30`.** */
  slowPeriod?: number;
  /** Which moving average the two lines are. **Default `'sma'`**; any
   *  {@link MaType}. */
  maType?: MaType;
  /** Source column. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'maCross'`.** */
  output?: Output;
}

/** The carried state of one cross machine: the last sign of `fast − slow`
 *  that was not zero, or `0` before one has been seen. Mutated in place. */
interface CrossState {
  lastSign: number;
}

/** One bar of the cross machine. */
function crossStep(
  state: CrossState,
  i: number,
  run: number,
  inputs: readonly Float64Array[],
  outputs: readonly Float64Array[],
): void {
  const fast = inputs[0]!;
  const slow = inputs[1]!;
  const out = outputs[0]!;
  const difference = fast[i]! - slow[i]!;
  const sign = difference > 0 ? 1 : difference < 0 ? -1 : 0;

  if (run === 1) {
    // The first bar of a run has nothing to have crossed FROM, so it reports
    // no event — the output keeps the `NaN` the kernel filled — and only
    // seeds the side.
    state.lastSign = sign;
    return;
  }
  if (sign === 0) {
    // Exactly equal is not a cross, and it does not clear the side either:
    // the machine remembers which side it was on, so a touch that retreats
    // is not reported when the original side comes back (see the docstring).
    out[i] = 0;
    return;
  }
  if (state.lastSign === 0) {
    // The run has not been on a side yet (its seed bar was exactly equal),
    // so arriving on one is not a crossing FROM anywhere.
    state.lastSign = sign;
    out[i] = 0;
    return;
  }
  if (sign !== state.lastSign) {
    state.lastSign = sign;
    out[i] = sign;
    return;
  }
  out[i] = 0;
}

/**
 * **Moving Average Cross** — a **signal column**: `+1` on the bar a fast
 * moving average crosses **above** a slow one, `−1` on the bar it crosses
 * **below**, and `0` on every other bar.
 *
 * ```
 * ${output}[i] = +1   if fast is above slow now and was below the last time they differed
 *              = −1   if fast is below slow now and was above the last time they differed
 *              =  0   otherwise
 * ```
 *
 * Appends **one** column. The two averages themselves are
 * {@link movingAverage}'s business and are **not** emitted here — a caller
 * who wants to chart them calls that study twice with different `output`
 * names, and gets exactly the same numbers because this study calls the same
 * engine. What this adds is the **event**, which is the thing a rule reacts
 * to and the thing that is easy to get wrong.
 *
 * ## An event column, not a regime column
 *
 * The value is non-zero on the **crossing bar only**. The other reading — the
 * *regime*, `+1` for as long as the fast average is above — is one
 * expression away from the two average columns
 * (`Math.sign(fast − slow)`) and needs no study; this one is not, because it
 * requires memory of which side the pair was last on. That is the K6 shape,
 * so the machine is a {@link foldRows} step ([PND-SFOLD]) rather than a loop
 * of its own.
 *
 * ## The tie rule, which is the whole design
 *
 * Two averages can be **exactly equal** on a bar — not a float coincidence
 * but a routine event when the source is a stepped price or the two periods
 * overlap heavily — and the naive rule (`sign(d[i]) !== sign(d[i−1])`) gets
 * both of the cases that follow wrong:
 *
 * - **Equal on the bar is not a cross.** The bar reports `0`. A naive rule
 *   fires on the way *into* the tie and again on the way out, reporting two
 *   crossings for one crossing.
 * - **A touch that retreats is not a cross.** `below → equal → below` is one
 *   continuous regime, and this reports `0` throughout, because the machine
 *   carries **the last sign that was not zero** rather than the last sign.
 *   A naive rule reports a `−1` on the way out of the tie: a crossing back
 *   to a side it never left.
 * - **A crossing THROUGH equality is a cross**, reported on the bar the pair
 *   arrives on the far side (`below → equal → above` fires `+1` at the third
 *   bar). That is the same bar a reader watching the chart would call it.
 *
 * ## `maType`, not `type`
 *
 * The package's split is that a study whose **columns are the averages**
 * takes `type` ({@link movingAverage}, {@link guppy}, {@link rainbow}) and a
 * study with an average **inside** it takes `maType` ({@link keltner},
 * {@link movingAverageDeviation}). This study's column is a signal, not an
 * average — the averages are internal and never returned — so it is
 * `maType`. Both lines use the same type deliberately: a fast EMA against a
 * slow SMA is a different (and rarer) study, and offering two type options
 * would be a knob with no conventional setting.
 *
 * ## Warm-up, and what a gap does
 *
 * The first bar on which **both** averages exist reports **nothing**
 * (`undefined`), not `0`: it is the seed, and there is no earlier relation
 * for it to have crossed from. The first bar that can carry a signal is the
 * one after — bar 30 at the defaults, since the 30-bar average first prints
 * at bar 29.
 *
 * A missing cell **resets the machine** ([PND-SFOLD]): the incomplete bar is
 * `undefined`, and the next complete bar becomes a fresh seed reporting
 * nothing. That is the honest answer rather than a convenient one — a
 * machine that did not see a bar cannot know whether the pair crossed on it,
 * and reporting a crossing on the far side of a hole would date the event to
 * a bar it did not happen on.
 *
 * **What a gap actually costs depends entirely on `maType`**, because the
 * averages decide whether the machine ever sees an incomplete row, and the
 * K2 engine's two doors disagree by design. Measured on a 40-bar series with
 * bar 30's close removed, at `fastPeriod 3` / `slowPeriod 6`:
 *
 * | `maType` | what the column does |
 * | --- | --- |
 * | `sma` | **nothing at all** — the column door counts *rows*, so both averages skip the missing cell and stay defined; the reset never fires |
 * | `ema` `dema` `tema` | bars 30–31 `undefined` (the gap bar, then the fresh seed), back at 32 |
 * | `zlema` | bars 30–33 `undefined` — its lag term reads a bar the hole removed, so the seed lands two bars later than `ema`'s; back at 34 |
 * | `wma` `trima` | bars 30–36 `undefined` — the array door waits for `period` finite *values*, so the slow average is blank for a whole window; back at 37 |
 * | `hull` | bars 30–37 `undefined` — one bar longer than `wma`: the final √period smoothing waits on the two rebuilt WMAs; back at 38 |
 * | `smma` `kama` | `undefined` **to the end** — Wilder's recursion has no state to carry across a hole, so the inputs never complete again |
 *
 * (Every one of the ten `MaType`s, re-measured at integration on the same
 * 40-bar setup — a Layer-2 review caught `hull` a bar short and four types
 * missing from the first draft of this table.)
 *
 * The first row is the sharp edge worth naming: at the default `sma` this
 * study has **no gap behaviour of its own**, and a caller who needs the hole
 * respected should say so with the `maType` they choose (or fill first).
 * That asymmetry is the K2 engine's, documented on `movingAverageValues`,
 * and is inherited here rather than re-decided.
 *
 * ## Edges
 *
 * - **`fastPeriod` must be shorter than `slowPeriod`**, and an inverted pair
 *   is rejected rather than quietly negated. This is {@link macd}'s rule and
 *   for the same reason: the **sign** is the entire content of the reading,
 *   so a caller who swapped the two would get every signal backwards with
 *   nothing to tell them.
 * - **Invariant under any positive scale and any shift of the price.** Every
 *   type in the {@link MaType} menu is affine-equivariant, and only the sign
 *   of `fast − slow` is read, so neither transform can move a signal. Pinned
 *   as property tests. A *negative* scale swaps the two lines and is not
 *   claimed.
 * - **Only three values ever appear** (`−1`, `0`, `+1`), and there is no
 *   division anywhere, so no zero-denominator case.
 */
export function movingAverageCross<
  S extends SeriesSchema,
  const Output extends string = 'maCross',
>(series: TimeSeries<S>, options: MovingAverageCrossOptions<S, Output> = {}) {
  const fastPeriod = options.fastPeriod ?? 10;
  const slowPeriod = options.slowPeriod ?? 30;
  assertPeriod(fastPeriod, 'fastPeriod');
  assertPeriod(slowPeriod, 'slowPeriod');
  if (fastPeriod >= slowPeriod) {
    throw new TypeError(
      `movingAverageCross fastPeriod (${fastPeriod}) must be shorter than slowPeriod (${slowPeriod})`,
    );
  }
  const maType = options.maType ?? 'sma';
  assertMaType(maType);

  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const output = (options.output ?? 'maCross') as Output;
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const [values] = foldRows(
    [
      movingAverageColumn(wide, column, fastPeriod, maType),
      movingAverageColumn(wide, column, slowPeriod, maType),
    ],
    1,
    { lastSign: 0 },
    crossStep,
  );
  return series.withColumn(output, values!);
}
