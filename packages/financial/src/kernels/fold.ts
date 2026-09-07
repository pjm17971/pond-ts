/**
 * **The stateful row fold** — kernel **K6** of the corpus assessment
 * (`docs/notes/financial-indicators-assessment-2026-07.md` §4, gap **G2**).
 *
 * A per-bar fold with carried state over several row-aligned columns. It is
 * the one shape none of the other kernels can express: `rollingValues` and
 * friends compute cell `i` from a bounded window, `wilderValues` and
 * `emaValues` carry state but read **one** column, and core's `scan` is
 * likewise single-column (`time-series.ts`, verified). Parabolic SAR's step
 * reads `high` and `low` and carries `{sar, ep, af, long}`; SuperTrend reads
 * `high`, `low`, `close` and an ATR and carries two ratcheted bands and a
 * side. Neither is a window and neither is one column.
 *
 * ## The contract
 *
 * The kernel owns exactly two things — **the loop** and **the gap rule** —
 * and the study owns the arithmetic:
 *
 * > For each row, if **every** input cell is finite, call
 * > `step(state, i, run)` where `run` is how many *consecutive* complete rows
 * > end at `i` (so `run === 1` is the first bar of a fresh run). If any input
 * > cell is missing, write nothing (the outputs stay `NaN`) and reset `run`
 * > to 0.
 *
 * That is the whole interface, and it is deliberately smaller than the
 * `init` / `seed` / `step` triple the first draft had. `run` **is** the
 * seeded flag: a machine that needs one bar to seed branches on `run === 1`,
 * one that needs a predecessor (SAR reads bar `i−1`) branches on `run === 2`
 * and lets `run === 1` fall through to the `NaN` the outputs already hold.
 * A separate seed hook would have carried the same information in a second
 * place.
 *
 * Outputs are allocated here, pre-filled with `NaN`, and handed back. That is
 * not a convenience: **the warm-up and the gap rule are the same fill**, and
 * a study that allocated its own buffers could silently lose both by
 * forgetting it (`new Float64Array(n)` is zero-filled, and a zero SAR is a
 * price, not a gap).
 *
 * ## The gap rule: a missing cell RESETS the machine
 *
 * [PND-STUDYBOX] marks a missing cell `NaN`. A rolling mean can skip such a
 * bar and a window kernel recovers once it leaves the window, but a state
 * machine has no such luxury, and there are only two honest answers:
 *
 * - **(a) reset** — emit `NaN` for the incomplete bar and re-seed from the
 *   next complete one; or
 * - **(b) hold** — emit `NaN` for that bar but carry the state across it.
 *
 * **This kernel resets, and (b) is wrong for every machine that has asked for
 * it.** Ask what the *true* answer is: a Parabolic SAR that did not see a bar
 * cannot know whether it flipped. The missing bar might have printed a new
 * extreme (which would have advanced the acceleration factor), or penetrated
 * the SAR (which would have reversed the side and reset the factor), or
 * neither — and nothing in the surrounding bars distinguishes those. Holding
 * would resume with a side, an extreme point and an acceleration factor that
 * are **not** what the definition says they are, and because the machine is a
 * recursion the error never washes out: a SAR carried on the wrong side stays
 * on the wrong side until the next genuine reversal, which may be a hundred
 * bars away. Resetting throws away real information (the state before the
 * gap), but everything it then emits is exactly what the definition says,
 * computed from bars the machine actually saw. Option (b) trades a visible
 * absence for an invisible lie, which is the trade this package never takes
 * (see the flat-window rule in the studies README).
 *
 * The same argument covers the volume machines: an NVI that missed a bar
 * cannot know whether volume fell, so it cannot know whether to compound that
 * bar's return; and it cannot know the return either, because the return is
 * measured against a close it did not see.
 *
 * **Contrast `wilderValues`, which propagates to the end instead**, and the
 * difference is the cost of a seed. Wilder's seed is the mean of `period`
 * bars, so restarting it mid-series would silently restate what "a 14-bar
 * average" means at that point; there is no cheap honest restart, so it
 * stops. A K6 machine's seed is one or two bars, which is exactly what a
 * chart does when a halted instrument resumes — so restarting is both cheap
 * and the conventional answer. The asymmetry is stated, not reconciled.
 *
 * Note that a study whose inputs include a Wilder-smoothed array inherits
 * Wilder's rule through the front door: `superTrend` reads `atrValues`, so an
 * interior gap in `high`/`low`/`close` leaves the ATR `NaN` **to the end**,
 * every later row is therefore incomplete, and the reset never gets a chance
 * to fire. The reset is visible for the machines that read raw columns —
 * `parabolicSar`, `negativeVolumeIndex`, `positiveVolumeIndex` — and for a
 * gap that lands in a column the ATR does not read.
 *
 * ## Cost
 *
 * O(N·k) for `k` input columns: one finite test per cell, one call per
 * complete row, no allocation on the hot path (the state object is the
 * caller's and is mutated in place). Measured at 1M rows on the package's
 * bench, a two-column fold over a no-op step is within a small multiple of
 * `ema()`'s 2.5 ms — see `scripts/perf-studies.mjs`, whose
 * `foldRows(2 cols) [bare kernel]` entry exists to keep it that way.
 */

/**
 * One bar of a K6 machine.
 *
 * @param state  The machine's own state, allocated once by the study and
 *   **mutated in place** — carried fields plus whatever configuration the
 *   step needs (`multiplier`, the acceleration step, …), so the step can be a
 *   top-level function with no closure to allocate.
 * @param index  The row to read and write.
 * @param run    How many consecutive complete rows end at `index`; `1` on the
 *   first bar of a fresh run (the series start, or the bar after a gap).
 * @param inputs The row-aligned input columns, in the order given to
 *   {@link foldRows}. No cell at `index` is `NaN` — an infinite one still
 *   arrives, since only `NaN` marks a gap ([PND-STUDYBOX]).
 * @param outputs The row-aligned output columns, pre-filled with `NaN`. A
 *   step that writes nothing leaves the warm-up in place.
 */
export type FoldStep<State> = (
  state: State,
  index: number,
  run: number,
  inputs: readonly Float64Array[],
  outputs: readonly Float64Array[],
) => void;

/**
 * Run a K6 {@link FoldStep} over row-aligned columns and return
 * `outputCount` row-aligned output arrays.
 *
 * @throws TypeError if the inputs are empty or not all the same length, or if
 *   `outputCount` is not a positive integer — all three are programming
 *   errors in a study rather than data conditions.
 */
export function foldRows<State>(
  inputs: readonly Float64Array[],
  outputCount: number,
  state: State,
  step: FoldStep<State>,
): Float64Array[] {
  if (inputs.length === 0) {
    throw new TypeError('foldRows needs at least one input column');
  }
  if (!Number.isInteger(outputCount) || outputCount < 1) {
    throw new TypeError('foldRows outputCount must be a positive integer');
  }
  const length = inputs[0]!.length;
  for (const input of inputs) {
    if (input.length !== length) {
      throw new TypeError('foldRows input columns must be the same length');
    }
  }

  const outputs: Float64Array[] = [];
  for (let o = 0; o < outputCount; o += 1) {
    // Filled here rather than in the study: this single fill IS both the
    // length-preserving warm-up and the gap rule (see the header).
    outputs.push(new Float64Array(length).fill(NaN));
  }

  const columns = inputs.length;
  let run = 0;
  for (let i = 0; i < length; i += 1) {
    let complete = true;
    for (let c = 0; c < columns; c += 1) {
      const v = inputs[c]![i]!;
      // `v !== v` is the NaN test without a call; a missing cell is NaN
      // ([PND-STUDYBOX]) and an infinite one is a real (if silly) number the
      // machine is free to act on, exactly as the arithmetic kernels do.
      if (v !== v) {
        complete = false;
        break;
      }
    }
    if (!complete) {
      run = 0;
      continue;
    }
    run += 1;
    step(state, i, run, inputs, outputs);
  }
  return outputs;
}
