import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import { foldRows } from '../kernels/fold.js';
import { assertNoColumn, columnValues } from '../kernels/rolling.js';

export interface ZigZagOptions<S extends SeriesSchema, Prefix extends string> {
  /** The reversal threshold, in **percent** of the leg's extreme.
   *  **Default `5`.** A leg ends when price retraces this far from its
   *  running extreme — see the study docstring for which side the percent is
   *  measured against. */
  deviation?: number;
  /** High column — the up-legs' extreme. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column — the down-legs' extreme. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Column-family prefix — appends `${prefix}Pivot`, `${prefix}Direction`
   *  and `${prefix}Line`. **Default `'zz'`.** */
  prefix?: Prefix;
}

/**
 * The machine's carried state plus the confirmed-pivot record the second
 * pass reads. Allocated once per call and mutated in place by
 * {@link zigZagStep}.
 *
 * While `dir === 0` (before the first pivot of a run is confirmed) the two
 * pairs hold the run's **running maximum high** (`ext*`) and **running
 * minimum low** (`ctr*`). Once a direction is in force, `ext*` is the current
 * leg's extreme — the highest high of a rising leg, the lowest low of a
 * falling one — and `ctr*` is the counter-extreme reached **after** it, which
 * is what the retracement is measured over. `ctrIdx < 0` means "not set on a
 * later bar yet".
 */
interface ZigZagState {
  /** `deviation / 100`, so the tests are one multiply. */
  readonly threshold: number;
  /** `0` while seeding, `+1` on a rising leg, `−1` on a falling one. */
  dir: number;
  extVal: number;
  extIdx: number;
  ctrVal: number;
  ctrIdx: number;
  /** Which run of consecutive complete rows the machine is in; a gap starts
   *  a new one, and no leg is ever drawn across the boundary. */
  segment: number;
  /** The last complete row of each segment, by segment id. */
  segmentEnd: number[];
  // The confirmed pivots, as parallel arrays (no object per pivot).
  pivotIdx: number[];
  pivotVal: number[];
  /** The direction of the leg **starting** at this pivot: `+1` from a low,
   *  `−1` from a high. */
  pivotDir: number[];
  pivotSeg: number[];
}

/** Record a confirmed pivot and write it to the pivot column. */
function confirm(
  state: ZigZagState,
  index: number,
  value: number,
  dirAfter: number,
  pivotOut: Float64Array,
): void {
  state.pivotIdx.push(index);
  state.pivotVal.push(value);
  state.pivotDir.push(dirAfter);
  state.pivotSeg.push(state.segment);
  pivotOut[index] = value;
}

/**
 * One bar of the ZigZag confirmation machine (kernel **K6**).
 *
 * Three states, and the whole rule:
 *
 * - **Seeding** (`dir === 0`): track the run's running high and running low.
 *   The first pivot is the **earlier** of the two, and it is confirmed the
 *   moment the later one is `deviation` percent away from it. If the two are
 *   the same bar nothing fires — a bar's own high and low have no order.
 * - **Rising** (`dir === +1`): a new high extends the leg (and discards any
 *   retracement so far); otherwise the lowest low **since** that high is the
 *   counter-extreme, and once it is `deviation` percent below the high, the
 *   high becomes a confirmed pivot and the leg turns.
 * - **Falling** (`dir === −1`): the mirror image.
 *
 * The pivot is written at the bar the **extreme occurred**, not the bar the
 * reversal confirmed it — that is what makes the column repaint, and it is
 * the whole point of the study (see the docstring).
 *
 * Two invariants worth stating, because they are what keep the pivots
 * strictly increasing and one-per-bar:
 *
 * 1. **At a confirmation, the counter-extreme is at the current bar.** The
 *    test's two operands only change on the bar one of them is updated, so
 *    if neither moved at bar `i` the test already ran — and failed — at bar
 *    `i − 1`.
 * 2. **A bar cannot reverse itself.** The counter-extreme is only ever taken
 *    from a bar *after* the extreme's, so a single wide bar never confirms a
 *    pivot on itself, and two pivots can never land on one row.
 */
function zigZagStep(
  state: ZigZagState,
  i: number,
  run: number,
  inputs: readonly Float64Array[],
  outputs: readonly Float64Array[],
): void {
  const high = inputs[0]!;
  const low = inputs[1]!;
  const pivotOut = outputs[0]!;
  const h = high[i]!;
  const l = low[i]!;

  if (run === 1) {
    // A fresh run: re-seed. The provisional leg in force before a gap is
    // DISCARDED — its extreme never saw the reversal that would confirm it
    // ([PND-SFOLD]).
    state.segment += 1;
    state.segmentEnd.push(i);
    state.dir = 0;
    state.extVal = h;
    state.extIdx = i;
    state.ctrVal = l;
    state.ctrIdx = i;
    return;
  }
  state.segmentEnd[state.segment] = i;

  const t = state.threshold;
  if (state.dir === 0) {
    if (h > state.extVal) {
      state.extVal = h;
      state.extIdx = i;
    }
    if (l < state.ctrVal) {
      state.ctrVal = l;
      state.ctrIdx = i;
    }
    const gap = state.extVal - state.ctrVal;
    if (state.extIdx > state.ctrIdx) {
      // The high came after the low: a rise from the trough, measured
      // against the trough.
      if (gap >= t * state.ctrVal) {
        confirm(state, state.ctrIdx, state.ctrVal, 1, pivotOut);
        state.dir = 1;
        // The leg's extreme is the running high already held in `ext*`.
        state.ctrIdx = -1;
      }
    } else if (state.ctrIdx > state.extIdx) {
      // The low came after the high: a fall from the peak, measured against
      // the peak.
      if (gap >= t * state.extVal) {
        confirm(state, state.extIdx, state.extVal, -1, pivotOut);
        state.dir = -1;
        state.extVal = state.ctrVal;
        state.extIdx = state.ctrIdx;
        state.ctrIdx = -1;
      }
    }
    return;
  }

  if (state.dir === 1) {
    if (h > state.extVal) {
      state.extVal = h;
      state.extIdx = i;
      state.ctrIdx = -1;
      return;
    }
    if (state.ctrIdx < 0 || l < state.ctrVal) {
      state.ctrVal = l;
      state.ctrIdx = i;
    }
    if (state.extVal - state.ctrVal >= t * state.extVal) {
      confirm(state, state.extIdx, state.extVal, -1, pivotOut);
      state.dir = -1;
      state.extVal = state.ctrVal;
      state.extIdx = state.ctrIdx;
      state.ctrIdx = -1;
    }
    return;
  }

  if (l < state.extVal) {
    state.extVal = l;
    state.extIdx = i;
    state.ctrIdx = -1;
    return;
  }
  if (state.ctrIdx < 0 || h > state.ctrVal) {
    state.ctrVal = h;
    state.ctrIdx = i;
  }
  if (state.ctrVal - state.extVal >= t * state.extVal) {
    confirm(state, state.extIdx, state.extVal, 1, pivotOut);
    state.dir = 1;
    state.extVal = state.ctrVal;
    state.extIdx = state.ctrIdx;
    state.ctrIdx = -1;
  }
}

/**
 * **ZigZag** — the price path reduced to its swings: every move smaller than
 * `deviation` percent is discarded, and what is left is a chain of alternating
 * high and low pivots. Appends three columns:
 *
 * ```
 * ${prefix}Pivot      the pivot price, on the pivot's OWN bar; undefined elsewhere
 * ${prefix}Direction  +1 on a rising leg, −1 on a falling one
 * ${prefix}Line       the straight line between consecutive pivots, per bar
 * ```
 *
 * A leg turns when price retraces `deviation` percent from the leg's running
 * extreme: from a peak the retracement is measured **against the peak**, and
 * from a trough the rise is measured **against the trough** (so a round trip
 * is not two equal absolute moves). Extremes come from `high` and `low` — the
 * standard — with the **close-based** variant one call away and needing no
 * option of its own, since the two column names are already knobs:
 *
 * ```ts
 * zigZag(bars, { high: 'close', low: 'close' })     // the close-based fork
 * zigZag(withSma, { high: 'sma', low: 'sma' })      // over another study
 * ```
 *
 * ## This study REPAINTS, and every column does
 *
 * A pivot is written at the bar its extreme occurred — but it is not *known*
 * until a later bar reverses far enough to confirm it. So the value at row
 * `i` can depend on bars after `i`, in all three columns:
 *
 * - `${prefix}Pivot[k]` appears only once some bar `i > k` confirms it.
 * - `${prefix}Line` between two pivots interpolates towards a pivot that is
 *   still in the future at every bar in between.
 * - `${prefix}Direction` names the leg a bar **belongs to**, which for the
 *   bars between a pivot and its confirmation is the opposite of what the
 *   machine believed at the time.
 *
 * That is not an implementation choice — it is what ZigZag *is*, and it is
 * the corpus assessment's gap **G6**. The columns are deliberately uniform
 * about it rather than mixing one causal column in among two that repaint:
 * **do not feed any of them to a backtest or a feature matrix without
 * lagging them**, and treat the whole study as a description of the past.
 * The causal reading a strategy can take is `${prefix}Direction`'s value at
 * the bar the direction *changes*, which is the confirmation bar.
 *
 * ## The last leg is absent, on purpose
 *
 * The leg in force at the last bar has no confirmed end, so it has **no
 * pivot and no line**: `${prefix}Pivot` and `${prefix}Line` stop at the last
 * confirmed pivot. No `${prefix}Provisional` column ships — it would be
 * `undefined` on every row but one, and the one number it holds is three
 * lines away from the data already on the series:
 *
 * ```ts
 * // The provisional extreme: the running high (or low) since the last pivot.
 * const dir = last(col(r, 'zzDirection'));
 * const from = lastIndexWhere(col(r, 'zzPivot'), (v) => v !== undefined);
 * const provisional = dir === 1 ? max(highs.slice(from)) : min(lows.slice(from));
 * ```
 *
 * `${prefix}Direction` **does** cover the provisional leg — its direction is
 * the one thing about it that is known — so the tail is not blank.
 *
 * ## Missing cells: a gap RESETS the machine, and ends the leg
 *
 * The [PND-SFOLD] rule, applied with the one addition ZigZag needs: a hole
 * in `high` or `low` resets the machine, **and the provisional leg in force
 * at the hole is discarded** — its extreme never saw the reversal that would
 * have confirmed it, and confirming it would be inventing one. No line is
 * drawn across the hole either: the interpolation runs between consecutive
 * pivots **of the same run**, so a gap ends one chain and the next complete
 * bar starts a fresh one, seeding from scratch.
 *
 * The alternative — *ending the study* at the first gap, as `wilderValues`
 * does — was considered and rejected. Wilder's seed is a `period`-bar mean,
 * so restarting it restates what the statistic means; ZigZag's seed is two
 * bars and a threshold, which is exactly what a chart does when a halted
 * instrument resumes. Resetting costs the one leg that spanned the hole and
 * nothing else.
 *
 * ## Warm-up and the line's geometry
 *
 * Length-preserving. All three columns are `undefined` before the run's
 * first confirmed pivot, which is data-dependent rather than a fixed bar
 * count — there is no `period` here. The interpolation is linear in **row
 * index**, not in time: `${prefix}Line` on an irregular series is straight in
 * bars, which is what a bar chart draws.
 *
 * ## Definition, verified
 *
 * TA-Lib has no ZigZag, and platforms disagree on the details, so the oracle
 * is a pandas **transcription** of this same machine (not an independent
 * derivation) whose value is the separations it measures. On the oracle's 80
 * bars at the default 5%: the **close-based** fork moves every pivot value
 * (99.15 / 108.64 / 97.81 against 100.00 / 108.04 / 98.65) and at 2% it
 * changes the pivot **set** as well (6 pivots against 4); the
 * **absolute-deviation** fork — reading `deviation` as a price move rather
 * than a percent — finds a **fourth** pivot at bar 69 that the percent rule
 * does not (at 2% the two agree, so the separation is deviation-dependent
 * and the generator asserts the one that bites).
 *
 * The two rules the fixture cannot separate are pinned by unit tests: a bar
 * whose own range exceeds the threshold (which confirms nothing), and a run
 * whose high and low extremes fall on the same bar (which seeds nothing).
 *
 * ## Edges
 *
 * - **Scale-equivariant, and NOT shift-invariant.** Multiplying every price
 *   by `k` multiplies `Pivot` and `Line` by `k` and leaves the pivot bars and
 *   `Direction` identical — `deviation` is a percent, so the threshold scales
 *   with it. **Adding** a constant does *not* leave the study alone: it
 *   changes what a percent is worth, and both the pivot set and the columns
 *   move. That is asserted as a real property, not waived.
 * - **`deviation` is a percent, not a bar count**, so it is validated as a
 *   positive finite number rather than by `assertPeriod`.
 * - **Prices are assumed positive**, as everywhere a percent is taken in this
 *   package. A non-positive extreme makes the threshold meaningless (a
 *   negative base inverts the comparison) rather than throwing — the same
 *   answer `percentChange` gives.
 * - **Cost** is O(N): one fold pass, then one pass over the pivots to fill
 *   the direction and the interpolated line. State is O(1) plus one entry per
 *   confirmed pivot.
 */
export function zigZag<
  S extends SeriesSchema,
  const Prefix extends string = 'zz',
>(series: TimeSeries<S>, options: ZigZagOptions<S, Prefix> = {}) {
  const deviation = options.deviation ?? 5;
  if (!Number.isFinite(deviation) || deviation <= 0) {
    throw new TypeError(
      'zigZag deviation must be a positive finite number (percent)',
    );
  }

  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const prefix = (options.prefix ?? 'zz') as Prefix;
  const pivotName = `${prefix}Pivot` as const;
  const directionName = `${prefix}Direction` as const;
  const lineName = `${prefix}Line` as const;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, pivotName);
  assertNoColumn(wide, directionName);
  assertNoColumn(wide, lineName);

  const state: ZigZagState = {
    threshold: deviation / 100,
    dir: 0,
    extVal: NaN,
    extIdx: -1,
    ctrVal: NaN,
    ctrIdx: -1,
    segment: -1,
    segmentEnd: [],
    pivotIdx: [],
    pivotVal: [],
    pivotDir: [],
    pivotSeg: [],
  };
  const [pivot, direction, line] = foldRows(
    [columnValues(wide, highName), columnValues(wide, lowName)],
    3,
    state,
    zigZagStep,
  );

  // The second pass: the leg structure is only knowable once the pivots are
  // all in, so `Direction` and `Line` are filled here rather than in the
  // fold. It walks the pivots, not the bars, and writes each leg's span once.
  const dirOut = direction!;
  const lineOut = line!;
  for (let k = 0; k < state.pivotIdx.length; k += 1) {
    const start = state.pivotIdx[k]!;
    const segment = state.pivotSeg[k]!;
    const hasNext =
      k + 1 < state.pivotIdx.length && state.pivotSeg[k + 1] === segment;
    // The leg runs to the next pivot of the SAME run, or — for the last,
    // provisional leg — to the last bar that run saw.
    const stop = hasNext ? state.pivotIdx[k + 1]! : state.segmentEnd[segment]!;
    const legDir = state.pivotDir[k]!;
    // Each leg owns [start, stop): the next pivot'''s bar belongs to the NEXT
    // leg, and writes it on the next iteration. (Writing it here too would
    // change nothing — iteration k+1 overwrites it — but half-open spans are
    // what make that true without having to check.)
    for (let j = start; j < stop; j += 1) dirOut[j] = legDir;
    if (hasNext) {
      const from = state.pivotVal[k]!;
      const to = state.pivotVal[k + 1]!;
      const span = stop - start;
      // The endpoints are written from the pivots themselves rather than
      // computed: the line AT a pivot must be that pivot exactly, and an
      // interpolation that merely lands within a rounding of it would be a
      // sharp edge for a consumer joining the two columns.
      lineOut[start] = from;
      lineOut[stop] = to;
      for (let j = start + 1; j < stop; j += 1) {
        lineOut[j] = from + ((to - from) * (j - start)) / span;
      }
    } else {
      // The provisional leg has a direction but no end, so no line — and
      // the final bar of the run still carries the direction.
      dirOut[stop] = legDir;
    }
  }

  return series
    .withColumn(pivotName, pivot!)
    .withColumn(directionName, dirOut)
    .withColumn(lineName, lineOut);
}
