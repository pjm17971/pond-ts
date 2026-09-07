import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV, DEFAULT_SOURCE } from '../contract/columns.js';
import { foldRows } from '../kernels/fold.js';
import { assertNoColumn, columnValues } from '../kernels/rolling.js';

export interface VolumeIndexOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Price column the return is taken over. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Volume column. **Default `'volume'`.** */
  volume?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'nvi'` / `'pvi'`.** */
  output?: Output;
  /** The index's starting level. **Default `1000`** (Fosback's). */
  start?: number;
}

/** One number, plus the previous bar's inputs and the direction this index
 *  compounds on. Mutated in place. */
interface VolumeIndexState {
  readonly start: number;
  /** `true` for NVI (compound when volume FELL), `false` for PVI. */
  readonly onFall: boolean;
  value: number;
  prevClose: number;
  prevVolume: number;
}

/**
 * One bar of Fosback's conditional cumulative index — the smallest K6
 * machine in the package (state is one number plus the previous row).
 */
function volumeIndexStep(
  state: VolumeIndexState,
  i: number,
  run: number,
  inputs: readonly Float64Array[],
  outputs: readonly Float64Array[],
): void {
  const close = inputs[0]!;
  const volume = inputs[1]!;
  const out = outputs[0]!;
  const c = close[i]!;
  const v = volume[i]!;

  if (run === 1) {
    state.value = state.start;
  } else {
    const fell = v < state.prevVolume;
    if (state.onFall === fell && v !== state.prevVolume) {
      // The division is the OUTPUT here, so the zero-base guard is live: a
      // zero previous close makes the return infinite, and an index that
      // cannot be continued is `undefined` from here on rather than a
      // number. `NaN` carries itself forward through both branches, so the
      // machine is dead without a second flag.
      state.value =
        state.prevClose === 0
          ? NaN
          : state.value * (1 + (c - state.prevClose) / state.prevClose);
    }
    // else: hold. An unchanged volume holds on BOTH indices (Fosback's
    // rule is a strict comparison in each direction), which is why the
    // `v !== state.prevVolume` term is there and not implied by `fell`.
  }

  out[i] = state.value;
  state.prevClose = c;
  state.prevVolume = v;
}

/**
 * The shared body of {@link negativeVolumeIndex} and
 * {@link positiveVolumeIndex} — the two differ only in which direction of
 * volume change compounds the return.
 */
function volumeIndex(
  series: TimeSeries<SeriesSchema>,
  options: VolumeIndexOptions<SeriesSchema, string>,
  onFall: boolean,
  label: string,
): Float64Array {
  const start = options.start ?? 1000;
  if (!Number.isFinite(start) || start <= 0) {
    throw new TypeError(`${label} start must be a positive finite number`);
  }
  const columnName = (options.column ?? DEFAULT_SOURCE) as string;
  const volumeName = (options.volume ?? DEFAULT_OHLCV.volume) as string;

  const state: VolumeIndexState = {
    start,
    onFall,
    value: NaN,
    prevClose: NaN,
    prevVolume: NaN,
  };
  const [values] = foldRows(
    [columnValues(series, columnName), columnValues(series, volumeName)],
    1,
    state,
    volumeIndexStep,
  );
  return values!;
}

/**
 * **Negative Volume Index** (Paul Dysart, popularised by Norman Fosback,
 * _Stock Market Logic_ 1976) — a cumulative index that compounds a bar's
 * return **only when that bar's volume was lower than the previous bar's**.
 *
 * ```
 * NVI[0] = start                                   (1000 by convention)
 * NVI[i] = volume[i] < volume[i−1]
 *          ? NVI[i−1] × (1 + (close[i] − close[i−1]) / close[i−1])
 *          : NVI[i−1]
 * ```
 *
 * The premise is Dysart's: the "smart money" trades on quiet days and the
 * crowd on busy ones, so the line built from quiet days alone is read as the
 * informed side of the tape. {@link positiveVolumeIndex} is the same
 * construction on the busy days.
 *
 * Appends **one** column (`nvi` by default). Like {@link obv} there is **no
 * `period`** — there is no window to size — and no warm-up: bar 0 carries
 * `start`.
 *
 * ## Conventions pinned
 *
 * - **A flat volume holds.** `volume[i] === volume[i−1]` compounds on
 *   *neither* index. Fosback's rule is a strict comparison on each side, so
 *   an unchanged volume is neither a "down-volume" nor an "up-volume" bar.
 *   That is worth stating because the obvious implementation — `<` for NVI
 *   and `>=` for PVI — silently makes the two indices partition every bar
 *   and gives PVI a bar Fosback does not give it.
 * - **`start` is a base, not data.** `1000` is Fosback's; some vendors use
 *   `100` or `0`-based percentages. It scales the whole line, so the study
 *   is *invariant* to it in shape.
 * - **The return is a simple price return**, `close[i]/close[i−1] − 1`, on
 *   whichever `column` is named — this is a ratio index, not a sum, so
 *   there is no `× volume` term anywhere (contrast {@link priceVolumeTrend},
 *   which is the volume-weighted cousin).
 *
 * TA-Lib has no NVI/PVI, so the oracle is a pandas replication, with cases
 * separating it from the two nearby misreadings: compounding on a flat
 * volume, and compounding the wrong side.
 *
 * ## Edges
 *
 * - **Scale-invariant in both inputs.** Multiplying every price by `k` leaves
 *   the index unchanged (the return is a ratio); multiplying every volume by
 *   `k` leaves it unchanged (only the *comparison* is read). Both are
 *   asserted as property tests.
 * - **A zero previous close** makes the return undefined, and an index that
 *   cannot be continued reads `undefined` from that bar onwards rather than
 *   inventing a level. The guard is live because the division is at the
 *   output.
 * - **A missing `close` or `volume` re-bases the index.** Per the K6 gap
 *   rule ([PND-SFOLD], `kernels/fold.ts`) the machine resets: the gap bar is
 *   `undefined` and the next complete bar restarts at `start`. That is a
 *   deliberate difference from {@link obv}, which propagates to the end, and
 *   the difference is what the level means. OBV accumulates *volume* — a
 *   quantity with units, where the distance between two points is the
 *   reading — so a hole makes every later level wrong. NVI accumulates
 *   *returns* from an arbitrary base, so re-basing after a hole loses only
 *   the base, and the shape from there on is exactly right. A caller who
 *   needs one continuous index across a hole fills before running it.
 */
export function negativeVolumeIndex<
  S extends SeriesSchema,
  const Output extends string = 'nvi',
>(series: TimeSeries<S>, options: VolumeIndexOptions<S, Output> = {}) {
  const output = (options.output ?? 'nvi') as Output;
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);
  return series.withColumn(
    output,
    volumeIndex(
      wide,
      options as VolumeIndexOptions<SeriesSchema, string>,
      true,
      'negativeVolumeIndex',
    ),
  );
}

/**
 * **Positive Volume Index** (Fosback) — {@link negativeVolumeIndex}'s twin,
 * compounding a bar's return **only when that bar's volume was higher than
 * the previous bar's**:
 *
 * ```
 * PVI[0] = start                                   (1000 by convention)
 * PVI[i] = volume[i] > volume[i−1]
 *          ? PVI[i−1] × (1 + (close[i] − close[i−1]) / close[i−1])
 *          : PVI[i−1]
 * ```
 *
 * Every convention, edge and gap rule on {@link negativeVolumeIndex} applies
 * unchanged — including that an **unchanged volume holds**, so NVI and PVI do
 * not partition the bars between them: a flat-volume bar is compounded into
 * neither.
 *
 * Appends one column (`pvi` by default).
 */
export function positiveVolumeIndex<
  S extends SeriesSchema,
  const Output extends string = 'pvi',
>(series: TimeSeries<S>, options: VolumeIndexOptions<S, Output> = {}) {
  const output = (options.output ?? 'pvi') as Output;
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);
  return series.withColumn(
    output,
    volumeIndex(
      wide,
      options as VolumeIndexOptions<SeriesSchema, string>,
      false,
      'positiveVolumeIndex',
    ),
  );
}
