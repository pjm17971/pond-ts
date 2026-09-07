import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_SOURCE } from '../contract/columns.js';
import { highestLowestValues } from '../kernels/highest-lowest.js';
import type { MaType } from '../kernels/moving-average.js';
import {
  assertMaType,
  movingAverageValues,
} from '../kernels/moving-average.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';

/** How many averages the rainbow stacks — ten, Widner's own, and a constant
 *  rather than an option for the same reason Guppy's twelve are. */
const RAINBOW_DEPTH = 10;

/**
 * The ten **recursive** averages: each one smooths the previous one, not the
 * source. Shared by {@link rainbow} (which appends them) and
 * {@link rainbowOscillator} (which reduces them), so the two cannot drift on
 * what "the rainbow" is.
 *
 * Every stage goes through the K2 engine's **array** door, so each waits for
 * `period` finite values and therefore steps over the previous stage's
 * warm-up: at `period 2` stage `k` first lands on bar `k`, which is what
 * makes ten 2-bar averages a ten-bar look-back rather than a two-bar one.
 */
function rainbowStackValues(
  values: Float64Array,
  period: number,
  type: MaType,
): Float64Array[] {
  const stack: Float64Array[] = [];
  let current = values;
  for (let stage = 0; stage < RAINBOW_DEPTH; stage += 1) {
    current = movingAverageValues(current, period, type);
    stack.push(current);
  }
  return stack;
}

export interface RainbowOptions<S extends SeriesSchema, Prefix extends string> {
  /** Source column. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Length of **each** average in **bars**. **Default `2`**, Widner's own. */
  period?: number;
  /** Which moving average — any of the shared {@link MaType} menu.
   *  **Default `'sma'`.** */
  type?: MaType;
  /** Column-family prefix — appends `${prefix}1 … ${prefix}10`.
   *  **Default `'rainbow'`.** */
  prefix?: Prefix;
}

/**
 * **Rainbow Moving Average** (Mel Widner, *Stocks & Commodities*, July 1997)
 * — ten averages where each one smooths the **previous average**, not the
 * price:
 *
 * ```
 * ${prefix}1  = MA(column, period)
 * ${prefix}2  = MA(${prefix}1, period)
 * …
 * ${prefix}10 = MA(${prefix}9, period)
 * ```
 *
 * Appends **ten** columns. Plotted together they fan out into the bands the
 * name comes from: at `period 2` the first is barely smoothed and the tenth
 * lags by ten bars, so the width of the fan is how fast the trend is moving
 * and its ordering is which way.
 *
 * ## Recursive, not ten lengths
 *
 * The other thing published under "rainbow" is ten averages of *increasing
 * length* over the same source. That is a different study and this is not it:
 * a recursive 2-bar average is a **binomial** filter (weights `C(k, j)/2^k`
 * at stage `k`), not a box, so its shape and its phase both differ from an
 * SMA of the same support. Measured on the oracle input, stage 10 sits **0.83
 * points** from the 11-bar SMA covering the same eleven bars at
 * `{ period: 2, type: 'sma' }`, and **1.44** from the 21-bar one at
 * `{ period: 3, type: 'ema' }`, on a series whose whole range is 19.4 — the
 * generator asserts that separation so a fixture cannot pin the wrong one.
 *
 * `period` is the length of **each** stage, not of the chain — the same
 * convention {@link trix} states for its three EMAs. `2` is Widner's.
 *
 * ## Definition, verified
 *
 * No TA-Lib function, so the oracle is a **pandas replication** of the
 * recursion above, built on the same `_ma_over` helper the TA-Lib-verified K2
 * engine cases use rather than a private smoother, with each stage's analytic
 * first-valid bar asserted.
 *
 * ## Warm-up — per column, `stage · (period − 1)`
 *
 * Every stage goes through the engine's **array** door, where a type waits
 * for `period` finite values, so each stage steps over the previous one's
 * warm-up: at `period 2` the columns start on bars 1, 2, 3 … 10, and at
 * `period 3` on 2, 4, 6 … 20. Length-preserving; each column emitted where it
 * is defined rather than all ten waiting for the tenth.
 *
 * ## Edges
 *
 * - **Linear in price** — a chain of moving averages, so scaling scales every
 *   column and shifting shifts every column (both pinned).
 * - **A leading gap shifts every column**, including under `'sma'`: the array
 *   door counts finite values, not rows, so unlike {@link guppy}'s column
 *   door there is no `'sma'` exception here.
 * - **An interior gap** costs the bar and then `stage · (period − 1)` bars
 *   more with each level of the chain — the window types recover, `ema` skips
 *   and recovers, `smma` and `kama` propagate to the end.
 * - **`period 1` is the identity ten times over**, so all ten columns equal
 *   the source. Allowed rather than rejected: it is the honest answer, and
 *   every K2 type is the identity at 1.
 */
export function rainbow<
  S extends SeriesSchema,
  const Prefix extends string = 'rainbow',
>(series: TimeSeries<S>, options: RainbowOptions<S, Prefix> = {}) {
  const period = options.period ?? 2;
  assertPeriod(period);
  const type = options.type ?? 'sma';
  assertMaType(type);
  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const prefix = (options.prefix ?? 'rainbow') as Prefix;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  for (let stage = 1; stage <= RAINBOW_DEPTH; stage += 1) {
    assertNoColumn(wide, `${prefix}${stage}`);
  }

  const stack = rainbowStackValues(columnValues(wide, column), period, type);

  return series
    .withColumn(`${prefix}1` as const, stack[0]!)
    .withColumn(`${prefix}2` as const, stack[1]!)
    .withColumn(`${prefix}3` as const, stack[2]!)
    .withColumn(`${prefix}4` as const, stack[3]!)
    .withColumn(`${prefix}5` as const, stack[4]!)
    .withColumn(`${prefix}6` as const, stack[5]!)
    .withColumn(`${prefix}7` as const, stack[6]!)
    .withColumn(`${prefix}8` as const, stack[7]!)
    .withColumn(`${prefix}9` as const, stack[8]!)
    .withColumn(`${prefix}10` as const, stack[9]!);
}

export interface RainbowOscillatorOptions<
  S extends SeriesSchema,
  Prefix extends string,
> {
  /** Source column — read as the price, as the stack's input, and as the
   *  range the reading is normalised by. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Length of each average in the stack, in **bars**. **Default `2`.** */
  period?: number;
  /** Look-back for the highest/lowest of `column`, in **bars**. **Default
   *  `10`.** */
  lookback?: number;
  /** Which moving average the stack uses. **Default `'sma'`.** */
  type?: MaType;
  /** Column-family prefix — appends `${prefix}`, `${prefix}Upper` and
   *  `${prefix}Lower`. **Default `'rbo'`.** */
  prefix?: Prefix;
}

/**
 * **Rainbow Oscillator** — how far the price sits from the {@link rainbow}
 * stack's centre, as a percentage of the recent range, with the width of the
 * stack itself drawn as a band around it:
 *
 * ```
 * ${prefix}      = 100 · (price − mean(rainbow1…rainbow10)) / (HH − LL)
 * ${prefix}Upper = 100 · (max(rainbow1…rainbow10) − min(…))  / (HH − LL)
 * ${prefix}Lower = −${prefix}Upper
 * ```
 *
 * where `HH` / `LL` are the highest and lowest **of `column`** over
 * `lookback` bars. Appends three columns. The line is the momentum reading;
 * the bands are the fan's own width, so a line pushing outside them is price
 * moving faster than the stack has spread.
 *
 * ## The definition is ChartIQ's, and this one is genuinely ambiguous
 *
 * "Rainbow Oscillator" is published in several incompatible forms, so the
 * source is named rather than averaged. This is **ChartIQ's**: the numerator
 * is the distance from the stack's **mean**, the denominator is the
 * **range of the source column** over `lookback` bars, and the bands are the
 * stack's own max-minus-min over the same denominator, mirrored about zero.
 *
 * The two nearest alternatives, and how far away they are — measured on the
 * oracle input at the defaults, where the reading itself spans −68.6 to
 * +63.8:
 *
 * | variant | max distance from this |
 * | --- | --- |
 * | normalise by the **price** (`100·(c − mean)/c`) rather than the range | **67.27** |
 * | numerator against the **first** average rather than the stack's mean | **49.16** |
 *
 * Both are the size of the reading itself, so neither is a rounding
 * difference, and the generator asserts the separation so a fixture cannot
 * silently pin one of them.
 *
 * `lookback` and `period` are separate knobs because they answer different
 * questions — how much history the stack smooths, and what range the answer
 * is a percentage of.
 *
 * ## Edges
 *
 * - **A flat `lookback` window is `undefined`, not `0`.** Apply the test
 *   rather than the precedent: unlike a flat *bar*, whose close-location
 *   numerator is **forced** to zero, nothing here forces these numerators.
 *   The stack reaches back past the `lookback` window, so on a series that
 *   rises and then holds a level for `lookback` bars the reading is a real
 *   number over zero — **not** a `0/0` — which is an infinity, not a value.
 *   Measured on such a series at `period 2`: with `lookback: 3` the stack's
 *   mean is **more than 10 points** from the flat level, and even with the
 *   default `lookback: 10`, by which point the stack has nearly caught up, it
 *   is still **0.0044** away. Both are pinned by tests, because the "it
 *   converges, so it is really 0/0" intuition is the one that would talk
 *   someone into returning `0` here.
 * - **Scale-invariant and shift-invariant.** Numerator and denominator are
 *   both differences of prices, so `k·price + c` leaves all three columns
 *   unchanged (both pinned).
 * - **Warm-up is shared** by all three columns: the deepest average's
 *   (`RAINBOW_DEPTH · (period − 1)`) or the range's (`lookback − 1`),
 *   whichever is later — bar 10 at the defaults. The bands and the line warm
 *   up together because they read the same complete stack.
 * - **Not bounded.** `HH − LL` is the range of the *source* over `lookback`
 *   bars while the stack reaches further back, so a fast move takes the line
 *   outside ±100 honestly rather than clamped.
 * - **An interior gap** blanks the bar and every window the stack carries it
 *   into; the range itself **skips** a missing cell (core's reducer policy),
 *   so the denominator survives a hole the numerator does not.
 */
export function rainbowOscillator<
  S extends SeriesSchema,
  const Prefix extends string = 'rbo',
>(series: TimeSeries<S>, options: RainbowOscillatorOptions<S, Prefix> = {}) {
  const period = options.period ?? 2;
  assertPeriod(period);
  const lookback = options.lookback ?? 10;
  assertPeriod(lookback, 'lookback');
  const type = options.type ?? 'sma';
  assertMaType(type);
  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const prefix = (options.prefix ?? 'rbo') as Prefix;
  const lineName = `${prefix}` as const;
  const upperName = `${prefix}Upper` as const;
  const lowerName = `${prefix}Lower` as const;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, lineName);
  assertNoColumn(wide, upperName);
  assertNoColumn(wide, lowerName);

  const price = columnValues(wide, column);
  const stack = rainbowStackValues(price, period, type);
  // The same column on both sides of `highestLowestValues`: the rainbow's
  // denominator is the range of ONE series, not of a bar's high and low, and
  // one call still gets both extremes from one scan.
  const { highest, lowest } = highestLowestValues(
    wide,
    column,
    column,
    lookback,
  );

  const length = price.length;
  const line = new Float64Array(length);
  const upper = new Float64Array(length);
  const lower = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    let sum = 0;
    let high = -Infinity;
    let low = Infinity;
    for (let stage = 0; stage < RAINBOW_DEPTH; stage += 1) {
      const v = stack[stage]![i]!;
      sum += v;
      if (v > high) high = v;
      if (v < low) low = v;
    }
    const range = highest[i]! - lowest[i]!;
    // `sum` is NaN as soon as ANY of the ten is missing, which is the whole
    // completeness rule for all three columns in one test — a max and a min
    // over nine of ten averages would be a confident answer to a question
    // the stack cannot yet answer. A flat window (`range === 0`) is a
    // genuine 0/0 here; see the docstring for why nothing forces the
    // numerators to zero.
    if (Number.isNaN(sum) || range === 0) {
      line[i] = NaN;
      upper[i] = NaN;
      lower[i] = NaN;
      continue;
    }
    line[i] = (100 * (price[i]! - sum / RAINBOW_DEPTH)) / range;
    const spread = high - low;
    upper[i] = (100 * spread) / range;
    lower[i] = -upper[i]!;
  }

  return series
    .withColumn(lineName, line)
    .withColumn(upperName, upper)
    .withColumn(lowerName, lower);
}
