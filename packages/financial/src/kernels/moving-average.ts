import type { SeriesSchema, TimeSeries } from 'pond-ts';
import { rollingMeanValues } from './rolling-mean.js';
import {
  assertPeriod,
  columnValues,
  emaValues,
  rollingValues,
} from './rolling.js';
import { wilderValues } from './wilder.js';

/**
 * **The moving-average vocabulary** — kernel K2 of the corpus assessment
 * (`docs/notes/financial-indicators-assessment-2026-07.md` §4).
 *
 * ~25 studies in the corpus expose a "MA Type" input, and every one of them
 * means the same menu. Naming that menu once, in one place, is what keeps
 * those studies thin assemblies rather than 25 private smoothers that drift
 * apart — a `keltner` with its own EMA and a `disparityIndex` with another is
 * exactly the failure mode the studies README's step 0 exists to stop.
 *
 * Deliberately **not** in the menu (deferred, not forgotten): MAMA/FAMA
 * (Ehlers' Hilbert-transform adaptive MA — a different kind of animal, and
 * TA-Lib's own implementation is the only reference), T3 (Tillson: six
 * chained EMAs plus a volume-factor knob), VIDYA (CMO-adaptive, so it needs
 * the K6 recursion the package has not built yet) and the time-series /
 * linear-regression forecast MA (kernel **K7**, rolling regression — a
 * different kernel, not a smoothing rate). Each is a study-sized decision of
 * its own and none of the fan-out's consumers need them.
 */
export type MaType =
  | 'sma'
  | 'ema'
  | 'wma'
  | 'smma'
  | 'dema'
  | 'tema'
  | 'trima'
  | 'hull'
  | 'kama'
  | 'zlema';

/** The menu as an object so the compiler checks it is exhaustive: add a
 *  member to {@link MaType} without adding it here and this line fails to
 *  type-check, rather than silently leaving the new type unvalidated and
 *  outside every fan-out test. */
const MA_TYPE_MENU = {
  sma: true,
  ema: true,
  wma: true,
  smma: true,
  dema: true,
  tema: true,
  trima: true,
  hull: true,
  kama: true,
  zlema: true,
} as const satisfies Record<MaType, true>;

/** Every {@link MaType}, in menu order — the vocabulary a study's `type`
 *  option accepts, and what {@link assertMaType} validates against. */
export const MA_TYPES: readonly MaType[] = Object.keys(
  MA_TYPE_MENU,
) as MaType[];

/** Throw if `type` is not one of {@link MA_TYPES} — a study that took an
 *  unknown type would otherwise fall through to a silent default. */
export function assertMaType(type: string): asserts type is MaType {
  if (!(MA_TYPES as readonly string[]).includes(type)) {
    throw new TypeError(
      `unknown moving-average type '${type}'; expected one of ${MA_TYPES.join(', ')}`,
    );
  }
}

/**
 * **One moving average over a raw array**, row-aligned, `NaN` for a cell the
 * type cannot yet produce.
 *
 * Over an **array** rather than a series column because most of its callers
 * derive their input: Keltner smooths typical price, Coppock's WMA smooths a
 * sum of two ROCs, the Price Oscillator subtracts two of these from each
 * other, Hull and TRIMA and DEMA feed one of these into another. A series
 * column is the special case, and {@link movingAverageColumn} is the door for
 * it.
 *
 * ## Definitions
 *
 * | type | definition |
 * | --- | --- |
 * | `sma` | mean of the last `period` finite values (`rollingMeanValues`) — the same arithmetic as `sma()`, bit-for-bit on gap-free input; on this array door the window waits for contributors, like every other type |
 * | `ema` | `α = 2/(period+1)`, seeded on the **first sample** — pond's convention, bit-for-bit what `ema()` / `smooth('ema')` gives |
 * | `wma` | linear weights `1…period`, newest heaviest, over `period(period+1)/2` |
 * | `smma` | Wilder's `(prev·(period−1) + x)/period`, SMA-seeded — the `wilderValues` kernel RSI and ATR already run on |
 * | `dema` | `2·EMA − EMA(EMA)` |
 * | `tema` | `3·EMA − 3·EMA(EMA) + EMA(EMA(EMA))` |
 * | `trima` | TA-Lib's triangular: `SMA(SMA(x, p), q)` with `p = q = (n+1)/2` for odd `n`, `p = n/2+1, q = n/2` for even |
 * | `hull` | `WMA(2·WMA(x, ⌊n/2⌋) − WMA(x, n), round(√n))` |
 * | `kama` | Kaufman adaptive, TA-Lib's fast 2 / slow 30, efficiency ratio over `period` |
 * | `zlema` | `EMA(2·x − x[i−lag])`, `lag = ⌊(period−1)/2⌋` |
 *
 * **The `zlema` lag floors.** `(period − 1)/2` is not an integer on an even
 * `period`, and the only two answers are to floor it or to interpolate
 * between two bars. Flooring is what every published implementation does and
 * is the only one that keeps the study a pure re-indexing of its input; the
 * cost is that `zlema(10)` and `zlema(11)` share a lag of 4 and differ only
 * in their EMA rate.
 *
 * ## Warm-up — length-preserving, and where the first value lands
 *
 * Every type keeps the row count and emits `NaN` until its definition has
 * enough bars. First valid index on a gap-free input, `n = period`:
 *
 * | `sma` `ema` `wma` `smma` `trima` | `n−1` |
 * | `dema` | `2n−2` |
 * | `tema` | `3n−3` |
 * | `hull` | `n − 2 + round(√n)` |
 * | `kama` | `n` (the efficiency ratio needs `n` differences, so `n+1` bars) |
 * | `zlema` | `⌊(n−1)/2⌋ + n − 1` |
 *
 * Those match TA-Lib's lookbacks exactly for the seven types TA-Lib ships
 * (`MA(matype=…)`); the oracle asserts the null **mask**, not just the
 * values, so a warm-up off-by-one fails the generator.
 *
 * **A leading run of gaps steps the warm-up over**, rather than poisoning it
 * — the common source of one is another study's own warm-up, and a study run
 * over `sma(...)`'s output must start late rather than come back empty
 * (the `rsi(sma(...))` failure the property tests exist for).
 *
 * **`sma` is the one exception, deliberately.** Its window counts *rows*, not
 * contributors — it emits once the window spans `period` rows and averages
 * whichever of them are finite — so a leading gap does **not** shift its
 * first value. That is `sma()`'s contract, the one `sma∘sma` pins; giving the
 * engine's `sma` a step-over would make `movingAverage({ type: 'sma' })` a
 * second SMA that quietly disagreed with the first. Every other type in the
 * menu waits for `period` finite values and therefore shifts.
 *
 * ## Interior gaps: which types recover and which propagate
 *
 * The Wilder asymmetry, stated per type rather than averaged into a slogan.
 * A window kernel recovers once the gap leaves the window; a recursion has no
 * state to carry across a hole, so whether it recovers depends on whether the
 * recursion **skips** the missing bar or **consumes** it.
 *
 * | type | interior gap |
 * | --- | --- |
 * | `sma` | **recovers** — the window still spans `period` rows and averages the finite ones (`sma()`'s contract, rows not contributors) |
 * | `wma` | **recovers** — but the gap bar and the `period−1` after it read `NaN`: a linear weight is position-specific, so dropping one cell would silently reweight the rest |
 * | `trima` | **recovers** — `wma`'s rule (both SMA stages want a finite window; a triangular weight is positional too) |
 * | `hull` | **recovers** — `wma`'s rule, three windows deep |
 * | `ema` | **recovers** — the recursion skips the missing bar and carries on (core's `smooth('ema')`) |
 * | `dema` `tema` `zlema` | **recovers** — built on `ema`, same skip |
 * | `smma` | **propagates to the end** — Wilder consumes every bar, so a hole makes the state unknown forever (`wilderValues`) |
 * | `kama` | **propagates to the end** — the efficiency ratio sums `period` consecutive differences, and the recursion carries the result forward |
 *
 * Neither answer is universally right and neither is fixable in the kernel:
 * a caller who needs continuity across interior gaps fills before smoothing.
 *
 * ## Cost
 *
 * **O(N) per type, independent of `period`** — nothing here rescans a window.
 * The one that has to be written for it is `wma`: the definition is a
 * `period`-term dot product per bar (O(N·period)), and this runs the
 * **running weighted sum** instead, `W(i) = W(i−1) − S(i−1) + period·x(i)`
 * alongside the plain window sum `S`. Both accumulators are rebuilt from the
 * window on rows where `i % period === 0` — one extra accumulation per row
 * amortised, the same trick and the same cadence `ranged.ts` uses, so the
 * cancellation in `W − S` cannot accumulate across a million bars.
 *
 * The composed types pay their parts: `dema` two EMA passes, `tema` three,
 * `trima` two SMA passes, `hull` four WMA passes (`n/2`, `n`, and the outer
 * `√n`, plus the combine). All still O(N).
 */
export function movingAverageValues(
  values: Float64Array,
  period: number,
  type: MaType,
): Float64Array {
  // Validated here as well as in the study, because this is a public export
  // and a `period` of 0 would divide the weighted sum by 0 rather than throw.
  assertPeriod(period);
  switch (type) {
    case 'sma':
      return smaValues(values, period);
    case 'ema':
      return emaArrayValues(values, period);
    case 'wma':
      return wmaValues(values, period);
    case 'smma':
      return wilderValues(values, period);
    case 'dema':
      return demaValues(values, period);
    case 'tema':
      return temaValues(values, period);
    case 'trima':
      return trimaValues(values, period);
    case 'hull':
      return hullValues(values, period);
    case 'kama':
      return kamaValues(values, period);
    case 'zlema':
      return zlemaValues(values, period);
    default: {
      // An unreachable branch that still fails loudly if `MaType` grows a
      // member and this switch does not.
      const unknown: never = type;
      throw new TypeError(`unknown moving-average type '${String(unknown)}'`);
    }
  }
}

/**
 * {@link movingAverageValues} over a **series column** — the door a study
 * uses.
 *
 * `sma` and `ema` are routed to the existing `rollingValues` / `emaValues`
 * helpers rather than through the array kernel, and that is load-bearing
 * twice over. It keeps `movingAverage({ type: 'sma' })` and `sma()` the same
 * call and therefore the same doubles (a second implementation that agreed
 * "to rounding" would be a second definition), and it keeps both on their
 * accelerated paths — `rollingValues`' opt-in parallel accelerator and
 * `smooth('ema')`'s columnar fast path. Every other type reads the column
 * once and runs on the array.
 */
export function movingAverageColumn(
  series: TimeSeries<SeriesSchema>,
  column: string,
  period: number,
  type: MaType,
): Float64Array {
  if (type === 'sma') return rollingValues(series, column, 'avg', period);
  if (type === 'ema') return emaValues(series, column, period);
  return movingAverageValues(columnValues(series, column), period, type);
}

/**
 * SMA of a raw array — `rollingMeanValues`: the same range-exact arithmetic
 * `sma()` runs, but the window waits for `period` **finite** values, like
 * every other type on this door. That is the rule for derived inputs (the
 * studies README): a derived array's leading `NaN` warm-up steps the window
 * over rather than being averaged as if it were data, which is exactly how
 * core's row-counting window put slow `%K` one bar early. The **column**
 * door ({@link movingAverageColumn}) keeps `sma()`'s rows-not-contributors
 * contract for `'sma'`, so `movingAverage({ type: 'sma' })` over a column
 * and `sma()` remain one SMA; the two doors differ only on a column with
 * missing cells, and a test pins both sides.
 */
function smaValues(values: Float64Array, period: number): Float64Array {
  return rollingMeanValues(values, period);
}

/**
 * Span-EMA of a raw array — `α = 2/(period+1)`, seeded on the first finite
 * sample, missing cells skipped, emitted once `period` samples have been
 * consumed.
 *
 * Deliberately the same recurrence, in the same order, as core's
 * `smooth('ema')` columnar fast path (`α·x + (1−α)·prev`), so
 * `movingAverageValues(v, n, 'ema')` and `emaValues(series, col, n)` are
 * bit-identical on the same values — pinned by a test, because the whole
 * point of routing `ema` around this kernel is that the two are one
 * definition rather than two.
 */
function emaArrayValues(values: Float64Array, period: number): Float64Array {
  const length = values.length;
  const out = new Float64Array(length).fill(NaN);
  const alpha = 2 / (period + 1);
  let previous = 0;
  let seeded = false;
  let seen = 0;
  for (let i = 0; i < length; i += 1) {
    const raw = values[i]!;
    if (!Number.isFinite(raw)) continue;
    previous = seeded ? alpha * raw + (1 - alpha) * previous : raw;
    seeded = true;
    seen += 1;
    if (seen >= period) out[i] = previous;
  }
  return out;
}

/**
 * Linearly weighted moving average — weights `1…period` with the heaviest on
 * the newest bar, normalised by `period(period+1)/2`.
 *
 * O(N) by the running weighted sum described on {@link movingAverageValues};
 * a gap contributes 0 to both accumulators and is counted separately, so the
 * `NaN` it produces is confined to the windows that actually contain it
 * rather than poisoning the accumulator for the rest of the column.
 */
function wmaValues(values: Float64Array, period: number): Float64Array {
  const length = values.length;
  const out = new Float64Array(length).fill(NaN);
  const denominator = (period * (period + 1)) / 2;
  // The window is [i - period + 1, i], with out-of-range rows reading 0 —
  // which is what makes one recurrence cover the warm-up rows too.
  let sum = 0;
  let weighted = 0;
  let missing = 0;

  for (let i = 0; i < length; i += 1) {
    const entering = values[i]!;
    const leavingIndex = i - period;
    // W(i) = W(i-1) - S(i-1) + period * x(i): every retained cell loses one
    // unit of weight (that is S(i-1) less the leaver, whose own weight was
    // exactly 1) and the new cell enters at full weight.
    weighted =
      weighted - sum + period * (Number.isFinite(entering) ? entering : 0);
    if (Number.isFinite(entering)) sum += entering;
    else missing += 1;
    if (leavingIndex >= 0) {
      const leaving = values[leavingIndex]!;
      if (Number.isFinite(leaving)) sum -= leaving;
      else missing -= 1;
    }

    // Rebuild on the same cadence and the same alignment `ranged.ts` uses:
    // `W - S` cancels, and nothing that happened more than one window
    // turnover ago should be able to reach the answer.
    if (i % period === 0) {
      const low = i - period + 1;
      sum = 0;
      weighted = 0;
      missing = 0;
      for (let k = low > 0 ? low : 0; k <= i; k += 1) {
        const x = values[k]!;
        if (!Number.isFinite(x)) {
          missing += 1;
          continue;
        }
        sum += x;
        weighted += (k - low + 1) * x;
      }
    }

    if (i >= period - 1 && missing === 0) out[i] = weighted / denominator;
  }
  return out;
}

/** `2·EMA − EMA(EMA)` — the double EMA, which trades the single EMA's lag for
 *  overshoot. Warm-up `2·period − 2` (TA-Lib's DEMA lookback). */
function demaValues(values: Float64Array, period: number): Float64Array {
  const first = emaArrayValues(values, period);
  const second = emaArrayValues(first, period);
  const out = new Float64Array(values.length);
  for (let i = 0; i < out.length; i += 1) out[i] = 2 * first[i]! - second[i]!;
  return out;
}

/** `3·EMA − 3·EMA(EMA) + EMA(EMA(EMA))` — the triple EMA. Warm-up
 *  `3·period − 3` (TA-Lib's TEMA lookback). */
function temaValues(values: Float64Array, period: number): Float64Array {
  const first = emaArrayValues(values, period);
  const second = emaArrayValues(first, period);
  const third = emaArrayValues(second, period);
  const out = new Float64Array(values.length);
  for (let i = 0; i < out.length; i += 1) {
    out[i] = 3 * first[i]! - 3 * second[i]! + third[i]!;
  }
  return out;
}

/**
 * Triangular moving average — TA-Lib's, which is an SMA of an SMA whose two
 * lengths sum to `period + 1`.
 *
 * The split is the definition, not an implementation detail: convolving a box
 * of length `p` with one of length `q` gives weights that ramp to `min(p, q)`
 * over `p + q − 1` bars, so odd `period` takes `p = q = (period+1)/2` (a
 * single peak) and even `period` takes `p = period/2 + 1`, `q = period/2` (a
 * two-bar plateau, TA-Lib's `i·(i+1)` normaliser). Dropping the `+ 1` gives a
 * triangle one bar too narrow.
 *
 * Which of the two lengths runs first is **immaterial** — convolution
 * commutes, and so does the warm-up (`p−1` then `q−1` either way). A mutation
 * that swapped them changed no number, which is worth knowing before someone
 * "fixes" the order.
 *
 * **Both** stages wait for their window's worth of **finite** values
 * (`rollingMeanValues`), not of rows. The second stage must: an SMA of an SMA
 * over core's row-counting window emits while its input is still warming up,
 * putting the first value `period/2` bars early. The first stage must for the
 * same reason one stage down — TRIMA is a *weighted* average, and a stage-1
 * cell computed from one contributor is not the triangle's `p`-bar box, so a
 * leading gap would shift TRIMA by less than it shifts its input. That makes
 * TRIMA mask on an interior gap the way `wma` does rather than average
 * around it the way `sma` does, which is the right side for a weighted mean.
 */
function trimaValues(values: Float64Array, period: number): Float64Array {
  const odd = period % 2 === 1;
  const firstLength = odd ? (period + 1) / 2 : period / 2 + 1;
  const secondLength = odd ? (period + 1) / 2 : period / 2;
  return rollingMeanValues(
    rollingMeanValues(values, firstLength),
    secondLength,
  );
}

/**
 * Hull moving average — `WMA(2·WMA(x, ⌊n/2⌋) − WMA(x, n), round(√n))`.
 *
 * Both derived lengths floor at 1 so `period 1` and `period 2` stay defined
 * (`⌊1/2⌋` and `round(√1)` would otherwise be 0 and an invalid window);
 * `hull(1)` is then the identity, which is the right answer.
 */
function hullValues(values: Float64Array, period: number): Float64Array {
  const half = Math.max(1, Math.floor(period / 2));
  const root = Math.max(1, Math.round(Math.sqrt(period)));
  const fast = wmaValues(values, half);
  const slow = wmaValues(values, period);
  const raw = new Float64Array(values.length);
  for (let i = 0; i < raw.length; i += 1) raw[i] = 2 * fast[i]! - slow[i]!;
  return wmaValues(raw, root);
}

/** TA-Lib's KAMA constants: the smoothing constant interpolates between a
 *  `fast` 2-bar EMA and a `slow` 30-bar one, and is then squared. */
const KAMA_SLOW = 2 / (30 + 1);
const KAMA_DIFF = 2 / (2 + 1) - KAMA_SLOW;

/**
 * Kaufman's adaptive moving average — TA-Lib's, constants included.
 *
 * `ER = |x[i] − x[i−n]| / Σ|x[j] − x[j−1]|` over the same `n` bars (direction
 * over path: 1 for a clean trend, →0 for chop), `SC = (ER·(2/3 − 2/31) +
 * 2/31)²`, and `KAMA[i] = KAMA[i−1] + SC·(x[i] − KAMA[i−1])`, seeded on
 * `x[n−1]` so the first value lands on bar `n`.
 *
 * No clamp on `ER`: the denominator is a sum of the absolute steps whose
 * signed total is the numerator, so the triangle inequality already bounds it
 * by 1.
 *
 * **The flat-window guard is load-bearing.** A window that has not moved
 * makes `ER` a `0/0`, and TA-Lib's answer is `ER = 1`. It is tempting to read
 * that as cosmetic — "nothing moved, so nothing happens" — but this is a
 * *recursion*, and `previous` need not be anywhere near the flat level it is
 * converging on: an unguarded `0/0` puts `NaN` in `previous` and empties the
 * column from that bar to the end. Pinned against TA-Lib on a series that
 * flattens while KAMA is still 1.5 below it.
 *
 * Recursive, so an interior gap propagates — though it is `previous` that
 * carries it, not the difference sum: the gap bar makes `x[i] − previous`
 * `NaN` first, and a recursion never gives that back.
 */
function kamaValues(values: Float64Array, period: number): Float64Array {
  const length = values.length;
  const out = new Float64Array(length).fill(NaN);

  // Step over a leading run of gaps, as `wilderValues` does — a source's own
  // warm-up shifts the seed rather than emptying the column.
  let first = 0;
  while (first < length && !Number.isFinite(values[first]!)) first += 1;
  const seedAt = first + period;
  if (seedAt >= length) return out;

  let sum = 0;
  for (let i = first + 1; i <= seedAt; i += 1) {
    sum += Math.abs(values[i]! - values[i - 1]!);
  }
  let previous = values[seedAt - 1]!;

  for (let i = seedAt; i < length; i += 1) {
    if (i > seedAt) {
      sum +=
        Math.abs(values[i]! - values[i - 1]!) -
        Math.abs(values[i - period]! - values[i - period - 1]!);
    }
    const change = Math.abs(values[i]! - values[i - period]!);
    // `sum === 0` is the flat window (so `change` is 0 too) and takes
    // TA-Lib's ER = 1; see the note above on why it cannot be left to 0/0.
    const efficiency = sum === 0 ? 1 : change / sum;
    const constant = (efficiency * KAMA_DIFF + KAMA_SLOW) ** 2;
    previous += constant * (values[i]! - previous);
    out[i] = previous;
  }
  return out;
}

/**
 * Zero-lag EMA — an EMA of the input with its own lag subtracted forward:
 * `EMA(2·x[i] − x[i−lag], period)`, `lag = ⌊(period−1)/2⌋` (the centre of
 * gravity of a `period`-bar window).
 *
 * The first `lag` bars have no `x[i−lag]` and read `NaN`; the EMA then steps
 * over that head, so the first value lands on `lag + period − 1`. Reading
 * `x[0]` for the missing predecessor instead would emit `lag` bars of a
 * de-lagging that had not happened yet.
 */
function zlemaValues(values: Float64Array, period: number): Float64Array {
  const length = values.length;
  const lag = Math.floor((period - 1) / 2);
  const shifted = new Float64Array(length).fill(NaN);
  for (let i = lag; i < length; i += 1) {
    shifted[i] = 2 * values[i]! - values[i - lag]!;
  }
  return emaArrayValues(shifted, period);
}
