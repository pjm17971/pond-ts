import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_SOURCE } from '../contract/columns.js';
import { movingAverageValues } from '../kernels/moving-average.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';

export interface TrueStrengthIndexOptions<
  S extends SeriesSchema,
  Prefix extends string,
> {
  /** Source column. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Span of the **first** (slower) EMA applied to both legs, in **bars**.
   *  **Default `25`.** */
  longPeriod?: number;
  /** Span of the **second** EMA, applied to the first's output, in **bars**.
   *  **Default `13`.** */
  shortPeriod?: number;
  /** Signal EMA span in **bars**, taken over the TSI line. **Default `7`.** */
  signalPeriod?: number;
  /** Column-family prefix — appends `${prefix}` and `${prefix}Signal`.
   *  **Default `'tsi'`.** */
  prefix?: Prefix;
}

/**
 * **True Strength Index** (William Blau) — the bar-over-bar change, smoothed
 * twice, divided by its own magnitude smoothed the same way:
 *
 * ```
 * Δ        = price[i] − price[i−1]
 * ${prefix}       = 100 · EMA(EMA(Δ, longPeriod), shortPeriod)
 *                       / EMA(EMA(|Δ|, longPeriod), shortPeriod)
 * ${prefix}Signal = EMA(${prefix}, signalPeriod)
 * ```
 *
 * Appends two columns, bounded `−100 … +100`. The numerator is *net*
 * movement and the denominator is *total* movement over the same smoothing,
 * so the ratio is "what fraction of the recent motion went one way" — `+100`
 * is an unbroken run of up bars, `0` is as much down as up. Double smoothing
 * is what makes it readable: a single-smoothed version of the same ratio is
 * as noisy as the price.
 *
 * **The line is named `${prefix}`, not `${prefix}Line`** — the {@link trix}
 * shape, with the signal keeping the family suffix.
 *
 * ## Option names — `longPeriod` / `shortPeriod`, not `long` / `short`
 *
 * Blau writes them `r` and `s`, and most vendors label them "Long" and
 * "Short". Both bare words are **position** vocabulary in a financial
 * package — "long" reads as a side, not a length — and every other period
 * option here ends in `Period` (`kPeriod`, `atrPeriod`, `signalPeriod`,
 * `wmaPeriod`, …). So they carry the suffix, matching {@link coppock}'s
 * `longPeriod` / `shortPeriod`.
 *
 * **The order is not symmetric**: `longPeriod` is applied **first**, to the
 * raw change, and `shortPeriod` to its output. Swapping them is a real
 * change, not a relabelling — measured on the oracle input, the swapped
 * build sits **15.93** away on a line that spans −28.7 … +80.7 — and the
 * generator asserts that separation.
 *
 * ## Definition, verified
 *
 * **No TA-Lib `TSI`**, so the oracle is a pandas replication. What it *can*
 * borrow from TA-Lib is the smoothing itself: the generator rebuilds an EMA
 * stage on TA-Lib's own SMA seed over the change array and requires it to
 * match `talib.EMA` bit-exactly (**5.6e-16**, measured), so the stage
 * arithmetic is vendor-checked even though the assembly is not. The EMAs
 * themselves are pond's — first-sample seed, `α = 2/(span+1)` — as
 * everywhere else in the package (the {@link macd} precedent).
 *
 * Some write-ups smooth the **percentage** change rather than the price
 * change. That is a different indicator; this is Blau's, on the raw
 * difference, which is what makes the ratio scale-invariant without a
 * division per bar.
 *
 * ## Warm-up
 *
 * The change costs bar 0; each EMA stage then steps over the previous
 * stage's warm-up rather than seeding on it, so on gap-free input the line
 * first lands on bar **`longPeriod + shortPeriod − 1`** (37 at the defaults)
 * and the signal `signalPeriod − 1` later (**43**). Each column is emitted
 * where it is defined rather than both waiting for the slower.
 *
 * ## Edges
 *
 * - **A zero denominator → `undefined`, and there is deliberately NO guard
 *   for it.** The denominator is an EMA of absolute values, so it is zero
 *   only when every change the recursion has consumed is exactly zero — a
 *   perfectly flat column. Apply the test rather than the precedent: the
 *   numerator is then **forced** to zero too (`|numerator| ≤ denominator` by
 *   construction), so the division is a literal `0/0`, which is already
 *   `NaN` and already a missing cell. An `if (den === 0)` branch here would
 *   change no output — measured, mutating it away fails no test — so it is
 *   not written. (Contrast {@link disparityIndex}, whose numerator is *not*
 *   forced to zero: there a zero denominator gives `±∞` and the guard is
 *   load-bearing.) A test on a constant series pins that both columns come
 *   back empty.
 * - **Bounded `−100 … +100`** by the same inequality, on any input.
 * - **Scale-invariant AND shift-invariant**: both legs are built from
 *   differences, so `k·price + c` leaves both columns unchanged (pinned).
 *   This is the opposite of {@link kst} and {@link priceMomentumOscillator},
 *   whose rates of change read *levels* and therefore move under a shift.
 * - **A leading gap shifts the start**; an **interior** gap costs the bar and
 *   the bar after it (the difference reads a predecessor), and the EMAs then
 *   skip and carry on — the `ema` family's rule, so nothing propagates to
 *   the end.
 */
export function trueStrengthIndex<
  S extends SeriesSchema,
  const Prefix extends string = 'tsi',
>(series: TimeSeries<S>, options: TrueStrengthIndexOptions<S, Prefix> = {}) {
  const longPeriod = options.longPeriod ?? 25;
  const shortPeriod = options.shortPeriod ?? 13;
  const signalPeriod = options.signalPeriod ?? 7;
  assertPeriod(longPeriod, 'longPeriod');
  assertPeriod(shortPeriod, 'shortPeriod');
  assertPeriod(signalPeriod, 'signalPeriod');

  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const prefix = (options.prefix ?? 'tsi') as Prefix;
  const lineName = `${prefix}` as const;
  const signalName = `${prefix}Signal` as const;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, lineName);
  assertNoColumn(wide, signalName);

  const values = columnValues(wide, column);
  const length = values.length;
  // Bar 0 has no predecessor; `NaN` marks it and propagates on its own
  // ([PND-STUDYBOX]), which is what makes the EMA below step over it.
  const deltas = new Float64Array(length);
  const magnitudes = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    const d = i === 0 ? NaN : values[i]! - values[i - 1]!;
    deltas[i] = d;
    magnitudes[i] = Math.abs(d);
  }

  // Long first, then short — the order is the definition (see the docstring
  // for the measured separation from the swap). Both stages go through the
  // K2 engine's array door, so they are the same recursion `ema()` runs and
  // each steps over the previous stage's NaN head.
  const smooth = (v: Float64Array) =>
    movingAverageValues(
      movingAverageValues(v, longPeriod, 'ema'),
      shortPeriod,
      'ema',
    );
  const numerator = smooth(deltas);
  const denominator = smooth(magnitudes);

  const line = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    // NO zero-denominator guard, deliberately — see the docstring. A zero
    // denominator forces a zero numerator (`|num| <= den` by construction),
    // so the division is a literal `0/0` and JavaScript already answers
    // `NaN`, which `withColumn` records as a missing cell. A guard here
    // would be dead code: measured, mutating it away fails no test.
    line[i] = (100 * numerator[i]!) / denominator[i]!;
  }

  // The signal is an EMA OF THE LINE, through the same array door, so it
  // steps over the line's own warm-up with no arithmetic here.
  const signal = movingAverageValues(line, signalPeriod, 'ema');

  return series.withColumn(lineName, line).withColumn(signalName, signal);
}
