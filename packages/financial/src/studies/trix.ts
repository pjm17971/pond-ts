import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_SOURCE } from '../contract/columns.js';
import { movingAverageValues } from '../kernels/moving-average.js';
import { percentChangeValues } from '../kernels/rate-of-change.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';

export interface TrixOptions<S extends SeriesSchema, Prefix extends string> {
  /** Span of **each** of the three EMAs, in **bars**. **Default `15`.** */
  period?: number;
  /** Signal EMA span in **bars**, taken over the TRIX line. **Default `9`.** */
  signalPeriod?: number;
  /** Source column. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Column-family prefix — appends `${prefix}` and `${prefix}Signal`.
   *  **Default `'trix'`.** */
  prefix?: Prefix;
}

/**
 * **TRIX** (Jack Hutson) — the 1-bar rate of change of a **triple-smoothed
 * EMA**, in percent, with its own EMA as a signal line:
 *
 * ```
 * T          = EMA(EMA(EMA(column, period), period), period)
 * ${prefix}       = 100 × (T[i] − T[i−1]) / T[i−1]
 * ${prefix}Signal = EMA(${prefix}, signalPeriod)
 * ```
 *
 * Appends two columns. Triple smoothing strips the cycles shorter than
 * `period` almost entirely, so what is left is a slow, nearly noise-free
 * momentum oscillator that crosses zero when the smoothed trend turns.
 *
 * **The line is named `${prefix}`, not `${prefix}Line`** — a deliberate
 * departure from {@link macd}'s `macdLine` / `macdSignal` shape, because
 * "the TRIX" *is* the line and `trixLine` reads as a stutter. The signal
 * keeps the family suffix.
 *
 * ## Definition — TA-Lib's, exactly
 *
 * `100 · (T[i]/T[i−1] − 1)`, a **percent** rate of change, which is what
 * TA-Lib's `TRIX` computes and what the generator asserts against it (see
 * below). Some write-ups say a *log* rate of change instead
 * (`100 · ln(T[i]/T[i−1])`); the two agree to about 0.5% of the reading at
 * a 1% bar-over-bar move and diverge from there, so the choice is real, and
 * this is TA-Lib's.
 *
 * **`period` is the span of each EMA, not of the chain.** `trix({ period:
 * 15 })` is three 15-bar EMAs, not a 5-bar one applied three times — the
 * universal convention, restated because "period" on a triple-smoothed
 * study is ambiguous on its face.
 *
 * **This is not TEMA.** The K2 menu's `tema` is Patrick Mulloy's
 * `3·EMA − 3·EMA² + EMA³` — a *de-lagged* average built from the same three
 * chained EMAs. TRIX wants the chain itself, `EMA³`, so it composes three
 * `movingAverageValues(…, 'ema')` passes rather than reaching for `tema`.
 * (Reusing `tema` here would be silently wrong, not slightly wrong: the
 * combination changes the phase, and its rate of change is a different
 * indicator.)
 *
 * ## The EMA seed, and what the oracle asserts
 *
 * The EMAs are pond's — first-sample seed, `α = 2/(period+1)` — as
 * everywhere else in the package, so `trix` cannot disagree with `ema()`
 * inside the package (the {@link macd} precedent; TA-Lib seeds each EMA on
 * the SMA of its first `n` values instead). The generator therefore checks
 * the two questions separately: it rebuilds the same formula on **TA-Lib's**
 * SMA seed and requires bit-level agreement with `talib.TRIX` (so a wrong
 * coefficient, a dropped stage or a log-vs-percent slip fails at 1e-9), then
 * bounds pond's seed transient over the last 20 shared bars. **Measured on
 * the oracle input**: the SMA-seeded replication matches `talib.TRIX` to
 * **2.2e-14** at both `period 15` and `period 5`, with identical null masks;
 * pond's seed transient is 8.75% of scale at the first shared bar and
 * 0.85% over the last 20 at `period 15` (11.01% → 0.0000% at `period 5`).
 * That is a looser tail than the MA family's because TRIX's own scale is a
 * percent rate of change (0.45 here) rather than a price, and at `period 15`
 * only 37 bars are shared at all — the generator carries the measured
 * separation from the wrong smoothing rates (8.5% and 9.8%).
 *
 * **The signal line has no vendor reference** — TA-Lib's `TRIX` returns the
 * line alone. `signalPeriod` defaults to **9**, which is ChartIQ's default
 * (and MACD's), and the signal is a pandas replication in the oracle.
 *
 * ## Warm-up: per column
 *
 * Each EMA stage steps over the previous stage's warm-up rather than
 * poisoning its seed with it, so on gap-free input `T` first lands on bar
 * `3·period − 3` and the rate of change one bar later, at **`3·period −
 * 2`** — TA-Lib's TRIX lookback (`3·(period−1) + 1`) exactly, which the
 * generator asserts as a null **mask**, not just a magnitude. The signal
 * starts `signalPeriod − 1` bars after the line. Each column is emitted
 * where it is defined rather than both waiting for the slower.
 *
 * ## Edges
 *
 * - **Scale-invariant.** A rate of change of a linear filter of the price
 *   is a ratio, so multiplying every bar by any positive constant leaves
 *   both columns unchanged — pinned as a property test. (Contrast
 *   {@link macd}, which is linear in price.)
 * - **A zero previous value of the smoothed line → `undefined`.** Percent
 *   change off zero has no answer, and `x/0` would be an infinity that
 *   `withColumn` rejects outright. Unreachable on real prices — `T` is a
 *   convex combination of positive prices — but reachable when `column` is
 *   another study's output that crosses zero (a MACD histogram, say), which
 *   is a supported thing to run this over.
 * - **A leading gap shifts the start**; an **interior** gap blanks the bar
 *   and the bar after it (the rate of change reads a predecessor) and the
 *   EMAs then carry on — the `ema` family's skip rule, three stages deep.
 */
export function trix<
  S extends SeriesSchema,
  const Prefix extends string = 'trix',
>(series: TimeSeries<S>, options: TrixOptions<S, Prefix> = {}) {
  const period = options.period ?? 15;
  const signalPeriod = options.signalPeriod ?? 9;
  assertPeriod(period);
  assertPeriod(signalPeriod, 'signalPeriod');

  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const prefix = (options.prefix ?? 'trix') as Prefix;
  const lineName = `${prefix}` as const;
  const signalName = `${prefix}Signal` as const;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, lineName);
  assertNoColumn(wide, signalName);

  // Three chained passes of the K2 engine's raw-array EMA — the same
  // recursion `ema()` runs, one definition rather than a private chain. Each
  // stage steps over the previous stage's NaN head (that is the array door's
  // rule for derived inputs), which is what makes the warm-up compose to
  // 3·period − 3 instead of collapsing to nothing.
  const first = movingAverageValues(columnValues(wide, column), period, 'ema');
  const second = movingAverageValues(first, period, 'ema');
  const third = movingAverageValues(second, period, 'ema');

  // 1-bar percent rate of change, sharing `percentChange`'s definition —
  // including its zero-base guard — rather than restating it.
  const line = percentChangeValues(third, 1);
  // The signal is an EMA *of the line*, through the same array door, so it
  // steps over the line's own warm-up with no arithmetic here.
  const signal = movingAverageValues(line, signalPeriod, 'ema');

  return series.withColumn(lineName, line).withColumn(signalName, signal);
}
