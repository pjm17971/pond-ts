import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_SOURCE } from '../contract/columns.js';
import {
  assertColumn,
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';

export interface PerformanceIndexOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Look-back in **bars** — the growth of each side is measured over this
   *  many bars. **Default `20`.** */
  period?: number;
  /** Source column. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** The comparison column — **required**, and a column **on this same
   *  series**. Join the benchmark in first. */
  benchmark: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'perf'`.** */
  output?: Output;
}

/**
 * **Performance Index** — how much `column` grew over the last `period` bars
 * as a multiple of how much `benchmark` grew over the same bars:
 *
 * ```
 * ${output} = (column[i] / column[i−period]) / (benchmark[i] / benchmark[i−period])
 * ```
 *
 * Appends one column that oscillates around **`1`**: `1.05` is five percent
 * of out-performance accumulated over the window, `0.95` five percent of
 * under-performance, `1` exact parity. `undefined` for the first `period`
 * rows — the look-back reads a **bar**, not a window, so the warm-up is
 * `period` and not `period − 1` ({@link percentChange}'s rule).
 *
 * It is {@link priceRelative} with the arbitrary level divided out: the raw
 * ratio's height depends on the two instruments' price units and says
 * nothing, while this asks the only question a level can answer — *relative
 * to `period` bars ago*.
 *
 * ## Which "Performance Index" — F-AMBIG, and the fork is named
 *
 * The corpus (`docs/notes/financial-indicators-assessment-2026-07.md` §6.7)
 * carries this from a vendor study menu with the note "normalized relative
 * performance", and more than one thing ships under the name:
 *
 * - **What ships here is the LOOK-BACK form** above — each side's own
 *   `period`-bar growth, divided. It is exactly the missing member of this
 *   family: `(perf − 1) × 100` is `percentChange(priceRelative, period)`
 *   **identically** (the `benchmark` terms cancel), which is asserted by a
 *   test, so the study adds a name and a normalisation rather than new math.
 * - **A moving-average form is published under the same name** — Trading
 *   Technologies gives `PI = (close / benchmark) × (MA(benchmark) /
 *   MA(close))`, i.e. the price relative measured against a *smoothed*
 *   baseline rather than a lagged one
 *   (<https://library.tradingtechnologies.com/trade/chrt-ti-performance-index.html>).
 *   That variant is **not** shipped because it is already a composition of
 *   two shipped primitives — {@link priceRelative} and a moving average —
 *   whereas the look-back form is the one this family lacked. If you want
 *   it: `sma` each side, then two `priceRelative` calls and a ratio.
 * - **The scaling is a presentation fork.** Some vendors publish `× 100`
 *   (100 = parity) or `− 1` / `(x − 1) × 100` (0 = parity). The **ratio**
 *   ships, matching {@link priceRelative} beside it, so the two lines in this
 *   family read on the same kind of axis; the percent form is one
 *   subtraction away and is spelled out above.
 *
 * ## The comparison series is a COLUMN, not a second `TimeSeries`
 *
 * Join the benchmark in first and name its column — see
 * {@link correlation}'s docstring for the recipe and the reasoning. A
 * missing `benchmark` column throws.
 *
 * ## Edges
 *
 * - **No TA-Lib function**, so the oracle is a pandas replication at
 *   `period` 20 and 5, with the analytic first valid bar (`period`) asserted
 *   and a measured separation from the **difference** form
 *   (`colGrowth − benchGrowth`), the substitution that keeps the curve's
 *   shape and moves every value.
 * - **Three zero guards, all LIVE**, because all three divisions are the
 *   study's own last steps and `withColumn` throws on a non-finite value
 *   rather than recording it as a gap: a zero `column[i−period]`
 *   (`x/0 = Infinity`), a zero `benchmark[i]` (the benchmark's growth is
 *   `Infinity`, so the reading would be `0`) and a zero
 *   `benchmark[i−period]` (the benchmark's growth is `0`, so the reading
 *   would be `Infinity`). Each has its own unit test. As in
 *   {@link percentChangeValues} the test is `=== 0`, not `<= 0` — a ratio
 *   against a negative level is defined if unusual.
 * - **Invariant to an independent SCALE of either column, not to a shift.**
 *   Both growths are ratios, so `k · column` cancels exactly; adding a
 *   constant does not cancel and moves every reading. Both pinned, the
 *   second as a deliberate inequality.
 * - **A missing cell blanks its own row and the row `period` bars later**,
 *   and nothing else — a window kernel's recovery rule with a look-back of
 *   one bar on each side.
 * - **`benchmark` must differ from `column`**: the reading would be `1`
 *   everywhere.
 */
export function performanceIndex<
  S extends SeriesSchema,
  const Output extends string = 'perf',
>(series: TimeSeries<S>, options: PerformanceIndexOptions<S, Output>) {
  const period = options.period ?? 20;
  assertPeriod(period);
  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const benchmark = options.benchmark as string;
  const output = (options.output ?? 'perf') as Output;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  if (benchmark === column) {
    throw new TypeError(
      `performanceIndex benchmark '${benchmark}' is the same column as 'column'; the reading would be 1 on every bar`,
    );
  }
  assertColumn(wide, benchmark, 'benchmark');
  assertNoColumn(wide, output);

  const value = columnValues(wide, column);
  const base = columnValues(wide, benchmark);
  const length = value.length;
  // The warm-up is the LOOP BOUND, not a branch inside it. Written as an
  // `if (i < period) continue` it is unobservable — an out-of-range
  // look-back on a `Float64Array` reads `undefined`, whose arithmetic is
  // already `NaN` — so the mutation matrix reports it as a survivor either
  // way, and stating it in the bounds at least leaves nothing that looks
  // like a live guard. (`percentChangeValues` has the same property; its
  // branch predates this note.)
  const out = new Float64Array(length).fill(NaN);
  for (let i = period; i < length; i += 1) {
    const valuePrevious = value[i - period]!;
    const basePrevious = base[i - period]!;
    const baseNow = base[i]!;
    // All three guards are LIVE: every division below is this study's own
    // last arithmetic, so an `Infinity` (or a wrong `0`) would reach
    // `withColumn`. See "Three zero guards" above.
    if (valuePrevious === 0 || basePrevious === 0 || baseNow === 0) {
      out[i] = NaN;
      continue;
    }
    out[i] = (value[i]! / valuePrevious) * (basePrevious / baseNow);
  }
  return series.withColumn(output, out);
}
