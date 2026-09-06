import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_SOURCE } from '../contract/columns.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
  rollingValues,
} from '../kernels/rolling.js';
import { wilderValues } from '../kernels/wilder.js';

export interface RelativeVolatilityIndexOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Wilder-smoothing look-back in **bars**. **Default `14`.** */
  period?: number;
  /** Standard-deviation window in **bars**. **Default `10`.** */
  stdevPeriod?: number;
  /** Source column. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'relVol'`** — deliberately not
   *  `rvi`, which is {@link relativeVigorIndex}'s prefix, so the two studies
   *  can sit on one series. */
  output?: Output;
}

/**
 * **Relative Volatility Index** (Donald Dorsey, 1993) — {@link rsi}'s form
 * applied to the **standard deviation** of the price instead of to the price
 * change: of the recent volatility, how much of it happened on up bars?
 *
 * ```
 * σ[i]  = stdev(column over stdevPeriod)          population, ddof = 0
 * up[i] = σ[i] if column[i] >  column[i−1] else 0
 * dn[i] = σ[i] if column[i] <= column[i−1] else 0
 * U = Wilder(up, period);  D = Wilder(dn, period)
 * ${output} = 100 × U / (U + D)
 * ```
 *
 * Appends one column, bounded `0..100`. Dorsey built it as a **confirming**
 * indicator rather than a standalone one: momentum studies and volatility
 * studies fail on different kinds of market, so a breakout that RSI likes and
 * this one does not is a breakout on falling volatility. The conventional
 * thresholds are RSI's — above 50 (or 60) for confirmation of a long, below
 * for a short.
 *
 * ## The name — read this before charting it beside `rvi`
 *
 * "RVI" is used in the wild for **both** this and the Relative *Vigor* Index,
 * which is a completely different study (see {@link relativeVigorIndex}: a
 * ratio of smoothed candle bodies to smoothed ranges, not bounded, not built
 * on σ). So this study's default output is **`relVol`**, not `rvi` — the
 * shorter name stays with the Vigor Index, which claimed it first and whose
 * option is a `prefix` shared with a signal column. The two can therefore
 * both be appended to one series with no `output` juggling, which is the
 * point of not reusing it.
 *
 * ## Which Relative Volatility Index — the corpus flags this **F-AMBIG**
 *
 * Three real forks, all shipped by someone:
 *
 * - **Dorsey's revision (1995), which is what ships**: σ over **10** bars,
 *   Wilder-smoothed over **14**, on the **close**. His 1993 original ran the
 *   whole thing on a 9-bar σ with a 14-bar smoothing; the revision is what
 *   every current write-up states, and both lengths are options here so the
 *   original is `{ period: 14, stdevPeriod: 9 }`.
 * - **The high/low variant**, also Dorsey's: compute the whole index
 *   separately on the highs and on the lows and average the two readings.
 *   That is a second study composed of two of these, and it is deliberately
 *   not an option — a caller who wants it runs this twice with
 *   `column: 'high'` and `column: 'low'` and averages, which is three lines
 *   and is *visible*.
 * - **The EMA-smoothed variant** (TradingView's built-in `rvi` smooths the
 *   two legs with an EMA rather than Wilder's recursion). **This ships
 *   Wilder's**, because Dorsey defined it as "RSI's calculation with σ
 *   substituted for the price change" and RSI's smoothing *is* Wilder's — the
 *   {@link rsi} kernel's own argument, that a recursive average is defined by
 *   its seed as much as by its rate. The two differ materially, not
 *   cosmetically; the measured separation on the oracle input is recorded in
 *   the generator.
 *
 * **TA-Lib has no RVI of either kind**, so the oracle is a pandas
 * replication with the analytic first-valid bar asserted and a measured
 * separation from the EMA-smoothed fork.
 *
 * ## Conventions
 *
 * - **Population σ (`ddof = 0`)** — the package convention
 *   ({@link bollinger}, {@link rollingStdev}, {@link zScore},
 *   {@link historicalVolatility}, and TA-Lib's own `STDDEV`). It comes from
 *   {@link rollingValues}' `stdev`, so it is `rollingStdev`'s number
 *   bit-for-bit rather than a second implementation. A sample σ (`ddof = 1`)
 *   is a constant factor larger and **cancels between the two legs**, so —
 *   unusually — the choice does not move this reading at all; it is stated
 *   for consistency with the rest of the package rather than because it
 *   matters here.
 * - **An unchanged close counts as a *down* bar.** Dorsey's rule is "up if
 *   the close rose", so everything else — including a flat bar — is the other
 *   leg. This is a **deliberate asymmetry against {@link rsi}**, whose
 *   {@link upDownLegValues} split gives a flat bar `0` on *both* legs; there,
 *   a flat bar contributes nothing, here it contributes its σ to the
 *   downside. Both are their authors' definitions and neither is being
 *   reconciled — the fork is stated per study, which is the package's rule
 *   for exactly this.
 *
 * ## Warm-up
 *
 * σ first lands on bar `stdevPeriod − 1`; the legs need a direction as well,
 * so from bar 1; the Wilder seed then steps over the leading gap and lands
 * `period − 1` bars later. The column first appears on
 * **`stdevPeriod + period − 2`** — bar 22 at the defaults. Length-preserving.
 *
 * **Over another study's output the σ starts earlier than that rule
 * suggests**, because it reads the *column* door: its window counts **rows**
 * and computes over the finite cells it holds, so a window with one value
 * reports `σ = 0` rather than nothing ({@link rollingStdev}'s contract, the
 * one {@link historicalVolatility} flags in its own edges). Measured:
 * `relativeVolatilityIndex({ column: 'sma', period: 3, stdevPeriod: 3 })`
 * over an `sma(3)` first lands on bar **5**, one earlier than the
 * `2 + 3 + 3 − 2` the rule alone would give. Pinned by a test.
 *
 * ## Edges
 *
 * - **Scale-invariant, and shift-invariant.** σ is homogeneous of degree one
 *   in price (so scaling cancels between the two legs) and unchanged by
 *   adding a constant, and the up/down test is a comparison of two prices, so
 *   neither transformation moves the reading. Both are pinned by property
 *   tests.
 * - **Bounded `0..100`**, both ends attainable: a window whose every
 *   direction was up reads `100`, and its mirror reads `0`. Pinned.
 * - **A flat window is `0/0` and needs no guard.** `U + D = 0` means every σ
 *   in the smoother's memory was zero, which forces `U = 0` with it — so the
 *   ratio is JavaScript's own `NaN` and the study reports `undefined` on a
 *   halted instrument with no branch (the {@link commodityChannelIndex}
 *   finding: a guard no input can distinguish is dead code). σ is never
 *   negative, so there is no crossing-columns case here either; this is a
 *   genuinely guard-free study, unlike {@link ulcerIndex} and
 *   {@link choppinessIndex} in the same batch.
 * - **A `stdevPeriod` of 1 makes every σ zero**, so the whole column is
 *   `undefined` — honest, and the reason the option is validated as a period
 *   rather than special-cased.
 * - **A misnamed `column` reads all-missing rather than throwing**, and that
 *   is a **reducer**-level split rather than a study-level one worth knowing
 *   about: {@link rollingValues}' `stdev` (and `avg`) take the range-exact
 *   kernel, which reads a column that is not there as all-`NaN`, while its
 *   `max` / `min` fall through to core's sweep, which rejects the name. So
 *   this study answers empty where {@link ulcerIndex} and
 *   {@link verticalHorizontalFilter} — on the same door, one reducer along —
 *   throw. Both are pinned by tests; neither is being reconciled here,
 *   because the fix belongs in the kernel.
 * - **A leading gap shifts the start** rather than emptying the study — the
 *   Wilder kernel steps its seed over one, so running this over another
 *   study's output begins late and is otherwise unaffected. An **interior**
 *   gap **propagates to the end of the series**: a recursion has no state to
 *   carry across a hole. That is {@link rsi}'s and {@link atr}'s behaviour
 *   for the same structural reason, and it is the sharpest difference between
 *   this study and the other six in its batch, all of which are windows and
 *   recover. Fill before smoothing if you need continuity.
 */
export function relativeVolatilityIndex<
  S extends SeriesSchema,
  const Output extends string = 'relVol',
>(
  series: TimeSeries<S>,
  options: RelativeVolatilityIndexOptions<S, Output> = {},
) {
  const period = options.period ?? 14;
  const stdevPeriod = options.stdevPeriod ?? 10;
  assertPeriod(period);
  assertPeriod(stdevPeriod, 'stdevPeriod');

  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const output = (options.output ?? 'relVol') as Output;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const v = columnValues(wide, column);
  // The same population σ `rollingStdev` and `bollinger` read, off the
  // range-exact kernel — one definition of "the standard deviation of this
  // column over n bars" in the package.
  const sigma = rollingValues(wide, column, 'stdev', stdevPeriod);

  // Route σ to one leg or the other by the direction of the bar. This is NOT
  // `upDownLegValues`: that splits a change into its own two parts, and what
  // is split here is a DIFFERENT array (σ) selected by the sign of the
  // change. Bar 0 has no direction, and a missing σ or a missing predecessor
  // leaves both legs unknown ([PND-STUDYBOX]) rather than zero — the
  // distinction `upDownLegValues` documents, applied by hand because the
  // shape differs.
  const length = v.length;
  const up = new Float64Array(length);
  const down = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    const s = sigma[i]!;
    const delta = i === 0 ? NaN : v[i]! - v[i - 1]!;
    if (Number.isNaN(s) || Number.isNaN(delta)) {
      up[i] = NaN;
      down[i] = NaN;
      continue;
    }
    // Strictly greater is up; everything else — a fall AND an unchanged
    // close — is down. Dorsey's rule, and a documented delta from `rsi`.
    up[i] = delta > 0 ? s : 0;
    down[i] = delta > 0 ? 0 : s;
  }

  // `start = 1` says why bar 0 is skipped (no direction), the way `atr` says
  // why its bar 0 is; the kernel would step over the leading `NaN` anyway.
  const smoothedUp = wilderValues(up, period, 1);
  const smoothedDown = wilderValues(down, period, 1);

  const out = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    const u = smoothedUp[i]!;
    // No zero-denominator branch: both legs are non-negative, so a zero total
    // forces a zero numerator and `0/0` is already `NaN`. See "Edges".
    out[i] = (100 * u) / (u + smoothedDown[i]!);
  }
  return series.withColumn(output, out);
}
