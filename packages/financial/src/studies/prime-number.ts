import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV, DEFAULT_SOURCE } from '../contract/columns.js';
import {
  nearestPrimeDistanceValues,
  primeBandValues,
} from '../kernels/prime.js';
import { assertNoColumn, columnValues } from '../kernels/rolling.js';

export interface PrimeNumberBandsOptions<
  S extends SeriesSchema,
  Prefix extends string,
> {
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Column-family prefix — appends `${prefix}Upper` / `${prefix}Lower`.
   *  **Default `'pnb'`.** */
  prefix?: Prefix;
}

export interface PrimeNumberOscillatorOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Source column. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'pno'`.** */
  output?: Output;
}

/**
 * **Prime Number Bands** — the prime numbers immediately bracketing each
 * bar:
 *
 * ```
 * ${prefix}Upper = the smallest prime ≥ high
 * ${prefix}Lower = the largest prime ≤ low
 * ```
 *
 * Appends **two** columns and no centre — there is no "average prime" to
 * draw, and the bar itself is the middle. Reads **high and low**, each named
 * by an option defaulting to its `DEFAULT_OHLCV` name.
 *
 * ## What this is
 *
 * It is a **step function of the price level**, not a statistic: the bands
 * hold flat while price moves inside a prime gap and jump when it crosses
 * one, so the chart is a staircase that tightens where primes are dense and
 * widens where they are sparse. Nothing about the series' history enters it
 * — two instruments trading at the same price have identical bands.
 *
 * The corpus lists it (assessment §6.2) and it is perfectly deterministic,
 * which is why it ships; whether the arithmetic of the integers says
 * anything about a market is the reader's business, and this docstring is
 * not going to pretend otherwise. **No TA-Lib function and no vendor
 * agreement to match**: the definition above (bands that *contain* the bar)
 * is the one the corpus names, and the oracle is a pure-Python replication
 * over a sieve.
 *
 * ## Edges
 *
 * - **No warm-up.** Each bar's bands come from its own two prices, so row 0
 *   is defined. Length-preserving.
 * - **A price below 2 has no bands** and both columns read `undefined`
 *   there. Two is the smallest prime; below it there is nothing to bracket
 *   with. That is the whole domain rule, and it is the kernel's — see
 *   `kernels/prime.ts`. Above `Number.MAX_SAFE_INTEGER` likewise: past 2⁵³
 *   a double no longer names consecutive integers.
 * - **Non-integer prices are the normal case** and nothing rounds them: the
 *   upper band searches up from `ceil(high)`, the lower down from
 *   `floor(low)`.
 * - **Neither scale- nor shift-equivariant, and that is not a defect.**
 *   Doubling every price does **not** double the bands (the primes near
 *   `2p` are not twice the primes near `p`), and adding a constant does not
 *   translate them. This is the only study in the package with no
 *   homogeneity property at all, and the property tests assert the
 *   *absence* — that scaling and shifting both genuinely move the reading
 *   off the transformed one — rather than skipping the check.
 * - **The bands always contain the bar** when `low ≤ high`, by
 *   construction, and the upper band is `≥` the lower whenever both exist.
 *   Redirecting `high`/`low` at crossed columns can invert them; nothing
 *   reorders.
 * - **A gap in either input costs that column on that bar only.**
 * - **Cost grows with the price LEVEL**, not the series length — the one
 *   operator here that does, and steeply. Measured at 1M bars: **78 ms** at
 *   ordinary equity prices, **6.5 s** at ~1e7, and per bar 7.7 ms at ~1e12 and 290 ms at ~1e15 (see the kernel's table; ~1e9 is the practical ceiling). Know that number before
 *   running this over a high-priced instrument at scale; the kernel's cost
 *   note explains why there is no sieve and what would change that.
 */
export function primeNumberBands<
  S extends SeriesSchema,
  const Prefix extends string = 'pnb',
>(series: TimeSeries<S>, options: PrimeNumberBandsOptions<S, Prefix> = {}) {
  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const prefix = (options.prefix ?? 'pnb') as Prefix;
  const upperName = `${prefix}Upper` as const;
  const lowerName = `${prefix}Lower` as const;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, upperName);
  assertNoColumn(wide, lowerName);

  const { upper, lower } = primeBandValues(
    columnValues(wide, highName),
    columnValues(wide, lowName),
  );

  return series.withColumn(upperName, upper).withColumn(lowerName, lower);
}

/**
 * **Prime Number Oscillator** — how far the price sits from the prime
 * nearest it:
 *
 * ```
 * pno = price − nearestPrime(price)
 * ```
 *
 * Positive when the price is **above** the prime nearest it, negative below,
 * and exactly `0` when the price *is* an integer prime. Appends one column,
 * default `'pno'`.
 *
 * ## The convention, pinned
 *
 * The published description of this study is "the difference between the
 * price and the nearest prime number", which fixes the magnitude and leaves
 * two things open. Both are decided here rather than left to be discovered:
 *
 * - **The sign is `price − prime`**, following the phrase's own word order.
 *   The mirror convention (`prime − price`) is exactly `−pno`; a caller who
 *   wants it negates the column.
 * - **"Nearest" is nearest on either side**, not the next prime above.
 *   Where two primes are equidistant — `6` sits one from both `5` and `7` —
 *   **the tie goes to the lower**, so `pno(6) = +1`. Arbitrary, and stated
 *   and tested rather than emergent.
 *
 * No TA-Lib function; the oracle is a pure-Python replication over a sieve
 * (the generator does not assume `sympy` is installed).
 *
 * ## Edges
 *
 * - **No warm-up**; row 0 is defined. Length-preserving.
 * - **Bounded by half the local prime gap** in either direction, so the
 *   reading widens slowly as prices rise — it is roughly `±½·ln(p)` at price
 *   `p`, not a fixed band. A reading that looks bigger at higher prices is
 *   the number line, not the market.
 * - **A price below 2 is outside the domain** and reads `undefined`, as do
 *   values past `Number.MAX_SAFE_INTEGER` — the kernel's rule, shared with
 *   {@link primeNumberBands} so the two agree about where the study stops.
 * - **Neither scale- nor shift-invariant**, and the property tests assert
 *   the absence rather than skipping it. Multiplying every price by `k` does
 *   not multiply the reading by anything, and adding a constant does not
 *   leave it alone — the primes do not move with the data.
 * - **Runs over any column**, including another study's output — but mind
 *   the magnitude: a dollar-volume column at 1e12 costs ~8 ms **per bar**
 *   (the kernel's cost table), so "any column" means any price-like one; the
 *   composition rule is the same as everywhere else, and a source warm-up
 *   simply carries through (`NaN` in, `NaN` out).
 * - **Cost grows with the price level**, steeply: measured at 1M bars,
 *   **55 ms** at ordinary equity prices and **6.5 s** at ~1e7. See
 *   {@link primeNumberBands} and the kernel's cost note.
 */
export function primeNumberOscillator<
  S extends SeriesSchema,
  const Output extends string = 'pno',
>(
  series: TimeSeries<S>,
  options: PrimeNumberOscillatorOptions<S, Output> = {},
) {
  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const output = (options.output ?? 'pno') as Output;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  return series.withColumn(
    output,
    nearestPrimeDistanceValues(columnValues(wide, column)),
  );
}
