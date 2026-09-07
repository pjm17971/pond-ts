/**
 * **Prime-number neighbourhoods of a price.** Kernel for the corpus' two
 * prime studies — Prime Number Bands (assessment §6.2) and the Prime Number
 * Oscillator (§6.3) — which ask, per bar, for the prime immediately above a
 * price, the one immediately below it, or the nearer of the two.
 *
 * It is the odd one out in this package: every other kernel here computes a
 * statistic of the data, and this one computes a property of the *number
 * line* that the data happens to land on. The studies are in the corpus and
 * they are perfectly deterministic, so they ship; what they mean is the
 * reader's business.
 *
 * ## The domain, and where the edges go
 *
 * **A value below 2 has no prime neighbourhood and every function here
 * returns `NaN` for it.** Two is the smallest prime, so there is no prime
 * below any such value, and calling the prime *above* it "2" would be a
 * distance measured across an empty half-line rather than a reading — a
 * price of −1000 is not "1002 away from its nearest prime" in any sense a
 * chart should draw. One rule for all three functions, so the bands and the
 * oscillator agree about where the study stops.
 *
 * **Non-integer prices are fine and are the normal case.** Primes are
 * integers, so the prime above `100.37` is `101` and the one below it `97`;
 * the oscillator's reading is then the fractional distance to whichever is
 * nearer. Nothing rounds the input.
 *
 * **`NaN` marks a gap** ([PND-STUDYBOX]) and comes straight back out.
 * Anything above `Number.MAX_SAFE_INTEGER` also returns `NaN`: past 2⁵³ the
 * integers are no longer exactly representable, so "the next prime" is not a
 * question a double can be asked.
 *
 * ## Cost
 *
 * Primality is **trial division** by 2, 3 and then the `6k ± 1` ladder up to
 * `√n`, so a single test is `O(√n / log n)` divisions and a search walks the
 * prime gap above (or below) the value, which averages `O(log n)` candidates.
 * The per-bar cost therefore **grows with the price level**, not with the
 * series length — the one operator in this package whose cost depends on the
 * *magnitude* of the data, and the growth is steep.
 *
 * **Measured** (`scripts/perf-studies.mjs`, 1M bars, medians):
 *
 * ```
 *                          price ~1e2     price ~1e7
 * primeNumberBands            78 ms         6465 ms
 * primeNumberOscillator       55 ms         6548 ms
 * ```
 *
 * At ordinary equity prices both studies sit between `sma()` (26 ms) and
 * `bollinger()` (72 ms) — unremarkable. At 1e7 they cost **~6.5 seconds per
 * million bars**, an ~85× jump, which is the number to know before running
 * either over a high-priced instrument at scale. It is published here rather
 * than buried because it is the sharpest edge in the package.
 *
 * The cost keeps growing as √n past that, and the domain guard is only
 * `MAX_SAFE_INTEGER`, so the **usable** ceiling matters more than the legal
 * one. Measured per bar, one `nearestPrime` call: 0.002 ms at ~1e2, 0.02 ms
 * at ~1e7, 0.13 ms at ~1e9, 0.9 ms at ~1e10, **7.7 ms at ~1e12** and
 * **290 ms at ~1e15** (Layer-2 review of #711 measured the same shape). A
 * dollar-volume or market-cap column sits at 1e10–1e12: ten thousand bars
 * of it is a minute, not a blink. Treat ~1e9 as the practical ceiling for
 * interactive use; above it the study is correct but slow, and the sieve
 * this docstring describes is the fix if a consumer needs it.
 *
 * **No sieve — considered, and rejected on the numbers above.** A segmented
 * sieve over the observed price range would collapse the 1e7 column to
 * roughly the 1e2 one, and it needs: a `Uint8Array` the width of the price
 * range (megabytes for an instrument that has trended, and unbounded for one
 * that has trended a long way), a fallback for the ranges too wide to sieve,
 * and a second code path to test — all for two studies whose reading is a
 * property of the integers rather than of the market. Trial division is
 * twenty lines and its cost is now measured and documented. **If a caller
 * reports this, the sieve is the next step and this note is the brief for
 * it.**
 */

/** The largest value these functions will answer for — past it, doubles do
 *  not represent consecutive integers, so "the next prime" is meaningless. */
const MAX_PRIME_INPUT = Number.MAX_SAFE_INTEGER;

/**
 * Is `n` — a non-negative **integer** — prime? Trial division by 2, 3, then
 * the `6k ± 1` ladder to `√n`.
 *
 * Callers in this file only ever pass integers in `[2, MAX_PRIME_INPUT]`; the
 * guards below are cheap and keep the function honest as a standalone.
 */
export function isPrime(n: number): boolean {
  if (!Number.isInteger(n) || n < 2 || n > MAX_PRIME_INPUT) return false;
  if (n < 4) return true; // 2 and 3
  if (n % 2 === 0 || n % 3 === 0) return false;
  // Every prime above 3 is 6k ± 1, so step the ladder rather than testing
  // every odd number: two thirds of the odd candidates are skipped.
  for (let f = 5; f * f <= n; f += 6) {
    if (n % f === 0 || n % (f + 2) === 0) return false;
  }
  return true;
}

/**
 * The **smallest prime ≥ `x`** — the value Prime Number Bands draws above
 * the bar's high. `NaN` for a gap, for `x < 2` (see the file's domain note)
 * and for anything past `MAX_PRIME_INPUT`.
 *
 * `x` need not be an integer: the search starts at `Math.ceil(x)`.
 */
export function primeAtOrAbove(x: number): number {
  if (!Number.isFinite(x) || x < 2 || x > MAX_PRIME_INPUT) return NaN;
  for (let n = Math.ceil(x); n <= MAX_PRIME_INPUT; n += 1) {
    if (isPrime(n)) return n;
  }
  return NaN;
}

/**
 * The **largest prime ≤ `x`** — the value Prime Number Bands draws below the
 * bar's low. `NaN` under the same conditions as {@link primeAtOrAbove}; the
 * search starts at `Math.floor(x)`.
 */
export function primeAtOrBelow(x: number): number {
  if (!Number.isFinite(x) || x < 2 || x > MAX_PRIME_INPUT) return NaN;
  for (let n = Math.floor(x); n >= 2; n -= 1) {
    if (isPrime(n)) return n;
  }
  return NaN;
}

/**
 * The **prime nearest `x`**, whichever side it falls on — what the Prime
 * Number Oscillator measures its distance to.
 *
 * **A tie goes to the LOWER prime.** `6` is one away from both `5` and `7`,
 * and something has to break it; the lower one is chosen so the rule is
 * stated rather than left to whichever comparison happened to be written
 * first, and a test pins it. `NaN` under the same conditions as
 * {@link primeAtOrAbove}.
 *
 * **It carries no domain guard of its own, deliberately.** Outside the
 * domain — a gap, `x < 2`, or anything past `MAX_PRIME_INPUT` — both
 * brackets below return `NaN`, the comparison against a `NaN` is `false`,
 * and the (also `NaN`) upper bracket is returned. A duplicate guard here
 * was written first and **the mutation matrix removed it without killing a
 * single test**: it was dead code, so it is gone rather than tested. The
 * same reasoning covers the one region where the two brackets could
 * disagree — the last 110 integers below 2⁵³, above the largest prime a
 * double can name — which reads `NaN` by the same path.
 */
export function nearestPrime(x: number): number {
  const below = primeAtOrBelow(x);
  const above = primeAtOrAbove(x);
  // `<=` is the tie-break: equal distances take the lower prime. A `NaN` on
  // either side makes this `false` and returns `above`, which is the
  // domain rule falling out of the arithmetic (see above).
  return x - below <= above - x ? below : above;
}

/**
 * The two band arrays in **one pass** — `upper[i]` is the smallest prime at
 * or above `high[i]`, `lower[i]` the largest at or below `low[i]`.
 *
 * The two columns are independent (each reads its own input), so this is one
 * loop rather than one shared search; it exists so the study stays free of a
 * data loop, per the studies README.
 *
 * O(N) calls, each `O(log p · √p / log p)` — see the file's cost note.
 */
export function primeBandValues(
  high: Float64Array,
  low: Float64Array,
): { upper: Float64Array; lower: Float64Array } {
  const length = high.length;
  const upper = new Float64Array(length);
  const lower = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    upper[i] = primeAtOrAbove(high[i]!);
    lower[i] = primeAtOrBelow(low[i]!);
  }
  return { upper, lower };
}

/**
 * The **signed distance from each value to its nearest prime**,
 * `value − nearestPrime(value)` — the Prime Number Oscillator's reading.
 *
 * Positive means the value sits *above* the prime nearest it, negative
 * below, and exactly `0` means the value **is** an integer prime. Bounded by
 * half the local prime gap in either direction, so it widens slowly as
 * prices rise.
 */
export function nearestPrimeDistanceValues(values: Float64Array): Float64Array {
  const length = values.length;
  const out = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    out[i] = values[i]! - nearestPrime(values[i]!);
  }
  return out;
}
