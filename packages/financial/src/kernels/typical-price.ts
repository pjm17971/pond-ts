/**
 * **Typical price** — `(high + low + close) / 3`, the one-number summary of
 * a bar that the volume-weighted and money-flow studies price on: VWAP
 * weights it by volume, and CCI, MFI and the Keltner centre line all read
 * it. A named, reusable quantity, so it lives here rather than in any one
 * of them.
 *
 * `NaN` marks a gap ([PND-STUDYBOX]) and propagates through the sum, so a
 * bar with any of its three prices missing has no typical price.
 *
 * O(N), one pass, one allocation.
 */
export function typicalPriceValues(
  high: Float64Array,
  low: Float64Array,
  close: Float64Array,
): Float64Array {
  const length = high.length;
  const out = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    out[i] = (high[i]! + low[i]! + close[i]!) / 3;
  }
  return out;
}

/**
 * **Median price** — `(high + low) / 2`, the midpoint of a bar's range and
 * the *other* one-number bar summary the corpus keeps asking for: the
 * Awesome Oscillator is a difference of two SMAs of it, Bill Williams'
 * Alligator smooths it with three displaced SMMAs, the Gator histogram is
 * built from those, and High-Low Bands are a percent envelope around an MA
 * of it (assessment §6.1 / §6.2).
 *
 * It lives beside {@link typicalPriceValues} rather than in a file of its own
 * because it is the same idea at a different arity — a named per-bar price
 * summary — and because the alternative is a `(h + l) / 2` loop inside a
 * study, which is exactly what the studies README's "studies contain no data
 * loop" rule exists to stop. One consumer ships today ({@link
 * awesomeOscillator}); the three named above are the reason it is exported
 * rather than kept private to it.
 *
 * `NaN` marks a gap ([PND-STUDYBOX]) and propagates through the sum, so a bar
 * missing either price has no median price.
 *
 * O(N), one pass, one allocation.
 */
export function medianPriceValues(
  high: Float64Array,
  low: Float64Array,
): Float64Array {
  const length = high.length;
  const out = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    out[i] = (high[i]! + low[i]!) / 2;
  }
  return out;
}

/**
 * **Bar range** — `high − low`, the span a bar covered *within itself*, and
 * the third per-bar summary this file names (after {@link
 * typicalPriceValues} and {@link medianPriceValues}).
 *
 * ## Why it is here rather than inside a study
 *
 * Two studies want exactly this array and nothing else from the bar:
 * {@link chaikinVolatility} smooths it with an EMA and reads that EMA's rate
 * of change, and {@link massIndex} takes the ratio of one EMA of it to a
 * second EMA of the first. Sharing the derivation is what stops the two from
 * disagreeing about what "the range" is — the studies README's rule that a
 * study is options-validation plus kernel calls.
 *
 * **This is plain range, not {@link trueRangeValues}.** True range widens a
 * bar that opened away from the previous close; this does not, so a gap up
 * shows here only as whatever the bar traded through afterwards. Both
 * definitions are in the corpus and the two families do not mix: the
 * volatility studies built on Wilder's work (ATR, Keltner, the Choppiness
 * Index) take *true* range, while Chaikin's and Dorsey's take the plain one,
 * because that is what each author defined. Neither study exposes a knob to
 * swap them — that would be two indicators behind a flag.
 *
 * `NaN` marks a gap ([PND-STUDYBOX]) and propagates through the subtraction,
 * so a bar missing either price has no range. Nothing clamps the sign: a
 * caller who redirects `high` and `low` at two columns that cross gets
 * negative ranges, honestly, and each consumer documents what that means for
 * it.
 *
 * O(N), one pass, one allocation.
 */
export function barRangeValues(
  high: Float64Array,
  low: Float64Array,
): Float64Array {
  const length = high.length;
  const out = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    out[i] = high[i]! - low[i]!;
  }
  return out;
}

/**
 * **Weighted close** — `(high + low + 2·close) / 4`, the per-bar price
 * summary that counts the close twice. TA-Lib's `WCLPRICE`.
 *
 * The fourth member of this file's family, after {@link typicalPriceValues},
 * {@link medianPriceValues} and {@link barRangeValues}: a bar reduced to one
 * number, with the close given twice the weight of either extreme on the
 * argument that the price the bar *settled* at says more about it than the
 * two it merely touched.
 *
 * ## Why it lives here with only one consumer
 *
 * The two-consumer rule this package applies to kernel helpers would keep
 * this inside `weightedClose` — it has exactly one caller today. It is here
 * anyway because **this file _is_ the named per-bar price-summary family**:
 * two of the four already lived here before this one did, and splitting the
 * family so `(h+l+c)/3` is a kernel while `(h+l+2c)/4` is a loop inside a
 * study would leave a reader hunting for the second half. It is **not**
 * exported from the package barrel, so it adds no public surface — the same
 * treatment `alphaEmaValues` gets.
 *
 * **The summation order is TA-Lib's**, `(h + l + c·2) / 4`, not a
 * rearrangement of it: floating-point addition does not associate, so
 * reordering the terms costs the bar-for-bar `WCLPRICE` agreement the oracle
 * asserts.
 *
 * `NaN` marks a gap ([PND-STUDYBOX]) and propagates through the sum, so a bar
 * with any of its three prices missing has no weighted close.
 *
 * O(N), one pass, one allocation.
 */
export function weightedCloseValues(
  high: Float64Array,
  low: Float64Array,
  close: Float64Array,
): Float64Array {
  const length = high.length;
  const out = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    out[i] = (high[i]! + low[i]! + close[i]! * 2) / 4;
  }
  return out;
}

/**
 * **Average price** — `(high + low + close + open) / 4`, the mean of all four
 * bar prices. TA-Lib's `AVGPRICE`, and the only member of this family that
 * reads the **open**.
 *
 * Here rather than inside its study for the reason {@link
 * weightedCloseValues} gives, and with the same **TA-Lib summation order**
 * (`h + l + c + o`, *not* the OHLC order the name suggests) so the oracle's
 * `AVGPRICE` agreement is bar-for-bar exact: summing in OHLC order instead
 * moves the reading by up to **2.8e-14** on the oracle's own fixture
 * (measured — `scripts/oracle/generate.py`'s `average_price` asserts both the
 * exact agreement and that gap), which is the last-bit artefact of addition
 * not associating rather than a different definition.
 *
 * `NaN` marks a gap ([PND-STUDYBOX]) and propagates through the sum, so a bar
 * with any of its four prices missing has no average price.
 *
 * O(N), one pass, one allocation.
 */
export function averagePriceValues(
  open: Float64Array,
  high: Float64Array,
  low: Float64Array,
  close: Float64Array,
): Float64Array {
  const length = high.length;
  const out = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    out[i] = (high[i]! + low[i]! + close[i]! + open[i]!) / 4;
  }
  return out;
}
