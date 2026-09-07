import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import {
  assertMaType,
  movingAverageColumn,
} from '../kernels/moving-average.js';
import type { MaType } from '../kernels/moving-average.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';
import { atrValues } from '../kernels/true-range.js';

export interface StarcBandsOptions<
  S extends SeriesSchema,
  Prefix extends string,
> {
  /** Centre-line look-back in **bars**. **Default `20`.** */
  period?: number;
  /** ATR look-back in **bars**. **Default `15`** (Stoller's own). */
  atrPeriod?: number;
  /** Band half-width in ATRs. **Default `2`.** */
  multiplier?: number;
  /** Centre-line moving average — any of the shared {@link MaType} menu.
   *  **Default `'sma'`** (Stoller's definition names a simple average). */
  maType?: MaType;
  /** High column (ATR input). **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column (ATR input). **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Close column — the centre line's input **and** an ATR input.
   *  **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Column-family prefix — appends `${prefix}Middle` / `${prefix}Upper` /
   *  `${prefix}Lower`. **Default `'starc'`.** */
  prefix?: Prefix;
}

/**
 * **STARC Bands** (Manning Stoller — *Stoller Average Range Channel*) — a
 * moving average of the **close** with bands a multiple of the **average
 * true range** away:
 *
 * ```
 * middle = MA(close, period)                       ${prefix}Middle
 * upper  = middle + multiplier × ATR(atrPeriod)    ${prefix}Upper
 * lower  = middle − multiplier × ATR(atrPeriod)    ${prefix}Lower
 * ```
 *
 * Appends three columns. Reads **high, low and close** (the ATR needs all
 * three; the centre needs the close), each named by an option defaulting to
 * its `DEFAULT_OHLCV` name — the {@link atr} shape.
 *
 * ## Three ATR channels ship, and they are genuinely different studies
 *
 * The package now has {@link keltner}, {@link atrBands} and this one, and
 * the difference between them is worth stating once because "MA ± k·ATR"
 * describes all three:
 *
 * | study        | centre                       | bands           | columns |
 * | ------------ | ---------------------------- | --------------- | ------- |
 * | `keltner`    | MA of **typical price**      | ±k·ATR          | 3       |
 * | `starcBands` | MA of the **close**          | ±k·ATR          | 3       |
 * | `atrBands`   | **an existing column** as-is | ±k·ATR          | 2       |
 *
 * So `starcBands` is not a rename of `atrBands`: `atrBands` draws its bands
 * around a column the caller already has and appends **no** centre, while
 * this study **computes** a moving average that is not otherwise on the
 * series and therefore appends it (the {@link keltner} / {@link bollinger}
 * rule — a computed centre is worth a column, a copy of the caller's own
 * field is not).
 *
 * It **is**, deliberately, the same arithmetic: composed by hand,
 * `starcBands` is `movingAverage(…)` followed by
 * `atrBands({ column: <that average> })`, and a test pins the two equal
 * bar-for-bar rather than leaving the claim in prose. The study ships
 * because that composition needs a scratch column and the corpus names this
 * one (assessment §6.2) — vocabulary over a kernel, which is what this
 * package is.
 *
 * ## Which parameters — the formula is uncontested, the defaults are not
 *
 * Stoller's construction (a simple average of the close, ±2 ATRs) is not in
 * dispute; the parameter set published with it is. Short centres (5–6 bars)
 * appear beside 20-bar ones, and `atrPeriod 15` is the value most commonly
 * quoted with the study. What ships is `period 20`, `atrPeriod 15`,
 * `multiplier 2`, `maType 'sma'` — the 20 chosen to sit beside
 * {@link keltner}'s and {@link bollinger}'s rather than because a vendor
 * defaults to it, and every one of the four is an option. Said here rather
 * than left to be discovered from a chart that does not line up.
 *
 * `maType` is the shared {@link MaType} menu, spelled `maType` because the
 * average is an *ingredient* rather than the output (the `keltner` side of
 * that split). TA-Lib has no STARC to arbitrate; the oracle is a pandas
 * replication over the same `_atr_series` reference TA-Lib's `ATR` is
 * asserted against, so the numbers are TA-Lib's ATR with arithmetic on top.
 *
 * ## Warm-up: per column, not per study
 *
 * The centre starts at the MA's own first bar (`period − 1` for the window
 * types on gap-free input, later for the composed ones — see
 * {@link movingAverageColumn}); the ATR at bar `atrPeriod` (true range needs
 * a previous close, so a `period`-bar average of it lands one bar later than
 * a `period`-bar window would). The **bands therefore start at the later of
 * the two**, and the centre is emitted where it is genuinely defined rather
 * than masked back to them — the {@link macd} per-column rule. At the
 * defaults that is bar 19 for the centre and bar 19 for the bands (the MA's
 * 19 beats the ATR's 15); at `{ period: 5, atrPeriod: 20 }` the centre lands
 * at bar 4 and the bands at bar 20.
 *
 * ## Edges
 *
 * - **In the units of the price**, like {@link atr} and {@link keltner}:
 *   scaling every bar scales the whole channel, and adding a constant
 *   translates it without changing its width. Both pinned by property tests.
 * - **A leading gap shifts the start** rather than emptying the study.
 * - **An interior gap in the close is the Wilder asymmetry**, and the two
 *   halves genuinely differ. The centre takes the K2 engine's **column**
 *   door, where `'sma'` keeps core's count-window contract — `minSamples`
 *   counts **rows**, and `avg` **skips** a missing cell — so the centre is
 *   drawn straight through the gap (over `period − 1` contributors on the
 *   windows containing it). The ATR is Wilder over **true** range, which
 *   reads the *previous* close, so the gap costs the **next** bar's true
 *   range and the recursion never gives it back: the bands are defined on
 *   the gap bar itself and blank from the one after it to the end. Measured
 *   and pinned in `study-missing-cells.test.ts`. The other `maType`s follow
 *   their own rules ({@link movingAverageColumn}): the `ema` family skips
 *   the bar, `smma` and `kama` propagate. A caller who needs continuity
 *   fills before smoothing. The same asymmetry {@link keltner} documents,
 *   though `keltner`'s centre smooths a *derived* typical price and so
 *   loses the bar where this one does not.
 * - **No division anywhere**, so there is no zero-denominator case: a flat
 *   stretch gives `ATR = 0` and a zero-width channel, which is the honest
 *   reading (contrast {@link bollinger}, whose zero-width *statistical* band
 *   is emitted as `undefined`).
 */
export function starcBands<
  S extends SeriesSchema,
  const Prefix extends string = 'starc',
>(series: TimeSeries<S>, options: StarcBandsOptions<S, Prefix> = {}) {
  const period = options.period ?? 20;
  const atrPeriod = options.atrPeriod ?? 15;
  assertPeriod(period);
  assertPeriod(atrPeriod, 'atrPeriod');
  const maType = options.maType ?? 'sma';
  assertMaType(maType);
  const multiplier = options.multiplier ?? 2;
  if (!Number.isFinite(multiplier) || multiplier <= 0) {
    throw new TypeError(
      'starcBands multiplier must be a positive finite number',
    );
  }

  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const closeName = (options.close ?? DEFAULT_OHLCV.close) as string;
  const prefix = (options.prefix ?? 'starc') as Prefix;
  const middleName = `${prefix}Middle` as const;
  const upperName = `${prefix}Upper` as const;
  const lowerName = `${prefix}Lower` as const;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  for (const name of [middleName, upperName, lowerName]) {
    assertNoColumn(wide, name);
  }

  const high = columnValues(wide, highName);
  const low = columnValues(wide, lowName);
  const close = columnValues(wide, closeName);

  // The centre goes through the K2 engine's COLUMN door, not its array
  // door: the input here is a raw price column, not a derived array, so
  // `'sma'` keeps `sma()`'s rows-not-contributors contract and its
  // accelerated path. That is also what makes `starcMiddle` equal
  // `movingAverage({ column: close })` BIT-for-bit — the identity the
  // "it is atrBands around an MA" test relies on, which the array door
  // would break on any column with a missing cell.
  const middle = movingAverageColumn(wide, closeName, period, maType);
  // The same `atrValues` call `atr()` and `atrBands()` make, so
  // `starcUpper − starcMiddle` is `multiplier × atr()` bit-for-bit.
  const atr = atrValues(high, low, close, atrPeriod);

  // A missing centre or ATR is `NaN` ([PND-STUDYBOX]) and propagates through
  // the arithmetic on its own, so the per-column warm-up needs no branch.
  const band = (sign: 1 | -1): Float64Array => {
    const out = new Float64Array(middle.length);
    for (let i = 0; i < out.length; i += 1) {
      out[i] = middle[i]! + sign * multiplier * atr[i]!;
    }
    return out;
  };

  return series
    .withColumn(middleName, middle)
    .withColumn(upperName, band(1))
    .withColumn(lowerName, band(-1));
}
