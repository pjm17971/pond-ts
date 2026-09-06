import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import { accumulationDistributionValues } from '../kernels/close-location.js';
import { movingAverageValues } from '../kernels/moving-average.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';

export interface ChaikinOscillatorOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Fast EMA span in **bars**. **Default `3`.** */
  fastPeriod?: number;
  /** Slow EMA span in **bars**. **Default `10`.** */
  slowPeriod?: number;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Close column. **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Volume column. **Default `'volume'`.** */
  volume?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'chaikinOsc'`.** */
  output?: Output;
}

/**
 * **Chaikin Oscillator** — a MACD *of the Accumulation/Distribution line*:
 *
 * ```
 * chaikinOsc = EMA(AD, fastPeriod) − EMA(AD, slowPeriod)        3 and 10
 * ```
 *
 * {@link accumulationDistribution} is a level whose absolute value means
 * nothing; this reads its **momentum**, so a positive oscillator says
 * accumulation is accelerating and a cross of zero is the signal Chaikin
 * named it for. Appends one column, `undefined` until the slow EMA has its
 * `slowPeriod` samples.
 *
 * The A/D line comes from {@link accumulationDistributionValues} — the same
 * array the standalone study appends, not a second derivation — and both
 * EMAs from the K2 engine's array door ({@link movingAverageValues}), so
 * this study owns no loop of its own.
 *
 * ## Definition, verified — and the seed, for once, agrees
 *
 * **TA-Lib's `ADOSC`, exactly**: cross-checked bar-for-bar in the oracle
 * fixture at `{3, 10}` and `{4, 12}` (delta `0`, identical warm-up masks).
 *
 * That is worth a sentence, because every other EMA-family study here
 * carries a documented seed delta from TA-Lib ({@link macd}, {@link trix},
 * {@link priceOscillator}: pond seeds an EMA on the **first sample**, TA-Lib
 * on the SMA of the first `n`, and the difference is a decaying transient).
 * `ADOSC` is the exception in TA-Lib's own library — its C implementation
 * seeds **both** EMAs with the first A/D value and then runs the recursion,
 * which is pond's convention exactly. Measured on 40 bars: pond's seed
 * matches `ADOSC` to `0.0`, while the SMA-seeded reconstruction of the same
 * formula differs by up to `294.8`. So there is no transient to bound here
 * and the oracle asserts equality.
 *
 * ## Edges
 *
 * - **Warm-up is the slow EMA's** — `undefined` for the first
 *   `slowPeriod − 1` rows, length-preserving.
 * - **A leading gap shifts the A/D seed**, so the oscillator starts late
 *   rather than coming back empty.
 * - **An interior gap ends the line** (a flat bar does not — it adds `0`).
 *   The A/D level is unknown from there on
 *   ({@link accumulationDistributionValues}), and so is every average of it.
 *   This is the {@link obv} asymmetry inherited whole, and the documented
 *   delta from TA-Lib's flat-bar handling comes with it.
 * - **Linear in volume, invariant under an affine change of price** — the
 *   A/D line's properties, preserved by the difference of two averages.
 *   Pinned by property tests.
 * - `fastPeriod` must be **shorter** than `slowPeriod`; a caller who swaps
 *   them wants the negated series, and obliging silently would make the sign
 *   of every reading meaningless ({@link priceOscillator}'s rule).
 */
export function chaikinOscillator<
  S extends SeriesSchema,
  const Output extends string = 'chaikinOsc',
>(series: TimeSeries<S>, options: ChaikinOscillatorOptions<S, Output> = {}) {
  const fastPeriod = options.fastPeriod ?? 3;
  const slowPeriod = options.slowPeriod ?? 10;
  assertPeriod(fastPeriod, 'fastPeriod');
  assertPeriod(slowPeriod, 'slowPeriod');
  if (fastPeriod >= slowPeriod) {
    throw new TypeError(
      `chaikinOscillator fastPeriod (${fastPeriod}) must be shorter than slowPeriod (${slowPeriod})`,
    );
  }
  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const closeName = (options.close ?? DEFAULT_OHLCV.close) as string;
  const volumeName = (options.volume ?? DEFAULT_OHLCV.volume) as string;
  const output = (options.output ?? 'chaikinOsc') as Output;
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const ad = accumulationDistributionValues(
    columnValues(wide, highName),
    columnValues(wide, lowName),
    columnValues(wide, closeName),
    columnValues(wide, volumeName),
  );
  const fast = movingAverageValues(ad, fastPeriod, 'ema');
  const slow = movingAverageValues(ad, slowPeriod, 'ema');
  // Missing cells are NaN and survive the subtraction on their own
  // ([PND-STUDYBOX]) — the warm-up needs no explicit mask.
  const out = new Float64Array(ad.length);
  for (let i = 0; i < out.length; i += 1) out[i] = fast[i]! - slow[i]!;
  return series.withColumn(output, out);
}
