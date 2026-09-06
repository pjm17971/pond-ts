import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';
import { rollingMeanValues } from '../kernels/rolling-mean.js';
import { medianPriceValues } from '../kernels/typical-price.js';

export interface AwesomeOscillatorOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Fast SMA length in **bars**. **Default `5`.** */
  fastPeriod?: number;
  /** Slow SMA length in **bars**. **Default `34`.** */
  slowPeriod?: number;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Appended column name. **Default `'ao'`.** */
  output?: Output;
}

/**
 * **Awesome Oscillator** (Bill Williams) — the spread between a fast and a
 * slow simple moving average of the **median price**:
 *
 * ```
 * median = (high + low) / 2
 * AO     = SMA(median, fastPeriod) − SMA(median, slowPeriod)     5 and 34
 * ```
 *
 * The median price is the point: AO measures the market's driving force from
 * the *bars*, not from where each bar happened to close, which is why it
 * reads the range's midpoint rather than the close. Conventionally drawn as
 * a histogram coloured by whether the bar rose or fell against the previous
 * one — that comparison is a chart-side decision (assessment §11) and is not
 * a column here.
 *
 * Reads **high and low**, each named by an option defaulting to its
 * `DEFAULT_OHLCV` column. There is no `close` option: the definition never
 * reads one.
 *
 * ## Why no `maType`, and why not `priceOscillator` on a median column
 *
 * Williams' definition names simple moving averages; the 5/34 pair on an SMA
 * *is* the indicator, and an MA-type knob here would let `awesomeOscillator`
 * return something nobody calls an Awesome Oscillator. {@link priceOscillator}
 * is where the MA-type question lives — but it reads a **column**, and the
 * median price is not one, so pointing a caller at
 * `priceOscillator({ column: 'median' })` would mean making them materialise
 * a median-price column first. The two studies stay separate: one is
 * "any column, any average, absolute or percent", this one is a named
 * indicator with a fixed shape.
 *
 * ## Definition source
 *
 * **TA-Lib has no Awesome Oscillator**, so the oracle is a pandas
 * replication of the definition above, with the analytic first-valid bar
 * asserted and the fixture checked to separate the two SMA legs.
 *
 * ## Edges
 *
 * - **Warm-up**: the slow leg's — first value on bar `slowPeriod − 1` (bar
 *   33 at the default 34). Length-preserving; earlier rows `undefined`.
 * - **Both SMAs emit only once their last `slowPeriod` / `fastPeriod` rows
 *   are all finite** ({@link rollingMeanValues}) — a leading gap steps the
 *   window over rather than being averaged — because the median price is a
 *   **derived array**: that is the studies README's rule for derived inputs,
 *   and it is what keeps a leading gap from being averaged as if it were
 *   data. It also means an interior gap masks the windows that contain it
 *   rather than averaging around it — the same call {@link stochastic} makes
 *   for its smoothing.
 * - **An interior gap** in either `high` or `low` costs that bar's median
 *   price, and therefore the `fastPeriod` bars of the fast leg and
 *   `slowPeriod` bars of the slow leg whose windows contain it; the windows
 *   recover once it leaves them.
 * - **Linear in the input, and shift-invariant**: scaling every price by `k`
 *   scales AO by `k` (it is a difference of prices, the {@link macd} side of
 *   the scale pair); adding a constant to every price cancels between the two
 *   legs. Both pinned by property tests.
 * - `fastPeriod` must be **shorter** than `slowPeriod` — swapping them
 *   negates every reading, so it throws rather than silently obliging
 *   ({@link macd}'s rule).
 */
export function awesomeOscillator<
  S extends SeriesSchema,
  const Output extends string = 'ao',
>(series: TimeSeries<S>, options: AwesomeOscillatorOptions<S, Output> = {}) {
  const fastPeriod = options.fastPeriod ?? 5;
  const slowPeriod = options.slowPeriod ?? 34;
  assertPeriod(fastPeriod, 'fastPeriod');
  assertPeriod(slowPeriod, 'slowPeriod');
  if (fastPeriod >= slowPeriod) {
    throw new TypeError(
      `awesomeOscillator fastPeriod (${fastPeriod}) must be shorter than slowPeriod (${slowPeriod})`,
    );
  }
  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const output = (options.output ?? 'ao') as Output;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const median = medianPriceValues(
    columnValues(wide, highName),
    columnValues(wide, lowName),
  );
  const fast = rollingMeanValues(median, fastPeriod);
  const slow = rollingMeanValues(median, slowPeriod);
  const out = new Float64Array(median.length);
  // A missing average is NaN and survives the subtraction ([PND-STUDYBOX]).
  for (let i = 0; i < out.length; i += 1) out[i] = fast[i]! - slow[i]!;

  return series.withColumn(output, out);
}
