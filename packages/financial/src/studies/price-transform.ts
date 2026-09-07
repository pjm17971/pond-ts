import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import { assertNoColumn, columnValues } from '../kernels/rolling.js';
import {
  averagePriceValues,
  medianPriceValues,
  typicalPriceValues,
  weightedCloseValues,
} from '../kernels/typical-price.js';

/**
 * Options shared by the three **high/low/close** price transforms
 * ({@link typicalPrice}, {@link medianPrice}, {@link weightedClose}).
 *
 * `medianPrice` never reads `close`, and passing it is harmless — the option
 * is on the shared interface because these are one family with one shape,
 * and three near-identical option types spelled out separately would be
 * three places to drift. (The one that reads the **open** has its own type:
 * {@link AveragePriceOptions}.)
 */
export interface PriceTransformOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Close column. **Default `'close'`.** Unused by {@link medianPrice}. */
  close?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default the transform's own name.** */
  output?: Output;
}

/** Options for {@link averagePrice} — the one transform that reads the open. */
export interface AveragePriceOptions<
  S extends SeriesSchema,
  Output extends string,
> extends PriceTransformOptions<S, Output> {
  /** Open column. **Default `'open'`.** */
  open?: NumericColumnNameForSchema<S>;
}

/* -------------------------------------------------------------------------- */
/* The four K3 price transforms (assessment §6.8).                            */
/*                                                                            */
/* Each reduces one bar to one number and appends it. They are the smallest   */
/* studies in the package and they share one shape, so they share one file —  */
/* the `rolling-stat.ts` / `moving-average.ts` precedent (a family of         */
/* one-line studies over a shared body lives together; a study with its own   */
/* substantial docstring and edges gets its own file).                        */
/*                                                                            */
/* All four are **exact** against TA-Lib (`TYPPRICE` / `MEDPRICE` /           */
/* `WCLPRICE` / `AVGPRICE`), asserted bar-for-bar in the oracle, and all four */
/* have **no warm-up at all**: a bar's own prices are all they read, so bar 0 */
/* is defined and the column is missing only where an input is.               */
/* -------------------------------------------------------------------------- */

/**
 * **Typical price** — `(high + low + close) / 3`. TA-Lib's `TYPPRICE`,
 * matched bar-for-bar.
 *
 * The one-number summary of a bar that the volume-weighted and money-flow
 * studies price on: {@link vwap} weights it by volume, {@link moneyFlowIndex}
 * splits it up/down, {@link commodityChannelIndex} measures its deviation and
 * {@link keltner}'s centre line is a moving average of it. This study appends
 * it as a **column**, which is the point — those studies derive it privately,
 * and a caller who wants to chart it, or to run *another* study over it
 * (`sma({ column: 'typicalPrice' })`), needs it on the series.
 *
 * Appends one column, default `'typicalPrice'`. Reads **high, low and
 * close**, each named by an option defaulting to its `DEFAULT_OHLCV` name —
 * the {@link atr} shape.
 *
 * ## Edges
 *
 * - **No warm-up.** Every bar carries its own answer, so row 0 is defined.
 *   Length-preserving, trivially.
 * - **Linear and shift-equivariant**: scaling every price by `k` scales the
 *   output by `k`, and adding `d` to every price adds `d` to it — it is a
 *   weighted *mean* of prices, so it lives in the same units. Both pinned by
 *   property tests.
 * - **A gap in any of the three costs exactly that bar** and nothing else;
 *   there is no recursion and no window to carry it.
 * - **Nothing is clamped or ordered.** Redirect `high` and `low` at columns
 *   that cross and the mean is still their mean — a study cannot know which
 *   of its inputs the caller meant.
 */
export function typicalPrice<
  S extends SeriesSchema,
  const Output extends string = 'typicalPrice',
>(series: TimeSeries<S>, options: PriceTransformOptions<S, Output> = {}) {
  const output = (options.output ?? 'typicalPrice') as Output;
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);
  const values = typicalPriceValues(
    columnValues(wide, (options.high ?? DEFAULT_OHLCV.high) as string),
    columnValues(wide, (options.low ?? DEFAULT_OHLCV.low) as string),
    columnValues(wide, (options.close ?? DEFAULT_OHLCV.close) as string),
  );
  return series.withColumn(output, values);
}

/**
 * **Median price** — `(high + low) / 2`, the midpoint of the bar's range.
 * TA-Lib's `MEDPRICE`, matched bar-for-bar.
 *
 * The *other* one-number bar summary the corpus keeps reaching for:
 * {@link awesomeOscillator} is a difference of two SMAs of it,
 * {@link easeOfMovement} measures how far it travelled, and
 * {@link highLowBands} is a percent envelope around a moving average of it.
 *
 * Appends one column, default `'medianPrice'`. **Reads only high and low** —
 * `close` is accepted on the shared options type and ignored, so a caller
 * who spreads one options object across the family is not surprised by a
 * throw.
 *
 * Same edges as {@link typicalPrice}: no warm-up, linear in price and
 * shift-equivariant, a gap costs one bar, nothing is clamped (a `low` above
 * a `high` still averages).
 */
export function medianPrice<
  S extends SeriesSchema,
  const Output extends string = 'medianPrice',
>(series: TimeSeries<S>, options: PriceTransformOptions<S, Output> = {}) {
  const output = (options.output ?? 'medianPrice') as Output;
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);
  const values = medianPriceValues(
    columnValues(wide, (options.high ?? DEFAULT_OHLCV.high) as string),
    columnValues(wide, (options.low ?? DEFAULT_OHLCV.low) as string),
  );
  return series.withColumn(output, values);
}

/**
 * **Weighted close** — `(high + low + 2·close) / 4`. TA-Lib's `WCLPRICE`,
 * matched bar-for-bar.
 *
 * The typical price's sibling, with the close given twice the weight of
 * either extreme: the argument is that the price a bar *settled* at carries
 * more information than the two it merely traded through. It sits between
 * {@link typicalPrice} (close weighted 1/3) and the close itself (1), which
 * is the whole spectrum this transform family spans.
 *
 * Appends one column, default `'weightedClose'`. Reads **high, low and
 * close**.
 *
 * Same edges as {@link typicalPrice}: no warm-up, linear in price and
 * shift-equivariant, a gap costs one bar, nothing is clamped.
 */
export function weightedClose<
  S extends SeriesSchema,
  const Output extends string = 'weightedClose',
>(series: TimeSeries<S>, options: PriceTransformOptions<S, Output> = {}) {
  const output = (options.output ?? 'weightedClose') as Output;
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);
  const values = weightedCloseValues(
    columnValues(wide, (options.high ?? DEFAULT_OHLCV.high) as string),
    columnValues(wide, (options.low ?? DEFAULT_OHLCV.low) as string),
    columnValues(wide, (options.close ?? DEFAULT_OHLCV.close) as string),
  );
  return series.withColumn(output, values);
}

/**
 * **Average price** — `(open + high + low + close) / 4`, the unweighted mean
 * of all four bar prices. TA-Lib's `AVGPRICE`, matched bar-for-bar.
 *
 * The only member of this family that reads the **open**, and therefore the
 * only one that is not computable from a high/low/close feed. Appends one
 * column, default `'averagePrice'`.
 *
 * The summation order inside the kernel is TA-Lib's (`h + l + c + o`) rather
 * than the OHLC order the name suggests — see {@link averagePriceValues};
 * summing in OHLC order instead moves the reading by up to **2.8e-14** on the
 * oracle's fixture, which is enough to lose bar-for-bar `AVGPRICE` agreement
 * while changing nothing anyone could read off a chart.
 *
 * Same edges as {@link typicalPrice}: no warm-up, linear in price and
 * shift-equivariant, a gap in **any of the four** costs that bar, nothing is
 * clamped.
 */
export function averagePrice<
  S extends SeriesSchema,
  const Output extends string = 'averagePrice',
>(series: TimeSeries<S>, options: AveragePriceOptions<S, Output> = {}) {
  const output = (options.output ?? 'averagePrice') as Output;
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);
  const values = averagePriceValues(
    columnValues(wide, (options.open ?? DEFAULT_OHLCV.open) as string),
    columnValues(wide, (options.high ?? DEFAULT_OHLCV.high) as string),
    columnValues(wide, (options.low ?? DEFAULT_OHLCV.low) as string),
    columnValues(wide, (options.close ?? DEFAULT_OHLCV.close) as string),
  );
  return series.withColumn(output, values);
}
