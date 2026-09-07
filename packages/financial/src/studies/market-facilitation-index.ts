import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import { assertNoColumn, columnValues } from '../kernels/rolling.js';
import { barRangeValues } from '../kernels/typical-price.js';

export interface MarketFacilitationIndexOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Volume column. **Default `'volume'`.** */
  volume?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'bwmfi'`** — *not* `'mfi'`,
   *  which {@link moneyFlowIndex} already uses. See the docstring. */
  output?: Output;
}

/**
 * **Market Facilitation Index** (Bill Williams) — how much price movement
 * each unit of volume bought:
 *
 * ```
 * bwmfi = (high − low) / volume
 * ```
 *
 * Williams reads it **against the change in volume**, not on its own: a bar
 * where both the index and volume rose is a move the market is funding
 * ("green"); one where the index rose while volume fell is a move on thin
 * participation ("fake"); and so on for the other two quadrants. The study
 * appends the index; the quadrant is a comparison a caller makes with it and
 * `percentChange({ column: 'volume' })`.
 *
 * Appends one column. Reads **high, low and volume**, each named by an
 * option defaulting to its `DEFAULT_OHLCV` name.
 *
 * ## The output is `bwmfi`, and the collision is the reason
 *
 * {@link moneyFlowIndex} — Quong & Soudack's RSI-on-money-flow, an entirely
 * different indicator — already appends **`mfi`**, which is the abbreviation
 * both studies are published under. Two studies cannot share a default
 * output name: appending both to one series would throw, and whichever
 * landed first would silently define what a downstream `'mfi'` column meant.
 *
 * So this one defaults to **`bwmfi`** — Bill Williams' initials in front of
 * the abbreviation, which is what MetaTrader and several other packages call
 * it for exactly the same reason. The older study keeps `mfi` because it
 * shipped first and renaming it would break every caller. `output` overrides
 * either. (Same problem, same treatment as the two "RVI"s already in the
 * package: `relativeVigorIndex` keeps `rvi` and `relativeVolatilityIndex`
 * appends `relVol`.)
 *
 * ## No `scale` option
 *
 * The reading is the **raw ratio**, which on equity-sized volumes is a small
 * number (a 0.70-wide bar on 1500 shares is `0.00047`). Vendors multiply by
 * a constant to bring it onto a legible axis, and they do not agree on which
 * — unlike {@link easeOfMovement}, whose `100_000_000` is a *published*
 * constant that StockCharts and ChartIQ share, and which is exposed here for
 * that reason. There is no such constant for this study, so adding a `scale`
 * knob would be inventing a default rather than matching one; a caller who
 * wants a legible axis multiplies the column, and a caller comparing against
 * a chart has to know that chart's factor either way.
 *
 * No TA-Lib function; the oracle is a pandas replication.
 *
 * ## Edges
 *
 * - **No warm-up.** Each bar's value comes from its own three inputs, so row
 *   0 is defined. Length-preserving.
 * - **A bar with zero volume has no value** — the division is `x / 0`, which
 *   is `±Infinity` and not a reading. `undefined`, the package's answer for
 *   a zero denominator everywhere. **The guard is live**: the division is the
 *   last thing before `withColumn`, which rejects an infinity loudly rather
 *   than mapping it to a gap, so without it a halted bar would throw.
 * - **A flat bar (`high === low`) reads `0`**, not `undefined` — the
 *   numerator is a genuine zero (the bar covered no ground) over a real
 *   volume, so there is no `0/0` unless the volume is zero too, and that
 *   case is caught by the guard above.
 * - **Linear in price and inversely proportional to volume**: scaling every
 *   price by `k` scales the reading by `k`, and scaling every volume by `k`
 *   divides it by `k`. It is **shift-invariant in price** (the range is a
 *   difference). All three pinned by property tests — "linear in price like
 *   every other absolute study" is only two thirds of the story.
 * - **A gap in any of the three costs that bar** and nothing else.
 * - **A negative range is reported honestly** (redirect `high`/`low` at
 *   crossed columns and the index goes negative); nothing clamps, as
 *   {@link barRangeValues} documents.
 */
export function marketFacilitationIndex<
  S extends SeriesSchema,
  const Output extends string = 'bwmfi',
>(
  series: TimeSeries<S>,
  options: MarketFacilitationIndexOptions<S, Output> = {},
) {
  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const volumeName = (options.volume ?? DEFAULT_OHLCV.volume) as string;
  const output = (options.output ?? 'bwmfi') as Output;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  // The same `barRangeValues` kernel `chaikinVolatility` and `massIndex`
  // read — plain range, deliberately not true range (Williams' definition
  // is the bar's own span; the fork is documented on the kernel).
  const range = barRangeValues(
    columnValues(wide, highName),
    columnValues(wide, lowName),
  );
  const volume = columnValues(wide, volumeName);
  const out = new Float64Array(range.length);
  for (let i = 0; i < out.length; i += 1) {
    const v = volume[i]!;
    // `v === 0` is the halted bar: a real range over no volume is
    // ±Infinity, which `withColumn` rejects rather than treating as a gap.
    // A missing volume is NaN, which is not `=== 0`, so it falls through to
    // the division and propagates ([PND-STUDYBOX]).
    out[i] = v === 0 ? NaN : range[i]! / v;
  }

  return series.withColumn(output, out);
}
