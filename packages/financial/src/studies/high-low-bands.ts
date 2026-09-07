import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import {
  assertMaType,
  movingAverageValues,
} from '../kernels/moving-average.js';
import type { MaType } from '../kernels/moving-average.js';
import {
  assertNoColumn,
  assertPeriod,
  columnValues,
} from '../kernels/rolling.js';
import { medianPriceValues } from '../kernels/typical-price.js';

export interface HighLowBandsOptions<
  S extends SeriesSchema,
  Prefix extends string,
> {
  /** Centre-line look-back in **bars**. **Default `10`.** */
  period?: number;
  /** Band half-width as a **percent** of the centre line. **Default `1`.**
   *  (`envelope` spells the same quantity `percent`; see the docstring.) */
  percent?: number;
  /** Centre-line moving average — any of the shared {@link MaType} menu.
   *  **Default `'trima'`.** */
  maType?: MaType;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Column-family prefix — appends `${prefix}Middle` / `${prefix}Upper` /
   *  `${prefix}Lower`. **Default `'hlb'`.** */
  prefix?: Prefix;
}

/**
 * **High Low Bands** — a smoothed **median price** with bands a fixed
 * percentage either side of it:
 *
 * ```
 * middle = MA((high + low) / 2, period)      ${prefix}Middle
 * upper  = middle × (1 + percent/100)          ${prefix}Upper
 * lower  = middle × (1 − percent/100)          ${prefix}Lower
 * ```
 *
 * Appends three columns. Reads **high and low** — never the close, which is
 * what separates it from {@link envelope} (a percent band around a smoothed
 * *close*) and from {@link starcBands} (a *volatility* band around one).
 *
 * ## What this is, exactly — and its relation to `envelope`
 *
 * The arithmetic is `envelope`'s applied to a different centre. With
 * {@link medianPrice} on the series, this study **is**
 * `envelope({ column: 'medianPrice', maType, percent })`, and a test
 * pins the two equal bar-for-bar rather than leaving that in prose.
 *
 * The one place the identity can part company is `maType: 'sma'` over a
 * series with **missing cells**: `envelope` reads a column and takes the K2
 * engine's column door (rows, not contributors), while this study smooths a
 * *derived* array and takes the array door (`period` finite values). At the
 * default `'trima'`, and for every type but `sma`/`ema`, both routes are the
 * same call and the identity is bit-exact.
 *
 * It ships anyway, for the reason this package ships vocabulary at all: the
 * composition needs a scratch column on the series (and a caller who does
 * not want `medianPrice` in their schema has to drop it again), the corpus
 * names the study (assessment §6.2), and "High Low Bands" on a chart legend
 * should not require the reader to reconstruct which column the envelope was
 * pointed at. The **step-0 finding is recorded rather than hidden**: nothing
 * new is computed here.
 *
 * ## `percent`, spelled as `envelope` spells it
 *
 * The half-width option is **`percent`** — the same number, in the same
 * units (percent of the centre), as {@link envelope}'s `percent`, and the
 * same spelling on purpose. ChartIQ draws this indicator with the label
 * "shift"; the builder first followed that label, and the integrator
 * renamed it: a package that spells one knob two ways is a footgun, and a
 * consumer who has written `envelope({ percent })` should be able to write
 * `highLowBands({ percent })` without looking it up. (The `type` /
 * `maType` split is different in kind — those name different roles.)
 *
 * ## The default `maType` is a choice, not a citation
 *
 * The corpus entry names only "MA of median price × (1 ± shift%)" and does
 * not pin the average. The **triangular** moving average is the one this
 * study is conventionally drawn with — it double-smooths, which is the point
 * of a band you want to read as a slow envelope rather than a fast one — so
 * `'trima'` is the default. The whole shared {@link MaType} menu is
 * available; `maType` (not `type`) because the average is an ingredient
 * rather than the output.
 *
 * TA-Lib has no High Low Bands, so the oracle is a pandas replication with
 * the analytic first valid bar asserted and the identity against
 * `envelope`-over-`medianPrice` checked on the TypeScript side, where the
 * two can be compared as the same expression rather than through a fixture.
 *
 * ## Warm-up and edges
 *
 * - **All three columns start together**, at the chosen average's own first
 *   bar over the derived median-price array. `trima` at `period 10` lands at
 *   bar 9; the composed types land later (see {@link movingAverageValues}).
 *   The median price is a **derived** array, so it goes through the K2
 *   engine's ARRAY door — every window type, `sma` included, waits for
 *   `period` **finite values**, so a gap blanks every window containing it
 *   rather than averaging a short window.
 * - **Multiplicative, not additive**: the bands are a percentage of the
 *   centre, so the channel is **wider at higher prices**. That makes the
 *   study **linear in price** (scaling scales all three columns) but **not**
 *   shift-equivariant — adding a constant moves the centre by it and the
 *   bands by more. Both pinned by property tests, and it is the honest
 *   consequence of a percent band ({@link starcBands}, an ATR band, is the
 *   translation-invariant alternative).
 * - **A negative centre inverts the bands.** `middle × (1 + percent/100)` is
 *   *below* `middle` when the centre is negative — reachable only by
 *   redirecting `high`/`low` at columns that can go negative. Nothing
 *   reorders them; `envelope` has the same property, and clamping would hide
 *   an input problem rather than fix one.
 * - **No division**, so no zero-denominator case: a flat stretch gives three
 *   parallel lines a fixed percentage apart.
 */
export function highLowBands<
  S extends SeriesSchema,
  const Prefix extends string = 'hlb',
>(series: TimeSeries<S>, options: HighLowBandsOptions<S, Prefix> = {}) {
  const period = options.period ?? 10;
  assertPeriod(period);
  const maType = options.maType ?? 'trima';
  assertMaType(maType);
  const percent = options.percent ?? 1;
  if (!Number.isFinite(percent) || percent <= 0) {
    throw new TypeError(
      'highLowBands percent must be a positive finite number',
    );
  }

  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const prefix = (options.prefix ?? 'hlb') as Prefix;
  const middleName = `${prefix}Middle` as const;
  const upperName = `${prefix}Upper` as const;
  const lowerName = `${prefix}Lower` as const;

  const wide = series as unknown as TimeSeries<SeriesSchema>;
  for (const name of [middleName, upperName, lowerName]) {
    assertNoColumn(wide, name);
  }

  // The same `medianPriceValues` kernel `medianPrice` and
  // `awesomeOscillator` read, so the three cannot disagree about what the
  // midpoint is.
  const middle = movingAverageValues(
    medianPriceValues(
      columnValues(wide, highName),
      columnValues(wide, lowName),
    ),
    period,
    maType,
  );

  const f = percent / 100;
  // A missing centre is `NaN` ([PND-STUDYBOX]) and survives the multiply, so
  // the warm-up needs no per-cell check.
  const scale = (factor: number): Float64Array => {
    const out = new Float64Array(middle.length);
    for (let i = 0; i < out.length; i += 1) out[i] = middle[i]! * factor;
    return out;
  };

  return series
    .withColumn(middleName, middle)
    .withColumn(upperName, scale(1 + f))
    .withColumn(lowerName, scale(1 - f));
}
