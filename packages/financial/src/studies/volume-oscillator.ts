import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import type { MaType } from '../kernels/moving-average.js';
import { priceOscillator } from './price-oscillator.js';

export interface VolumeOscillatorOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Fast moving-average length in **bars**. **Default `5`.** */
  fastPeriod?: number;
  /** Slow moving-average length in **bars**. **Default `10`.** */
  slowPeriod?: number;
  /** Which moving average — any of the shared {@link MaType} menu.
   *  **Default `'sma'`.** */
  maType?: MaType;
  /** Volume column. **Default `'volume'`.** */
  volume?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'volOsc'`.** */
  output?: Output;
}

/**
 * **Volume Oscillator** — the spread between a fast and a slow moving average
 * **of volume**, as a percent of the slow one:
 *
 * ```
 * volOsc = 100 · (MA(volume, fastPeriod) − MA(volume, slowPeriod))
 *              / MA(volume, slowPeriod)
 * ```
 *
 * Volume's own trend, made comparable: positive means recent activity is
 * running above its baseline (a move with participation behind it), negative
 * that it is drying up. Because it is a percent it reads the same on a stock
 * trading thousands of shares and one trading millions. Appends one column.
 *
 * ## This is `priceOscillator` pointed at volume — literally
 *
 * The formula above **is** {@link priceOscillator}'s `'percent'` mode
 * (TA-Lib's `PPO` shape) with `column: 'volume'`, so this study *calls* it
 * rather than restating the arithmetic:
 *
 * ```ts
 * volumeOscillator(bars, { fastPeriod: 5, slowPeriod: 10 })
 * // ≡ priceOscillator(bars, {
 * //     column: 'volume', mode: 'percent', maType: 'sma',
 * //     fastPeriod: 5, slowPeriod: 10, output: 'volOsc',
 * //   })
 * ```
 *
 * A test pins that identity, so the two can never drift. What the wrapper
 * adds is the **name and the defaults**, and those are the whole point: the
 * corpus lists the Volume Oscillator as its own study with a `5 / 10` SMA
 * pair, against the Price Oscillator's `12 / 26` EMA pair — a caller reaching
 * for "the volume oscillator" and getting `priceOscillator`'s defaults over
 * volume would get a differently-named indicator. Step 0 of the studies
 * README asks whether a formula is already shipped; when it is, and only the
 * vocabulary is new, a thin alias is the honest answer — not a second
 * implementation, and not a silence that leaves the study undiscoverable.
 *
 * The `'absolute'` mode is deliberately **not** re-exposed: a difference of
 * two volume averages is a share count, which is what the percent form
 * exists to normalise away. A caller who wants it has `priceOscillator({
 * column: 'volume', mode: 'absolute' })`.
 *
 * ## Definition, verified
 *
 * TA-Lib has no volume oscillator, so the oracle is a **pandas replication**
 * at `{5, 10, sma}` and `{4, 12, ema}` — cases that pin what the delegation
 * cannot be checked for by `priceOscillator`'s own cases: that this study
 * reads the **volume** column and applies **these** defaults.
 *
 * ## Edges
 *
 * Inherited from {@link priceOscillator}, and identical to them:
 *
 * - **Warm-up is the slow average's**, per the chosen `maType`'s table on
 *   {@link movingAverageValues}; length-preserving.
 * - **A zero slow average** — a whole window of zero-volume bars — reports
 *   **`undefined`**, not `Infinity` and not `0`. Untraded bars are the one
 *   realistic way to reach a zero denominator anywhere in this package.
 * - **An interior gap** costs whatever the `maType` costs; window types
 *   recover once it leaves the window.
 * - **Invariant under scaling volume** (both averages scale together) and
 *   completely **independent of price** — it never reads one. Pinned by
 *   property tests.
 * - `fastPeriod` must be **shorter** than `slowPeriod` (throws).
 */
export function volumeOscillator<
  S extends SeriesSchema,
  const Output extends string = 'volOsc',
>(series: TimeSeries<S>, options: VolumeOscillatorOptions<S, Output> = {}) {
  return priceOscillator(series, {
    fastPeriod: options.fastPeriod ?? 5,
    slowPeriod: options.slowPeriod ?? 10,
    maType: options.maType ?? 'sma',
    mode: 'percent',
    column: (options.volume ??
      DEFAULT_OHLCV.volume) as NumericColumnNameForSchema<S>,
    output: (options.output ?? 'volOsc') as Output,
  });
}
