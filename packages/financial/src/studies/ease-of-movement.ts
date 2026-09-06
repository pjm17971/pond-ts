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

export interface EaseOfMovementOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Smoothing length in **bars**. **Default `14`.** */
  period?: number;
  /** Which moving average smooths the 1-bar value — any of the shared
   *  {@link MaType} menu. **Default `'sma'`.** */
  maType?: MaType;
  /** Volume divisor in the box ratio. **Default `100_000_000`**
   *  (StockCharts' / ChartIQ's convention). A pure scale factor — see the
   *  docstring. */
  scale?: number;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Volume column. **Default `'volume'`.** */
  volume?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'eom'`.** */
  output?: Output;
}

/**
 * **Ease of Movement** (Richard Arms) — how far the bar's midpoint travelled
 * per unit of volume it took to get there:
 *
 * ```
 * distance  = (high + low)/2 − (prevHigh + prevLow)/2
 * boxRatio  = (volume / scale) / (high − low)
 * eom₁      = distance / boxRatio                       the 1-bar value
 * eom       = MA(eom₁, period)                          Arms' 14-bar SMA
 * ```
 *
 * Arms' idea from his Equivolume charts: a bar that moves the midpoint a
 * long way on little volume in a narrow range moved *easily*, and that is
 * bullish; the same move on heavy volume was hard-won. Large positive
 * readings mean price is rising easily, large negative that it is falling
 * easily, and a value near zero that price is not moving despite the volume.
 * Appends one column.
 *
 * The midpoint is {@link medianPriceValues} — the same kernel
 * {@link awesomeOscillator} reads — and the smoothing is the K2 engine
 * ({@link movingAverageValues}), so `maType` is the whole shared menu rather
 * than a private smoother. `'sma'` is the default because Arms' definition
 * and every publication of it name a simple average.
 *
 * ## The scale constant, and why it is an option
 *
 * `100_000_000` is StockCharts' and ChartIQ's published constant, and it is
 * what ships. It exists only to bring the reading onto a legible axis: the
 * whole expression is `distance · (high − low) · scale / volume`, so `scale`
 * is a **pure linear multiplier** — it cannot change a sign, a crossing, or
 * the shape of the line, and a property test pins that.
 *
 * It is exposed anyway because the right constant depends on the
 * instrument's units, not on taste: 100 million is calibrated to US equity
 * share counts, and a crypto pair quoting fractional volume or a futures
 * contract quoting lots reads as a wall of zeros (or of millions) at that
 * setting. The alternative considered was to fix it and tell callers to
 * multiply the output column themselves — rejected because a caller who
 * needs a different constant would then be silently *incomparable* with the
 * chart package they are reading beside, for the sake of removing an option
 * that has no other effect.
 *
 * ## Definition, verified
 *
 * TA-Lib has no Ease of Movement, so the oracle is a **pandas replication**
 * at `{14, sma}` and `{5, ema}`, with the analytic first valid bar
 * (`period` — see below) asserted and the result separated from a version
 * that drops the `(high − low)` factor, so the box ratio is pinned rather
 * than assumed.
 *
 * ## Edges
 *
 * - **Warm-up is `period` rows.** The 1-bar value needs a previous
 *   midpoint, so bar 0 has none and the K2 array door waits for `period`
 *   finite values. Length-preserving.
 * - **A flat bar (`high === low`) has no value.** The box ratio divides by
 *   the range, so a bar with no range has no box — `x / 0`. Reported as
 *   `undefined` rather than as the `0` the algebraically-simplified form
 *   would produce, which would claim the price did not move when it may
 *   well have. (The simplified form is what the code computes, for one pass
 *   and no cancellation; the guard is what keeps it honest.)
 * - **A bar with zero volume has no value** either — the box ratio is `0`
 *   and the division by it is `±Infinity`, which is not a reading. Both
 *   guards report `undefined`, the package's answer for a zero denominator
 *   everywhere. The zero-volume guard looks redundant on the window MA
 *   types, which mask a non-finite cell anyway — but it is **load-bearing on
 *   the carrying ones**: `smma` is Wilder's recursion and passes what it is
 *   given straight through, so an unguarded `±Infinity` would arrive at
 *   `withColumn`, which rejects an infinity loudly rather than mapping it to
 *   a gap. (Found by the mutation matrix: removing the guard killed no test
 *   until one ran `maType: 'smma'` over a zero-volume bar.)
 * - **A gap in any input costs two bars** (its own and the next, whose
 *   distance reads the missing midpoint) plus whatever the chosen `maType`
 *   costs — window types recover once it leaves the window, the `ema` family
 *   skips the bar, `smma` and `kama` propagate to the end. Stated per type
 *   on {@link movingAverageValues}.
 * - **Scale behaviour, and it is the odd one here:** EOM is **quadratic in
 *   price** — the distance and the range both scale, so scaling every price
 *   by `k` scales the reading by `k²` — **inversely proportional to
 *   volume**, and **linear in `scale`**. All three are pinned by property
 *   tests, because "linear in price like every other absolute study" is the
 *   plausible wrong assumption here.
 */
export function easeOfMovement<
  S extends SeriesSchema,
  const Output extends string = 'eom',
>(series: TimeSeries<S>, options: EaseOfMovementOptions<S, Output> = {}) {
  const period = options.period ?? 14;
  assertPeriod(period);
  const maType = options.maType ?? 'sma';
  assertMaType(maType);
  const scale = options.scale ?? 100_000_000;
  if (!Number.isFinite(scale) || scale <= 0) {
    throw new TypeError(
      `easeOfMovement scale must be a positive finite number, got ${String(scale)}`,
    );
  }
  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const volumeName = (options.volume ?? DEFAULT_OHLCV.volume) as string;
  const output = (options.output ?? 'eom') as Output;
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const high = columnValues(wide, highName);
  const low = columnValues(wide, lowName);
  const volume = columnValues(wide, volumeName);
  const mid = medianPriceValues(high, low);
  const raw = new Float64Array(mid.length).fill(NaN);
  // `distance / boxRatio` written out: distance · range · scale / volume.
  // Bar 0 has no previous midpoint; a flat bar (range 0) and a bar with no
  // volume both have no value — see the docstring. Missing cells are NaN
  // and propagate through the arithmetic on their own ([PND-STUDYBOX]).
  for (let i = 1; i < raw.length; i += 1) {
    const range = high[i]! - low[i]!;
    const v = volume[i]!;
    if (range === 0 || v === 0) continue;
    raw[i] = ((mid[i]! - mid[i - 1]!) * range * scale) / v;
  }
  return series.withColumn(output, movingAverageValues(raw, period, maType));
}
