import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_SOURCE } from '../contract/columns.js';
import { assertNoColumn, assertPeriod } from '../kernels/rolling.js';
import {
  assertMaType,
  movingAverageColumn,
} from '../kernels/moving-average.js';
import type { MaType } from '../kernels/moving-average.js';

/** How the spread between the two moving averages is expressed. */
export type PriceOscillatorMode = 'percent' | 'absolute';

export interface PriceOscillatorOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Fast moving-average length in **bars**. **Default `12`.** */
  fastPeriod?: number;
  /** Slow moving-average length in **bars**. **Default `26`.** */
  slowPeriod?: number;
  /** Which moving average — any of the shared {@link MaType} menu.
   *  **Default `'ema'`.** */
  maType?: MaType;
  /** `'percent'` — `100 · (fast − slow) / slow`, TA-Lib's **PPO**;
   *  `'absolute'` — `fast − slow` in the source's own units, TA-Lib's
   *  **APO**. **Default `'percent'`.** */
  mode?: PriceOscillatorMode;
  /** Source column. **Default `'close'`.** */
  column?: NumericColumnNameForSchema<S>;
  /** Appended column name. **Default `'priceOsc'`.** */
  output?: Output;
}

/**
 * **Price Oscillator** — the spread between a fast and a slow moving average
 * of one column, either as a **percent of the slow average** (`mode:
 * 'percent'`, the default) or in the **source's own units** (`mode:
 * 'absolute'`):
 *
 * ```
 * percent    100 · (MA(fastPeriod) − MA(slowPeriod)) / MA(slowPeriod)
 * absolute         MA(fastPeriod) − MA(slowPeriod)
 * ```
 *
 * Both averages come from the shared K2 engine, so `maType` is the whole
 * {@link MaType} menu rather than a private pair of smoothers.
 *
 * ## Why `mode` defaults to `'percent'`
 *
 * TA-Lib ships the two forms as **two functions** — `APO` (absolute) and
 * `PPO` (percent) — over identical inputs, so the name alone does not settle
 * a default. Two things do:
 *
 * - **The absolute form at the default parameters is already shipped.**
 *   `priceOscillator({ mode: 'absolute', maType: 'ema', fastPeriod: 12,
 *   slowPeriod: 26 })` is {@link macd}'s `macdLine`, bar for bar — a test
 *   pins that identity. Defaulting to it would make the headline call of a
 *   new study a rename of an existing column, which is what the studies
 *   README's step 0 exists to catch. Defaulting to `'percent'` makes the
 *   default call the thing MACD cannot give you.
 * - **The percent form is the comparable one.** It is scale-invariant
 *   (a property test pins it), so a reading of `1.8` means the same thing on
 *   a $4 stock and a $4,000 one; the absolute form is in price units and is
 *   linear in them (pinned too).
 *
 * The knob is a `mode` string rather than a `percent: boolean` so the two
 * settings read as what they are at the call site, and rather than two
 * exported functions (`apo` / `ppo`) because one study with one knob is the
 * shape {@link stochastic} already set with `slowing: 1` for the fast
 * stochastic.
 *
 * ## Definition and TA-Lib
 *
 * TA-Lib's `APO`/`PPO(matype)` are exactly `MA(fast) − MA(slow)` and
 * `100·(MA(fast) − MA(slow))/MA(slow)` over the same `MA(matype)` — verified
 * in the oracle generator, which rebuilds both from `talib.MA` and asserts
 * they agree to `8.9e-16` before using them as the reference.
 *
 * On the types TA-Lib ships that are **not** in the EMA family, we match it
 * outright: `{ maType: 'sma', fastPeriod: 5, slowPeriod: 13, mode:
 * 'absolute' }` agrees with `APO(matype=0)` to `7.1e-14` with an identical
 * warm-up mask (oracle case).
 *
 * On `'ema'` (and `dema`/`tema`) the **{@link macd} seed precedent applies**:
 * pond's EMA is seeded on the first sample, TA-Lib's on the SMA of the first
 * `n`, so the values differ by a decaying transient. Reseeding here would
 * make `priceOscillator({ maType: 'ema' })` disagree with `ema()` and with
 * `macd()` inside this package — a worse surprise than a sub-percent
 * divergence from a vendor whose bar-for-bar parity is an explicit non-goal.
 * The generator therefore splits the check the way the K2 engine's does:
 * the **formula** is rebuilt on TA-Lib's own SMA seed and required to match
 * `PPO`/`APO` exactly (matched to `2.8e-14`), and the **seed transient** is
 * bounded separately over the last 20 shared bars. Measured on the oracle
 * input at `{12, 26, ema, percent}`: **6.61% of scale at the first shared
 * bar, 0.41% at its worst over the last 20** (0.089% at the last bar), masks
 * identical. The bound discriminates: the same replication run on a wrong
 * rate is 5.09% (`2/(n+2)`), 5.99% (`2/n`) or 44.4% (`1/n`) over those same
 * 20 bars.
 *
 * ## Edges
 *
 * - **Warm-up is the slower average's**, per the K2 engine's table — bar
 *   `slowPeriod − 1` for the window types and `ema`, later for `dema` /
 *   `tema` / `hull` / `kama` / `zlema`. Length-preserving; earlier rows are
 *   `undefined`.
 * - **A leading gap shifts the start** for every `maType` except `'sma'`,
 *   which keeps `sma()`'s row-counting window (the column door's documented
 *   asymmetry). Running over another study's output therefore starts late
 *   rather than coming back empty.
 * - **An interior gap** costs whatever the chosen `maType` costs — window
 *   types recover once it leaves the window, the `ema` family skips the bar,
 *   `smma` and `kama` propagate to the end. Stated per type on
 *   {@link movingAverageValues}, not averaged into a slogan.
 * - **A zero slow average** (only reachable on a column that can be zero or
 *   negative — a return series, another oscillator) makes the percent form a
 *   `0/0` or a division by zero. It reports **no value**, not `Infinity` and
 *   not `0`: the ratio is genuinely undefined there, and a `±Infinity`
 *   reaching a chart's y-domain is worse than a gap. The absolute form has no
 *   such case and keeps reporting the spread.
 * - `fastPeriod` must be **shorter** than `slowPeriod` — a caller who swaps
 *   them wants the negated series, and silently obliging would make the sign
 *   of every reading meaningless. Throws, like {@link macd}.
 */
export function priceOscillator<
  S extends SeriesSchema,
  const Output extends string = 'priceOsc',
>(series: TimeSeries<S>, options: PriceOscillatorOptions<S, Output> = {}) {
  const fastPeriod = options.fastPeriod ?? 12;
  const slowPeriod = options.slowPeriod ?? 26;
  assertPeriod(fastPeriod, 'fastPeriod');
  assertPeriod(slowPeriod, 'slowPeriod');
  if (fastPeriod >= slowPeriod) {
    throw new TypeError(
      `priceOscillator fastPeriod (${fastPeriod}) must be shorter than slowPeriod (${slowPeriod})`,
    );
  }
  const maType = options.maType ?? 'ema';
  assertMaType(maType);
  const mode = options.mode ?? 'percent';
  if (mode !== 'percent' && mode !== 'absolute') {
    throw new TypeError(
      `priceOscillator mode must be 'percent' or 'absolute', got '${String(mode)}'`,
    );
  }

  const column = (options.column ?? DEFAULT_SOURCE) as string;
  const output = (options.output ?? 'priceOsc') as Output;
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const fast = movingAverageColumn(wide, column, fastPeriod, maType);
  const slow = movingAverageColumn(wide, column, slowPeriod, maType);
  const length = fast.length;
  const out = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    const f = fast[i]!;
    const sl = slow[i]!;
    if (mode === 'absolute') {
      // A missing average is NaN and survives the subtraction on its own
      // ([PND-STUDYBOX]) — no per-cell `undefined` check needed.
      out[i] = f - sl;
    } else {
      // `sl === 0` would give ±Infinity (or NaN when the numerator is 0 too);
      // both are reported as "no value" instead. See the docstring.
      out[i] = sl === 0 ? NaN : (100 * (f - sl)) / sl;
    }
  }

  return series.withColumn(output, out);
}
