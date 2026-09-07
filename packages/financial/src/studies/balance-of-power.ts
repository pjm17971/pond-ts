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

export interface BalanceOfPowerOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** Optional smoothing length in **bars**. **Omitted by default** — the
   *  study is TA-Lib's raw per-bar `BOP` unless this is set. See the
   *  docstring's "one indicator, two conventions" note. */
  period?: number;
  /** Which moving average smooths the per-bar ratio — any of the shared
   *  {@link MaType} menu. **Only meaningful with `period`**, and passing it
   *  without one **throws** rather than silently doing nothing. */
  maType?: MaType;
  /** Open column. **Default `'open'`.** */
  open?: NumericColumnNameForSchema<S>;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Close column. **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'bop'`.** */
  output?: Output;
}

/**
 * **Balance of Power** (Igor Livshin) — how much of the bar's range the
 * buyers or the sellers actually took:
 *
 * ```
 * bop = (close − open) / (high − low)
 * ```
 *
 * The body over the range. `+1` is a bar that opened on its low and closed
 * on its high (buyers took the whole thing), `−1` the mirror, `0` a bar that
 * closed where it opened however far it ranged in between. Bounded `[−1, +1]`
 * on real bars, because the body cannot exceed the range.
 *
 * Appends one column, default `'bop'`. Reads **open, high, low and close**,
 * each named by an option defaulting to its `DEFAULT_OHLCV` name.
 *
 * ## One indicator, two conventions — the raw form ships (F-AMBIG)
 *
 * TA-Lib's `BOP` is the **raw per-bar ratio** above, with no smoothing at
 * all; ChartIQ and several other packages draw it through a moving average,
 * because the raw line is noisy enough to be hard to read. Both are in
 * circulation and they are the same indicator, so this study ships the raw
 * form **by default** — that is the one an oracle can pin bar-for-bar
 * against TA-Lib — and exposes `period` to get the smoothed one:
 *
 * ```ts
 * balanceOfPower(bars);                              // TA-Lib BOP, exact
 * balanceOfPower(bars, { period: 14 });              // the smoothed form
 * balanceOfPower(bars, { period: 14, maType: 'ema' });
 * ```
 *
 * `maType` is the shared {@link MaType} menu (spelled `maType`, not `type`,
 * because the average is an *ingredient* here rather than the output —
 * the `keltner` / `envelope` side of that split). It **throws** when passed
 * without a `period`: an option that silently does nothing is worse than an
 * error, and there is no sensible "smooth with an EMA but over no bars".
 *
 * ## A flat bar is `0`, not missing — and that is the algebra, not TA-Lib
 *
 * `high === low` makes the denominator zero. It also makes the numerator
 * **exactly** zero on any real bar: a bar with no range traded at one price,
 * so its open and close are that price and `close − open` is 0. The value is
 * therefore `0` — the limit from either side, and the honest reading (no
 * side took anything, because there was nothing to take). This is the
 * {@link clvValues} rule and the same argument #699 settled there, **not** a
 * copy of TA-Lib's guard, though the two agree: TA-Lib returns `0.0` when
 * `high − low < 1e-8`.
 *
 * The one deliberate delta is that threshold. TA-Lib treats any range under
 * `1e-8` as flat; this study tests `range === 0` exactly, so a bar with a
 * genuine but sub-`1e-8` range reports the (large) ratio rather than `0`.
 * At the scale of real prices such a bar is a rounding artefact either way;
 * the exact test is used because an epsilon that is right for dollars is
 * wrong for a column of basis points, and the oracle asserts the fixture
 * has no bar in the gap so the TA-Lib agreement is exact where it is
 * claimed. A **redirected** `open` or `close` (pointed at a smoothed column,
 * say) can put a non-zero body over a zero range; that reads `0` too, as
 * `clvValues` does — a bar with no range has nowhere to place anything.
 *
 * ## Edges
 *
 * - **No warm-up in the raw form**: bar 0 is defined. With `period`, the
 *   warm-up is the chosen average's own over the derived array (`period − 1`
 *   for the window types, later for the composed ones — see
 *   {@link movingAverageValues}).
 * - **Scale- AND shift-invariant**: the body and the range are both
 *   differences of prices, so a common multiplier cancels in the ratio and a
 *   common offset cancels in each difference. This is the {@link rsi} side of
 *   the scale pair, not the {@link atr} side — both pinned by property tests.
 * - **A gap in any of the four inputs costs that bar** in the raw form, plus
 *   whatever the chosen `maType` costs when smoothing (window types recover,
 *   the `ema` family skips, `smma` and `kama` propagate).
 * - **The zero-range guard is at the output** and therefore live: the
 *   division is the last thing that happens before `withColumn` in the raw
 *   form, and the guard is what stops `±Infinity` reaching the carrying
 *   `maType`s in the smoothed one.
 */
export function balanceOfPower<
  S extends SeriesSchema,
  const Output extends string = 'bop',
>(series: TimeSeries<S>, options: BalanceOfPowerOptions<S, Output> = {}) {
  const period = options.period;
  if (period !== undefined) assertPeriod(period);
  if (options.maType !== undefined) {
    assertMaType(options.maType);
    if (period === undefined) {
      throw new TypeError(
        'balanceOfPower maType needs a period — without one the study is the ' +
          'raw per-bar ratio and there is nothing to smooth',
      );
    }
  }
  const output = (options.output ?? 'bop') as Output;
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const open = columnValues(
    wide,
    (options.open ?? DEFAULT_OHLCV.open) as string,
  );
  const high = columnValues(
    wide,
    (options.high ?? DEFAULT_OHLCV.high) as string,
  );
  const low = columnValues(wide, (options.low ?? DEFAULT_OHLCV.low) as string);
  const close = columnValues(
    wide,
    (options.close ?? DEFAULT_OHLCV.close) as string,
  );

  const raw = new Float64Array(high.length);
  for (let i = 0; i < raw.length; i += 1) {
    const range = high[i]! - low[i]!;
    const body = close[i]! - open[i]!;
    // `range === 0` is the flat bar (see the docstring): the body is forced
    // to zero on a real one, so the value is 0 — but only when the body is
    // present, or a missing open/close would be reported as a reading.
    // A NaN input makes `range` NaN, which is not `=== 0`, so it falls
    // through to the division and propagates ([PND-STUDYBOX]).
    raw[i] = range === 0 ? (body === body ? 0 : NaN) : body / range;
  }

  const values =
    period === undefined
      ? raw
      : movingAverageValues(raw, period, options.maType ?? 'sma');
  return series.withColumn(output, values);
}
