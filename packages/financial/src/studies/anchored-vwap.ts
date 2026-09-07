import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import { anchoredVwapValues } from '../kernels/anchored-vwap.js';
import { assertNoColumn, columnValues } from '../kernels/rolling.js';
import { typicalPriceValues } from '../kernels/typical-price.js';

export interface AnchoredVwapOptions<
  S extends SeriesSchema,
  Output extends string,
> {
  /** **Required.** The bar to anchor on, as a `Date` or epoch **milliseconds**.
   *  The line starts at the first bar at or after it; earlier bars read
   *  `undefined`. There is no default — see the study docstring. */
  anchor: Date | number;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Close column. **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Volume column. **Default `'volume'`.** */
  volume?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'avwap'`.** */
  output?: Output;
}

/**
 * **Anchored VWAP** — the volume-weighted average price of every bar **from a
 * chosen bar onwards**, accumulated rather than windowed:
 *
 * ```
 * ${output}[i] = Σ typicalPrice·volume / Σ volume     over bars anchor .. i
 * typicalPrice = (high + low + close) / 3
 * ```
 *
 * Appends one column, `undefined` on every bar **before** the anchor and
 * defined from the anchor bar itself (where it is simply that bar's typical
 * price). This is the VWAP of the execution desk: the average price actually
 * traded since some event — an earnings print, a gap, a swing low — which is
 * what a trader means by "we are above VWAP from the low".
 *
 * ## The half of VWAP `vwap()` deliberately left open
 *
 * {@link vwap} ships the **rolling** form and its docstring names this one as
 * the sibling it is not: a count window is emitted only once it spans
 * `period` rows, so `period = length` gives one value at the last bar, not a
 * running line. The two are genuinely different studies and this is the one
 * with no window at all.
 *
 * The **third** form — VWAP that **resets every session** — is
 * {@link sessionVwap}, which needed the session boundaries the trading
 * calendar supplies ([PND-TCAL] / the corpus' **G4**) and now shares this
 * study's kernel: it is this same accumulation re-anchored at every session
 * open. What made *this* form shippable before the calendar was that its
 * anchor is a **user parameter**, so no calendar is consulted.
 *
 * ## `anchor` is REQUIRED, and it is a TIME, not an index
 *
 * There is no default: the anchor is the whole study — "VWAP from where?" is
 * the question a caller is answering — and every candidate default is a
 * different indicator. The series start would be a cumulative-from-inception
 * VWAP, which is dominated by ancient bars; the last bar would be a constant.
 *
 * It is a `Date` or epoch **milliseconds** rather than a row index, because
 * that is what the caller actually has (the timestamp of the event they are
 * anchoring to) and because a row index does not survive a filter, a join or
 * a resample. The line starts at the first bar whose key is **at or after**
 * `anchor`, so an anchor between two bars snaps forward to the next one and
 * an anchor before the series covers the whole of it. An anchor after the
 * last bar leaves the column entirely `undefined` — no bars qualify, and
 * that is not an error.
 *
 * The key read is the key column's **start** (`keyColumn().begin`), so on a
 * `timeRange`- or `interval`-keyed series the comparison is against each
 * bar's beginning.
 *
 * ## Composed, not new arithmetic
 *
 * {@link typicalPriceValues} for the price and `anchoredVwapValues` for the
 * ratio of running sums. The only thing this study owns is the anchor: it
 * builds a **single anchor group** (`NaN` before the anchor bar, one id from
 * there on) and the kernel does the rest, with no arithmetic here at all.
 * {@link sessionVwap} is the same call with each bar's **session id** in place
 * of that one group — which is why the two studies share a kernel rather than
 * a family resemblance.
 *
 * The denominator is blanked wherever the **numerator** is, before either is
 * accumulated (in the kernel). That is not tidiness: a bar with a volume but
 * a missing `high` would otherwise contribute to `Σ volume` and not to
 * `Σ price·volume`, which is a VWAP quietly biased toward zero — the #710
 * rule (two accumulations of two columns must consume the same bars),
 * pinned by a test.
 *
 * ## Edges
 *
 * - **An interior gap ENDS the line.** Both sums are running sums, so a
 *   missing term makes every later level a known sum plus an unknown. That
 *   is {@link obv}'s rule and the A/D line's, and it is right here for the
 *   same reason it is right there: the reading is a *level*, and skipping
 *   the bar would report an average price that silently excludes volume that
 *   traded. It is **not** {@link negativeVolumeIndex}'s re-seed, which is
 *   for an index measured from an arbitrary base. A caller who needs
 *   continuity fills first, or re-anchors after the hole —
 *   {@link sessionVwap}, on the same kernel, re-anchors automatically at the
 *   next session open.
 * - **Zero cumulative volume → `undefined`.** Reachable when the anchor
 *   falls on a run of zero-volume bars: there is nothing to weight by, so
 *   there is no average price — not `0`, and not the plain mean of typical
 *   price. The guard is live and at the output; without it the reading would
 *   be `±Infinity`, which `withColumn` rejects outright.
 * - **Scales with price, not with volume** — doubling every price doubles
 *   it, doubling every volume leaves it unchanged, exactly as `vwap` does.
 *   Both are pinned as property tests.
 */
export function anchoredVwap<
  S extends SeriesSchema,
  const Output extends string = 'avwap',
>(series: TimeSeries<S>, options: AnchoredVwapOptions<S, Output>) {
  const anchor =
    options.anchor instanceof Date ? options.anchor.getTime() : options.anchor;
  if (typeof anchor !== 'number' || !Number.isFinite(anchor)) {
    throw new TypeError(
      `anchoredVwap anchor must be a Date or a finite epoch-millisecond number; got ${String(options.anchor)}`,
    );
  }

  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const closeName = (options.close ?? DEFAULT_OHLCV.close) as string;
  const volumeName = (options.volume ?? DEFAULT_OHLCV.volume) as string;
  const output = (options.output ?? 'avwap') as Output;
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const typical = typicalPriceValues(
    columnValues(wide, highName),
    columnValues(wide, lowName),
    columnValues(wide, closeName),
  );
  const volume = columnValues(wide, volumeName);
  // The key axis' START — `begin` is the field every key variant carries, so
  // a timeRange- or interval-keyed series anchors on each bar's beginning.
  const keys = (wide.keyColumn() as unknown as { begin: Float64Array }).begin;

  // One anchor group, opening at the first bar at or after `anchor` and
  // running to the end of the series — the whole of this study's difference
  // from `sessionVwap`, which passes each bar's session id instead.
  const length = typical.length;
  const anchors = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    anchors[i] = keys[i]! < anchor ? NaN : 0;
  }
  return series.withColumn(
    output,
    anchoredVwapValues(typical, volume, anchors),
  );
}
