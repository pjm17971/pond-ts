import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import { cumulativeValues } from '../kernels/cumulative.js';
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
 * The **third** form — VWAP that **resets every session** — is still not
 * here, and that is the calendar gap rather than an oversight: it needs
 * session boundaries, which is [PND-TCAL] / the corpus' **G4**. What makes
 * this form shippable today is that its anchor is a **user parameter**, so
 * no calendar is consulted. A caller who wants the session form today
 * partitions on the session and runs this per partition with each session's
 * first timestamp.
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
 * {@link typicalPriceValues} for the price and {@link cumulativeValues} for
 * both running sums — the same two kernels `vwap` and `obv` are built from.
 * The only thing this study owns is the anchor: it blanks both terms before
 * the anchor bar, and `cumulativeValues`' documented rule that **a leading
 * run of gaps shifts the start** then does the rest, with no arithmetic here
 * at all.
 *
 * The denominator is also blanked wherever the **numerator** is, before
 * either is accumulated. That is not tidiness: a bar with a volume but a
 * missing `high` would otherwise contribute to `Σ volume` and not to
 * `Σ price·volume`, which is a VWAP quietly biased toward zero — the #710
 * rule (two accumulations of two columns must consume the same bars),
 * pinned by a test.
 *
 * ## Edges
 *
 * - **An interior gap ENDS the line.** Both sums are `cumulativeValues`, so a
 *   missing term makes every later level a known sum plus an unknown. That
 *   is {@link obv}'s rule and the A/D line's, and it is right here for the
 *   same reason it is right there: the reading is a *level*, and skipping
 *   the bar would report an average price that silently excludes volume that
 *   traded. It is **not** {@link negativeVolumeIndex}'s re-seed, which is
 *   for an index measured from an arbitrary base. A caller who needs
 *   continuity fills first, or re-anchors after the hole.
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

  const length = typical.length;
  const flow = new Float64Array(length);
  const weight = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    // Before the anchor the bar is not part of this VWAP at all, and a
    // `NaN` here is exactly the "leading gap" `cumulativeValues` steps over.
    // The two arrays are blanked TOGETHER so the sums consume the same bars
    // (see the docstring).
    const f = keys[i]! < anchor ? NaN : typical[i]! * volume[i]!;
    flow[i] = f;
    weight[i] = Number.isNaN(f) ? NaN : volume[i]!;
  }

  const numerator = cumulativeValues(flow);
  const denominator = cumulativeValues(weight);
  const values = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    const d = denominator[i]!;
    // Live guard: the division is at the OUTPUT, and a run of zero-volume
    // bars does not force the numerator to zero once `high`/`low`/`close`
    // can be redirected.
    values[i] = d === 0 ? NaN : numerator[i]! / d;
  }
  return series.withColumn(output, values);
}
