import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import {
  resolveSessionIds,
  type SessionAnchorOptions,
} from '../contract/session-anchor.js';
import { anchoredVwapValues } from '../kernels/anchored-vwap.js';
import { assertNoColumn, columnValues } from '../kernels/rolling.js';
import { typicalPriceValues } from '../kernels/typical-price.js';

export interface SessionVwapOptions<
  S extends SeriesSchema,
  Output extends string,
> extends SessionAnchorOptions<S> {
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Close column. **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Volume column. **Default `'volume'`.** */
  volume?: NumericColumnNameForSchema<S>;
  /** Name of the appended column. **Default `'svwap'`.** */
  output?: Output;
}

/**
 * **Session VWAP** — the volume-weighted average price accumulated from each
 * session's open and **reset at the next one**:
 *
 * ```
 * ${output}[i] = Σ typicalPrice·volume / Σ volume    over this session's bars up to i
 * typicalPrice = (high + low + close) / 3
 * ```
 *
 * This is the VWAP an intraday desk actually watches — "we are above VWAP" on
 * a trading floor always means *today's* VWAP, from today's open, not a
 * rolling window and not a line running since the series began. It is the
 * third form {@link vwap} named and deliberately left open and
 * {@link anchoredVwap} could only half-provide: the reset needs session
 * boundaries, which is the corpus' **G4** gate, and the trading calendar is
 * what closed it.
 *
 * Appends one column, defined from each session's first bar (where it is
 * simply that bar's typical price) and `undefined` on any bar that falls in
 * **closed time** — a gap between sessions, an overnight print, a weekend bar
 * on a 24/7 feed. A bar with no session has no session VWAP; that is a fact
 * about the bar, not a hole to fill.
 *
 * ## Where the sessions come from
 *
 * Exactly one of two doors — see {@link SessionAnchorOptions}:
 *
 * ```ts
 * const cal = TradingCalendar.fromRules(
 *   { timeZone: 'America/New_York', open: '09:30', close: '16:00' },
 *   { from: '2024-01-02', to: '2024-12-31' },
 * );
 * sessionVwap(bars, { sessions: cal });            // the primary door
 * sessionVwap(cal.tagSessions(bars), { session: 'session' });  // already partitioned
 * ```
 *
 * Both are the same anchoring: `tagSessions` and the `sessions` door run the
 * same `sessionIdValues` walk, and a test pins the two routes equal under both
 * stamp conventions. `sessions` also accepts a raw `Session[]` — the schedule
 * table a feed hands you — which is validated (sorted, non-overlapping) before
 * it is used.
 *
 * ## Composed, not new arithmetic
 *
 * {@link typicalPriceValues} for the price and `anchoredVwapValues` for the
 * ratio of running sums — the *same* kernel {@link anchoredVwap} runs on. The
 * only difference between the two studies is what they pass as the anchor
 * group: `anchoredVwap` passes one group starting at the anchor bar, this
 * passes each bar's session id. Saying "session VWAP is anchored VWAP
 * re-anchored every session" is therefore a fact about the code, not a
 * comment.
 *
 * ## Edges
 *
 * - **An interior gap ends THIS SESSION's line, and the next session
 *   re-seeds.** Both sums are running sums, so after a missing term every
 *   later level in that session is a known sum plus an unknown — `obv`'s rule
 *   and {@link anchoredVwap}'s. The difference is that the reset recovers it
 *   automatically at the next open, where `anchoredVwap` needs the caller to
 *   re-anchor. A gap in *any* of the four inputs counts: the denominator is
 *   blanked wherever the numerator is, so the two sums consume the same bars
 *   and a bar with volume but a missing `high` cannot bias the average toward
 *   zero (the #710 rule).
 * - **Zero accumulated volume → `undefined`.** Reachable on a session that
 *   opens on a run of zero-volume bars: there is nothing to weight by, so
 *   there is no average price — not `0`, not the plain mean of typical price.
 *   The guard is live and at the output; without it the reading would be
 *   `±Infinity`, which `withColumn` rejects outright.
 * - **A session the schedule has but the data doesn't** simply contributes
 *   nothing — the line is built from bars, not from the calendar.
 * - **Scales with price, not with volume** — doubling every price doubles it,
 *   doubling every volume leaves it unchanged, exactly as `vwap` and
 *   `anchoredVwap` do. Both are pinned as property tests.
 */
export function sessionVwap<
  S extends SeriesSchema,
  const Output extends string = 'svwap',
>(series: TimeSeries<S>, options: SessionVwapOptions<S, Output>) {
  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const closeName = (options.close ?? DEFAULT_OHLCV.close) as string;
  const volumeName = (options.volume ?? DEFAULT_OHLCV.volume) as string;
  const output = (options.output ?? 'svwap') as Output;
  const wide = series as unknown as TimeSeries<SeriesSchema>;
  assertNoColumn(wide, output);

  const ids = resolveSessionIds(
    wide,
    options as SessionAnchorOptions<SeriesSchema>,
    'sessionVwap',
  );
  const typical = typicalPriceValues(
    columnValues(wide, highName),
    columnValues(wide, lowName),
    columnValues(wide, closeName),
  );
  return series.withColumn(
    output,
    anchoredVwapValues(typical, columnValues(wide, volumeName), ids),
  );
}
