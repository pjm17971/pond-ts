import type { Session } from '../calendar/session.js';

/**
 * **Per-bar session ids** — for each key instant, the `open` of the session
 * that contains it, or `NaN` in closed time (a gap between sessions, a
 * weekend bar on a 24/7 feed, an instant outside the schedule's range).
 * A session's intraday `breaks` do NOT split it — see `SessionAnchorOptions`.
 *
 * This is the single walk behind both session doors: `TradingCalendar
 * .tagSessions` appends exactly this array as a column, and the
 * session-anchored studies read it straight. Having one implementation is
 * what makes the two doors *provably* the same anchoring rather than two
 * loops that agree until one is edited.
 *
 * `keys` must be **ascending** (every pond `TimeSeries` key column is) and
 * `sessions` sorted and non-overlapping (`normalizeSessions` guarantees it),
 * so the session cursor only ever moves forward: **O(N + sessions)**, one
 * merge walk, no per-bar search. A calendar covering ten years costs the same
 * per bar as one covering a week.
 *
 * ## `stamped` — which side of a session edge a bar lands on
 *
 * - `'open'` (default) — a session owns `[open, close)`. A bar stamped at its
 *   *open* belongs to the session; one stamped at the *close* is closed time.
 *   The tick / open-stamp convention.
 * - `'close'` — a session owns `(open, close]`. A bar stamped at the close
 *   (the 16:00 bar covering 15:55–16:00) belongs to the session that just
 *   closed. The OHLC / close-stamp convention.
 *
 * See {@link Session} and the trading-calendar RFC §6.1; the knob lives at
 * this level because binning a *bar* series is where the stamp convention
 * bites, while instantaneous point queries stay half-open.
 */
export function sessionIdValues(
  keys: ArrayLike<number>,
  sessions: readonly Session[],
  stamped: 'open' | 'close' = 'open',
): Float64Array {
  const length = keys.length;
  const out = new Float64Array(length);
  const closeStamped = stamped === 'close';
  let c = 0;
  for (let i = 0; i < length; i += 1) {
    const t = keys[i]!;
    // Keys ascend and sessions are sorted by open, so the cursor only moves
    // forward. Skip sessions that already ended relative to `t`; for
    // close-stamped bars a bar *at* the close still belongs to that session,
    // so only skip once `t` is strictly past it.
    while (
      c < sessions.length &&
      (closeStamped ? sessions[c]!.close < t : sessions[c]!.close <= t)
    )
      c += 1;
    const s = c < sessions.length ? sessions[c] : undefined;
    // Only the OPEN side is a live test. The cursor above has already skipped
    // every session that ended before `t`, so whatever `s` is left satisfies
    // the close side by construction — open-stamped `s.close > t`,
    // close-stamped `s.close >= t`. Re-asserting it would be dead code, and
    // the mutation matrix agrees: flipping `<` to `<=` there failed nothing.
    const inSession =
      s !== undefined && (closeStamped ? t > s.open : t >= s.open);
    out[i] = inSession ? s!.open : NaN;
  }
  return out;
}

/** The previous session's aggregate bar, broadcast onto every row of the
 *  current session — the three arrays {@link previousSessionHlcValues}
 *  returns. */
export interface PreviousSessionHlc {
  /** The previous session's highest `high`. */
  readonly high: Float64Array;
  /** The previous session's lowest `low`. */
  readonly low: Float64Array;
  /** The previous session's **last** present `close`. */
  readonly close: Float64Array;
}

/**
 * **The previous session's high / low / close, held across the current
 * session** — the `aggregate` + hold-broadcast the pivot family is built on,
 * as one pass over row-aligned columns.
 *
 * For every row, the three outputs carry the aggregate bar of the session
 * *before* the one that row belongs to:
 *
 * - `high` = max of the previous session's `high`s,
 * - `low` = min of its `low`s,
 * - `close` = its **last present** `close`.
 *
 * `NaN` on every row of the **first** session with bars (there is no previous
 * one) and on every row in closed time. "Previous" means the previous session
 * **that has bars in this series**, not the previous session on the calendar:
 * a holiday the feed simply skipped has no aggregate, and inventing one from a
 * calendar the data never visited would be worse than reporting nothing.
 *
 * ## Missing cells
 *
 * Each aggregate is taken over the cells that are **present** — a bar with a
 * missing `high` still contributes its `low`. That is deliberately *not* the
 * blank-both-together rule the VWAP accumulators follow (see
 * `anchoredVwapValues`): there the two sums are numerator and denominator of
 * one ratio, so consuming different bars biases the answer. Here `max` / `min`
 * / `last` are three independent order statistics of one session, and dropping
 * a whole bar because one of its three fields is missing would discard
 * information for no gain. A session with **no** present cell in a column
 * yields `NaN` for that column, which then propagates through every level
 * computed from it.
 *
 * O(N), one pass, three allocations; `ids` is the session-id array from
 * {@link sessionIdValues}. A change of id starts a new session — ids are
 * expected to be **contiguous** (they are, for any ordered series tagged
 * against a non-overlapping schedule), so a run of one id is one session.
 */
export function previousSessionHlcValues(
  ids: Float64Array,
  high: Float64Array,
  low: Float64Array,
  close: Float64Array,
): PreviousSessionHlc {
  const length = ids.length;
  const outHigh = new Float64Array(length);
  const outLow = new Float64Array(length);
  const outClose = new Float64Array(length);

  let currentId = NaN;
  let seen = false;
  let curHigh = NaN;
  let curLow = NaN;
  let curClose = NaN;
  let prevHigh = NaN;
  let prevLow = NaN;
  let prevClose = NaN;

  for (let i = 0; i < length; i += 1) {
    const id = ids[i]!;
    if (Number.isNaN(id)) {
      // Closed time: no session, so no levels. The in-progress aggregate is
      // NOT reset — a bar the schedule does not cover is simply not part of
      // any session, and the session it sits between is unaffected.
      outHigh[i] = NaN;
      outLow[i] = NaN;
      outClose[i] = NaN;
      continue;
    }
    if (id !== currentId) {
      if (seen) {
        prevHigh = curHigh;
        prevLow = curLow;
        prevClose = curClose;
      }
      currentId = id;
      seen = true;
      curHigh = NaN;
      curLow = NaN;
      curClose = NaN;
    }
    // Written BEFORE this bar folds into the current session's aggregate —
    // the levels a bar reads are the previous session's, never its own.
    outHigh[i] = prevHigh;
    outLow[i] = prevLow;
    outClose[i] = prevClose;

    const h = high[i]!;
    if (!Number.isNaN(h) && (Number.isNaN(curHigh) || h > curHigh)) curHigh = h;
    const l = low[i]!;
    if (!Number.isNaN(l) && (Number.isNaN(curLow) || l < curLow)) curLow = l;
    const c = close[i]!;
    if (!Number.isNaN(c)) curClose = c;
  }

  return { high: outHigh, low: outLow, close: outClose };
}
