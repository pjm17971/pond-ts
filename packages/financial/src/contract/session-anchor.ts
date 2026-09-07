import type {
  NumericColumnNameForSchema,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { normalizeSessions, type Session } from '../calendar/session.js';
import { TradingCalendar } from '../calendar/trading-calendar.js';
import { sessionIdValues } from '../kernels/session.js';
import { assertColumn, columnValues } from '../kernels/rolling.js';

/**
 * Where a session-anchored study gets its sessions from: a
 * {@link TradingCalendar}, or the same thing one step earlier — an explicit
 * {@link Session} list (from `generateSessions`, from a feed's own schedule
 * table, or written by hand). A raw list is validated and sorted exactly as
 * `TradingCalendar.fromSessions` would, so an overlapping or malformed
 * schedule throws here rather than producing quietly wrong anchors.
 */
export type SessionSource = TradingCalendar | readonly Session[];

/**
 * **The session anchor** — how {@link sessionVwap} and {@link pivotPoints} are
 * told where their sessions are. Exactly **one** of the two doors is given;
 * passing both, or neither, throws.
 *
 * ## `sessions` — the primary door
 *
 * A calendar (or a session list). The study asks it for the sessions
 * overlapping the series' own key range and walks bars against them once,
 * `O(N + sessions)`. This is the door the trading-calendar RFC's §6.1 picture
 * points at: one calendar object shared by the data ops, the bar building and
 * the axis, so a study and a chart cannot disagree about where a session
 * starts.
 *
 * ## `session` — the door for a series that is already partitioned
 *
 * The **name of a session-id column** already on the series. It is what
 * `TradingCalendar.tagSessions(series)` appends, and a consumer who followed
 * the RFC's `partitionBy(sessionId)` stopgap already has it — asking them to
 * hand the calendar back in would mean walking it twice. Any numeric column
 * works: a run of one value is one session, `undefined` is closed time. The
 * two doors are the same anchoring by construction — `tagSessions` and the
 * `sessions` door call the same `sessionIdValues` walk — and a test pins the
 * two routes equal under both stamp conventions.
 *
 * ## `stamped`
 *
 * Belongs to the `sessions` door only: it is how a *bar instant* is placed
 * against a session edge, and a session-id column has already had that
 * decision applied to it (pass `stamped` to `tagSessions` instead). Passing it
 * beside `session` throws rather than being silently ignored.
 */
export interface SessionAnchorOptions<S extends SeriesSchema> {
  /** The trading calendar, or an explicit session list. **The primary door.**
   *  Mutually exclusive with `session`. */
  sessions?: SessionSource;
  /** The name of a session-id column already on the series (what
   *  `TradingCalendar.tagSessions` appends). Mutually exclusive with
   *  `sessions`. */
  session?: NumericColumnNameForSchema<S>;
  /** Bar-stamp convention for the `sessions` door — `'open'` (default) treats
   *  a session as `[open, close)`, `'close'` as `(open, close]`. Rejected
   *  beside `session`. */
  stamped?: 'open' | 'close';
}

/**
 * Resolve a study's session anchor to one **session id per row** (`NaN` in
 * closed time) — the array both session-anchored studies compute from.
 *
 * `study` names the caller for the error messages. Throws when neither or both
 * doors are given, when `stamped` rides along with the column door, and — via
 * `normalizeSessions` / `assertColumn` — on a malformed schedule or a
 * session column that isn't there.
 */
export function resolveSessionIds(
  series: TimeSeries<SeriesSchema>,
  options: SessionAnchorOptions<SeriesSchema>,
  study: string,
): Float64Array {
  const { sessions, session, stamped } = options;
  if ((sessions === undefined) === (session === undefined)) {
    throw new TypeError(
      `${study} needs exactly one of 'sessions' (a TradingCalendar or Session[]) ` +
        `or 'session' (the name of a session-id column, e.g. from ` +
        `TradingCalendar.tagSessions); got ${sessions === undefined ? 'neither' : 'both'}`,
    );
  }

  if (session !== undefined) {
    if (stamped !== undefined) {
      throw new TypeError(
        `${study} 'stamped' applies to the 'sessions' door; a session-id column ` +
          `already carries its stamp convention — pass stamped to tagSessions instead`,
      );
    }
    // The column door is REQUIRED-option shaped (there is no default session
    // column to fall back on), so a typo throws rather than reading
    // all-missing — the `assertColumn` rule.
    assertColumn(series, session, 'session');
    return columnValues(series, session);
  }

  if (stamped !== undefined && stamped !== 'open' && stamped !== 'close') {
    throw new TypeError(
      `${study} 'stamped' must be 'open' or 'close'; got ${JSON.stringify(stamped)}`,
    );
  }

  // The key axis' START — `begin` is the field every key variant carries, so a
  // timeRange- or interval-keyed series anchors on each bar's beginning, the
  // same read `anchoredVwap` makes.
  const keys = (series.keyColumn() as unknown as { begin: Float64Array }).begin;
  const length = series.length;
  let list: readonly Session[];
  if (sessions instanceof TradingCalendar) {
    // Narrow to the sessions the series can actually touch — a decade-long
    // calendar over one day of bars then costs a bisection, not a walk.
    // `start` is nudged back one millisecond because under close-stamping a
    // bar AT a session's close belongs to it, and `sessionsInRange` is
    // half-open: `close >= keys[0]` is exactly `close > keys[0] - 1`.
    list =
      length === 0
        ? []
        : sessions.sessionsInRange({
            start: keys[0]! - 1,
            end: keys[length - 1]! + 1,
          });
  } else {
    // A raw list gets the same validation the calendar constructor applies:
    // sorted, dates unique, non-overlapping. An overlapping schedule would
    // otherwise anchor silently on whichever session the walk reached first.
    list = normalizeSessions(sessions!);
  }
  return sessionIdValues(keys, list, stamped ?? 'open');
}
