import type {
  NumericColumnNameForSchema,
  OptionalNumberColumn,
  SeriesSchema,
  TimeSeries,
} from 'pond-ts';
import { DEFAULT_OHLCV } from '../contract/columns.js';
import {
  resolveSessionIds,
  type SessionAnchorOptions,
} from '../contract/session-anchor.js';
import { assertNoColumn, columnValues } from '../kernels/rolling.js';
import { previousSessionHlcValues } from '../kernels/session.js';
import {
  assertPivotMethod,
  pivotLevelValues,
  type PivotMethod,
} from '../kernels/pivot.js';

export interface PivotPointsOptions<
  S extends SeriesSchema,
  Prefix extends string,
  Method extends PivotMethod,
> extends SessionAnchorOptions<S> {
  /** Which formula set. **Default `'standard'`.** `'camarilla'` appends a
   *  fourth pair (`${prefix}R4` / `${prefix}S4`); the other three do not. */
  method?: Method;
  /** High column. **Default `'high'`.** */
  high?: NumericColumnNameForSchema<S>;
  /** Low column. **Default `'low'`.** */
  low?: NumericColumnNameForSchema<S>;
  /** Close column. **Default `'close'`.** */
  close?: NumericColumnNameForSchema<S>;
  /** Column-family prefix. **Default `'pp'`.** */
  prefix?: Prefix;
}

/** The seven columns `'standard'` / `'fibonacci'` / `'woodie'` append. */
export type PivotPointsSchema<
  S extends SeriesSchema,
  Prefix extends string,
> = readonly [
  ...S,
  OptionalNumberColumn<`${Prefix}Pivot`>,
  OptionalNumberColumn<`${Prefix}R1`>,
  OptionalNumberColumn<`${Prefix}R2`>,
  OptionalNumberColumn<`${Prefix}R3`>,
  OptionalNumberColumn<`${Prefix}S1`>,
  OptionalNumberColumn<`${Prefix}S2`>,
  OptionalNumberColumn<`${Prefix}S3`>,
];

/** The nine columns `'camarilla'` appends — the seven above plus its fourth
 *  pair, which is the set's own breakout level and has no counterpart in the
 *  other three. */
export type CamarillaPivotPointsSchema<
  S extends SeriesSchema,
  Prefix extends string,
> = readonly [
  ...S,
  OptionalNumberColumn<`${Prefix}Pivot`>,
  OptionalNumberColumn<`${Prefix}R1`>,
  OptionalNumberColumn<`${Prefix}R2`>,
  OptionalNumberColumn<`${Prefix}R3`>,
  OptionalNumberColumn<`${Prefix}R4`>,
  OptionalNumberColumn<`${Prefix}S1`>,
  OptionalNumberColumn<`${Prefix}S2`>,
  OptionalNumberColumn<`${Prefix}S3`>,
  OptionalNumberColumn<`${Prefix}S4`>,
];

/** {@link pivotPoints}' return type: the column set depends on `method`, so
 *  the type does too. */
export type PivotPointsResult<
  S extends SeriesSchema,
  Prefix extends string,
  Method extends PivotMethod,
> = TimeSeries<
  Method extends 'camarilla'
    ? CamarillaPivotPointsSchema<S, Prefix>
    : PivotPointsSchema<S, Prefix>
>;

/**
 * **Pivot Points** — each session's support/resistance ladder, computed from
 * the **previous session's** high, low and close and held flat across every
 * bar of the current session.
 *
 * Appends `${prefix}Pivot`, `${prefix}R1..R3` and `${prefix}S1..S3` (plus
 * `${prefix}R4` / `${prefix}S4` for `'camarilla'`). The levels are constant
 * within a session — that is the point of them: a floor trader writes the
 * numbers down before the open and trades against them all day.
 *
 * ```ts
 * const cal = TradingCalendar.fromRules(
 *   { timeZone: 'America/New_York', open: '09:30', close: '16:00' },
 *   { from: '2024-01-02', to: '2024-12-31' },
 * );
 * pivotPoints(bars, { sessions: cal });                       // standard
 * pivotPoints(bars, { sessions: cal, method: 'camarilla' });  // +R4/S4
 * pivotPoints(cal.tagSessions(bars), { session: 'session' }); // column door
 * ```
 *
 * This is the study the corpus assessment (§6.9) gates on **G4**: the
 * arithmetic was never the hard part — "prior-period H/L/C via `aggregate` +
 * hold-broadcast is easy" — the *anchor rule* was, and the trading calendar is
 * what supplies it. Where sessions come from is
 * {@link SessionAnchorOptions}: a calendar (the primary door) or a session-id
 * column on an already-partitioned series.
 *
 * ## `method` is a variant menu, not two studies behind a flag
 *
 * All four sets read the **same three inputs** and produce the same shape — a
 * central pivot with a ladder of support and resistance either side — and
 * differ in constants. That is the test `keltner`'s `maType` had to pass and
 * `keltner`'s hypothetical "band source" flag failed: a flag that swaps which
 * *indicator* you are computing is two studies wearing one name, and this is
 * not that. Each set is written out in `pivotLevelValues`; briefly:
 *
 * - **`'standard'`** — the floor-trader/classic set, `P = (H+L+C)/3` with
 *   `R1 = 2P − L`, `R2 = P + (H−L)`, `R3 = H + 2(P−L)` and the mirrored
 *   supports. The unqualified "pivot points" of every vendor.
 * - **`'fibonacci'`** — the same centre with the range scaled by 38.2 % /
 *   61.8 % / 100 %.
 * - **`'woodie'`** — the standard ladder over the close-weighted centre
 *   `P = (H + L + 2C)/4`.
 * - **`'camarilla'`** (Nick Scott, 1989) — four pairs at `1.1/12`, `1.1/6`,
 *   `1.1/4`, `1.1/2` of the range.
 *
 * Two deliberate deltas worth naming, since both appear in the wild:
 *
 * - **Camarilla measures from the CLOSE, not from the pivot.** Its levels are
 *   `C ± k·(H−L)`, so `P` rides along as the centre line but no level is
 *   computed from it. That is the definition; a build that centred them on `P`
 *   would be a different indicator, and the fixture separates the two.
 * - **Woodie's is the `(H + L + 2C)/4` form**, weighting the previous close.
 *   A second convention in circulation weights the **current session's open**
 *   instead (`(H + L + 2·Open)/4`). We ship the previous-close form: it needs
 *   nothing but the previous session's aggregate, matching every other method
 *   here, and the open form would make one menu entry read a column the other
 *   three don't.
 *
 * ## The column set varies with the method — on purpose
 *
 * Camarilla defines a fourth pair; the other three do not. The alternative
 * was a fixed nine columns with `R4`/`S4` permanently `undefined` for three
 * of the four methods, which is worse in every direction: it invents columns
 * that have no definition, makes "missing" ambiguous between *not in this
 * method* and *no previous session*, and would have every consumer of a
 * standard-pivot chart carrying two dead series. The return type follows the
 * option — {@link PivotPointsResult} is conditional on `method` — so
 * `.get('ppR4')` type-checks only where it exists.
 *
 * ## Warm-up, and where the missing rows are
 *
 * - Every bar of the **first session that has bars** reads `undefined` —
 *   there is no previous session to measure. Not an error, and not a hole to
 *   fill.
 * - Every bar in **closed time** reads `undefined`, for the same reason
 *   {@link sessionVwap} does: a bar in no session has no session's levels.
 * - "Previous session" means the previous session **with bars in this
 *   series**, not the previous entry on the calendar. A day the feed skipped
 *   has no aggregate, and reading the calendar for one would invent a bar.
 * - The columns share **one** warm-up rather than one each: every level of
 *   every method reads all three aggregates, so a missing input blanks the
 *   whole ladder together.
 *
 * ## Composed
 *
 * `previousSessionHlcValues` (the `aggregate` + hold-broadcast, one pass) and
 * `pivotLevelValues` (the four formula sets). The aggregates are taken over
 * the cells that are **present** — a bar with a missing `high` still
 * contributes its `low` — because `max`/`min`/`last` are three independent
 * order statistics, not the two halves of one ratio; see the kernel for why
 * that differs from the VWAP rule.
 *
 * ## Scale and shift
 *
 * Every level is an affine combination of `H`, `L` and `C` whose coefficients
 * sum to 1, so the whole ladder is **equivariant** under both: scaling every
 * price scales the levels, adding a constant to every price adds it to the
 * levels. Both are pinned as property tests.
 */
export function pivotPoints<
  S extends SeriesSchema,
  const Prefix extends string = 'pp',
  const Method extends PivotMethod = 'standard',
>(
  series: TimeSeries<S>,
  options: PivotPointsOptions<S, Prefix, Method>,
): PivotPointsResult<S, Prefix, Method> {
  const method = (options.method ?? 'standard') as PivotMethod;
  assertPivotMethod(method);
  const highName = (options.high ?? DEFAULT_OHLCV.high) as string;
  const lowName = (options.low ?? DEFAULT_OHLCV.low) as string;
  const closeName = (options.close ?? DEFAULT_OHLCV.close) as string;
  const prefix = (options.prefix ?? 'pp') as Prefix;
  const wide = series as unknown as TimeSeries<SeriesSchema>;

  // Resistances then supports, with Camarilla's fourth pair in its place in
  // each half — the order a reader of the chart legend expects. Named and
  // guarded before any work, so a collision on the last column does not cost
  // a whole session walk first (`guppy`'s rule).
  const names = [
    `${prefix}Pivot`,
    `${prefix}R1`,
    `${prefix}R2`,
    `${prefix}R3`,
    ...(method === 'camarilla' ? [`${prefix}R4`] : []),
    `${prefix}S1`,
    `${prefix}S2`,
    `${prefix}S3`,
    ...(method === 'camarilla' ? [`${prefix}S4`] : []),
  ];
  for (const name of names) assertNoColumn(wide, name);

  const ids = resolveSessionIds(
    wide,
    options as SessionAnchorOptions<SeriesSchema>,
    'pivotPoints',
  );
  const prev = previousSessionHlcValues(
    ids,
    columnValues(wide, highName),
    columnValues(wide, lowName),
    columnValues(wide, closeName),
  );
  const levels = pivotLevelValues(method, prev.high, prev.low, prev.close);

  const values = [
    levels.pivot,
    levels.r1,
    levels.r2,
    levels.r3,
    ...(levels.r4 === undefined ? [] : [levels.r4]),
    levels.s1,
    levels.s2,
    levels.s3,
    ...(levels.s4 === undefined ? [] : [levels.s4]),
  ];

  // The column SET depends on `method`, so the chain is written as a fold over
  // the list rather than the fixed `.withColumn().withColumn()` a same-shape
  // family (`guppy`, `bollinger`) uses. That erases the schema widening, which
  // is why the declared return type is the hand-written, method-conditional
  // shape — the same `as unknown as` step `TradingCalendar.tagSessions` makes.
  let out = wide;
  for (let i = 0; i < names.length; i += 1) {
    out = out.withColumn(
      names[i] as never,
      values[i]!,
    ) as unknown as TimeSeries<SeriesSchema>;
  }
  return out as unknown as PivotPointsResult<S, Prefix, Method>;
}
