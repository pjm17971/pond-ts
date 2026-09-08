import type { SeriesSchema, TimeSeries } from 'pond-ts';

/**
 * The picker's grouping — the assessment's §6 families
 * (`docs/notes/financial-indicators-assessment-2026-07.md`), one per study.
 */
export type StudyFamily =
  | 'moving-average'
  | 'bands'
  | 'momentum'
  | 'trend'
  | 'volatility'
  | 'volume'
  | 'statistical'
  | 'price'
  | 'session';

/** {@link StudyFamily} as a list, in menu order. */
export const STUDY_FAMILIES: readonly StudyFamily[] = [
  'moving-average',
  'bands',
  'momentum',
  'trend',
  'volatility',
  'volume',
  'statistical',
  'price',
  'session',
];

/**
 * What an output column is denominated in — the fact a consumer needs to
 * decide **axis membership**: only `'inherit'` may share the source's axis.
 *
 * - `'inherit'` — a level in the source column's own units (a moving
 *   average, a band edge, a pivot). Overlays the price axis.
 * - `'delta'` — the source's units but a *difference*, zero-centred (MACD,
 *   momentum, Elder Ray). Its own axis.
 * - `'percent'` — a percentage scale, bounded or not (RSI 0..100,
 *   Williams %R −100..0, a percent rate of change).
 * - `'ratio'` — dimensionless and unbounded (a z-score, correlation, R²,
 *   Fisher transform, a stochastic already scaled 0..1).
 * - `'signal'` — a small integer verdict (`+1` / `0` / `−1`, a direction, a
 *   cross event).
 * - `'volume'` — in the volume column's units (OBV, A/D, money flow).
 * - `'index'` — a cumulative index with an arbitrary origin (NVI/PVI at
 *   1000, accumulative swing index).
 * - `'bars'` — a bar count.
 */
export type StudyUnit =
  | 'inherit'
  | 'delta'
  | 'percent'
  | 'ratio'
  | 'signal'
  | 'volume'
  | 'index'
  | 'bars';

/** A column the study reads. The `role` is the options key it is passed by. */
export interface StudyInput {
  readonly role: string;
  /** The column read when the option is omitted. **Absent means required**
   *  (a `benchmark` has no sensible default). */
  readonly default?: string;
  readonly label?: string;
}

/**
 * A numeric option. Mirrors `@pond-ts/process`'s `NumberParam` so a
 * consumer's registry can map it rather than interpret it.
 */
export interface StudyNumberParam {
  readonly kind: 'number' | 'integer';
  /** The value used when the option is omitted. **Absent means required.** */
  readonly default?: number;
  /** For a **required** option only: a value that makes the study runnable —
   *  a UI placeholder, and what the catalog's own test runs with. */
  readonly example?: number;
  /** The legal range — declared only where the study **validates** it (the
   *  catalog test runs `min − 1` and expects a throw). For a real-valued
   *  option `min` is the infimum: a `stdDev` declares `min: 0` and the study
   *  still rejects exactly `0`. */
  readonly min?: number;
  readonly max?: number;
  /** The **useful** range, within `[min, max]` — what a control is drawn on.
   *  Advisory: nothing rejects a value outside it. */
  readonly suggest?: readonly [number, number];
  readonly label?: string;
}

/** A closed-menu option (`maType`, a pivot `method`). */
export interface StudyEnumParam {
  readonly kind: 'enum';
  readonly default?: string;
  readonly example?: string;
  readonly of: readonly string[];
  readonly label?: string;
}

export type StudyParam = StudyNumberParam | StudyEnumParam;

/** One appended column. */
export interface StudyOutput {
  /**
   * The column's name suffix under `prefix` naming (`'Upper'` in
   * `bbUpper`), or `''` under `output` naming, where the single column *is*
   * the `output` option's value.
   */
  readonly id: string;
  readonly unit: StudyUnit;
  readonly label?: string;
}

/**
 * How the study names what it appends: a single column named by the
 * `output` option, or a family named `${prefix}${output.id}`.
 */
export type StudyNaming =
  | { readonly kind: 'output'; readonly default: string }
  | { readonly kind: 'prefix'; readonly default: string };

/** The study function, widened to the wide schema for a registry to call. */
export type StudyRun = (
  series: TimeSeries<SeriesSchema>,
  options: Readonly<Record<string, unknown>>,
) => TimeSeries<SeriesSchema>;

/**
 * A study, described at runtime — every fact a registry or a picker needs
 * that the options interface and the return type carry only at the type
 * level. Built with {@link defineStudy}, which checks the description
 * against the study's own options interface at compile time; the catalog
 * test runs every one against the study at test time.
 */
export interface StudyDescriptor {
  /** The study's exported name, and its fluent method (`'atr'`). */
  readonly name: string;
  readonly family: StudyFamily;
  /** One line, for a menu. */
  readonly summary: string;
  /** The columns read, in options order. */
  readonly inputs: readonly StudyInput[];
  /** The numeric and menu options, keyed by the options key, in options
   *  order. Column inputs, `output`/`prefix` and the session anchor are
   *  not params. */
  readonly params: Readonly<Record<string, StudyParam>>;
  readonly naming: StudyNaming;
  /** The columns appended, in the order the study appends them, **under the
   *  default params** — a menu value can change the set (`pivotPoints`'
   *  `'camarilla'` adds an `R4`/`S4` pair); the study's docstring says so
   *  where it does. */
  readonly outputs: readonly StudyOutput[];
  /**
   * `'session'` for the two session-anchored studies (`sessionVwap`,
   * `pivotPoints`): besides the inputs and params here, the call needs
   * exactly one of `sessions` (a `TradingCalendar` or `Session[]`, with an
   * optional `stamped`) or `session` (a session-id column). A registry
   * supplies that from its own calendar; it is not a param.
   */
  readonly anchor?: 'session';
  readonly run: StudyRun;
}
