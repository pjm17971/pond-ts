import type { SeriesSchema, TimeSeries } from 'pond-ts';
import type {
  StudyDescriptor,
  StudyFamily,
  StudyInput,
  StudyOutput,
  StudyParam,
  StudyRun,
} from './types.js';

/*
 * The compile-time half of the catalog's drift guard.
 *
 * A descriptor is written against the study's own options interface,
 * instantiated on the wide schema (`AtrOptions<SeriesSchema, string>`).
 * On that instantiation every column option's type collapses to `never`
 * (`NumericColumnNameForSchema<SeriesSchema>` is `never`), every numeric
 * option is `number`, every menu option is a string-literal union, and
 * `output` / `prefix` are `string` — which is enough to CLASSIFY every
 * key mechanically and demand that the description covers each one:
 *
 * - `inputs` is a record over exactly the column keys;
 * - `params` is a record over exactly the numeric and menu keys;
 * - a menu's `of` must list every member of its union (`INCOMPLETE_MENUS`);
 * - an optional key must state its `default` (or `optional: true` with an
 *   `example`, when absence is a switch rather than a value); a required
 *   key must state an `example` instead (and may not claim a default);
 * - the session anchor (`sessions` / `session` / `stamped`) and the time
 *   anchor (`anchor`) are neither: the spec declares `anchor: 'session'` or
 *   `anchor: 'time'` and the consumer supplies the value;
 * - a key that is none of those (an object option) is reported as
 *   `UNCOVERED_OPTIONS`, so a new option shape cannot slip past
 *   undescribed.
 *
 * The runtime half is `test/catalog.test.ts`, which runs every descriptor
 * against its study and checks that the defaults and the column names it
 * claims are the ones the study actually uses.
 */

type Keys<O> = keyof O & string;
type Bare<O, K extends keyof O> = Exclude<O[K], undefined>;
type IsNever<T> = [T] extends [never] ? true : false;
type Optional<O, K extends keyof O> = {} extends Pick<O, K> ? true : false;

/** Column options: on the wide schema their type is `never`. */
type InputKeys<O> = {
  [K in Keys<O>]: IsNever<Bare<O, K>> extends true ? K : never;
}[Keys<O>];
type NumberKeys<O> = {
  [K in Keys<O>]: IsNever<Bare<O, K>> extends true
    ? never
    : [Bare<O, K>] extends [number]
      ? K
      : never;
}[Keys<O>];
/** Menu options: a string-literal union (not `string`, which is a name). */
type EnumKeys<O> = {
  [K in Keys<O>]: IsNever<Bare<O, K>> extends true
    ? never
    : [Bare<O, K>] extends [string]
      ? string extends Bare<O, K>
        ? never
        : K
      : never;
}[Keys<O>];

type NamingKeys = 'output' | 'prefix';
type AnchorKeys = 'sessions' | 'session' | 'stamped' | 'anchor';
type Excluded = NamingKeys | AnchorKeys;

type Uncovered<O> = Exclude<
  Keys<O>,
  InputKeys<O> | NumberKeys<O> | EnumKeys<O> | Excluded
>;

type InputSpec<O, K extends keyof O> =
  Optional<O, K> extends true
    ? { readonly default: string }
    : { readonly default?: never };

/**
 * An optional key states its `default` — or, when omitting it is not a
 * value but a switch (`balanceOfPower`'s `period`), `optional: true` with
 * an `example`. A required key states an `example` and nothing else.
 */
type Presence<V, O, K extends keyof O> =
  Optional<O, K> extends true
    ?
        | {
            readonly default: V;
            readonly example?: never;
            readonly optional?: never;
          }
        | {
            readonly default?: never;
            readonly example: V;
            readonly optional: true;
          }
    : {
        readonly default?: never;
        readonly example: V;
        readonly optional?: never;
      };

type NumberSpec<O, K extends keyof O> = {
  readonly kind: 'number' | 'integer';
  readonly min?: number;
  readonly max?: number;
  readonly suggest?: readonly [number, number];
  readonly requires?: Exclude<Keys<O>, K>;
} & Presence<number, O, K>;

type EnumSpec<O, K extends keyof O> = {
  readonly kind: 'enum';
  readonly of: readonly Bare<O, K>[];
  readonly requires?: Exclude<Keys<O>, K>;
} & Presence<Bare<O, K>, O, K>;

/**
 * What {@link defineStudy} takes for options interface `O` — see the file
 * comment for how each key is classified.
 */
export type StudySpec<O extends object> = {
  readonly name: string;
  readonly family: StudyFamily;
  readonly summary: string;
  readonly inputs: {
    readonly [K in Exclude<InputKeys<O>, Excluded>]: InputSpec<O, K>;
  };
  readonly params: {
    readonly [K in Exclude<NumberKeys<O>, Excluded>]: NumberSpec<O, K>;
  } & {
    readonly [K in Exclude<EnumKeys<O>, Excluded>]: EnumSpec<O, K>;
  };
  readonly naming: 'output' extends Keys<O>
    ? { readonly output: string }
    : 'prefix' extends Keys<O>
      ? { readonly prefix: string }
      : never;
  readonly outputs: readonly StudyOutput[];
  readonly run: (
    series: TimeSeries<SeriesSchema>,
    options: O,
  ) => TimeSeries<SeriesSchema>;
} & ('sessions' extends Keys<O>
  ? { readonly anchor: 'session' }
  : 'anchor' extends Keys<O>
    ? { readonly anchor: 'time' }
    : { readonly anchor?: never }) &
  (IsNever<Uncovered<O>> extends true
    ? { readonly UNCOVERED_OPTIONS?: never }
    : { readonly UNCOVERED_OPTIONS: Uncovered<O> });

/**
 * Every menu's `of` lists every member of its union. The spec's inferred
 * type carries the tuple (a `const` type parameter), so a member missing
 * from `of` is reported as `INCOMPLETE_MENUS: 'maType'`. This is why
 * {@link defineStudy} is curried: the options interface is given
 * explicitly and the spec is inferred, which TypeScript only allows across
 * two calls.
 */
type IncompleteMenus<O, D> = D extends { readonly params: infer P }
  ? {
      [K in Exclude<EnumKeys<O>, Excluded>]: K extends keyof P
        ? P[K] extends { readonly of: readonly (infer M)[] }
          ? [Bare<O, K & keyof O>] extends [M]
            ? never
            : K
          : K
        : K;
    }[Exclude<EnumKeys<O>, Excluded>]
  : never;

type MenusComplete<O, D> =
  IsNever<IncompleteMenus<O, D>> extends true
    ? { readonly INCOMPLETE_MENUS?: never }
    : { readonly INCOMPLETE_MENUS: IncompleteMenus<O, D> };

/**
 * Describe a study for the catalog, checked against its options interface.
 *
 * ```ts
 * export const atrDescriptor = defineStudy<AtrOptions<SeriesSchema, string>>()({
 *   name: 'atr',
 *   family: 'volatility',
 *   summary: "Wilder's average true range",
 *   inputs: {
 *     high: { default: 'high' },
 *     low: { default: 'low' },
 *     close: { default: 'close' },
 *   },
 *   params: {
 *     period: { kind: 'integer', default: 14, min: 1, suggest: [5, 50] },
 *   },
 *   naming: { output: 'atr' },
 *   outputs: [{ id: '', unit: 'delta' }],
 *   run: atr,
 * });
 * ```
 *
 * The record forms of `inputs` and `params` exist for the compiler: a
 * mapped type over the options keys is what makes a missed or misspelt key
 * an error. The descriptor hands them on as the consumer-facing shapes
 * (`inputs` an array in options order, `params` keyed by name).
 */
export function defineStudy<O extends object>(): <const D extends StudySpec<O>>(
  spec: D & MenusComplete<O, D>,
) => StudyDescriptor {
  return (spec) => build(spec as StudySpec<O>);
}

function build<O extends object>(spec: StudySpec<O>): StudyDescriptor {
  const inputs: StudyInput[] = [];
  for (const [role, def] of Object.entries(
    spec.inputs as Record<string, { default?: string }>,
  )) {
    inputs.push({
      role,
      ...(def.default !== undefined && { default: def.default }),
    });
  }
  const params: Record<string, StudyParam> = {};
  for (const [name, def] of Object.entries(
    spec.params as Record<string, StudyParam>,
  )) {
    params[name] = def;
  }
  const naming = spec.naming as { output?: string; prefix?: string };
  const descriptor: StudyDescriptor = {
    name: spec.name,
    family: spec.family,
    summary: spec.summary,
    inputs,
    params,
    naming:
      naming.output !== undefined
        ? { kind: 'output', default: naming.output }
        : { kind: 'prefix', default: naming.prefix! },
    outputs: spec.outputs,
    ...((spec as { anchor?: 'session' | 'time' }).anchor !== undefined && {
      anchor: (spec as { anchor: 'session' | 'time' }).anchor,
    }),
    run: spec.run as unknown as StudyRun,
  };
  return descriptor;
}
