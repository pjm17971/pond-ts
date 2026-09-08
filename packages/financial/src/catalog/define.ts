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
 * - an optional key must state its `default`; a required key must state an
 *   `example` instead (and may not claim a default);
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
type AnchorKeys = 'sessions' | 'session' | 'stamped';
type Excluded = NamingKeys | AnchorKeys;

type Uncovered<O> = Exclude<
  Keys<O>,
  InputKeys<O> | NumberKeys<O> | EnumKeys<O> | Excluded
>;

type InputSpec<O, K extends keyof O> = {
  readonly label?: string;
} & (Optional<O, K> extends true
  ? { readonly default: string }
  : { readonly default?: never });

type NumberSpec<O, K extends keyof O> = {
  readonly kind: 'number' | 'integer';
  readonly min?: number;
  readonly max?: number;
  readonly suggest?: readonly [number, number];
  readonly label?: string;
} & (Optional<O, K> extends true
  ? { readonly default: number; readonly example?: never }
  : { readonly default?: never; readonly example: number });

type EnumSpec<O, K extends keyof O> = {
  readonly kind: 'enum';
  readonly of: readonly Bare<O, K>[];
  readonly label?: string;
} & (Optional<O, K> extends true
  ? { readonly default: Bare<O, K>; readonly example?: never }
  : { readonly default?: never; readonly example: Bare<O, K> });

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
  : { readonly anchor?: never }) &
  (IsNever<Uncovered<O>> extends true
    ? { readonly UNCOVERED_OPTIONS?: never }
    : { readonly UNCOVERED_OPTIONS: Uncovered<O> });

/**
 * Describe a study for the catalog, checked against its options interface.
 *
 * ```ts
 * export const atrDescriptor = defineStudy<AtrOptions<SeriesSchema, string>>({
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
export function defineStudy<O extends object>(
  spec: StudySpec<O>,
): StudyDescriptor {
  const inputs: StudyInput[] = [];
  for (const [role, def] of Object.entries(
    spec.inputs as Record<string, { default?: string; label?: string }>,
  )) {
    inputs.push({
      role,
      ...(def.default !== undefined && { default: def.default }),
      ...(def.label !== undefined && { label: def.label }),
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
    ...((spec as { anchor?: 'session' }).anchor === 'session' && {
      anchor: 'session' as const,
    }),
    run: spec.run as unknown as StudyRun,
  };
  return descriptor;
}
