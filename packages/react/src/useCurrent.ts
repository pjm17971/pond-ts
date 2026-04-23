import { useMemo } from 'react';
import type {
  AggregateMap,
  AggregateReducer,
  ColumnValue,
  DurationInput,
  LiveSource,
  NormalizedValueForKind,
  ScalarKind,
  SeriesSchema,
  ValueColumnsForSchema,
} from 'pond-ts';
import {
  useSnapshot,
  type SnapshotSource,
  type UseSnapshotOptions,
} from './useSnapshot.js';

/**
 * Narrowed per-entry output for `useCurrent`. Derives each field's value
 * type from the source column kind and the reducer name so callers don't
 * have to `as`-cast. Numeric reducers (`'sum'`, `'avg'`, `'count'`) emit
 * `number`; `'unique'` and `` `top${number}` `` emit
 * `ReadonlyArray<ScalarValue>`; other reducers (including custom
 * functions) fall back to the source column kind.
 */
type CurrentResult<S extends SeriesSchema, Mapping> = {
  [K in keyof Mapping & string]:
    | NormalizedValueForKind<
        K extends ValueColumnsForSchema<S>[number]['name']
          ? Mapping[K] extends 'sum' | 'avg' | 'count'
            ? 'number'
            : Mapping[K] extends 'unique' | `top${number}`
              ? 'array'
              : CurrentColumnKind<S, K>
          : ScalarKind
      >
    | undefined;
};

type CurrentColumnKind<S extends SeriesSchema, K extends string> =
  Extract<ValueColumnsForSchema<S>[number], { name: K }> extends {
    kind: infer Kind extends ScalarKind;
  }
    ? Kind
    : ScalarKind;

export interface UseCurrentOptions extends UseSnapshotOptions {
  /**
   * Trailing window to evaluate the mapping over, expressed as a
   * `DurationInput` (e.g. `'30s'`, `'5m'`, or a number of milliseconds).
   * When omitted, the full snapshot is used.
   */
  tail?: DurationInput;
}

/**
 * Subscribe to a live source and return the current value of a reducer
 * mapping, updated on a throttle. Equivalent to
 * `useSnapshot(src).tail(tail).reduce(mapping)` but with one subscription,
 * one memo, and narrow per-entry types.
 *
 * ```ts
 * const current = useCurrent(live, { cpu: 'avg', host: 'unique' });
 * //   ^ { cpu: number | undefined; host: ReadonlyArray<ScalarValue> | undefined }
 *
 * const recent = useCurrent(live, { cpu: 'p95' }, { tail: '30s' });
 * ```
 *
 * Returns an empty-result object (every mapped field set to the
 * reducer's empty-bucket value) while the source has no events — the
 * shape of the return is stable regardless of source state, so
 * destructuring is always safe.
 */
export function useCurrent<
  S extends SeriesSchema,
  const Mapping extends AggregateMap<S>,
>(
  source: SnapshotSource<S> | LiveSource<S> | null,
  mapping: Mapping,
  options?: UseCurrentOptions,
): CurrentResult<S, Mapping> {
  const snap = useSnapshot(source, options);
  const tailOpt = options?.tail;

  return useMemo(() => {
    if (!snap) {
      // Stable empty-shape result so destructuring never explodes on
      // first render.
      const empty: Record<string, ColumnValue | undefined> = {};
      for (const key of Object.keys(mapping)) empty[key] = undefined;
      return empty as CurrentResult<S, Mapping>;
    }
    const scoped = tailOpt !== undefined ? snap.tail(tailOpt) : snap;
    return scoped.reduce(
      mapping as AggregateMap<
        typeof scoped extends { schema: infer T }
          ? T extends SeriesSchema
            ? T
            : SeriesSchema
          : SeriesSchema
      >,
    ) as unknown as CurrentResult<S, Mapping>;
  }, [snap, tailOpt, mapping]);
}

// Silence unused-type-param warnings for `AggregateReducer` re-export
// compatibility: kept in the signature surface for future extensions
// (custom reducers with typed output kind).
export type _UseCurrentReducerHint = AggregateReducer;
