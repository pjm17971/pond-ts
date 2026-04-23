import { useMemo } from 'react';
import type {
  AggregateMap,
  DurationInput,
  LiveSource,
  ReduceResult,
  SeriesSchema,
} from 'pond-ts';
import {
  useSnapshot,
  type SnapshotSource,
  type UseSnapshotOptions,
} from './useSnapshot.js';

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
 * one memo, and narrow per-entry types inherited from
 * `TimeSeries.reduce`.
 *
 * ```ts
 * const current = useCurrent(live, { cpu: 'avg', host: 'unique' });
 * //   ^ { cpu: number | undefined;
 * //       host: ReadonlyArray<ScalarValue> | undefined }
 *
 * const recent = useCurrent(live, { cpu: 'p95' }, { tail: '30s' });
 * ```
 *
 * Returns a stable-shape object while the source has no events (every
 * mapped field is `undefined`), so destructuring on first render is
 * safe.
 */
export function useCurrent<
  S extends SeriesSchema,
  const Mapping extends AggregateMap<S>,
>(
  source: SnapshotSource<S> | LiveSource<S> | null,
  mapping: Mapping,
  options?: UseCurrentOptions,
): ReduceResult<S, Mapping> {
  const snap = useSnapshot(source, options);
  const tailOpt = options?.tail;

  return useMemo(() => {
    if (!snap) {
      // Stable empty-shape result so destructuring never explodes on
      // first render.
      const empty: Record<string, unknown> = {};
      for (const key of Object.keys(mapping)) empty[key] = undefined;
      return empty as ReduceResult<S, Mapping>;
    }
    const scoped = tailOpt !== undefined ? snap.tail(tailOpt) : snap;
    return scoped.reduce(mapping) as ReduceResult<S, Mapping>;
  }, [snap, tailOpt, mapping]);
}
