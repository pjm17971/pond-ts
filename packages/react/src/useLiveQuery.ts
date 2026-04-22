import { useMemo } from 'react';
import type { LiveSource, SeriesSchema, TimeSeries } from 'pond-ts';
import {
  useSnapshot,
  type SnapshotSource,
  type UseSnapshotOptions,
} from './useSnapshot.js';

/**
 * Build a derived live view once, subscribe, and return both the view and a
 * throttled snapshot — combining `useMemo` + `useSnapshot` in one call.
 *
 * Typical dashboard code creates 5–10 derived views (filters, aggregations,
 * rolling windows). Without this hook each one needs a manual `useMemo` for
 * stability plus a separate `useSnapshot` for subscription. `useLiveQuery`
 * bundles that into a single call whose return shape matches `useLiveSeries`:
 *
 * ```ts
 * const [, rollingSnap] = useLiveQuery(
 *   () => live.rolling('1m', { cpu: 'avg' }),
 *   [live],
 *   { throttle: 200 },
 * );
 *
 * const [highCpu, highCpuSnap] = useLiveQuery(
 *   () => live.filter((e) => e.get('cpu') > 0.7),
 *   [live],
 * );
 * ```
 *
 * @param build  Factory that creates the live view. Called once per `deps` change.
 * @param deps   React dependency array — when any dep changes, the view is rebuilt.
 * @param options  Snapshot throttle options (default 100 ms).
 * @returns `[view, snapshot]` — the stable view and its throttled `TimeSeries`.
 */
export function useLiveQuery<
  T extends SnapshotSource<S> | LiveSource<S>,
  S extends SeriesSchema = T extends SnapshotSource<infer U>
    ? U
    : T extends LiveSource<infer V>
      ? V
      : SeriesSchema,
>(
  build: () => T,
  deps: readonly unknown[],
  options?: UseSnapshotOptions,
): [T, TimeSeries<S> | null] {
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const view = useMemo(build, deps);
  const snapshot = useSnapshot(view as SnapshotSource<S>, options);
  return [view, snapshot];
}
