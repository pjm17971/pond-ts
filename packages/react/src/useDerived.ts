import { useMemo } from 'react';
import type { TimeSeries, SeriesSchema } from 'pond-ts';

/**
 * Apply a batch transform to a `TimeSeries` snapshot, recomputing only when
 * the input snapshot changes.
 *
 * The `transform` function should be referentially stable (wrap in
 * `useCallback` if it captures changing dependencies). Returns `null` when the
 * input snapshot is `null`.
 *
 * @example
 * ```ts
 * const [, snapshot] = useLiveSeries(opts);
 * const smoothed = useDerived(snapshot, (s) =>
 *   s.smooth('cpu', 'ema', { alpha: 0.2 }),
 * );
 * ```
 */
export function useDerived<S extends SeriesSchema, R>(
  series: TimeSeries<S> | null,
  transform: (series: TimeSeries<S>) => R,
): R | null {
  // eslint-disable-next-line react-hooks/exhaustive-deps
  return useMemo(() => {
    if (series === null) return null;
    return transform(series);
  }, [series, transform]);
}
