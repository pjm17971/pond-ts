import { useEffect, useMemo } from 'react';
import type { LiveSource, SeriesSchema, TimeSeries } from 'pond-ts';
import type { RollingWindow } from 'pond-ts';
import { useSnapshot, type UseSnapshotOptions } from './useSnapshot.js';

/** A `LiveSource` that supports `.window()` — `LiveSeries`, `LiveView`, accumulators. */
type Windowable<S extends SeriesSchema> = LiveSource<S> & {
  window(size: RollingWindow): LiveSource<S>;
};

/**
 * Create a windowed view of a live source and return a throttled snapshot.
 *
 * The view is created once per `source`/`size` pair and disposed on cleanup.
 * Returns `null` when the window is empty.
 */
export function useWindow<S extends SeriesSchema>(
  source: Windowable<S>,
  size: RollingWindow,
  options?: UseSnapshotOptions,
): TimeSeries<S> | null {
  const view = useMemo(() => source.window(size), [source, size]);

  useEffect(() => {
    return () => {
      if ('dispose' in view && typeof (view as any).dispose === 'function') {
        (view as any).dispose();
      }
    };
  }, [view]);

  return useSnapshot(view, options);
}
