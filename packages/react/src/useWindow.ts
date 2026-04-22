import { useEffect, useState } from 'react';
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
 * The view is created and disposed inside an effect, so it works correctly
 * under React StrictMode's double-mount cycle.
 * Returns `null` when the window is empty.
 */
export function useWindow<S extends SeriesSchema>(
  source: Windowable<S>,
  size: RollingWindow,
  options?: UseSnapshotOptions,
): TimeSeries<S> | null {
  const [view, setView] = useState<LiveSource<S> | null>(null);

  useEffect(() => {
    const v = source.window(size);
    setView(v);

    return () => {
      if ('dispose' in v && typeof (v as any).dispose === 'function') {
        (v as any).dispose();
      }
    };
  }, [source, size]);

  return useSnapshot(view, options);
}
