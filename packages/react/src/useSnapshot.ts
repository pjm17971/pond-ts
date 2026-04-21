import { useEffect, useRef, useState } from 'react';
import type { LiveSource, SeriesSchema } from 'pond-ts';
import type { TimeSeries } from 'pond-ts';
import { takeSnapshot } from './takeSnapshot.js';

export interface UseSnapshotOptions {
  /** Minimum milliseconds between snapshot rebuilds. Default 100. Set to 0 for immediate. */
  throttle?: number;
}

/**
 * Subscribe to any `LiveSource` and return a throttled `TimeSeries` snapshot.
 *
 * The snapshot updates at most once per `throttle` interval (default 100 ms).
 * Returns `null` when the source is empty.
 */
export function useSnapshot<S extends SeriesSchema>(
  source: LiveSource<S>,
  options?: UseSnapshotOptions,
): TimeSeries<S> | null {
  const throttleMs = options?.throttle ?? 100;
  const [snapshot, setSnapshot] = useState<TimeSeries<S> | null>(() =>
    takeSnapshot(source),
  );

  // Track the latest source + throttle so the effect re-subscribes when they change.
  const sourceRef = useRef(source);
  sourceRef.current = source;

  useEffect(() => {
    // Re-snapshot on source change
    setSnapshot(takeSnapshot(source));

    let timer: ReturnType<typeof setTimeout> | null = null;
    let pending = false;

    const flush = () => {
      timer = null;
      pending = false;
      setSnapshot(takeSnapshot(sourceRef.current));
    };

    const unsub = source.on('event', () => {
      if (throttleMs === 0) {
        setSnapshot(takeSnapshot(sourceRef.current));
        return;
      }
      if (!pending) {
        pending = true;
        timer = setTimeout(flush, throttleMs);
      }
    });

    return () => {
      unsub();
      if (timer !== null) clearTimeout(timer);
    };
  }, [source, throttleMs]);

  return snapshot;
}
