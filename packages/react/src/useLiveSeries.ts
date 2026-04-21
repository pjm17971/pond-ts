import { useRef } from 'react';
import { LiveSeries } from 'pond-ts';
import type { LiveSeriesOptions, SeriesSchema, TimeSeries } from 'pond-ts';
import { useSnapshot, type UseSnapshotOptions } from './useSnapshot.js';

/**
 * Create and own a `LiveSeries` for the component's lifetime.
 *
 * Returns `[live, snapshot]`:
 * - `live` — a stable `LiveSeries` ref. Push events into it from effects or
 *   callbacks (e.g. WebSocket `onmessage`).
 * - `snapshot` — a throttled `TimeSeries` that updates when new events arrive.
 *   `null` when the series is empty.
 *
 * The `LiveSeries` is created once on mount. Changing `options` after mount has
 * no effect — pass stable options (literal or top-level constant).
 */
export function useLiveSeries<S extends SeriesSchema>(
  options: LiveSeriesOptions<S>,
  hookOptions?: UseSnapshotOptions,
): [LiveSeries<S>, TimeSeries<S> | null] {
  // Create the LiveSeries once and keep it for the component lifetime.
  const liveRef = useRef<LiveSeries<S> | undefined>(undefined);
  if (liveRef.current === undefined) {
    liveRef.current = new LiveSeries(options);
  }

  const snapshot = useSnapshot(liveRef.current, hookOptions);

  return [liveRef.current, snapshot];
}
