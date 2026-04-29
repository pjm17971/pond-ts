import { useEffect, useRef, useState } from 'react';
import type { LiveSource, RollingWindow, SeriesSchema } from 'pond-ts';
import type { UseSnapshotOptions } from './useSnapshot.js';

/**
 * `LiveSource` that supports `.window(duration)` — `LiveSeries`,
 * `LiveView`, and accumulators.
 */
type Windowable<S extends SeriesSchema> = LiveSource<S> & {
  window(size: RollingWindow): LiveSource<S> & {
    count(): number;
    eventRate(): number;
    on(type: 'event', fn: (...args: any[]) => void): () => void;
  };
};

/**
 * Subscribe to a live source, maintain a `.window(duration)` view
 * over it, and return its events-per-second as a reactive number.
 *
 * @example
 * ```tsx
 * const eventRate = useEventRate(liveSeries, '1m');
 * // <div>EVENT RATE {eventRate.toFixed(1)}/s</div>
 * ```
 *
 * Sugar over `live.window(duration).eventRate()` plus a subscription
 * that re-reads on each push and throttles updates. Closes the
 * boilerplate that the gRPC experiment's M1 friction notes
 * surfaced — `useCurrent(live, { cpu: 'count' }, { tail: '1m' }).cpu / 60`
 * collapses to one hook.
 *
 * Throttling matches `useSnapshot`: `throttle: 100` ms by default.
 * Returns `0` until the first event lands.
 *
 * @throws TypeError if `windowDuration` is a count-based window (a
 *   number) — `eventRate` requires a time denominator. Pass a
 *   duration string like `'1m'`, `'30s'`, or a number-of-ms via
 *   `DurationInput` instead.
 */
export function useEventRate<S extends SeriesSchema>(
  source: Windowable<S>,
  windowDuration: RollingWindow,
  options?: UseSnapshotOptions,
): number {
  const throttleMs = options?.throttle ?? 100;
  // Lazy initial value: take a one-shot read off a temporary view so a
  // hook mounted on an already-populated source renders the correct
  // rate on the first paint, not 0. The temporary view is GC'd when
  // the effect creates the long-lived view; no leak because we don't
  // subscribe.
  const [rate, setRate] = useState(() => {
    const initialView = source.window(windowDuration);
    const r = initialView.eventRate();
    if (
      'dispose' in initialView &&
      typeof (initialView as any).dispose === 'function'
    ) {
      (initialView as any).dispose();
    }
    return r;
  });
  const sourceRef = useRef(source);
  sourceRef.current = source;

  useEffect(() => {
    const view = source.window(windowDuration);
    setRate(view.eventRate());

    let timer: ReturnType<typeof setTimeout> | null = null;
    let pending = false;

    const flush = () => {
      timer = null;
      pending = false;
      setRate(view.eventRate());
    };

    const unsub = view.on('event', () => {
      if (throttleMs === 0) {
        setRate(view.eventRate());
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
      if ('dispose' in view && typeof (view as any).dispose === 'function') {
        (view as any).dispose();
      }
    };
  }, [source, windowDuration, throttleMs]);

  return rate;
}
