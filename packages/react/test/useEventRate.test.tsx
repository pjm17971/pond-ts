import { describe, expect, it } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { LiveSeries } from 'pond-ts';
import { useEventRate } from '../src/index.js';
import { schema } from './helpers.js';

/** Row helper with millisecond resolution (vs the 1-minute-stride
 *  helper in helpers.ts). Local to this file because rate-style
 *  tests need sub-second control over event timing. */
function rowMs(
  ts: number,
  cpu: number,
  host = 'api-1',
): [Date, number, string] {
  return [new Date(ts), cpu, host];
}

describe('useEventRate', () => {
  it('returns 0 for an empty source', () => {
    const live = new LiveSeries({ name: 'cpu', schema });
    const { result } = renderHook(() =>
      useEventRate(live, '1m', { throttle: 0 }),
    );
    expect(result.current).toBe(0);
  });

  it('reflects events-per-second over the windowed view', () => {
    const live = new LiveSeries({ name: 'cpu', schema });
    // 60 events evenly spaced across 1 minute → 1 event/sec
    const t0 = 1_700_000_000_000;
    for (let i = 0; i < 60; i += 1) {
      live.push(rowMs(t0 + i * 1000, 0.1));
    }
    const { result } = renderHook(() =>
      useEventRate(live, '1m', { throttle: 0 }),
    );
    expect(result.current).toBeCloseTo(1.0, 6);
  });

  it('updates as new events land', () => {
    const live = new LiveSeries({ name: 'cpu', schema });
    const t0 = 1_700_000_000_000;
    const { result } = renderHook(() =>
      useEventRate(live, '1m', { throttle: 0 }),
    );
    expect(result.current).toBe(0);

    act(() => {
      live.push(rowMs(t0, 0.5));
    });
    expect(result.current).toBeCloseTo(1 / 60, 6);

    act(() => {
      live.push(rowMs(t0 + 1, 0.5));
      live.push(rowMs(t0 + 2, 0.5));
    });
    expect(result.current).toBeCloseTo(3 / 60, 6);
  });

  it('drops as the window evicts older events', () => {
    const live = new LiveSeries({ name: 'cpu', schema });
    const t0 = 1_700_000_000_000;
    const { result } = renderHook(() =>
      useEventRate(live, '1s', { throttle: 0 }),
    );

    act(() => {
      live.push(rowMs(t0, 0.1));
      live.push(rowMs(t0 + 200, 0.2));
      live.push(rowMs(t0 + 400, 0.3));
      live.push(rowMs(t0 + 600, 0.4));
    });
    // 4 events in a 1-second window → 4/s
    expect(result.current).toBeCloseTo(4.0, 6);

    act(() => {
      // Push 5 seconds later — view drops everything older than (t-1s)
      live.push(rowMs(t0 + 5000, 0.5));
    });
    expect(result.current).toBeCloseTo(1.0, 6);
  });

  it('matches the gRPC experiment scenario (8 events/sec → 8.0/s)', () => {
    const live = new LiveSeries({ name: 'cpu', schema });
    const t0 = 1_700_000_000_000;
    // 60 seconds × 8 events/sec, hosts staggered by 1 ms inside each
    // 500 ms tick to avoid the LiveSeries strict-ordering check.
    for (let s = 0; s < 60; s += 1) {
      for (let h = 0; h < 4; h += 1) {
        live.push(rowMs(t0 + s * 1000 + h, 0.5, `host-${h}`));
      }
      for (let h = 0; h < 4; h += 1) {
        live.push(rowMs(t0 + s * 1000 + 500 + h, 0.5, `host-${h}`));
      }
    }
    const { result } = renderHook(() =>
      useEventRate(live, '1m', { throttle: 0 }),
    );
    expect(result.current).toBeCloseTo(8.0, 1);
  });
});
