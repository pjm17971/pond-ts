import { describe, it, expect } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { LiveSeries } from 'pond-ts';
import { useWindow } from '../src/index.js';
import { schema, row } from './helpers.js';

describe('useWindow', () => {
  it('returns null when the source is empty', () => {
    const live = new LiveSeries({ name: 'cpu', schema });
    const { result } = renderHook(() => useWindow(live, '3m', { throttle: 0 }));
    expect(result.current).toBeNull();
  });

  it('returns a snapshot of the windowed view', () => {
    const live = new LiveSeries({ name: 'cpu', schema });
    live.push(row(0, 0.1), row(1, 0.2), row(2, 0.3));

    const { result } = renderHook(() => useWindow(live, '3m', { throttle: 0 }));

    expect(result.current).not.toBeNull();
    // All 3 events are within a 3-minute window
    expect(result.current!.length).toBeGreaterThanOrEqual(2);
  });

  it('evicts events outside the window as new events arrive', () => {
    const live = new LiveSeries({ name: 'cpu', schema });
    // Seed with events at t=0..4m
    for (let i = 0; i < 5; i++) {
      live.push(row(i, 0.1 * (i + 1)));
    }

    const { result } = renderHook(() => useWindow(live, '2m', { throttle: 0 }));

    // Push an event at t=10m — only events within 2m of it should remain
    act(() => {
      live.push(row(10, 0.99));
    });

    const snap = result.current!;
    expect(snap).not.toBeNull();
    // Only the t=10 event should be in a 2-minute window
    expect(snap.length).toBe(1);
    expect(snap.at(0)!.get('cpu')).toBe(0.99);
  });

  it('updates reactively on new pushes', () => {
    const live = new LiveSeries({ name: 'cpu', schema });
    const { result } = renderHook(() => useWindow(live, '5m', { throttle: 0 }));

    expect(result.current).toBeNull();

    act(() => {
      live.push(row(0, 0.5));
    });

    expect(result.current).not.toBeNull();
    expect(result.current!.length).toBe(1);

    act(() => {
      live.push(row(1, 0.6));
    });

    expect(result.current!.length).toBe(2);
  });
});
