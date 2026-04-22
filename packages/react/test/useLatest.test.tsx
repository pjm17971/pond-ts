import { describe, expect, it } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { LiveSeries } from 'pond-ts';
import { useLatest } from '../src/index.js';
import { schema, row } from './helpers.js';

describe('useLatest', () => {
  it('returns null for an empty source', () => {
    const live = new LiveSeries({ name: 'cpu', schema });
    const { result } = renderHook(() => useLatest(live, { throttle: 0 }));
    expect(result.current).toBeNull();
  });

  it('returns null for a null source', () => {
    const { result } = renderHook(() => useLatest(null, { throttle: 0 }));
    expect(result.current).toBeNull();
  });

  it('returns the last event from a pre-populated source', () => {
    const live = new LiveSeries({ name: 'cpu', schema });
    live.push(row(0, 0.3), row(1, 0.8));

    const { result } = renderHook(() => useLatest(live, { throttle: 0 }));
    expect(result.current).not.toBeNull();
    expect(result.current!.get('cpu')).toBe(0.8);
  });

  it('updates when a new event is pushed', () => {
    const live = new LiveSeries({ name: 'cpu', schema });
    const { result } = renderHook(() => useLatest(live, { throttle: 0 }));

    expect(result.current).toBeNull();

    act(() => {
      live.push(row(0, 0.5));
    });

    expect(result.current).not.toBeNull();
    expect(result.current!.get('cpu')).toBe(0.5);

    act(() => {
      live.push(row(1, 0.9));
    });

    expect(result.current!.get('cpu')).toBe(0.9);
  });

  it('works with a LiveView source', () => {
    const live = new LiveSeries({ name: 'cpu', schema });
    live.push(row(0, 0.3), row(1, 0.8), row(2, 0.5));
    const view = live.filter((e) => (e.get('cpu') as number) > 0.4);

    const { result } = renderHook(() => useLatest(view, { throttle: 0 }));
    // Last passing event is row(2, 0.5)
    expect(result.current).not.toBeNull();
    expect(result.current!.get('cpu')).toBe(0.5);
  });
});
