import { describe, it, expect, vi } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useLiveSeries } from '../src/index.js';
import { schema, row } from './helpers.js';

describe('useLiveSeries', () => {
  it('returns a stable LiveSeries and initial null snapshot', () => {
    const { result } = renderHook(() =>
      useLiveSeries({ name: 'cpu', schema }, { throttle: 0 }),
    );

    const [live, snapshot] = result.current;
    expect(live).toBeDefined();
    expect(snapshot).toBeNull();
  });

  it('updates snapshot after push + flush', async () => {
    const { result } = renderHook(() =>
      useLiveSeries({ name: 'cpu', schema }, { throttle: 0 }),
    );

    const [live] = result.current;

    act(() => {
      live.push(row(0, 0.42));
    });

    const [, snapshot] = result.current;
    expect(snapshot).not.toBeNull();
    expect(snapshot!.length).toBe(1);
    expect(snapshot!.at(0)!.get('cpu')).toBe(0.42);
  });

  it('keeps the same LiveSeries across re-renders', () => {
    const { result, rerender } = renderHook(() =>
      useLiveSeries({ name: 'cpu', schema }, { throttle: 0 }),
    );

    const firstLive = result.current[0];
    rerender();
    expect(result.current[0]).toBe(firstLive);
  });

  it('throttles snapshot updates', async () => {
    vi.useFakeTimers();

    const { result } = renderHook(() =>
      useLiveSeries({ name: 'cpu', schema }, { throttle: 50 }),
    );

    const [live] = result.current;

    // Push several events rapidly
    act(() => {
      live.push(row(0, 0.1));
      live.push(row(1, 0.2));
      live.push(row(2, 0.3));
    });

    // Snapshot shouldn't have updated yet (timer pending)
    expect(result.current[1]).toBeNull();

    // Advance past throttle
    await act(async () => {
      vi.advanceTimersByTime(60);
    });

    // Now the snapshot should reflect all 3 events
    expect(result.current[1]).not.toBeNull();
    expect(result.current[1]!.length).toBe(3);

    vi.useRealTimers();
  });

  it('snapshot includes all schema columns', () => {
    const { result } = renderHook(() =>
      useLiveSeries({ name: 'cpu', schema }, { throttle: 0 }),
    );

    act(() => {
      result.current[0].push(row(0, 0.55, 'web-3'));
    });

    const snap = result.current[1]!;
    const event = snap.at(0)!;
    expect(event.get('cpu')).toBe(0.55);
    expect(event.get('host')).toBe('web-3');
  });
});
