import { describe, it, expect, vi } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { LiveSeries, Sequence } from 'pond-ts';
import { useSnapshot } from '../src/index.js';
import { schema, row } from './helpers.js';

function makeLive() {
  return new LiveSeries({ name: 'cpu', schema });
}

describe('useSnapshot', () => {
  it('returns null for an empty source', () => {
    const live = makeLive();
    const { result } = renderHook(() => useSnapshot(live, { throttle: 0 }));
    expect(result.current).toBeNull();
  });

  it('snapshots a LiveSeries via toTimeSeries', () => {
    const live = makeLive();
    live.push(row(0, 0.4), row(1, 0.5));

    const { result } = renderHook(() => useSnapshot(live, { throttle: 0 }));
    expect(result.current).not.toBeNull();
    expect(result.current!.length).toBe(2);
  });

  it('updates when the source emits events', () => {
    const live = makeLive();
    const { result } = renderHook(() => useSnapshot(live, { throttle: 0 }));

    expect(result.current).toBeNull();

    act(() => {
      live.push(row(0, 0.42));
    });

    expect(result.current).not.toBeNull();
    expect(result.current!.length).toBe(1);

    act(() => {
      live.push(row(1, 0.55));
    });

    expect(result.current!.length).toBe(2);
  });

  it('snapshots a LiveView (filter)', () => {
    const live = makeLive();
    live.push(row(0, 0.2), row(1, 0.8), row(2, 0.3));

    const view = live.filter((e) => (e.get('cpu') as number) > 0.5);

    const { result } = renderHook(() => useSnapshot(view, { throttle: 0 }));
    expect(result.current).not.toBeNull();
    expect(result.current!.length).toBe(1);
    expect(result.current!.at(0)!.get('cpu')).toBe(0.8);
  });

  it('snapshots a LiveAggregation', () => {
    const live = makeLive();
    // Push 6 events spanning 5+ minutes to close at least one bucket
    for (let i = 0; i < 6; i++) {
      live.push(row(i, 0.1 * (i + 1)));
    }

    const agg = live.aggregate(Sequence.every('5m'), {
      cpu: 'avg',
      host: 'last',
    });

    const { result } = renderHook(() => useSnapshot(agg, { throttle: 0 }));
    // Should have at least 1 closed bucket
    expect(result.current).not.toBeNull();
    expect(result.current!.length).toBeGreaterThanOrEqual(1);
  });

  it('snapshots a LiveRollingAggregation', () => {
    const live = makeLive();
    live.push(row(0, 0.2), row(1, 0.4), row(2, 0.6));

    const rolling = live.rolling('3m', { cpu: 'avg' });

    const { result } = renderHook(() => useSnapshot(rolling, { throttle: 0 }));
    // LiveRollingAggregation produces one output per source event
    expect(result.current).not.toBeNull();
    expect(result.current!.length).toBe(3);
  });

  it('re-subscribes when source changes', () => {
    const live1 = makeLive();
    live1.push(row(0, 0.1));

    const live2 = new LiveSeries({ name: 'mem', schema });
    live2.push(row(0, 0.9), row(1, 0.8));

    const { result, rerender } = renderHook(
      ({ src }) => useSnapshot(src, { throttle: 0 }),
      { initialProps: { src: live1 as any } },
    );

    expect(result.current!.length).toBe(1);

    rerender({ src: live2 as any });

    expect(result.current!.length).toBe(2);
  });
});
