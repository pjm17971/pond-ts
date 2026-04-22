import { describe, expect, it } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { LiveSeries } from 'pond-ts';
import { useLiveQuery } from '../src/index.js';
import { schema, row } from './helpers.js';

describe('useLiveQuery', () => {
  it('returns a stable view and null snapshot for empty source', () => {
    const live = new LiveSeries({ name: 'cpu', schema });
    const { result } = renderHook(() =>
      useLiveQuery(() => live.filter(() => true), [live], { throttle: 0 }),
    );

    const [view, snap] = result.current;
    expect(view).toBeDefined();
    expect(view.length).toBe(0);
    expect(snap).toBeNull();
  });

  it('snapshots a derived filter view', () => {
    const live = new LiveSeries({ name: 'cpu', schema });
    live.push(row(0, 0.3), row(1, 0.8), row(2, 0.5));

    const { result } = renderHook(() =>
      useLiveQuery(
        () => live.filter((e) => (e.get('cpu') as number) > 0.4),
        [live],
        { throttle: 0 },
      ),
    );

    const [view, snap] = result.current;
    expect(view.length).toBe(2);
    expect(snap).not.toBeNull();
    expect(snap!.length).toBe(2);
  });

  it('updates snapshot when source emits new events', () => {
    const live = new LiveSeries({ name: 'cpu', schema });

    const { result } = renderHook(() =>
      useLiveQuery(() => live.filter(() => true), [live], { throttle: 0 }),
    );

    expect(result.current[1]).toBeNull();

    act(() => {
      live.push(row(0, 0.5));
    });

    expect(result.current[1]).not.toBeNull();
    expect(result.current[1]!.length).toBe(1);
  });

  it('keeps the same view reference across re-renders', () => {
    const live = new LiveSeries({ name: 'cpu', schema });
    const { result, rerender } = renderHook(() =>
      useLiveQuery(() => live.filter(() => true), [live], { throttle: 0 }),
    );

    const view1 = result.current[0];
    rerender();
    const view2 = result.current[0];
    expect(view1).toBe(view2);
  });

  it('works with rolling aggregation', () => {
    const live = new LiveSeries({ name: 'cpu', schema });
    live.push(row(0, 0.5), row(1, 0.8));

    const { result } = renderHook(() =>
      useLiveQuery(() => live.rolling('5m', { cpu: 'avg' }), [live], {
        throttle: 0,
      }),
    );

    const [, snap] = result.current;
    expect(snap).not.toBeNull();
    expect(snap!.length).toBe(2);
  });
});
