import { describe, it, expect } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { LiveSeries } from 'pond-ts';
import { useSnapshot, useDerived } from '../src/index.js';
import { schema, row } from './helpers.js';

describe('useDerived', () => {
  it('returns null when snapshot is null', () => {
    const { result } = renderHook(() =>
      useDerived(null, (s) => s.select('cpu')),
    );
    expect(result.current).toBeNull();
  });

  it('applies a batch transform to a snapshot', () => {
    const live = new LiveSeries({ name: 'cpu', schema });
    live.push(row(0, 0.2), row(1, 0.8), row(2, 0.3));

    const { result } = renderHook(() => {
      const snap = useSnapshot(live, { throttle: 0 });
      return useDerived(snap, (s) =>
        s.filter((e) => (e.get('cpu') as number) > 0.5),
      );
    });

    expect(result.current).not.toBeNull();
    expect(result.current!.length).toBe(1);
    expect(result.current!.at(0)!.get('cpu')).toBe(0.8);
  });

  it('recomputes when the snapshot changes', () => {
    const live = new LiveSeries({ name: 'cpu', schema });
    live.push(row(0, 0.42));

    const { result } = renderHook(() => {
      const snap = useSnapshot(live, { throttle: 0 });
      return useDerived(snap, (s) => s.length);
    });

    expect(result.current).toBe(1);

    act(() => {
      live.push(row(1, 0.55));
    });

    expect(result.current).toBe(2);
  });

  it('returns the same reference when snapshot is unchanged', () => {
    const live = new LiveSeries({ name: 'cpu', schema });
    live.push(row(0, 0.42));

    const transform = (s: any) => s.select('cpu');

    const { result, rerender } = renderHook(() => {
      const snap = useSnapshot(live, { throttle: 0 });
      return useDerived(snap, transform);
    });

    const first = result.current;
    rerender();
    // Snapshot reference doesn't change if no new events, so derived stays stable
    expect(result.current).toBe(first);
  });
});
