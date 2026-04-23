import { describe, expect, it } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useMemo } from 'react';
import { LiveSeries } from 'pond-ts';
import { useCurrent } from '../src/index.js';
import { schema, row } from './helpers.js';

describe('useCurrent', () => {
  it('returns a stable empty-shape result for an empty source', () => {
    const live = new LiveSeries({ name: 'cpu', schema });
    const { result } = renderHook(() =>
      useCurrent(live, { cpu: 'avg', host: 'unique' }, { throttle: 0 }),
    );
    // Destructuring should work without null-checks on first render.
    const { cpu, host } = result.current;
    expect(cpu).toBeUndefined();
    expect(host).toBeUndefined();
  });

  it('returns the same shape when the source is null', () => {
    const { result } = renderHook(() =>
      useCurrent(null, { cpu: 'avg', host: 'unique' }, { throttle: 0 }),
    );
    const { cpu, host } = result.current;
    expect(cpu).toBeUndefined();
    expect(host).toBeUndefined();
  });

  it('reduces the full series when no tail is given', () => {
    const live = new LiveSeries({ name: 'cpu', schema });
    live.push(row(0, 0.3, 'api-1'), row(1, 0.5, 'api-2'), row(2, 0.7, 'api-1'));

    const { result } = renderHook(() =>
      useCurrent(live, { cpu: 'avg', host: 'unique' }, { throttle: 0 }),
    );

    expect(result.current.cpu).toBeCloseTo(0.5, 6);
    expect(result.current.host).toEqual(['api-1', 'api-2']);
  });

  it('reduces only the trailing window when tail is given', () => {
    const live = new LiveSeries({ name: 'cpu', schema });
    // events at 0, 60s, 120s, 180s
    live.push(
      row(0, 0.1, 'api-1'),
      row(1, 0.2, 'api-1'),
      row(2, 0.3, 'api-2'),
      row(3, 0.4, 'api-3'),
    );

    const { result } = renderHook(() =>
      useCurrent(
        live,
        { cpu: 'avg', host: 'unique' },
        { tail: '90s', throttle: 0 },
      ),
    );

    // lastBegin = 180s, cutoff = 180s - 90s = 90s
    // keep events where begin > 90s -> t=120s (api-2) and t=180s (api-3)
    expect(result.current.cpu).toBeCloseTo(0.35, 6);
    expect(result.current.host).toEqual(['api-2', 'api-3']);
  });

  it('updates as events are pushed', () => {
    const live = new LiveSeries({ name: 'cpu', schema });
    const { result } = renderHook(() =>
      useCurrent(live, { cpu: 'avg' }, { throttle: 0 }),
    );

    expect(result.current.cpu).toBeUndefined();

    act(() => {
      live.push(row(0, 0.4));
    });
    expect(result.current.cpu).toBe(0.4);

    act(() => {
      live.push(row(1, 0.6));
    });
    expect(result.current.cpu).toBeCloseTo(0.5, 6);
  });

  it('narrows the return type so no casts are needed', () => {
    // Compile-time check: the result fields should narrow to their reducer
    // output kinds AND thread through the source column kind.
    // 'host' is declared kind: 'string', so `unique` narrows to
    // `ReadonlyArray<string>` — not the wide scalar union.
    const live = new LiveSeries({ name: 'cpu', schema });
    live.push(row(0, 0.5, 'api-1'));

    const { result } = renderHook(() =>
      useCurrent(live, { cpu: 'avg', host: 'unique' }, { throttle: 0 }),
    );

    const typed: {
      cpu: number | undefined;
      host: ReadonlyArray<string> | undefined;
    } = result.current;
    expect(typed.cpu).toBe(0.5);
    expect(typed.host).toEqual(['api-1']);
  });

  it('works with a LiveView as the source', () => {
    const live = new LiveSeries({ name: 'cpu', schema });
    live.push(row(0, 0.3, 'api-1'), row(1, 0.8, 'api-1'), row(2, 0.5, 'api-2'));
    const hot = live.filter((e) => (e.get('cpu') as number) > 0.4);

    const { result } = renderHook(() =>
      useCurrent(hot, { host: 'unique' }, { throttle: 0 }),
    );

    expect(result.current.host).toEqual(['api-1', 'api-2']);
  });

  describe('reference stability', () => {
    it('returns the same reference when a push leaves the aggregate unchanged', () => {
      // Pushing identical rows leaves avg and unique-hosts unchanged.
      const live = new LiveSeries({ name: 'cpu', schema });
      live.push(row(0, 0.5, 'api-1'));

      const { result } = renderHook(() =>
        useCurrent(live, { cpu: 'avg', host: 'unique' }, { throttle: 0 }),
      );

      const first = result.current;
      expect(first.cpu).toBe(0.5);
      expect(first.host).toEqual(['api-1']);

      // Push another identical row. The snapshot changes (new event
      // in the buffer), the reduce runs again, but the output values
      // are the same -> stable reference at both the top level and
      // the per-field level.
      act(() => {
        live.push(row(1, 0.5, 'api-1'));
      });

      expect(result.current).toBe(first);
      expect(result.current.host).toBe(first.host);
    });

    it('preserves stable array reference even when a sibling scalar changes', () => {
      // Dashboard use case: `cpu` changes every push, `host` doesn't.
      // Consumer memos keyed off `host` should NOT re-fire on every
      // cpu update — per-field stability covers that.
      const live = new LiveSeries({ name: 'cpu', schema });
      live.push(row(0, 0.5, 'api-1'));

      const { result } = renderHook(() =>
        useCurrent(live, { cpu: 'avg', host: 'unique' }, { throttle: 0 }),
      );

      const firstHost = result.current.host;
      expect(firstHost).toEqual(['api-1']);

      // Push a different cpu value for the same host.
      act(() => {
        live.push(row(1, 0.7, 'api-1'));
      });

      // Top-level ref differs because cpu changed...
      expect(result.current.cpu).toBeCloseTo(0.6, 6);
      // ...but the host array ref is still the original.
      expect(result.current.host).toBe(firstHost);
    });

    it('returns a new reference when a scalar value changes', () => {
      const live = new LiveSeries({ name: 'cpu', schema });
      live.push(row(0, 0.5, 'api-1'));

      const { result } = renderHook(() =>
        useCurrent(live, { cpu: 'avg' }, { throttle: 0 }),
      );
      const first = result.current;
      expect(first.cpu).toBe(0.5);

      act(() => {
        live.push(row(1, 0.7, 'api-1'));
      });

      expect(result.current).not.toBe(first);
      expect(result.current.cpu).toBeCloseTo(0.6, 6);
    });

    it('returns a new reference when an array field gains an element', () => {
      const live = new LiveSeries({ name: 'cpu', schema });
      live.push(row(0, 0.5, 'api-1'));

      const { result } = renderHook(() =>
        useCurrent(live, { host: 'unique' }, { throttle: 0 }),
      );
      const first = result.current;
      expect(first.host).toEqual(['api-1']);

      // New host appears — unique array now has length 2.
      act(() => {
        live.push(row(1, 0.5, 'api-2'));
      });

      expect(result.current).not.toBe(first);
      expect(result.current.host).toEqual(['api-1', 'api-2']);
    });

    it('returns a new reference when an array field keeps length but swaps an element', () => {
      // Rolling / tail semantics: a trailing window that evicts one
      // host and gains another shifts the unique set while preserving
      // length. Elementwise compare catches that.
      const live = new LiveSeries({ name: 'cpu', schema });
      live.push(row(0, 0.5, 'api-1'), row(1, 0.5, 'api-2'));

      const { result } = renderHook(() =>
        useCurrent(live, { host: 'unique' }, { tail: '90s', throttle: 0 }),
      );
      const first = result.current;
      expect(first.host).toEqual(['api-1', 'api-2']);

      // Push an event at t=4min — tail('90s') evicts api-1 + api-2 and
      // admits api-3 only. Length stays at 1, but previously it was 2.
      // Actually with only api-3 left, length changes. Let me pick a
      // cleaner test of equal-length-different-elements:
      //
      // Push api-3 at t=2min, then the window (t=2min - 90s, t=2min]
      // includes row(1,api-2) and row(2,api-3). Unique = [api-2, api-3]
      // — same length 2 as before ([api-1, api-2]), different elements.
      act(() => {
        live.push(row(2, 0.5, 'api-3'));
      });

      expect(result.current).not.toBe(first);
      expect(result.current.host).toEqual(['api-2', 'api-3']);
    });

    it('keeps the empty-shape reference stable across null-source renders', () => {
      const { result, rerender } = renderHook(() =>
        useCurrent(null, { cpu: 'avg', host: 'unique' }, { throttle: 0 }),
      );
      const first = result.current;
      expect(first.cpu).toBeUndefined();
      expect(first.host).toBeUndefined();

      rerender();

      expect(result.current).toBe(first);
    });

    it('stabilizes the value inside a consumer useMemo dep array', () => {
      // Downstream useMemo keyed off the array field should NOT re-run
      // when the host set stays constant, even as new cpu samples keep
      // arriving. Counting factory invocations pins that behavior.
      const live = new LiveSeries({ name: 'cpu', schema });
      live.push(row(0, 0.5, 'api-1'));

      let memoInvocations = 0;
      const { result } = renderHook(() => {
        const current = useCurrent(
          live,
          { cpu: 'avg', host: 'unique' },
          { throttle: 0 },
        );
        const hosts = useMemo(() => {
          memoInvocations += 1;
          return (current.host ?? []).join(',');
        }, [current.host]);
        return { current, hosts };
      });

      expect(memoInvocations).toBe(1);
      expect(result.current.hosts).toBe('api-1');

      // Same host again — `host` array ref stays stable — consumer
      // memo does NOT re-run, even though `cpu` (a different field)
      // changed.
      act(() => {
        live.push(row(1, 0.6, 'api-1'));
      });
      expect(memoInvocations).toBe(1);

      // New host — `host` array ref changes — memo re-runs.
      act(() => {
        live.push(row(2, 0.7, 'api-2'));
      });
      expect(memoInvocations).toBe(2);
      expect(result.current.hosts).toBe('api-1,api-2');
    });
  });
});
