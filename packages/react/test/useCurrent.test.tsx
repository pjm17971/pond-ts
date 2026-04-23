import { describe, expect, it } from 'vitest';
import { renderHook, act } from '@testing-library/react';
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
});
