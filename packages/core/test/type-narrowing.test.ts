/**
 * Type-level tests verifying that event.get() narrows correctly.
 * These tests mostly just compile — if TypeScript accepts the assignments,
 * the types are correct.
 */
import { describe, expect, it } from 'vitest';
import { LiveSeries, Event, TimeSeries, Time, Sequence } from '../src/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'number' },
  { name: 'host', kind: 'string' },
  { name: 'healthy', kind: 'boolean' },
] as const;

describe('event.get() type narrowing', () => {
  it('narrows on LiveSeries.on() callback', () => {
    const live = new LiveSeries({ name: 'test', schema });
    live.push([0, 0.5, 'api-1', true]);

    live.on('event', (e) => {
      // These should compile without casts
      const cpu: number = e.get('cpu');
      const host: string = e.get('host');
      const healthy: boolean = e.get('healthy');
      expect(typeof cpu).toBe('number');
      expect(typeof host).toBe('string');
      expect(typeof healthy).toBe('boolean');
    });
  });

  it('narrows on LiveView filter callback', () => {
    const live = new LiveSeries({ name: 'test', schema });
    live.push([0, 0.5, 'api-1', true]);
    const view = live.filter((e) => {
      // Should compile without cast
      const cpu: number = e.get('cpu');
      return cpu > 0.3;
    });
    expect(view.length).toBe(1);
  });

  it('narrows on LiveView.on() callback', () => {
    const live = new LiveSeries({ name: 'test', schema });
    const view = live.filter(() => true);
    live.push([0, 0.5, 'api-1', true]);

    view.on('event', (e) => {
      const cpu: number = e.get('cpu');
      const host: string = e.get('host');
      expect(typeof cpu).toBe('number');
      expect(typeof host).toBe('string');
    });
  });

  it('narrows on TimeSeries.at()', () => {
    const ts = new TimeSeries({
      name: 'test',
      schema,
      rows: [[0, 0.5, 'api-1', true]],
    });
    const e = ts.at(0)!;
    // Should compile without casts
    const cpu: number = e.get('cpu');
    const host: string = e.get('host');
    const healthy: boolean = e.get('healthy');
    expect(cpu).toBe(0.5);
    expect(host).toBe('api-1');
    expect(healthy).toBe(true);
  });

  it('narrows through view.toTimeSeries()', () => {
    const live = new LiveSeries({ name: 'test', schema });
    live.push([0, 0.5, 'api-1', true]);
    const snap = live.filter(() => true).toTimeSeries();
    const e = snap.at(0)!;
    const cpu: number = e.get('cpu');
    expect(cpu).toBe(0.5);
  });

  it('narrows through LiveAggregation.closed()', () => {
    const live = new LiveSeries({ name: 'test', schema });
    live.push([0, 0.5, 'api-1', true], [60_000, 0.8, 'api-2', false]);
    const agg = live.aggregate(Sequence.every('1m'), { cpu: 'avg' });

    const snap = agg.closed();
    const e = snap.at(0);
    if (e) {
      // Should narrow to number | undefined (not ScalarValue | undefined)
      const cpu: number | undefined = e.get('cpu');
      expect(typeof cpu).toBe('number');
    }
  });

  it('narrows through LiveAggregation.at()', () => {
    const live = new LiveSeries({ name: 'test', schema });
    live.push([0, 0.5, 'api-1', true], [60_000, 0.8, 'api-2', false]);
    const agg = live.aggregate(Sequence.every('1m'), { cpu: 'avg' });

    const e = agg.at(0);
    if (e) {
      const cpu: number | undefined = e.get('cpu');
      expect(typeof cpu).toBe('number');
    }
  });

  it('narrows through LiveRollingAggregation', () => {
    const live = new LiveSeries({ name: 'test', schema });
    live.push([0, 0.5, 'api-1', true], [1000, 0.8, 'api-2', false]);
    const rolling = live.rolling('5m', { cpu: 'avg' });

    const e = rolling.at(-1);
    if (e) {
      const cpu: number | undefined = e.get('cpu');
      expect(typeof cpu).toBe('number');
    }
  });
});
