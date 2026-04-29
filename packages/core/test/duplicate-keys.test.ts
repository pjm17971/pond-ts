/**
 * Pins the library's behavior around events with duplicate temporal
 * keys. The gRPC experiment's M1 friction notes claimed `count`
 * "collapses same-ts events"; empirical reproduction shows the
 * opposite — every layer (LiveSeries.push, toTimeSeries, validate,
 * tail, reduce, aggregate, rolling, live aggregation) preserves
 * duplicates and counts them independently.
 *
 * Tests here lock that down so a future regression (intentional or
 * accidental) breaks visibly rather than silently changing event-
 * counting semantics.
 */
import { describe, expect, it } from 'vitest';
import {
  LiveAggregation,
  LiveRollingAggregation,
  LiveSeries,
  Sequence,
  TimeSeries,
} from '../src/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'number' },
  { name: 'host', kind: 'string' },
] as const;

describe('duplicate temporal keys', () => {
  describe('LiveSeries push semantics', () => {
    it('preserves all events when multiple share the same Time key', () => {
      const live = new LiveSeries({ name: 'test', schema });
      const t = Date.now();
      live.push(
        [t, 0.1, 'api-1'],
        [t, 0.2, 'api-2'],
        [t, 0.3, 'api-3'],
        [t, 0.4, 'api-4'],
      );
      expect(live.length).toBe(4);
      // Per-row pushes (no variadic) preserve duplicates too.
      const live2 = new LiveSeries({ name: 'test2', schema });
      const t2 = Date.now();
      live2.push([t2, 0.1, 'api-1']);
      live2.push([t2, 0.2, 'api-2']);
      live2.push([t2, 0.3, 'api-3']);
      live2.push([t2, 0.4, 'api-4']);
      expect(live2.length).toBe(4);
    });

    it('preserves duplicates when pushed via Date instances (not ms numbers)', () => {
      const live = new LiveSeries({ name: 'test', schema });
      const baseT = new Date();
      live.push(
        [baseT, 0.1, 'api-1'],
        [baseT, 0.2, 'api-2'],
        [baseT, 0.3, 'api-3'],
        [baseT, 0.4, 'api-4'],
      );
      expect(live.length).toBe(4);
    });
  });

  describe('TimeSeries.reduce + count', () => {
    it('counts each duplicate-key event independently', () => {
      const live = new LiveSeries({ name: 'test', schema });
      const t = Date.now();
      live.push(
        [t, 0.1, 'api-1'],
        [t, 0.2, 'api-2'],
        [t, 0.3, 'api-3'],
        [t, 0.4, 'api-4'],
      );
      const ts = live.toTimeSeries();
      expect(ts.reduce({ cpu: 'count' })).toEqual({ cpu: 4 });
      expect(ts.reduce({ host: 'count' })).toEqual({ host: 4 });
    });

    it('matches the dashboard-defaults scenario (60s × 2 ticks/s × 4 hosts)', () => {
      // Default dashboard config: eventsPerSec=2, hostCount=4, all
      // events in a tick share `new Date()`. 60 ticks/min × 4 hosts =
      // 480 events. The experiment's friction note claimed the count
      // displayed as ~1.9/s instead of ~8/s; the library actually
      // returns 480, which is /60 = 8 events/sec.
      const live = new LiveSeries({ name: 'sim', schema });
      const HOSTS = ['api-1', 'api-2', 'api-3', 'api-4'];
      for (let tick = 0; tick < 120; tick++) {
        const t = new Date(Date.now() - (120 - tick) * 500);
        for (const h of HOSTS) {
          live.push([t, Math.random(), h]);
        }
      }
      const last = live.toTimeSeries();
      expect(last.length).toBe(480);
      expect(last.tail('1m').reduce({ cpu: 'count' })).toEqual({ cpu: 480 });
    });
  });

  describe('TimeSeries.aggregate buckets', () => {
    it('all duplicates land in the same bucket and contribute to count', () => {
      const live = new LiveSeries({ name: 'test', schema });
      const t0 = 0;
      live.push(
        [t0, 0.1, 'api-1'],
        [t0, 0.2, 'api-2'],
        [t0, 0.3, 'api-3'],
        [t0 + 1000, 0.4, 'api-1'], // distinct ts
      );
      const buckets = live
        .toTimeSeries()
        .aggregate(Sequence.every('5s'), { cpu: 'count' });
      // 4 events in one 5s bucket starting at t0=0
      expect(buckets.length).toBe(1);
      expect(buckets.at(0)!.get('cpu')).toBe(4);
    });
  });

  describe('TimeSeries.rolling', () => {
    it('rolling count over a window includes every duplicate', () => {
      const live = new LiveSeries({ name: 'test', schema });
      const t = 0;
      live.push([t, 0.1, 'api-1'], [t, 0.2, 'api-2'], [t, 0.3, 'api-3']);
      const rolling = live.toTimeSeries().rolling('1s', { cpu: 'count' });
      // Every output event sees all 3 source events in its trailing 1s.
      expect(rolling.length).toBe(3);
      for (let i = 0; i < rolling.length; i += 1) {
        expect(rolling.at(i)!.get('cpu')).toBe(3);
      }
    });
  });

  describe('LiveAggregation', () => {
    it('counts duplicates correctly in a sequence-driven live bucket', () => {
      const live = new LiveSeries({ name: 'test', schema });
      const agg = new LiveAggregation(live, Sequence.every('5s'), {
        cpu: 'count',
      });
      const t = 0;
      live.push([t, 0.1, 'a'], [t, 0.2, 'b'], [t, 0.3, 'c'], [t, 0.4, 'd']);
      // Push a sample 10s later to close the first bucket.
      live.push([10_000, 0.5, 'a']);
      // First closed bucket should report 4 events.
      const first = agg.at(0);
      expect(first?.get('cpu')).toBe(4);
      agg.dispose();
    });
  });

  describe('LiveRollingAggregation', () => {
    it('counts duplicates inside the rolling window', () => {
      const live = new LiveSeries({ name: 'test', schema });
      const tail = new LiveRollingAggregation(live, '1s', { cpu: 'count' });
      const t = Date.now();
      live.push([t, 0.1, 'a'], [t, 0.2, 'b'], [t, 0.3, 'c']);
      expect(tail.value().cpu).toBe(3);
      tail.dispose();
    });
  });

  describe('TimeSeries construction', () => {
    it('accepts duplicate keys without dedup', () => {
      const t = Date.now();
      const ts = new TimeSeries({
        name: 'test',
        schema,
        rows: [
          [t, 0.1, 'a'],
          [t, 0.2, 'b'],
          [t, 0.3, 'c'],
        ],
      });
      expect(ts.length).toBe(3);
      expect(ts.reduce({ cpu: 'count' })).toEqual({ cpu: 3 });
    });
  });
});
