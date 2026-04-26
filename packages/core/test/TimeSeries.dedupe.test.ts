import { describe, expect, it } from 'vitest';
import { TimeSeries } from '../src/index.js';
import { Event } from '../src/Event.js';
import { Time } from '../src/Time.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number', required: false },
  { name: 'host', kind: 'string', required: false },
] as const;

function makeWithDupes() {
  // Two duplicates at t=2000 (values 20a, 25b) and t=4000 (values 40c, 45d).
  return new TimeSeries({
    name: 'dupes',
    schema,
    rows: [
      [1000, 10, 'a'],
      [2000, 20, 'a'],
      [2000, 25, 'b'],
      [3000, 30, 'a'],
      [4000, 40, 'c'],
      [4000, 45, 'd'],
      [5000, 50, 'e'],
    ],
  });
}

describe('TimeSeries.dedupe', () => {
  describe('default (keep: last)', () => {
    it('collapses duplicates keeping the last occurrence', () => {
      const out = makeWithDupes().dedupe();
      expect(out.length).toBe(5);
      expect(out.at(1)?.get('value')).toBe(25);
      expect(out.at(1)?.get('host')).toBe('b');
      expect(out.at(3)?.get('value')).toBe(45);
      expect(out.at(3)?.get('host')).toBe('d');
    });

    it('preserves non-duplicate events untouched', () => {
      const out = makeWithDupes().dedupe();
      expect(out.at(0)?.get('value')).toBe(10);
      expect(out.at(2)?.get('value')).toBe(30);
      expect(out.at(4)?.get('value')).toBe(50);
    });

    it('empty series returns itself', () => {
      const empty = new TimeSeries({ name: 'e', schema, rows: [] });
      const out = empty.dedupe();
      expect(out.length).toBe(0);
    });

    it('series with no duplicates is unchanged in length and order', () => {
      const ts = new TimeSeries({
        name: 'unique',
        schema,
        rows: [
          [1000, 10, 'a'],
          [2000, 20, 'b'],
          [3000, 30, 'c'],
        ],
      });
      const out = ts.dedupe();
      expect(out.length).toBe(3);
      expect(out.at(0)?.get('value')).toBe(10);
      expect(out.at(2)?.get('value')).toBe(30);
    });

    it('handles three-way duplicates', () => {
      const ts = new TimeSeries({
        name: 'triple',
        schema,
        rows: [
          [1000, 10, 'a'],
          [1000, 20, 'b'],
          [1000, 30, 'c'],
        ],
      });
      const out = ts.dedupe();
      expect(out.length).toBe(1);
      expect(out.at(0)?.get('value')).toBe(30);
      expect(out.at(0)?.get('host')).toBe('c');
    });
  });

  describe('keep: first', () => {
    it('keeps the first occurrence', () => {
      const out = makeWithDupes().dedupe({ keep: 'first' });
      expect(out.length).toBe(5);
      expect(out.at(1)?.get('value')).toBe(20);
      expect(out.at(1)?.get('host')).toBe('a');
      expect(out.at(3)?.get('value')).toBe(40);
      expect(out.at(3)?.get('host')).toBe('c');
    });
  });

  describe('keep: error', () => {
    it('throws on the first duplicate timestamp seen', () => {
      expect(() => makeWithDupes().dedupe({ keep: 'error' })).toThrow(
        /2 events at the same timestamp/,
      );
    });

    it('does not throw when there are no duplicates', () => {
      const ts = new TimeSeries({
        name: 'unique',
        schema,
        rows: [
          [1000, 10, 'a'],
          [2000, 20, 'b'],
        ],
      });
      const out = ts.dedupe({ keep: 'error' });
      expect(out.length).toBe(2);
    });

    it('error message includes the timestamp ISO string', () => {
      const ts = new TimeSeries({
        name: 'dupes',
        schema,
        rows: [
          [1700000000000, 10, 'a'],
          [1700000000000, 20, 'b'],
        ],
      });
      expect(() => ts.dedupe({ keep: 'error' })).toThrow(
        /2023-11-14T22:13:20\.000Z/,
      );
    });
  });

  describe('keep: drop', () => {
    it('discards every event at any duplicate timestamp', () => {
      const out = makeWithDupes().dedupe({ keep: 'drop' });
      // 7 input events, 2 duplicate buckets (t=2000 and t=4000), each bucket
      // discarded entirely → 7 - 2 - 2 = 3 left.
      expect(out.length).toBe(3);
      expect(out.at(0)?.get('value')).toBe(10);
      expect(out.at(1)?.get('value')).toBe(30);
      expect(out.at(2)?.get('value')).toBe(50);
    });

    it('returns empty when every timestamp is duplicated', () => {
      const ts = new TimeSeries({
        name: 'all-dupes',
        schema,
        rows: [
          [1000, 10, 'a'],
          [1000, 20, 'b'],
        ],
      });
      const out = ts.dedupe({ keep: 'drop' });
      expect(out.length).toBe(0);
    });
  });

  describe('keep: { min: col }', () => {
    it('keeps the event with the smallest value at the named column', () => {
      const out = makeWithDupes().dedupe({ keep: { min: 'value' } });
      expect(out.length).toBe(5);
      // t=2000: values 20 vs 25 → min 20
      expect(out.at(1)?.get('value')).toBe(20);
      // t=4000: values 40 vs 45 → min 40
      expect(out.at(3)?.get('value')).toBe(40);
    });

    it('skips undefined values when picking min', () => {
      const ts = new TimeSeries({
        name: 'with-undef',
        schema,
        rows: [
          [1000, undefined, 'a'],
          [1000, 50, 'b'],
          [1000, 30, 'c'],
        ],
      });
      const out = ts.dedupe({ keep: { min: 'value' } });
      expect(out.length).toBe(1);
      expect(out.at(0)?.get('value')).toBe(30);
    });

    it('falls back to the first event when every value is undefined', () => {
      const ts = new TimeSeries({
        name: 'all-undef',
        schema,
        rows: [
          [1000, undefined, 'a'],
          [1000, undefined, 'b'],
        ],
      });
      const out = ts.dedupe({ keep: { min: 'value' } });
      expect(out.length).toBe(1);
      expect(out.at(0)?.get('host')).toBe('a');
    });

    it('breaks ties by keeping the earliest tied event', () => {
      const ts = new TimeSeries({
        name: 'ties',
        schema,
        rows: [
          [1000, 10, 'a'],
          [1000, 10, 'b'],
          [1000, 10, 'c'],
        ],
      });
      const out = ts.dedupe({ keep: { min: 'value' } });
      expect(out.length).toBe(1);
      expect(out.at(0)?.get('host')).toBe('a');
    });
  });

  describe('keep: { max: col }', () => {
    it('keeps the event with the largest value at the named column', () => {
      const out = makeWithDupes().dedupe({ keep: { max: 'value' } });
      expect(out.length).toBe(5);
      expect(out.at(1)?.get('value')).toBe(25);
      expect(out.at(3)?.get('value')).toBe(45);
    });
  });

  describe('keep: function', () => {
    it('receives only buckets with length >= 2', () => {
      const callCount = { n: 0 };
      const out = makeWithDupes().dedupe({
        keep: (events) => {
          callCount.n += 1;
          expect(events.length).toBeGreaterThanOrEqual(2);
          return events[0]!;
        },
      });
      expect(callCount.n).toBe(2); // 2 duplicate buckets in makeWithDupes
      expect(out.length).toBe(5);
    });

    it('can implement merge logic — averaging numeric fields', () => {
      const out = makeWithDupes().dedupe({
        keep: (events) => {
          const avg =
            events.reduce(
              (a, e) => a + ((e.get('value') as number | undefined) ?? 0),
              0,
            ) / events.length;
          // Build a fresh event at the same timestamp with averaged value.
          return new Event(new Time(events[0]!.begin()), {
            value: avg,
            host: 'merged',
          }) as never;
        },
      });
      // t=2000: avg(20, 25) = 22.5
      expect(out.at(1)?.get('value')).toBe(22.5);
      expect(out.at(1)?.get('host')).toBe('merged');
      // t=4000: avg(40, 45) = 42.5
      expect(out.at(3)?.get('value')).toBe(42.5);
    });
  });

  describe('multi-entity (cross-partition hazard)', () => {
    it('without partitionBy, dedupes across entity boundaries — DOCUMENTED foot-gun', () => {
      // host=A and host=B both have an event at t=2000. Bare dedupe
      // collapses them as if they were duplicates of each other.
      const ts = new TimeSeries({
        name: 'multi',
        schema,
        rows: [
          [1000, 10, 'A'],
          [2000, 20, 'A'],
          [2000, 22, 'B'],
          [3000, 30, 'A'],
          [3000, 33, 'B'],
        ],
      });
      const out = ts.dedupe();
      expect(out.length).toBe(3); // not 5
    });

    it('with partitionBy, dedupes within each entity', () => {
      const ts = new TimeSeries({
        name: 'multi',
        schema,
        rows: [
          [1000, 10, 'A'],
          [2000, 20, 'A'],
          [2000, 22, 'B'],
          [3000, 30, 'A'],
          [3000, 33, 'B'],
        ],
      });
      const out = ts.partitionBy('host').dedupe().collect();
      // Each (time, host) is unique here, so no collapses — all 5 survive.
      expect(out.length).toBe(5);
    });

    it('partitionBy + dedupe collapses only true (host, time) duplicates', () => {
      const ts = new TimeSeries({
        name: 'multi',
        schema,
        rows: [
          [1000, 10, 'A'],
          [1000, 11, 'A'], // duplicate within host A
          [1000, 22, 'B'],
          [2000, 20, 'A'],
          [2000, 22, 'B'],
          [2000, 23, 'B'], // duplicate within host B
        ],
      });
      const out = ts.partitionBy('host').dedupe({ keep: 'last' }).collect();
      expect(out.length).toBe(4);
      // Per-host last-wins: A@1000=11, A@2000=20, B@1000=22, B@2000=23
      // After concat the result is sorted by time, so order is:
      // A@1000, B@1000, A@2000, B@2000 (concat preserves stable order
      // within ties; partition order from Map insertion: A first, B second)
      const events = [...out.events].map((e) => ({
        time: e.begin(),
        value: e.get('value'),
        host: e.get('host'),
      }));
      // A's first-seen partition wins the tie ordering at t=1000, then B.
      expect(events).toEqual([
        { time: 1000, value: 11, host: 'A' },
        { time: 1000, value: 22, host: 'B' },
        { time: 2000, value: 20, host: 'A' },
        { time: 2000, value: 23, host: 'B' },
      ]);
    });
  });

  describe('chain composition', () => {
    it('composes naturally with partitionBy + fill', () => {
      const ts = new TimeSeries({
        name: 'chain',
        schema,
        rows: [
          [1000, 10, 'A'],
          [1000, 11, 'A'], // dup within A
          [2000, undefined, 'A'],
          [3000, 30, 'A'],
        ],
      });
      const out = ts
        .partitionBy('host')
        .dedupe({ keep: 'last' })
        .fill({ value: 'linear' })
        .collect();
      expect(out.length).toBe(3);
      // t=1000: dedupe-last → value 11
      expect(out.at(0)?.get('value')).toBe(11);
      // t=2000: linear interp between 11 and 30 → 20.5
      expect(out.at(1)?.get('value')).toBe(20.5);
      expect(out.at(2)?.get('value')).toBe(30);
    });
  });
});
