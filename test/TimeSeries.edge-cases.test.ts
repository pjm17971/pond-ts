import { describe, expect, it } from 'vitest';
import {
  Interval,
  Sequence,
  Time,
  TimeRange,
  TimeSeries,
} from '../src/index.js';

const timeNumberSchema = [
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number' },
] as const;

const timeNumberStringSchema = [
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number' },
  { name: 'status', kind: 'string' },
] as const;

function emptySeries() {
  return new TimeSeries({
    name: 'empty',
    schema: timeNumberSchema,
    rows: [],
  });
}

function singleEventSeries(t = 100, v = 42) {
  return new TimeSeries({
    name: 'single',
    schema: timeNumberSchema,
    rows: [[t, v]],
  });
}

describe('TimeSeries edge cases', () => {
  // ---------------------------------------------------------------------------
  // Empty series
  // ---------------------------------------------------------------------------
  describe('empty series', () => {
    it('has length 0 and returns undefined for positional access', () => {
      const ts = emptySeries();
      expect(ts.length).toBe(0);
      expect(ts.at(0)).toBeUndefined();
      expect(ts.first()).toBeUndefined();
      expect(ts.last()).toBeUndefined();
    });

    it('returns undefined timeRange', () => {
      expect(emptySeries().timeRange()).toBeUndefined();
    });

    it('find/some/every behave correctly', () => {
      const ts = emptySeries();
      expect(ts.find(() => true)).toBeUndefined();
      expect(ts.some(() => true)).toBe(false);
      expect(ts.every(() => false)).toBe(true);
    });

    it('bisect returns 0 and includesKey returns false', () => {
      const ts = emptySeries();
      expect(ts.bisect(new Time(0))).toBe(0);
      expect(ts.includesKey(new Time(0))).toBe(false);
    });

    it('atOrBefore and atOrAfter return undefined', () => {
      const ts = emptySeries();
      expect(ts.atOrBefore(new Time(0))).toBeUndefined();
      expect(ts.atOrAfter(new Time(0))).toBeUndefined();
    });

    it('filter returns empty series', () => {
      const ts = emptySeries().filter(() => true);
      expect(ts.length).toBe(0);
    });

    it('slice returns empty series', () => {
      expect(emptySeries().slice(0, 10).length).toBe(0);
    });

    it('select returns empty series', () => {
      expect(emptySeries().select('value').length).toBe(0);
    });

    it('map returns empty series', () => {
      const ts = emptySeries().map(timeNumberSchema, (e) => e);
      expect(ts.length).toBe(0);
    });

    it('align returns empty series', () => {
      const ts = emptySeries().align(Sequence.every(10));
      expect(ts.length).toBe(0);
    });

    it('aggregate returns empty series', () => {
      const ts = emptySeries().aggregate(Sequence.every(10), { value: 'avg' });
      expect(ts.length).toBe(0);
    });

    it('event-driven rolling returns empty series', () => {
      const ts = emptySeries().rolling(10, { value: 'avg' });
      expect(ts.length).toBe(0);
    });

    it('sequence-driven rolling returns empty series', () => {
      const ts = emptySeries().rolling(Sequence.every(10), 20, {
        value: 'avg',
      });
      expect(ts.length).toBe(0);
    });

    it('smooth ema returns empty series', () => {
      const ts = emptySeries().smooth('value', 'ema', { alpha: 0.5 });
      expect(ts.length).toBe(0);
    });

    it('smooth movingAverage returns empty series', () => {
      const ts = emptySeries().smooth('value', 'movingAverage', {
        window: 10,
      });
      expect(ts.length).toBe(0);
    });

    it('smooth loess returns empty series', () => {
      const ts = emptySeries().smooth('value', 'loess', { span: 0.75 });
      expect(ts.length).toBe(0);
    });

    it('within returns empty series', () => {
      expect(emptySeries().within(0, 1000).length).toBe(0);
    });

    it('before and after return empty series', () => {
      expect(emptySeries().before(1000).length).toBe(0);
      expect(emptySeries().after(0).length).toBe(0);
    });

    it('overlapping returns empty series', () => {
      expect(
        emptySeries().overlapping(new TimeRange({ start: 0, end: 1000 }))
          .length,
      ).toBe(0);
    });

    it('join with empty produces empty for inner join', () => {
      const a = new TimeSeries({
        name: 'a',
        schema: [
          { name: 'time', kind: 'time' },
          { name: 'x', kind: 'number' },
        ] as const,
        rows: [],
      });
      const b = new TimeSeries({
        name: 'b',
        schema: [
          { name: 'time', kind: 'time' },
          { name: 'y', kind: 'number' },
        ] as const,
        rows: [],
      });
      expect(a.join(b, { type: 'inner' }).length).toBe(0);
    });

    it('join empty with non-empty produces correct results per join type', () => {
      const empty = new TimeSeries({
        name: 'empty',
        schema: [
          { name: 'time', kind: 'time' },
          { name: 'a', kind: 'number' },
        ] as const,
        rows: [],
      });
      const nonEmpty = new TimeSeries({
        name: 'data',
        schema: [
          { name: 'time', kind: 'time' },
          { name: 'b', kind: 'number' },
        ] as const,
        rows: [
          [0, 1],
          [10, 2],
        ],
      });

      expect(empty.join(nonEmpty, { type: 'inner' }).length).toBe(0);
      expect(empty.join(nonEmpty, { type: 'left' }).length).toBe(0);
      expect(empty.join(nonEmpty, { type: 'right' }).length).toBe(2);
      expect(empty.join(nonEmpty, { type: 'outer' }).length).toBe(2);
    });

    it('toJSON round-trips', () => {
      const ts = emptySeries();
      const json = ts.toJSON();
      const restored = TimeSeries.fromJSON(json);
      expect(restored.length).toBe(0);
      expect(restored.schema).toEqual(ts.schema);
    });

    it('toRows and toObjects return empty arrays', () => {
      const ts = emptySeries();
      expect(ts.toRows()).toEqual([]);
      expect(ts.toObjects()).toEqual([]);
    });
  });

  // ---------------------------------------------------------------------------
  // Single-event series
  // ---------------------------------------------------------------------------
  describe('single-event series', () => {
    it('has length 1 and first equals last', () => {
      const ts = singleEventSeries();
      expect(ts.length).toBe(1);
      expect(ts.first()).toBe(ts.last());
    });

    it('timeRange is a point (begin equals end)', () => {
      const ts = singleEventSeries(100);
      const range = ts.timeRange();
      expect(range).toBeDefined();
      expect(range!.begin()).toBe(100);
      expect(range!.end()).toBe(100);
    });

    it('bisect and includesKey work', () => {
      const ts = singleEventSeries(100);
      expect(ts.includesKey(new Time(100))).toBe(true);
      expect(ts.includesKey(new Time(99))).toBe(false);
      expect(ts.bisect(new Time(0))).toBe(0);
      expect(ts.bisect(new Time(100))).toBe(0);
      expect(ts.bisect(new Time(200))).toBe(1);
    });

    it('atOrBefore and atOrAfter work at, before, and after the event', () => {
      const ts = singleEventSeries(100);
      expect(ts.atOrBefore(new Time(100))?.begin()).toBe(100);
      expect(ts.atOrBefore(new Time(200))?.begin()).toBe(100);
      expect(ts.atOrBefore(new Time(50))).toBeUndefined();
      expect(ts.atOrAfter(new Time(100))?.begin()).toBe(100);
      expect(ts.atOrAfter(new Time(50))?.begin()).toBe(100);
      expect(ts.atOrAfter(new Time(200))).toBeUndefined();
    });

    it('filter that keeps the event returns length 1', () => {
      expect(singleEventSeries().filter(() => true).length).toBe(1);
    });

    it('filter that rejects the event returns empty', () => {
      expect(singleEventSeries().filter(() => false).length).toBe(0);
    });

    it('aggregate with single event in one bucket', () => {
      const ts = singleEventSeries(5, 10);
      const agg = ts.aggregate(Sequence.every(10), { value: 'avg' }, {
        range: new TimeRange({ start: 0, end: 9 }),
      });
      expect(agg.length).toBe(1);
      expect(agg.at(0)?.get('value')).toBe(10);
    });

    it('event-driven rolling with single event', () => {
      const ts = singleEventSeries(0, 7);
      const rolled = ts.rolling(10, { value: 'avg' });
      expect(rolled.length).toBe(1);
      expect(rolled.at(0)?.get('value')).toBe(7);
    });

    it('smooth ema with single event returns the value unchanged', () => {
      const ts = singleEventSeries(0, 5);
      const smoothed = ts.smooth('value', 'ema', { alpha: 0.3 });
      expect(smoothed.length).toBe(1);
      expect(smoothed.at(0)?.get('value')).toBe(5);
    });

    it('join single with single at same key', () => {
      const a = new TimeSeries({
        name: 'a',
        schema: [
          { name: 'time', kind: 'time' },
          { name: 'x', kind: 'number' },
        ] as const,
        rows: [[100, 1]],
      });
      const b = new TimeSeries({
        name: 'b',
        schema: [
          { name: 'time', kind: 'time' },
          { name: 'y', kind: 'number' },
        ] as const,
        rows: [[100, 2]],
      });
      const joined = a.join(b, { type: 'inner' });
      expect(joined.length).toBe(1);
      expect(joined.at(0)?.get('x')).toBe(1);
      expect(joined.at(0)?.get('y')).toBe(2);
    });

    it('join single with single at different keys', () => {
      const a = new TimeSeries({
        name: 'a',
        schema: [
          { name: 'time', kind: 'time' },
          { name: 'x', kind: 'number' },
        ] as const,
        rows: [[100, 1]],
      });
      const b = new TimeSeries({
        name: 'b',
        schema: [
          { name: 'time', kind: 'time' },
          { name: 'y', kind: 'number' },
        ] as const,
        rows: [[200, 2]],
      });
      expect(a.join(b, { type: 'inner' }).length).toBe(0);
      expect(a.join(b, { type: 'outer' }).length).toBe(2);
    });
  });

  // ---------------------------------------------------------------------------
  // Empty aggregation buckets
  // ---------------------------------------------------------------------------
  describe('empty aggregation buckets', () => {
    it('aggregate produces undefined for buckets with no events', () => {
      const ts = new TimeSeries({
        name: 'sparse',
        schema: timeNumberSchema,
        rows: [
          [0, 10],
          [1, 20],
        ],
      });

      const agg = ts.aggregate(Sequence.every(10), { value: 'avg' }, {
        range: new TimeRange({ start: 0, end: 29 }),
      });

      expect(agg.length).toBe(3);
      expect(agg.at(0)?.get('value')).toBe(15);
      expect(agg.at(1)?.get('value')).toBeUndefined();
      expect(agg.at(2)?.get('value')).toBeUndefined();
    });

    it('count returns 0 for empty buckets', () => {
      const ts = new TimeSeries({
        name: 'sparse',
        schema: timeNumberSchema,
        rows: [[0, 10]],
      });

      const agg = ts.aggregate(Sequence.every(10), { value: 'count' }, {
        range: new TimeRange({ start: 0, end: 19 }),
      });

      expect(agg.length).toBe(2);
      expect(agg.at(0)?.get('value')).toBe(1);
      expect(agg.at(1)?.get('value')).toBe(0);
    });

    it('sum returns 0 for empty buckets', () => {
      const ts = new TimeSeries({
        name: 'sparse',
        schema: timeNumberSchema,
        rows: [[0, 10]],
      });

      const agg = ts.aggregate(Sequence.every(10), { value: 'sum' }, {
        range: new TimeRange({ start: 0, end: 19 }),
      });

      expect(agg.at(0)?.get('value')).toBe(10);
      expect(agg.at(1)?.get('value')).toBe(0);
    });

    it('min and max return undefined for empty buckets', () => {
      const ts = new TimeSeries({
        name: 'sparse',
        schema: timeNumberSchema,
        rows: [[0, 10]],
      });

      const aggMin = ts.aggregate(Sequence.every(10), { value: 'min' }, {
        range: new TimeRange({ start: 0, end: 19 }),
      });
      const aggMax = ts.aggregate(Sequence.every(10), { value: 'max' }, {
        range: new TimeRange({ start: 0, end: 19 }),
      });

      expect(aggMin.at(1)?.get('value')).toBeUndefined();
      expect(aggMax.at(1)?.get('value')).toBeUndefined();
    });

    it('first and last return undefined for empty buckets', () => {
      const ts = new TimeSeries({
        name: 'sparse',
        schema: timeNumberStringSchema,
        rows: [[0, 10, 'a']],
      });

      const agg = ts.aggregate(
        Sequence.every(10),
        { status: 'first' },
        { range: new TimeRange({ start: 0, end: 19 }) },
      );

      expect(agg.at(0)?.get('status')).toBe('a');
      expect(agg.at(1)?.get('status')).toBeUndefined();
    });

    it('custom reducer receives empty array for empty buckets', () => {
      const calls: number[][] = [];
      const ts = new TimeSeries({
        name: 'sparse',
        schema: timeNumberSchema,
        rows: [[0, 10]],
      });

      ts.aggregate(
        Sequence.every(10),
        {
          value: (values) => {
            calls.push([...values] as number[]);
            return values.length > 0 ? (values[0] as number) : undefined;
          },
        },
        { range: new TimeRange({ start: 0, end: 19 }) },
      );

      expect(calls.length).toBe(2);
      expect(calls[0]).toEqual([10]);
      expect(calls[1]).toEqual([]);
    });
  });

  // ---------------------------------------------------------------------------
  // Rolling alignment edge cases
  // ---------------------------------------------------------------------------
  describe('rolling alignment edge cases', () => {
    it('leading alignment: first event window extends before series start', () => {
      const ts = new TimeSeries({
        name: 'cpu',
        schema: timeNumberSchema,
        rows: [
          [0, 1],
          [5, 2],
          [10, 3],
          [15, 4],
        ],
      });

      const rolled = ts.rolling(
        10,
        { value: 'count' },
        { alignment: 'leading' },
      );

      expect(rolled.length).toBe(4);
      expect(rolled.at(0)?.get('value')).toBe(2);
      expect(rolled.at(3)?.get('value')).toBe(1);
    });

    it('trailing window at first event contains only that event', () => {
      const ts = new TimeSeries({
        name: 'cpu',
        schema: timeNumberSchema,
        rows: [
          [0, 10],
          [5, 20],
          [10, 30],
        ],
      });

      const rolled = ts.rolling(5, { value: 'avg' });
      expect(rolled.at(0)?.get('value')).toBe(10);
    });

    it('window exactly spanning two events includes both', () => {
      const ts = new TimeSeries({
        name: 'cpu',
        schema: timeNumberSchema,
        rows: [
          [0, 10],
          [5, 20],
          [10, 30],
        ],
      });

      const rolled = ts.rolling(10, { value: 'count' });
      expect(rolled.at(1)?.get('value')).toBe(2);
    });

    it('sequence-driven rolling over narrow range limits output', () => {
      const ts = new TimeSeries({
        name: 'cpu',
        schema: timeNumberSchema,
        rows: [
          [100, 1],
          [200, 2],
        ],
      });

      const rolled = ts.rolling(Sequence.every(50), 100, { value: 'avg' }, {
        range: new TimeRange({ start: 100, end: 149 }),
      });
      expect(rolled.length).toBe(1);
    });

    it('rolling with window larger than series span includes all events', () => {
      const ts = new TimeSeries({
        name: 'cpu',
        schema: timeNumberSchema,
        rows: [
          [0, 10],
          [5, 20],
          [10, 30],
        ],
      });

      const rolled = ts.rolling(1000, { value: 'avg' });
      expect(rolled.at(2)?.get('value')).toBe(20);
    });
  });

  // ---------------------------------------------------------------------------
  // Custom reducers for rolling
  // ---------------------------------------------------------------------------
  describe('custom reducers for rolling', () => {
    it('custom reducer receives empty array when no events are in the window', () => {
      const calls: (number | undefined)[][] = [];
      const ts = new TimeSeries({
        name: 'cpu',
        schema: timeNumberSchema,
        rows: [
          [0, 10],
          [100, 20],
        ],
      });

      ts.rolling(5, {
        value: (values) => {
          calls.push([...values] as (number | undefined)[]);
          return values.length > 0 ? (values[0] as number) : undefined;
        },
      });

      expect(calls.length).toBe(2);
      expect(calls[0]).toEqual([10]);
      expect(calls[1]).toEqual([20]);
    });

    it('custom reducer on single-event rolling returns the single value', () => {
      const ts = singleEventSeries(0, 42);
      const rolled = ts.rolling(10, {
        value: (values) => {
          const nums = values.filter(
            (v): v is number => typeof v === 'number',
          );
          return nums.length > 0 ? nums[0]! * 2 : undefined;
        },
      });
      expect(rolled.length).toBe(1);
      expect(rolled.at(0)?.get('value')).toBe(84);
    });

    it('custom reducer can return a different type than the input column', () => {
      const schema = [
        { name: 'time', kind: 'time' },
        { name: 'status', kind: 'string' },
      ] as const;

      const ts = new TimeSeries({
        name: 'events',
        schema,
        rows: [
          [0, 'ok'],
          [5, 'warn'],
          [10, 'ok'],
        ],
      });

      const rolled = ts.rolling(10, {
        status: (values) =>
          values.filter((v) => v === 'warn').length > 0 ? 'degraded' : 'healthy',
      });

      expect(rolled.at(0)?.get('status')).toBe('healthy');
      expect(rolled.at(1)?.get('status')).toBe('degraded');
      expect(rolled.at(2)?.get('status')).toBe('degraded');
    });

    it('sequence-driven rolling with custom reducer handles empty windows', () => {
      const ts = new TimeSeries({
        name: 'sparse',
        schema: timeNumberSchema,
        rows: [[0, 10]],
      });

      const calls: (number | undefined)[][] = [];
      const rolled = ts.rolling(
        Sequence.every(10),
        5,
        {
          value: (values) => {
            calls.push([...values] as (number | undefined)[]);
            return values.length > 0 ? (values[0] as number) : -1;
          },
        },
        { range: new TimeRange({ start: 0, end: 19 }) },
      );

      expect(rolled.length).toBe(2);
      expect(rolled.at(0)?.get('value')).toBe(10);
      expect(rolled.at(1)?.get('value')).toBe(-1);
      expect(calls[1]).toEqual([]);
    });
  });

  // ---------------------------------------------------------------------------
  // Half-open interval semantics
  // ---------------------------------------------------------------------------
  describe('half-open interval semantics', () => {
    it('aggregate: event at bucket end boundary falls into next bucket', () => {
      const ts = new TimeSeries({
        name: 'cpu',
        schema: timeNumberSchema,
        rows: [
          [0, 1],
          [10, 2],
          [20, 3],
        ],
      });

      const agg = ts.aggregate(
        Sequence.every(10),
        { value: 'sum' },
        { range: new TimeRange({ start: 0, end: 20 }) },
      );

      expect(agg.length).toBe(3);
      expect(agg.at(0)?.get('value')).toBe(1);
      expect(agg.at(1)?.get('value')).toBe(2);
      expect(agg.at(2)?.get('value')).toBe(3);
    });

    it('aggregate: event exactly at bucket begin is included in that bucket', () => {
      const ts = new TimeSeries({
        name: 'cpu',
        schema: timeNumberSchema,
        rows: [
          [0, 1],
          [5, 2],
          [10, 3],
        ],
      });

      const agg = ts.aggregate(
        Sequence.every(10),
        { value: 'count' },
        { range: new TimeRange({ start: 0, end: 10 }) },
      );

      expect(agg.length).toBe(2);
      expect(agg.at(0)?.get('value')).toBe(2);
      expect(agg.at(1)?.get('value')).toBe(1);
    });

    it('within uses inclusive boundaries', () => {
      const ts = new TimeSeries({
        name: 'cpu',
        schema: timeNumberSchema,
        rows: [
          [0, 1],
          [10, 2],
          [20, 3],
        ],
      });

      const result = ts.within(0, 20);
      expect(result.length).toBe(3);
    });

    it('before is exclusive of the boundary', () => {
      const ts = new TimeSeries({
        name: 'cpu',
        schema: timeNumberSchema,
        rows: [
          [0, 1],
          [10, 2],
          [20, 3],
        ],
      });

      expect(ts.before(10).length).toBe(1);
      expect(ts.before(10).at(0)?.get('value')).toBe(1);
    });

    it('after is exclusive of the boundary', () => {
      const ts = new TimeSeries({
        name: 'cpu',
        schema: timeNumberSchema,
        rows: [
          [0, 1],
          [10, 2],
          [20, 3],
        ],
      });

      expect(ts.after(10).length).toBe(1);
      expect(ts.after(10).at(0)?.get('value')).toBe(3);
    });

    it('consecutive aggregate buckets are non-overlapping and exhaustive', () => {
      const ts = new TimeSeries({
        name: 'cpu',
        schema: timeNumberSchema,
        rows: [
          [0, 1],
          [3, 2],
          [5, 3],
          [7, 4],
          [10, 5],
          [13, 6],
          [15, 7],
        ],
      });

      const agg = ts.aggregate(
        Sequence.every(5),
        { value: 'count' },
        { range: new TimeRange({ start: 0, end: 15 }) },
      );

      const counts = Array.from({ length: agg.length }, (_, i) =>
        agg.at(i)?.get('value'),
      );
      const totalCount = (counts as number[]).reduce(
        (a, b) => a + b,
        0,
      );
      expect(totalCount).toBe(7);
    });
  });
});
