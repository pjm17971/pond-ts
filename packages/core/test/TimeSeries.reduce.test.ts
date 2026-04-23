import { describe, expect, it } from 'vitest';
import { TimeSeries } from '../src/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number' },
  { name: 'status', kind: 'string' },
] as const;

function makeSeries() {
  return new TimeSeries({
    name: 'cpu',
    schema,
    rows: [
      [0, 10, 'ok'],
      [5, 20, 'ok'],
      [10, 30, 'warn'],
      [15, 40, 'ok'],
    ],
  });
}

describe('TimeSeries.reduce', () => {
  describe('single-column form', () => {
    it('avg reduces to the mean', () => {
      expect(makeSeries().reduce('value', 'avg')).toBe(25);
    });

    it('sum reduces to the total', () => {
      expect(makeSeries().reduce('value', 'sum')).toBe(100);
    });

    it('count reduces to the number of defined values', () => {
      expect(makeSeries().reduce('value', 'count')).toBe(4);
    });

    it('min and max reduce to extremes', () => {
      expect(makeSeries().reduce('value', 'min')).toBe(10);
      expect(makeSeries().reduce('value', 'max')).toBe(40);
    });

    it('first and last reduce to boundary values', () => {
      expect(makeSeries().reduce('status', 'first')).toBe('ok');
      expect(makeSeries().reduce('status', 'last')).toBe('ok');
    });

    it('custom reducer receives all values', () => {
      const result = makeSeries().reduce('value', (values) => {
        const nums = values.filter((v): v is number => typeof v === 'number');
        return nums.length > 0
          ? Math.max(...nums) - Math.min(...nums)
          : undefined;
      });
      expect(result).toBe(30);
    });

    it('returns undefined for avg/min/max on empty series', () => {
      const empty = new TimeSeries({
        name: 'empty',
        schema: [
          { name: 'time', kind: 'time' },
          { name: 'value', kind: 'number' },
        ] as const,
        rows: [],
      });
      expect(empty.reduce('value', 'avg')).toBeUndefined();
      expect(empty.reduce('value', 'min')).toBeUndefined();
      expect(empty.reduce('value', 'max')).toBeUndefined();
    });

    it('count returns 0 and sum returns 0 for empty series', () => {
      const empty = new TimeSeries({
        name: 'empty',
        schema: [
          { name: 'time', kind: 'time' },
          { name: 'value', kind: 'number' },
        ] as const,
        rows: [],
      });
      expect(empty.reduce('value', 'count')).toBe(0);
      expect(empty.reduce('value', 'sum')).toBe(0);
    });
  });

  describe('multi-column form (AggregateMap)', () => {
    it('reduces multiple columns in one call', () => {
      const result = makeSeries().reduce({
        value: 'avg',
        status: 'first',
      });
      expect(result.value).toBe(25);
      expect(result.status).toBe('ok');
    });

    it('mixes built-in and custom reducers', () => {
      const result = makeSeries().reduce({
        value: 'sum',
        status: (values) =>
          values.filter((v): v is string => typeof v === 'string').join(','),
      });
      expect(result.value).toBe(100);
      expect(result.status).toBe('ok,ok,warn,ok');
    });

    it('returns empty-bucket semantics for empty series', () => {
      const empty = new TimeSeries({
        name: 'empty',
        schema,
        rows: [],
      });
      const result = empty.reduce({ value: 'avg', status: 'last' });
      expect(result.value).toBeUndefined();
      expect(result.status).toBeUndefined();
    });
  });

  describe('multi-column form (AggregateOutputMap)', () => {
    it('supports named output columns from the same source', () => {
      const result = makeSeries().reduce({
        avg_value: { from: 'value', using: 'avg' },
        max_value: { from: 'value', using: 'max' },
      });
      expect(result.avg_value).toBe(25);
      expect(result.max_value).toBe(40);
    });

    it('supports custom reducers via output spec', () => {
      const result = makeSeries().reduce({
        value_range: {
          from: 'value',
          using: (values) => {
            const nums = values.filter(
              (v): v is number => typeof v === 'number',
            );
            return nums.length > 0
              ? Math.max(...nums) - Math.min(...nums)
              : undefined;
          },
          kind: 'number',
        },
      });
      expect(result.value_range).toBe(30);
    });
  });

  describe('single-event series', () => {
    it('reduces a single event correctly', () => {
      const ts = new TimeSeries({
        name: 'single',
        schema: [
          { name: 'time', kind: 'time' },
          { name: 'value', kind: 'number' },
        ] as const,
        rows: [[0, 42]],
      });
      expect(ts.reduce('value', 'avg')).toBe(42);
      expect(ts.reduce('value', 'sum')).toBe(42);
      expect(ts.reduce('value', 'count')).toBe(1);
      expect(ts.reduce('value', 'min')).toBe(42);
      expect(ts.reduce('value', 'max')).toBe(42);
    });
  });

  // These tests pin the per-entry narrowing landed in v0.5.2. If
  // `ReduceResult` regresses to the loose `ColumnValue | undefined`
  // shape, the typed assignments below will fail at build time —
  // the assertions also cover runtime values for completeness.
  describe('per-entry narrowing', () => {
    it('numeric reducers return a plain number (no cast needed)', () => {
      const s = makeSeries();
      const avg: number | undefined = s.reduce({ value: 'avg' }).value;
      const sum: number | undefined = s.reduce({ value: 'sum' }).value;
      const count: number | undefined = s.reduce({ status: 'count' }).status;
      const p50: number | undefined = s.reduce({ value: 'p50' }).value;
      expect(avg).toBe(25);
      expect(sum).toBe(100);
      expect(count).toBe(4);
      expect(p50).toBe(25);
    });

    it('unique returns a ReadonlyArray (no cast needed)', () => {
      const s = makeSeries();
      const hosts: ReadonlyArray<string | number | boolean> | undefined =
        s.reduce({ status: 'unique' }).status;
      expect(hosts).toEqual(['ok', 'warn']);
    });

    it('top${N} returns a ReadonlyArray (no cast needed)', () => {
      const s = makeSeries();
      const top: ReadonlyArray<string | number | boolean> | undefined =
        s.reduce({ status: 'top2' }).status;
      // 'ok' x3, 'warn' x1 -> ['ok', 'warn']
      expect(top).toEqual(['ok', 'warn']);
    });

    it('first / last / keep preserve the source column kind', () => {
      const s = makeSeries();
      const first: number | undefined = s.reduce({ value: 'first' }).value;
      const last: string | undefined = s.reduce({ status: 'last' }).status;
      const keep: string | undefined = s.reduce({ status: 'keep' }).status;
      expect(first).toBe(10);
      expect(last).toBe('ok');
      // Not all statuses match, so keep returns undefined
      expect(keep).toBeUndefined();
    });

    it('multiple fields narrow independently in the same call', () => {
      const s = makeSeries();
      const result = s.reduce({
        value: 'avg',
        status: 'unique',
      });
      // Typed assignments verify each field narrows independently.
      const avg: number | undefined = result.value;
      const hosts: ReadonlyArray<string | number | boolean> | undefined =
        result.status;
      expect(avg).toBe(25);
      expect(hosts).toEqual(['ok', 'warn']);
    });
  });
});
