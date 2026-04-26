import { describe, expect, it } from 'vitest';
import { TimeSeries } from '../src/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'number' },
  { name: 'host', kind: 'string' },
] as const;

function longSeries() {
  return new TimeSeries({
    name: 'metrics',
    schema,
    rows: [
      [0, 0.31, 'api-1'],
      [0, 0.52, 'api-2'],
      [60_000, 0.44, 'api-1'],
      [60_000, 0.48, 'api-2'],
      [120_000, 0.4, 'api-1'],
      // api-2 missing at 120_000
    ],
  });
}

describe('TimeSeries.pivotByGroup', () => {
  describe('basic pivot', () => {
    it('collapses rows sharing a timestamp into one wide row', () => {
      const wide = longSeries().pivotByGroup('host', 'cpu');
      expect(wide.length).toBe(3);
    });

    it('builds a column per distinct group value', () => {
      const wide = longSeries().pivotByGroup('host', 'cpu');
      expect(wide.schema.map((c) => c.name)).toEqual([
        'time',
        'api-1_cpu',
        'api-2_cpu',
      ]);
    });

    it('preserves the value column kind on every output column', () => {
      const wide = longSeries().pivotByGroup('host', 'cpu');
      for (const col of wide.schema.slice(1)) {
        expect(col.kind).toBe('number');
      }
    });

    it('puts the right value in each (ts, group) cell', () => {
      const wide = longSeries().pivotByGroup('host', 'cpu');
      const points = wide.toPoints();
      expect(points[0]).toEqual({
        ts: 0,
        'api-1_cpu': 0.31,
        'api-2_cpu': 0.52,
      });
      expect(points[1]).toEqual({
        ts: 60_000,
        'api-1_cpu': 0.44,
        'api-2_cpu': 0.48,
      });
    });

    it('uses undefined for missing (ts, group) cells', () => {
      const wide = longSeries().pivotByGroup('host', 'cpu');
      const points = wide.toPoints();
      expect(points[2]).toEqual({
        ts: 120_000,
        'api-1_cpu': 0.4,
        'api-2_cpu': undefined,
      });
    });

    it('sorts group columns alphabetically for stable output', () => {
      const ts = new TimeSeries({
        name: 'metrics',
        schema,
        rows: [
          [0, 1, 'zeta'],
          [0, 2, 'alpha'],
          [0, 3, 'mu'],
        ],
      });
      const wide = ts.pivotByGroup('host', 'cpu');
      expect(wide.schema.slice(1).map((c) => c.name)).toEqual([
        'alpha_cpu',
        'mu_cpu',
        'zeta_cpu',
      ]);
    });

    it('emits output rows in ascending timestamp order', () => {
      const wide = longSeries().pivotByGroup('host', 'cpu');
      expect(wide.toPoints().map((p) => p.ts)).toEqual([0, 60_000, 120_000]);
    });
  });

  describe('duplicate (ts, group) handling', () => {
    function dupes() {
      return new TimeSeries({
        name: 'metrics',
        schema,
        rows: [
          [0, 0.3, 'api-1'],
          [0, 0.5, 'api-1'], // duplicate
          [0, 0.7, 'api-2'],
        ],
      });
    }

    it('throws by default when (ts, group) has multiple rows', () => {
      expect(() => dupes().pivotByGroup('host', 'cpu')).toThrow(
        /events share timestamp .* and group "api-1"/,
      );
    });

    it('aggregates with avg when configured', () => {
      const wide = dupes().pivotByGroup('host', 'cpu', { aggregate: 'avg' });
      const point = wide.toPoints()[0];
      expect(point['api-1_cpu']).toBeCloseTo(0.4, 5);
      expect(point['api-2_cpu']).toBeCloseTo(0.7, 5);
    });

    it('aggregates with sum when configured', () => {
      const wide = dupes().pivotByGroup('host', 'cpu', { aggregate: 'sum' });
      expect(wide.toPoints()[0]['api-1_cpu']).toBeCloseTo(0.8, 5);
    });

    it('aggregates with last when configured', () => {
      const wide = dupes().pivotByGroup('host', 'cpu', { aggregate: 'last' });
      expect(wide.toPoints()[0]['api-1_cpu']).toBe(0.5);
    });

    it('aggregates with first when configured', () => {
      const wide = dupes().pivotByGroup('host', 'cpu', { aggregate: 'first' });
      expect(wide.toPoints()[0]['api-1_cpu']).toBe(0.3);
    });

    it('accepts a custom reducer function', () => {
      const wide = dupes().pivotByGroup('host', 'cpu', {
        aggregate: (values) =>
          (values
            .filter((v): v is number => typeof v === 'number')
            .reduce((a, b) => a + b, 0) /
            values.length) *
          2,
      });
      expect(wide.toPoints()[0]['api-1_cpu']).toBeCloseTo(0.8, 5);
    });
  });

  describe('edge cases', () => {
    it('empty series produces an empty wide series', () => {
      const ts = new TimeSeries({ name: 'empty', schema, rows: [] });
      const wide = ts.pivotByGroup('host', 'cpu');
      expect(wide.length).toBe(0);
      expect(wide.schema.map((c) => c.name)).toEqual(['time']);
    });

    it('single-group series produces one value column', () => {
      const ts = new TimeSeries({
        name: 'uniform',
        schema,
        rows: [
          [0, 1, 'only'],
          [60_000, 2, 'only'],
        ],
      });
      const wide = ts.pivotByGroup('host', 'cpu');
      expect(wide.schema.map((c) => c.name)).toEqual(['time', 'only_cpu']);
      expect(wide.toPoints()).toEqual([
        { ts: 0, only_cpu: 1 },
        { ts: 60_000, only_cpu: 2 },
      ]);
    });

    it('numeric group column coerces to string for column names', () => {
      const numericGroupSchema = [
        { name: 'time', kind: 'time' },
        { name: 'value', kind: 'number' },
        { name: 'shard', kind: 'number' },
      ] as const;
      const ts = new TimeSeries({
        name: 'shards',
        schema: numericGroupSchema,
        rows: [
          [0, 10, 1],
          [0, 20, 2],
        ],
      });
      const wide = ts.pivotByGroup('shard', 'value');
      expect(wide.schema.slice(1).map((c) => c.name)).toEqual([
        '1_value',
        '2_value',
      ]);
    });

    it('treats undefined group values as a literal "undefined" key', () => {
      const ts = new TimeSeries({
        name: 'metrics',
        schema: [
          { name: 'time', kind: 'time' },
          { name: 'cpu', kind: 'number' },
          { name: 'host', kind: 'string', required: false },
        ] as const,
        rows: [
          [0, 0.3, 'api-1'],
          [0, 0.4, undefined],
        ],
      });
      const wide = ts.pivotByGroup('host', 'cpu');
      expect(wide.schema.slice(1).map((c) => c.name)).toEqual([
        'api-1_cpu',
        'undefined_cpu',
      ]);
    });

    it('preserves string value kind through the pivot', () => {
      const stringValueSchema = [
        { name: 'time', kind: 'time' },
        { name: 'status', kind: 'string' },
        { name: 'host', kind: 'string' },
      ] as const;
      const ts = new TimeSeries({
        name: 'statuses',
        schema: stringValueSchema,
        rows: [
          [0, 'ok', 'api-1'],
          [0, 'fail', 'api-2'],
        ],
      });
      const wide = ts.pivotByGroup('host', 'status');
      for (const col of wide.schema.slice(1)) {
        expect(col.kind).toBe('string');
      }
      expect(wide.toPoints()[0]).toEqual({
        ts: 0,
        'api-1_status': 'ok',
        'api-2_status': 'fail',
      });
    });
  });

  describe('errors', () => {
    it('throws when value column is not in schema', () => {
      expect(() =>
        // @ts-expect-error invalid column at type level
        longSeries().pivotByGroup('host', 'doesNotExist'),
      ).toThrow(/value column "doesNotExist" not in schema/);
    });

    it('throws when group column is not in schema', () => {
      expect(() =>
        // @ts-expect-error invalid column at type level
        longSeries().pivotByGroup('doesNotExist', 'cpu'),
      ).toThrow(/group column "doesNotExist" not in schema/);
    });

    it('throws when aggregator output kind would not match value kind', () => {
      const stringSchema = [
        { name: 'time', kind: 'time' },
        { name: 'status', kind: 'string' },
        { name: 'host', kind: 'string' },
      ] as const;
      const ts = new TimeSeries({
        name: 'metrics',
        schema: stringSchema,
        rows: [[0, 'ok', 'api-1']],
      });
      expect(() =>
        ts.pivotByGroup('host', 'status', { aggregate: 'count' }),
      ).toThrow(/produces number output.*has kind "string"/);
    });

    it('throws when aggregator produces array output', () => {
      expect(() =>
        longSeries().pivotByGroup('host', 'cpu', { aggregate: 'unique' }),
      ).toThrow(/produces array output.*has kind "number"/);
    });

    it('accepts source-preserving aggregators on any value kind', () => {
      const stringSchema = [
        { name: 'time', kind: 'time' },
        { name: 'status', kind: 'string' },
        { name: 'host', kind: 'string' },
      ] as const;
      const ts = new TimeSeries({
        name: 'metrics',
        schema: stringSchema,
        rows: [
          [0, 'ok', 'api-1'],
          [0, 'fail', 'api-1'], // duplicate
        ],
      });
      // 'last' has outputKind 'source', so string-valued sources work.
      const wide = ts.pivotByGroup('host', 'status', { aggregate: 'last' });
      expect(wide.toPoints()[0]['api-1_status']).toBe('fail');
    });

    it('throws when input is not time-keyed', () => {
      const intervalSchema = [
        { name: 'window', kind: 'interval' },
        { name: 'cpu', kind: 'number' },
        { name: 'host', kind: 'string' },
      ] as const;
      const ts = new TimeSeries({
        name: 'binned',
        schema: intervalSchema,
        rows: [[{ value: '0', start: 0, end: 1000 }, 0.3, 'api-1']],
      });
      expect(() => ts.pivotByGroup('host', 'cpu')).toThrow(
        /requires a time-keyed series/,
      );
    });
  });

  describe('typed variant (declared groups)', () => {
    it('uses declaration order, not alphabetical, for output columns', () => {
      const ts = new TimeSeries({
        name: 'metrics',
        schema,
        rows: [
          [0, 0.31, 'zeta'],
          [0, 0.52, 'alpha'],
          [0, 0.4, 'mu'],
        ],
      });
      const wide = ts.pivotByGroup('host', 'cpu', {
        groups: ['zeta', 'alpha', 'mu'] as const,
      });
      expect(wide.schema.slice(1).map((c) => c.name)).toEqual([
        'zeta_cpu',
        'alpha_cpu',
        'mu_cpu',
      ]);
    });

    it('emits a column for declared groups even when the group has no events', () => {
      const ts = new TimeSeries({
        name: 'metrics',
        schema,
        rows: [
          [0, 0.31, 'api-1'],
          [60_000, 0.44, 'api-1'],
        ],
      });
      const wide = ts.pivotByGroup('host', 'cpu', {
        groups: ['api-1', 'api-2', 'api-3'] as const,
      });
      expect(wide.schema.slice(1).map((c) => c.name)).toEqual([
        'api-1_cpu',
        'api-2_cpu',
        'api-3_cpu',
      ]);
      const row = wide.toPoints()[0];
      expect(row['api-1_cpu']).toBe(0.31);
      expect(row['api-2_cpu']).toBeUndefined();
      expect(row['api-3_cpu']).toBeUndefined();
    });

    it('throws when runtime data has a group not in the declared set', () => {
      const ts = new TimeSeries({
        name: 'metrics',
        schema,
        rows: [
          [0, 0.31, 'api-1'],
          [0, 0.52, 'api-rogue'],
        ],
      });
      let captured: Error | undefined;
      try {
        ts.pivotByGroup('host', 'cpu', {
          groups: ['api-1', 'api-2'] as const,
        });
      } catch (e) {
        captured = e as Error;
      }
      expect(captured).toBeInstanceOf(TypeError);
      // Pin the three load-bearing pieces of the error message:
      // the offending value, the declared set, and the suggested fix.
      expect(captured!.message).toContain('"api-rogue"');
      expect(captured!.message).toContain('"api-1"');
      expect(captured!.message).toContain('"api-2"');
      expect(captured!.message).toMatch(
        /Drop the `groups` option|add this value to the declared set/,
      );
    });

    it('passes aggregator through alongside groups', () => {
      const ts = new TimeSeries({
        name: 'metrics',
        schema,
        rows: [
          [0, 0.3, 'api-1'],
          [0, 0.5, 'api-1'], // duplicate
          [0, 0.7, 'api-2'],
        ],
      });
      const wide = ts.pivotByGroup('host', 'cpu', {
        groups: ['api-1', 'api-2'] as const,
        aggregate: 'avg',
      });
      expect(wide.toPoints()[0]['api-1_cpu']).toBeCloseTo(0.4, 5);
    });

    it('declared empty groups + empty source produces a time-only schema', () => {
      const ts = new TimeSeries({ name: 'empty', schema, rows: [] });
      const wide = ts.pivotByGroup('host', 'cpu', {
        groups: [] as const,
      });
      expect(wide.schema.map((c) => c.name)).toEqual(['time']);
    });

    it('declared empty groups throws on any source row (not silent fallback)', () => {
      // Pins the difference between "declared empty respected (everything is
      // an extra value, throw)" and "alphabetical fallback over an empty
      // declared set (silently produce time-only schema)". The former is
      // the contract; the latter would be a silent bug.
      const ts = new TimeSeries({
        name: 'metrics',
        schema,
        rows: [[0, 0.31, 'api-1']],
      });
      expect(() =>
        ts.pivotByGroup('host', 'cpu', { groups: [] as const }),
      ).toThrow(/encountered group value "api-1"/);
    });

    it('still emits declared columns when source is empty', () => {
      const ts = new TimeSeries({ name: 'empty', schema, rows: [] });
      const wide = ts.pivotByGroup('host', 'cpu', {
        groups: ['api-1', 'api-2'] as const,
      });
      expect(wide.schema.slice(1).map((c) => c.name)).toEqual([
        'api-1_cpu',
        'api-2_cpu',
      ]);
      expect(wide.length).toBe(0);
    });
  });

  describe('composition', () => {
    it('chains with rolling for per-host smoothing', () => {
      const wide = longSeries().pivotByGroup('host', 'cpu');
      const smoothed = wide.rolling('60s', {
        'api-1_cpu': 'avg',
        'api-2_cpu': 'avg',
      });
      expect(smoothed.length).toBe(3);
      expect(smoothed.schema.map((c) => c.name)).toContain('api-1_cpu');
    });

    it('chains with fill for carry-forward on missing cells', () => {
      const wide = longSeries().pivotByGroup('host', 'cpu');
      const filled = wide.fill({ 'api-2_cpu': 'hold' });
      const points = filled.toPoints();
      expect(points[2]['api-2_cpu']).toBe(0.48); // carried from 60_000
    });
  });
});
