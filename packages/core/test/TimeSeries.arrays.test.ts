import { describe, expect, it } from 'vitest';
import { Sequence, TimeSeries, ValidationError } from '../src/index.js';

// A schema with a tag-like array column and a numeric column alongside.
const schema = [
  { name: 'time', kind: 'time' },
  { name: 'tags', kind: 'array' },
  { name: 'host', kind: 'string' },
] as const;

function makeSeries() {
  return new TimeSeries({
    name: 'tagged',
    schema,
    rows: [
      [0, ['web', 'east'], 'api-1'],
      [1000, ['web', 'west'], 'api-1'],
      [2000, ['db'], 'api-2'],
      [3000, ['web', 'db', 'east'], 'api-3'],
      [4000, [], 'api-1'],
    ],
  });
}

describe('array column validation', () => {
  it('accepts arrays of scalars', () => {
    const s = makeSeries();
    expect(s.length).toBe(5);
    expect(s.first()!.get('tags')).toEqual(['web', 'east']);
  });

  it('rejects a non-array value on an array column', () => {
    expect(
      () =>
        new TimeSeries({
          name: 'bad',
          schema,
          rows: [[0, 'web' as any, 'api']],
        }),
    ).toThrow(ValidationError);
  });

  it('rejects nested arrays', () => {
    expect(
      () =>
        new TimeSeries({
          name: 'bad',
          schema,
          rows: [[0, [[1, 2]] as any, 'api']],
        }),
    ).toThrow(ValidationError);
  });

  it('rejects NaN inside array cells', () => {
    expect(
      () =>
        new TimeSeries({
          name: 'bad',
          schema: [
            { name: 'time', kind: 'time' },
            { name: 'xs', kind: 'array' },
          ] as const,
          rows: [[0, [1, Number.NaN]]],
        }),
    ).toThrow(ValidationError);
  });

  it('freezes the stored array so callers cannot mutate it through event.get()', () => {
    const input: string[] = ['web', 'east'];
    const s = new TimeSeries({
      name: 'frozen',
      schema,
      rows: [[0, input, 'api-1']],
    });

    const arr = s.first()!.get('tags') as readonly string[];
    expect(Object.isFrozen(arr)).toBe(true);
    expect(() => (arr as string[]).push('south')).toThrow();
    // mutating the original caller reference must not affect stored state
    input.push('south');
    expect(s.first()!.get('tags')).toEqual(['web', 'east']);
  });
});

describe('array column JSON round-trip', () => {
  it('preserves array cells through toJSON + fromJSON', () => {
    const s = makeSeries();
    const json = s.toJSON();
    const restored = TimeSeries.fromJSON(json);
    expect(restored.length).toBe(s.length);
    for (let i = 0; i < s.length; i += 1) {
      expect(restored.at(i)!.get('tags')).toEqual(s.at(i)!.get('tags'));
      expect(restored.at(i)!.get('host')).toBe(s.at(i)!.get('host'));
    }
  });

  it('preserves array cells through object-row JSON', () => {
    const s = makeSeries();
    const json = s.toJSON({ rowFormat: 'object' });
    const restored = TimeSeries.fromJSON(json);
    expect(restored.length).toBe(s.length);
    expect(restored.at(0)!.get('tags')).toEqual(['web', 'east']);
    expect(restored.at(3)!.get('tags')).toEqual(['web', 'db', 'east']);
  });
});

describe('unique reducer (bucket aggregation)', () => {
  const scalarSchema = [
    { name: 'time', kind: 'time' },
    { name: 'host', kind: 'string' },
  ] as const;

  function scalarSeries() {
    return new TimeSeries({
      name: 'hosts',
      schema: scalarSchema,
      rows: [
        [0, 'api-1'],
        [1000, 'api-1'],
        [2000, 'api-2'],
        [3000, 'api-3'],
        [4000, 'api-1'],
      ],
    });
  }

  it('reduce: returns distinct sorted values as an array', () => {
    const result = scalarSeries().reduce('host', 'unique');
    expect(result).toEqual(['api-1', 'api-2', 'api-3']);
  });

  it('reduce: empty series returns empty array', () => {
    const empty = new TimeSeries({
      name: 'e',
      schema: scalarSchema,
      rows: [],
    });
    expect(empty.reduce('host', 'unique')).toEqual([]);
  });

  it('aggregate: produces an array-kind output column', () => {
    const agg = scalarSeries().aggregate(Sequence.every('2s'), {
      host: 'unique',
    });
    const hostCol = agg.schema.find((c) => c.name === 'host');
    expect(hostCol?.kind).toBe('array');
    expect(agg.at(0)!.get('host')).toEqual(['api-1']);
    expect(agg.at(1)!.get('host')).toEqual(['api-2', 'api-3']);
  });

  it('rolling: emits the distinct values in the current window', () => {
    const s = scalarSeries();
    const rolled = s.rolling('2s', { host: 'unique' });
    expect(rolled.length).toBe(s.length);
    expect(rolled.at(0)!.get('host')).toEqual(['api-1']);
    expect(rolled.at(1)!.get('host')).toEqual(['api-1']);
    expect(rolled.at(2)!.get('host')).toEqual(['api-1', 'api-2']);
    expect(rolled.at(3)!.get('host')).toEqual(['api-2', 'api-3']);
    expect(rolled.at(4)!.get('host')).toEqual(['api-1', 'api-3']);
  });
});

describe('TimeSeries.arrayContains', () => {
  it('keeps events whose array column contains the value', () => {
    const filtered = makeSeries().arrayContains('tags', 'web');
    expect(filtered.length).toBe(3);
    expect(filtered.toArray().map((e) => e.get('host'))).toEqual([
      'api-1',
      'api-1',
      'api-3',
    ]);
  });

  it('returns an empty series when no event contains the value', () => {
    expect(makeSeries().arrayContains('tags', 'south').length).toBe(0);
  });

  it('drops events with an undefined array cell', () => {
    const optionalSchema = [
      { name: 'time', kind: 'time' },
      { name: 'tags', kind: 'array', required: false },
    ] as const;
    const s = new TimeSeries({
      name: 'opt',
      schema: optionalSchema,
      rows: [
        [0, ['a']],
        [1000, undefined],
        [2000, ['a', 'b']],
      ],
    });
    expect(s.arrayContains('tags', 'a').length).toBe(2);
  });
});

describe('TimeSeries.arrayContainsAll', () => {
  it('keeps events whose array contains every value (AND)', () => {
    const filtered = makeSeries().arrayContainsAll('tags', ['web', 'east']);
    expect(filtered.length).toBe(2);
    expect(filtered.toArray().map((e) => e.get('host'))).toEqual([
      'api-1',
      'api-3',
    ]);
  });

  it('returns an empty series when no event contains all values', () => {
    expect(makeSeries().arrayContainsAll('tags', ['web', 'south']).length).toBe(
      0,
    );
  });

  it('treats an empty needle list as keeping every defined array', () => {
    expect(makeSeries().arrayContainsAll('tags', []).length).toBe(5);
  });
});

describe('TimeSeries.arrayContainsAny', () => {
  it('keeps events whose array contains at least one value (OR)', () => {
    const filtered = makeSeries().arrayContainsAny('tags', ['db', 'south']);
    // db is in rows 2 and 3
    expect(filtered.length).toBe(2);
    expect(filtered.toArray().map((e) => e.get('host'))).toEqual([
      'api-2',
      'api-3',
    ]);
  });

  it('is a union across multiple needles', () => {
    const filtered = makeSeries().arrayContainsAny('tags', ['east', 'db']);
    // east: rows 0, 3. db: rows 2, 3. Union: rows 0, 2, 3.
    expect(filtered.length).toBe(3);
  });

  it('returns an empty series when needle list is empty', () => {
    expect(makeSeries().arrayContainsAny('tags', []).length).toBe(0);
  });

  it('returns an empty series when no event matches any needle', () => {
    expect(
      makeSeries().arrayContainsAny('tags', ['south', 'north']).length,
    ).toBe(0);
  });
});

describe('TimeSeries.arrayAggregate', () => {
  it('count replaces the array column with its length (replace in place)', () => {
    const counted = makeSeries().arrayAggregate('tags', 'count');
    const col = counted.schema.find((c) => c.name === 'tags');
    expect(col?.kind).toBe('number');
    expect(counted.toArray().map((e) => e.get('tags'))).toEqual([
      2, 2, 1, 3, 0,
    ]);
    expect(counted.at(0)!.get('host')).toBe('api-1');
  });

  it('count with { as } appends a new column and keeps the source array', () => {
    const counted = makeSeries().arrayAggregate('tags', 'count', {
      as: 'tagCount',
    });
    const tagCount = counted.schema.find((c) => c.name === 'tagCount');
    expect(tagCount?.kind).toBe('number');
    // original tags column still present and unchanged
    const tagsCol = counted.schema.find((c) => c.name === 'tags');
    expect(tagsCol?.kind).toBe('array');
    expect(counted.at(0)!.get('tags')).toEqual(['web', 'east']);
    expect(counted.at(0)!.get('tagCount')).toBe(2);
    expect(counted.at(4)!.get('tagCount')).toBe(0);
  });

  it('treats undefined array cells as producing undefined output', () => {
    const optionalSchema = [
      { name: 'time', kind: 'time' },
      { name: 'tags', kind: 'array', required: false },
    ] as const;
    const s = new TimeSeries({
      name: 'opt',
      schema: optionalSchema,
      rows: [
        [0, ['a', 'b']],
        [1000, undefined],
      ],
    });
    const counted = s.arrayAggregate('tags', 'count', { as: 'n' });
    expect(counted.at(0)!.get('n')).toBe(2);
    expect(counted.at(1)!.get('n')).toBeUndefined();
  });

  it('sum/avg/min/max over a numeric array column', () => {
    const s = new TimeSeries({
      name: 'nums',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'xs', kind: 'array' },
      ] as const,
      rows: [
        [0, [1, 2, 3, 4]],
        [1000, [10, 20]],
      ],
    });

    expect(
      s
        .arrayAggregate('xs', 'sum', { as: 'total' })
        .toArray()
        .map((e) => e.get('total')),
    ).toEqual([10, 30]);
    expect(
      s
        .arrayAggregate('xs', 'avg', { as: 'mean' })
        .toArray()
        .map((e) => e.get('mean')),
    ).toEqual([2.5, 15]);
    expect(
      s
        .arrayAggregate('xs', 'min', { as: 'lo' })
        .toArray()
        .map((e) => e.get('lo')),
    ).toEqual([1, 10]);
    expect(
      s
        .arrayAggregate('xs', 'max', { as: 'hi' })
        .toArray()
        .map((e) => e.get('hi')),
    ).toEqual([4, 20]);
  });

  it('unique dedupes within an array and emits array kind', () => {
    const s = new TimeSeries({
      name: 'dups',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'tags', kind: 'array' },
      ] as const,
      rows: [
        [0, ['a', 'b', 'a', 'c']],
        [1000, ['a', 'a']],
      ],
    });
    const deduped = s.arrayAggregate('tags', 'unique');
    const col = deduped.schema.find((c) => c.name === 'tags');
    expect(col?.kind).toBe('array');
    expect(deduped.at(0)!.get('tags')).toEqual(['a', 'b', 'c']);
    expect(deduped.at(1)!.get('tags')).toEqual(['a']);
  });

  it('first with explicit kind override', () => {
    const s = new TimeSeries({
      name: 'first',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'xs', kind: 'array' },
      ] as const,
      rows: [
        [0, [10, 20, 30]],
        [1000, [40]],
      ],
    });
    const firsts = s.arrayAggregate('xs', 'first', {
      as: 'head',
      kind: 'number',
    });
    const head = firsts.schema.find((c) => c.name === 'head');
    expect(head?.kind).toBe('number');
    expect(firsts.toArray().map((e) => e.get('head'))).toEqual([10, 40]);
  });

  it('custom reducer: concat string tags with a separator', () => {
    const joined = makeSeries().arrayAggregate(
      'tags',
      (values) =>
        (values as readonly (string | undefined)[])
          .filter((v): v is string => typeof v === 'string')
          .join(','),
      { as: 'tagList' },
    );
    const col = joined.schema.find((c) => c.name === 'tagList');
    expect(col?.kind).toBe('string');
    expect(joined.toArray().map((e) => e.get('tagList'))).toEqual([
      'web,east',
      'web,west',
      'db',
      'web,db,east',
      '',
    ]);
  });
});

describe('TimeSeries.arrayExplode', () => {
  it('fans each event out into one event per element', () => {
    const exploded = makeSeries().arrayExplode('tags');
    const tagsCol = exploded.schema.find((c) => c.name === 'tags');
    expect(tagsCol?.kind).toBe('string');

    // Row 0: 2, Row 1: 2, Row 2: 1, Row 3: 3, Row 4: 0 -> 8 total
    expect(exploded.length).toBe(8);
    expect(exploded.at(0)!.get('tags')).toBe('web');
    expect(exploded.at(0)!.get('host')).toBe('api-1');

    const atT3 = exploded.toArray().filter((e) => e.begin() === 3000);
    expect(atT3.map((e) => e.get('tags'))).toEqual(['web', 'db', 'east']);
  });

  it('drops events with empty arrays', () => {
    const s = new TimeSeries({
      name: 's',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'tags', kind: 'array' },
      ] as const,
      rows: [
        [0, []],
        [1000, ['a', 'b']],
        [2000, []],
      ],
    });
    const exploded = s.arrayExplode('tags');
    expect(exploded.length).toBe(2);
    expect(exploded.toArray().map((e) => e.get('tags'))).toEqual(['a', 'b']);
  });

  it('accepts an explicit output kind', () => {
    const s = new TimeSeries({
      name: 's',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'xs', kind: 'array' },
      ] as const,
      rows: [
        [0, [1, 2, 3]],
        [1000, [4]],
      ],
    });
    const exploded = s.arrayExplode('xs', { kind: 'number' });
    const xsCol = exploded.schema.find((c) => c.name === 'xs');
    expect(xsCol?.kind).toBe('number');
    expect(exploded.toArray().map((e) => e.get('xs'))).toEqual([1, 2, 3, 4]);
  });

  it('with { as } appends a scalar column and keeps the source array', () => {
    const exploded = makeSeries().arrayExplode('tags', { as: 'tag' });
    const tagCol = exploded.schema.find((c) => c.name === 'tag');
    const tagsCol = exploded.schema.find((c) => c.name === 'tags');
    expect(tagCol?.kind).toBe('string');
    expect(tagsCol?.kind).toBe('array');

    // Each fanned-out event still carries the full original array
    expect(exploded.length).toBe(8);
    const firstFan = exploded.toArray().filter((e) => e.begin() === 0);
    expect(firstFan.map((e) => e.get('tag'))).toEqual(['web', 'east']);
    for (const e of firstFan) {
      expect(e.get('tags')).toEqual(['web', 'east']);
    }
  });
});

describe('array column composition', () => {
  it('aggregate(unique) → arrayAggregate(count): distinct count per bucket', () => {
    const s = new TimeSeries({
      name: 'hosts',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'host', kind: 'string' },
      ] as const,
      rows: [
        [0, 'api-1'],
        [500, 'api-2'],
        [1500, 'api-1'],
        [2000, 'api-3'],
        [3500, 'api-3'],
      ],
    });
    const distinctCount = s
      .aggregate(Sequence.every('1s'), { host: 'unique' })
      .arrayAggregate('host', 'count');
    expect(distinctCount.toArray().map((e) => e.get('host'))).toEqual([
      2, 1, 1, 1,
    ]);
  });

  it('aggregate(unique) → arrayExplode: flatten per-bucket distinct values', () => {
    const s = new TimeSeries({
      name: 'hosts',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'host', kind: 'string' },
      ] as const,
      rows: [
        [0, 'api-1'],
        [500, 'api-2'],
        [1500, 'api-1'],
      ],
    });
    const flat = s
      .aggregate(Sequence.every('1s'), { host: 'unique' })
      .arrayExplode('host');
    expect(flat.length).toBe(3);
    expect(flat.toArray().map((e) => e.get('host'))).toEqual([
      'api-1',
      'api-2',
      'api-1',
    ]);
  });

  it('arrayAggregate with { as } composes with scalar operators downstream', () => {
    const s = new TimeSeries({
      name: 'latencies',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'samples', kind: 'array' },
      ] as const,
      rows: [
        [0, [10, 20, 30]],
        [1000, [15, 25, 35]],
        [2000, [20, 30, 40]],
      ],
    });
    // derive a scalar avg column, then run a rolling avg over it
    const withAvg = s.arrayAggregate('samples', 'avg', { as: 'avg' });
    const avgs = withAvg.toArray().map((e) => e.get('avg'));
    expect(avgs).toEqual([20, 25, 30]);
  });
});
