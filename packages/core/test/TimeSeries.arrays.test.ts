import { describe, expect, it } from 'vitest';
import { Sequence, TimeSeries, ValidationError, top } from '../src/index.js';

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

  it('aggregate: flattens array-kind source columns (set union across arrays)', () => {
    const s = new TimeSeries({
      name: 'tags',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'tags', kind: 'array' },
      ] as const,
      rows: [
        [0, ['a', 'b']],
        [500, ['b', 'c']],
        [1500, ['a', 'd']],
      ],
    });
    const agg = s.aggregate(Sequence.every('1s'), { tags: 'unique' });
    // bucket [0,1s): union of ['a','b'] + ['b','c'] -> ['a','b','c']
    expect(agg.at(0)!.get('tags')).toEqual(['a', 'b', 'c']);
    // bucket [1s,2s): ['a','d']
    expect(agg.at(1)!.get('tags')).toEqual(['a', 'd']);
  });

  it('rolling: flattens array-kind source columns with correct eviction', () => {
    const s = new TimeSeries({
      name: 'tags',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'tags', kind: 'array' },
      ] as const,
      rows: [
        [0, ['a', 'b']],
        [1000, ['b', 'c']],
        [2000, ['d']],
      ],
    });
    // 1.5s trailing window: at each index the window spans (t-1500, t].
    const rolled = s.rolling('1500ms', { tags: 'unique' });
    expect(rolled.at(0)!.get('tags')).toEqual(['a', 'b']);
    // (−500, 1000]: both rows -> ['a','b','c']
    expect(rolled.at(1)!.get('tags')).toEqual(['a', 'b', 'c']);
    // (500, 2000]: row[1] + row[2] -> ['b','c','d']
    expect(rolled.at(2)!.get('tags')).toEqual(['b', 'c', 'd']);
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

describe('top reducer', () => {
  const scalarSchema = [
    { name: 'time', kind: 'time' },
    { name: 'host', kind: 'string' },
  ] as const;

  function hostSeries() {
    return new TimeSeries({
      name: 'hosts',
      schema: scalarSchema,
      rows: [
        [0, 'api-1'],
        [1000, 'api-2'],
        [2000, 'api-1'],
        [3000, 'api-3'],
        [4000, 'api-1'],
        [5000, 'api-2'],
        [6000, 'api-4'],
      ],
    });
  }

  it("top(n) helper returns the matching 'topN' string name", () => {
    expect(top(3)).toBe('top3');
    expect(top(10)).toBe('top10');
  });

  it("reduce: 'top3' returns the three most frequent values", () => {
    // api-1 x3, api-2 x2, api-3 x1, api-4 x1 -> top 3 are api-1, api-2, then
    // a tie between api-3 and api-4 broken by scalar ordering -> api-3.
    expect(hostSeries().reduce('host', 'top3')).toEqual([
      'api-1',
      'api-2',
      'api-3',
    ]);
  });

  it('reduce: top() helper is interchangeable with the string form', () => {
    expect(hostSeries().reduce('host', top(2))).toEqual(['api-1', 'api-2']);
  });

  it('reduce: N larger than unique count returns all unique values', () => {
    // Only 4 unique, request 10 -> all 4 back, ordered by frequency.
    expect(hostSeries().reduce('host', top(10))).toEqual([
      'api-1',
      'api-2',
      'api-3',
      'api-4',
    ]);
  });

  it('reduce: empty series returns empty array', () => {
    const empty = new TimeSeries({
      name: 'e',
      schema: scalarSchema,
      rows: [],
    });
    expect(empty.reduce('host', top(3))).toEqual([]);
  });

  it("reduce: ignores 'top0' and 'top-1' as reducer names", () => {
    // parseTopN rejects these, so the registry throws.
    expect(() => hostSeries().reduce('host', 'top0' as any)).toThrow(
      /unsupported aggregate reducer/,
    );
    expect(() => hostSeries().reduce('host', 'top-1' as any)).toThrow(
      /unsupported aggregate reducer/,
    );
  });

  it('aggregate: produces an array-kind output column', () => {
    const s = new TimeSeries({
      name: 'h',
      schema: scalarSchema,
      rows: [
        [0, 'a'],
        [500, 'a'],
        [600, 'b'],
        [1500, 'c'],
        [1700, 'a'],
        [1900, 'b'],
      ],
    });
    const agg = s.aggregate(Sequence.every('1s'), { host: top(2) });
    const hostCol = agg.schema.find((c) => c.name === 'host');
    expect(hostCol?.kind).toBe('array');
    // bucket [0,1s): a x2, b x1 -> [a, b]
    expect(agg.at(0)!.get('host')).toEqual(['a', 'b']);
    // bucket [1s,2s): a x1, b x1, c x1 -> three-way tie, scalar order
    expect(agg.at(1)!.get('host')).toEqual(['a', 'b']);
  });

  it('rolling: updates incrementally as the window slides', () => {
    const s = new TimeSeries({
      name: 'h',
      schema: scalarSchema,
      rows: [
        [0, 'a'],
        [1000, 'a'],
        [2000, 'b'],
        [3000, 'b'],
        [4000, 'b'],
        [5000, 'c'],
      ],
    });
    const rolled = s.rolling('3s', { host: top(1) });
    // (−3s, 0]  -> [a]           -> a
    expect(rolled.at(0)!.get('host')).toEqual(['a']);
    // (−2s, 1s] -> [a, a]        -> a
    expect(rolled.at(1)!.get('host')).toEqual(['a']);
    // (−1s, 2s] -> [a, a, b]     -> a
    expect(rolled.at(2)!.get('host')).toEqual(['a']);
    // (0s, 3s]  -> [a, b, b]     -> b
    expect(rolled.at(3)!.get('host')).toEqual(['b']);
    // (1s, 4s]  -> [b, b, b]     -> b
    expect(rolled.at(4)!.get('host')).toEqual(['b']);
    // (2s, 5s]  -> [b, b, c]     -> b
    expect(rolled.at(5)!.get('host')).toEqual(['b']);
  });

  it('arrayAggregate: top-N frequency across elements of each array', () => {
    const s = new TimeSeries({
      name: 'tags',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'tags', kind: 'array' },
      ] as const,
      rows: [
        [0, ['a', 'b', 'a', 'c']],
        [1000, ['b', 'b', 'a', 'd']],
      ],
    });
    const topped = s.arrayAggregate('tags', top(2), { as: 'topTags' });
    const col = topped.schema.find((c) => c.name === 'topTags');
    expect(col?.kind).toBe('array');
    // row 0: a x2, b x1, c x1 -> [a, b]
    expect(topped.at(0)!.get('topTags')).toEqual(['a', 'b']);
    // row 1: b x2, a x1, d x1 -> [b, a]
    expect(topped.at(1)!.get('topTags')).toEqual(['b', 'a']);
  });

  it('tie-break is deterministic across shuffles of the same input', () => {
    const schemaT = [
      { name: 'time', kind: 'time' },
      { name: 'v', kind: 'string' },
    ] as const;
    const a = new TimeSeries({
      name: 'a',
      schema: schemaT,
      rows: [
        [0, 'x'],
        [1, 'y'],
        [2, 'z'],
      ],
    });
    const b = new TimeSeries({
      name: 'b',
      schema: schemaT,
      rows: [
        [0, 'z'],
        [1, 'y'],
        [2, 'x'],
      ],
    });
    // All three values tie at count 1; tie-break by scalar order.
    expect(a.reduce('v', top(2))).toEqual(['x', 'y']);
    expect(b.reduce('v', top(2))).toEqual(['x', 'y']);
  });

  it('aggregate: flattens array-kind source columns (elements across all arrays)', () => {
    const s = new TimeSeries({
      name: 'tags',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'tags', kind: 'array' },
      ] as const,
      rows: [
        [0, ['5xx', 'timeout']],
        [500, ['5xx']],
        [700, ['retry', '5xx']],
        [1500, ['timeout']],
      ],
    });
    const agg = s.aggregate(Sequence.every('1s'), { tags: top(2) });
    // bucket [0,1s): 5xx x3, timeout x1, retry x1 -> [5xx, retry]
    // (retry and timeout tie at 1; scalar order picks retry)
    expect(agg.at(0)!.get('tags')).toEqual(['5xx', 'retry']);
    expect(agg.at(1)!.get('tags')).toEqual(['timeout']);
  });

  it('rolling: flattens array-kind inputs with correct eviction counts', () => {
    const s = new TimeSeries({
      name: 'tags',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'tags', kind: 'array' },
      ] as const,
      rows: [
        [0, ['a', 'a']],
        [1000, ['b']],
        [2000, ['b', 'c']],
        [3000, ['a']],
      ],
    });
    // 1500ms trailing window.
    const rolled = s.rolling('1500ms', { tags: top(1) });
    // (−1500, 0]: a x2 -> [a]
    expect(rolled.at(0)!.get('tags')).toEqual(['a']);
    // (−500, 1000]: a x2, b x1 -> [a]
    expect(rolled.at(1)!.get('tags')).toEqual(['a']);
    // (500, 2000]: b x2, c x1 -> [b]
    expect(rolled.at(2)!.get('tags')).toEqual(['b']);
    // (1500, 3000]: b x1, c x1, a x1 -> three-way tie, scalar order -> [a]
    expect(rolled.at(3)!.get('tags')).toEqual(['a']);
  });
});
