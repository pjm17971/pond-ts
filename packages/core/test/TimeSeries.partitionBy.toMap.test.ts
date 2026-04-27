import { describe, expect, it } from 'vitest';
import { TimeSeries } from '../src/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'number', required: false },
  { name: 'host', kind: 'string', required: false },
] as const;

const compositeSchema = [
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'number', required: false },
  { name: 'host', kind: 'string', required: false },
  { name: 'region', kind: 'string', required: false },
] as const;

function makeSeries(
  rows: ReadonlyArray<
    readonly [number, number | undefined, string | undefined]
  >,
) {
  return new TimeSeries({
    name: 'metrics',
    schema,
    rows: rows.map(
      (r) => [...r] as [number, number | undefined, string | undefined],
    ),
  });
}

describe('PartitionedTimeSeries.toMap', () => {
  describe('basic shape', () => {
    it('returns one entry per partition', () => {
      const ts = makeSeries([
        [0, 0.5, 'a'],
        [0, 0.3, 'b'],
        [60_000, 0.6, 'a'],
        [60_000, 0.4, 'b'],
      ]);
      const m = ts.partitionBy('host').toMap();
      expect(m.size).toBe(2);
      expect([...m.keys()].sort()).toEqual(['a', 'b']);
    });

    it('each entry is a TimeSeries with the same schema', () => {
      const ts = makeSeries([
        [0, 0.5, 'a'],
        [60_000, 0.6, 'a'],
      ]);
      const m = ts.partitionBy('host').toMap();
      const sub = m.get('a');
      expect(sub).toBeInstanceOf(TimeSeries);
      expect(sub?.schema).toEqual(schema);
      expect(sub?.length).toBe(2);
    });

    it('preserves event order within each partition', () => {
      const ts = makeSeries([
        [0, 0.5, 'a'],
        [0, 0.3, 'b'],
        [60_000, 0.6, 'a'],
        [60_000, 0.4, 'b'],
        [120_000, 0.7, 'a'],
      ]);
      const m = ts.partitionBy('host').toMap();
      const aTimes = [...(m.get('a')?.events ?? [])].map((e) => e.begin());
      const bTimes = [...(m.get('b')?.events ?? [])].map((e) => e.begin());
      expect(aTimes).toEqual([0, 60_000, 120_000]);
      expect(bTimes).toEqual([0, 60_000]);
    });

    it('returns an empty Map for an empty source', () => {
      const ts = new TimeSeries({ name: 'm', schema, rows: [] });
      const m = ts.partitionBy('host').toMap();
      expect(m.size).toBe(0);
    });

    it('Map iteration order matches partition first-seen order', () => {
      // 'b' appears before 'a' in the input → 'b' iterates first
      const ts = makeSeries([
        [0, 0.5, 'b'],
        [0, 0.3, 'a'],
        [60_000, 0.6, 'b'],
        [60_000, 0.4, 'a'],
      ]);
      const m = ts.partitionBy('host').toMap();
      const order = [...m.keys()];
      expect(order).toEqual(['b', 'a']);
    });
  });

  describe('with transform', () => {
    it('applies the transform per partition and returns Map<string, R>', () => {
      const ts = makeSeries([
        [0, 0.5, 'a'],
        [0, 0.3, 'b'],
        [60_000, 0.6, 'a'],
      ]);
      const m = ts.partitionBy('host').toMap((g) => g.length);
      expect(m.get('a')).toBe(2);
      expect(m.get('b')).toBe(1);
    });

    it('transform can apply a stateful per-partition op (diff)', () => {
      const ts = makeSeries([
        [0, 0.5, 'a'],
        [60_000, 0.6, 'a'],
        [120_000, 0.7, 'a'],
      ]);
      const m = ts.partitionBy('host').toMap((g) => g.diff('cpu'));
      const sub = m.get('a');
      expect(sub).toBeInstanceOf(TimeSeries);
      expect(sub?.length).toBe(3);
      // diff replaces cpu in place; first event has undefined, then the
      // per-event delta. Confirms the transform actually ran, not just
      // a passthrough.
      expect(sub?.at(0)?.get('cpu')).toBeUndefined();
      expect(sub?.at(1)?.get('cpu')).toBeCloseTo(0.1, 5);
      expect(sub?.at(2)?.get('cpu')).toBeCloseTo(0.1, 5);
    });

    it('transform can return arbitrary non-series values (toPoints)', () => {
      const ts = makeSeries([
        [0, 0.5, 'a'],
        [60_000, 0.6, 'a'],
      ]);
      const m = ts.partitionBy('host').toMap((g) => g.toPoints());
      const points = m.get('a');
      expect(Array.isArray(points)).toBe(true);
      expect(points?.length).toBe(2);
    });

    it('replaces the .collect().groupBy(col, fn) chain', () => {
      // Dashboard agent's exact pain point: getting Map<host, points[]>
      const ts = makeSeries([
        [0, 0.5, 'a'],
        [0, 0.3, 'b'],
        [60_000, 0.6, 'a'],
        [60_000, 0.4, 'b'],
      ]);
      const newWay = ts.partitionBy('host').toMap((g) => g.toPoints());
      const oldWay = ts
        .partitionBy('host')
        .collect()
        .groupBy('host', (g) => g.toPoints());
      expect([...newWay.keys()].sort()).toEqual([...oldWay.keys()].sort());
      expect(newWay.get('a')?.length).toBe(oldWay.get('a')?.length);
      expect(newWay.get('b')?.length).toBe(oldWay.get('b')?.length);
    });

    it('transform throwing leaves no partial Map state visible', () => {
      const ts = makeSeries([
        [0, 0.5, 'a'],
        [0, 0.3, 'b'],
        [60_000, 0.6, 'a'],
        [60_000, 0.4, 'b'],
      ]);
      let calls = 0;
      expect(() =>
        ts.partitionBy('host').toMap((g) => {
          calls += 1;
          if (calls === 2) throw new Error('boom');
          return g.toPoints();
        }),
      ).toThrow(/boom/);
      // The throw propagates without exposing a partial Map.
      // The impl builds the Map locally and only returns on successful
      // completion of every partition's transform — there's no
      // intermediate visibility for the caller.
    });
  });

  describe('composite partition keys', () => {
    function makeComposite(
      rows: ReadonlyArray<
        readonly [
          number,
          number | undefined,
          string | undefined,
          string | undefined,
        ]
      >,
    ) {
      return new TimeSeries({
        name: 'metrics',
        schema: compositeSchema,
        rows: rows.map(
          (r) =>
            [...r] as [
              number,
              number | undefined,
              string | undefined,
              string | undefined,
            ],
        ),
      });
    }

    it('encodes composite keys as JSON arrays', () => {
      const ts = makeComposite([
        [0, 0.5, 'a', 'eu'],
        [0, 0.3, 'a', 'us'],
        [60_000, 0.6, 'a', 'eu'],
        [60_000, 0.4, 'a', 'us'],
      ]);
      const m = ts.partitionBy(['host', 'region']).toMap();
      expect(m.size).toBe(2);
      expect(m.has('["a","eu"]')).toBe(true);
      expect(m.has('["a","us"]')).toBe(true);
    });

    it('does not collapse keys whose values contain separators', () => {
      // Naive `parts.join(' ')` would collapse these:
      //   ['a b', 'c'] and ['a', 'b c'] both → "a b c"
      // JSON encoding (with quote+escape) keeps them distinct.
      const ts = makeComposite([
        [0, 0.5, 'a b', 'c'],
        [0, 0.3, 'a', 'b c'],
        [60_000, 0.6, 'a b', 'c'],
        [60_000, 0.4, 'a', 'b c'],
      ]);
      const m = ts.partitionBy(['host', 'region']).toMap();
      expect(m.size).toBe(2);
    });
  });

  describe('undefined as a partition key value (regression)', () => {
    // Adversarial review of PR #80 flagged that `undefined` partition
    // key values were correct in the encoder but unpinned by tests.
    // Single-column path uses `' undefined'` with a leading space;
    // composite path uses JSON.stringify with `?? null`.

    it('single-column: undefined value uses the leading-space sentinel', () => {
      const ts = makeSeries([
        [0, 0.5, 'a'],
        [0, 0.3, undefined], // undefined host
        [60_000, 0.6, 'a'],
        [60_000, 0.4, undefined],
      ]);
      const m = ts.partitionBy('host').toMap();
      expect(m.size).toBe(2);
      expect(m.has('a')).toBe(true);
      expect(m.has(' undefined')).toBe(true); // leading space
      expect(m.get(' undefined')?.length).toBe(2);
    });

    it('single-column: undefined never collides with a string "undefined"', () => {
      // A string column whose value is the literal text 'undefined'
      // must NOT collapse with rows that have undefined at that column.
      const ts = makeSeries([
        [0, 0.5, 'undefined'], // string literal
        [0, 0.3, undefined], // missing
        [60_000, 0.6, 'undefined'],
        [60_000, 0.4, undefined],
      ]);
      const m = ts.partitionBy('host').toMap();
      expect(m.size).toBe(2);
      expect(m.has('undefined')).toBe(true); // string-value partition
      expect(m.has(' undefined')).toBe(true); // missing-value partition
      expect(m.get('undefined')?.length).toBe(2);
      expect(m.get(' undefined')?.length).toBe(2);
    });

    it('diverges from groupBy on undefined keys (intentional improvement)', () => {
      // Pin the documented divergence between toMap and groupBy.
      // groupBy uses bare 'undefined' for missing values — collides
      // with the literal string 'undefined'. toMap's leading-space
      // sentinel keeps them distinct.
      //
      // This is documented in toMap's JSDoc as an intentional
      // improvement; migrating from groupBy to toMap requires
      // updating any `.get('undefined')` lookup to `.get(' undefined')`.
      const ts = makeSeries([
        [0, 0.5, 'undefined'], // string literal
        [0, 0.3, undefined], // missing
        [60_000, 0.6, 'undefined'],
        [60_000, 0.4, undefined],
      ]);
      // groupBy collapses both rows under key 'undefined' — wrong but
      // historical. Verify by checking the bucket it produces.
      const fromGroupBy = ts.groupBy('host');
      expect(fromGroupBy.size).toBe(1); // collapsed
      expect(fromGroupBy.get('undefined')?.length).toBe(4); // all 4 events

      // toMap keeps them separate.
      const fromToMap = ts.partitionBy('host').toMap();
      expect(fromToMap.size).toBe(2); // distinct
      expect(fromToMap.get('undefined')?.length).toBe(2); // string-value only
      expect(fromToMap.get(' undefined')?.length).toBe(2); // missing only
    });

    it('composite: undefined becomes null in the JSON encoding', () => {
      const ts = new TimeSeries({
        name: 'metrics',
        schema: compositeSchema,
        rows: [
          [0, 0.5, 'a', 'eu'] as [number, number, string, string],
          [0, 0.3, 'a', undefined] as [
            number,
            number,
            string,
            string | undefined,
          ],
          [60_000, 0.6, 'a', 'eu'] as [number, number, string, string],
          [60_000, 0.4, 'a', undefined] as [
            number,
            number,
            string,
            string | undefined,
          ],
        ],
      });
      const m = ts.partitionBy(['host', 'region']).toMap();
      expect(m.size).toBe(2);
      expect(m.has('["a","eu"]')).toBe(true);
      expect(m.has('["a",null]')).toBe(true); // undefined → null in JSON
    });
  });
});
