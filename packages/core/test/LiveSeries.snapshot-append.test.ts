/**
 * Snapshot/append primitives on `LiveSeries` — codec-agnostic
 * typed-tuple primitives plus JSON sugar over them. Surfaced by
 * the gRPC experiment's M1 (WebSocket bridge), where every
 * aggregator+web pair was hand-rolling these against the existing
 * `TimeSeries` parallels and bumping into an `as never` cast on
 * the wire→push path.
 */
import { describe, expect, it } from 'vitest';
import { Event, Interval, LiveSeries, Time, TimeRange } from '../src/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'number' },
  { name: 'host', kind: 'string', required: false },
] as const;

function makeLive(): LiveSeries<typeof schema> {
  return new LiveSeries({ name: 'test', schema });
}

describe('LiveSeries.pushMany', () => {
  it('takes an array (not variadic) and matches push() behavior', () => {
    const live = makeLive();
    live.pushMany([
      [0, 0.1, 'api-1'],
      [1000, 0.2, 'api-1'],
      [2000, 0.3, 'api-2'],
    ]);
    expect(live.length).toBe(3);
    expect(live.at(2)!.get('cpu')).toBe(0.3);
    expect(live.at(2)!.get('host')).toBe('api-2');
  });

  it('preserves push() listener semantics — one batch fire, one retention pass', () => {
    const live = makeLive();
    let batchFires = 0;
    let batchSize = 0;
    live.on('batch', (events) => {
      batchFires += 1;
      batchSize = events.length;
    });
    live.pushMany([
      [0, 0.1, 'a'],
      [1000, 0.2, 'a'],
      [2000, 0.3, 'a'],
    ]);
    expect(batchFires).toBe(1);
    expect(batchSize).toBe(3);
  });

  it('push(...rows) is now a wrapper — identical results', () => {
    const a = makeLive();
    const b = makeLive();
    const rows: Array<[number, number, string]> = [
      [0, 0.1, 'a'],
      [1000, 0.2, 'a'],
    ];
    a.push(...rows);
    b.pushMany(rows);
    expect(a.length).toBe(b.length);
    expect(a.toRows()).toEqual(b.toRows());
  });

  it('no-op for empty rows array', () => {
    const live = makeLive();
    let fires = 0;
    live.on('batch', () => {
      fires += 1;
    });
    live.pushMany([]);
    expect(live.length).toBe(0);
    expect(fires).toBe(0);
  });
});

describe('LiveSeries.toRows / toObjects', () => {
  it('toRows() returns typed tuples [Time, ...values]', () => {
    const live = makeLive();
    live.pushMany([
      [0, 0.5, 'api-1'],
      [1000, 0.6, undefined],
    ]);
    const rows = live.toRows();
    expect(rows).toHaveLength(2);
    expect(rows[0]![0]).toBeInstanceOf(Time);
    expect((rows[0]![0] as Time).timestampMs()).toBe(0);
    expect(rows[0]![1]).toBe(0.5);
    expect(rows[0]![2]).toBe('api-1');
    expect(rows[1]![2]).toBeUndefined();
  });

  it('toObjects() returns schema-keyed object rows', () => {
    const live = makeLive();
    live.pushMany([[0, 0.5, 'api-1']]);
    const objs = live.toObjects() as Array<Record<string, unknown>>;
    expect(objs[0]!.time).toBeInstanceOf(Time);
    expect(objs[0]!.cpu).toBe(0.5);
    expect(objs[0]!.host).toBe('api-1');
  });
});

describe('LiveSeries.toJSON', () => {
  it('default (array form) emits numeric ms keys, null for undefined cells', () => {
    const live = makeLive();
    live.pushMany([
      [0, 0.5, 'api-1'],
      [1000, 0.6, undefined],
    ]);
    const out = live.toJSON();
    expect(out.name).toBe('test');
    expect(out.schema).toEqual(live.schema);
    const rows = out.rows as ReadonlyArray<readonly unknown[]>;
    expect(rows).toHaveLength(2);
    expect(rows[0]![0]).toBe(0);
    expect(rows[0]![1]).toBe(0.5);
    expect(rows[0]![2]).toBe('api-1');
    expect(rows[1]![2]).toBeNull();
  });

  it('rowFormat: object emits schema-keyed records', () => {
    const live = makeLive();
    live.pushMany([[0, 0.5, 'api-1']]);
    const out = live.toJSON({ rowFormat: 'object' });
    const rows = out.rows as ReadonlyArray<Record<string, unknown>>;
    expect(rows[0]).toEqual({ time: 0, cpu: 0.5, host: 'api-1' });
  });

  it('round-trips through fromJSON', () => {
    const live = makeLive();
    live.pushMany([
      [0, 0.5, 'api-1'],
      [1000, 0.6, 'api-2'],
    ]);
    const json = live.toJSON();
    const restored = LiveSeries.fromJSON(json);
    expect(restored.length).toBe(live.length);
    expect(restored.at(0)!.get('cpu')).toBe(0.5);
    expect(restored.at(1)!.get('host')).toBe('api-2');
  });
});

describe('LiveSeries.pushJson', () => {
  it('translates null cells to undefined and parses JSON keys', () => {
    const live = makeLive();
    live.pushJson([
      [0, 0.5, null],
      [1000, 0.6, 'api-2'],
    ]);
    expect(live.at(0)!.get('host')).toBeUndefined();
    expect(live.at(0)!.key()).toBeInstanceOf(Time);
    expect(live.at(0)!.begin()).toBe(0);
    expect(live.at(1)!.get('host')).toBe('api-2');
  });

  it('accepts the object-form variant', () => {
    const live = makeLive();
    live.pushJson([
      { time: 0, cpu: 0.5, host: 'api-1' },
      { time: 1000, cpu: 0.6, host: null },
    ]);
    expect(live.at(1)!.get('host')).toBeUndefined();
  });

  it('fires batch listeners exactly once for the bulk ingest', () => {
    const live = makeLive();
    let fires = 0;
    let total = 0;
    live.on('batch', (events) => {
      fires += 1;
      total = events.length;
    });
    live.pushJson([
      [0, 0.5, 'a'],
      [1000, 0.6, 'b'],
      [2000, 0.7, 'c'],
    ]);
    expect(fires).toBe(1);
    expect(total).toBe(3);
  });

  it('honors parse.timeZone when keys are local-calendar strings', () => {
    const tzSchema = [
      { name: 'time', kind: 'time' },
      { name: 'value', kind: 'number' },
    ] as const;
    const live = new LiveSeries({ name: 'tz', schema: tzSchema });
    live.pushJson([['2025-01-01T00:00', 0.5]], {
      timeZone: 'Europe/Madrid',
    });
    // Madrid is UTC+1 in January — 23:00 UTC the prior day.
    expect(live.at(0)!.begin()).toBe(
      new Date('2024-12-31T23:00:00Z').getTime(),
    );
  });

  it('no-op for empty rows array', () => {
    const live = makeLive();
    live.pushJson([]);
    expect(live.length).toBe(0);
  });
});

describe('LiveSeries.fromJSON (static factory)', () => {
  it('rebuilds a LiveSeries from a snapshot envelope', () => {
    const json = {
      name: 'src',
      schema,
      rows: [
        [0, 0.5, 'api-1'],
        [1000, 0.6, 'api-2'],
      ],
    };
    const live = LiveSeries.fromJSON(json);
    expect(live.name).toBe('src');
    expect(live.length).toBe(2);
    expect(live.at(0)!.get('cpu')).toBe(0.5);
  });

  it('forwards retention options to the constructed series', () => {
    const json = {
      name: 'src',
      schema,
      rows: [
        [0, 0.5, 'api-1'],
        [1000, 0.6, 'api-1'],
        [2000, 0.7, 'api-1'],
      ],
    };
    const live = LiveSeries.fromJSON(json, {
      retention: { maxEvents: 2 },
    });
    // Three rows ingested but maxEvents: 2 retains only the last 2.
    expect(live.length).toBe(2);
    expect(live.at(0)!.get('cpu')).toBe(0.6);
  });

  it('handles an empty rows array', () => {
    const live = LiveSeries.fromJSON({ name: 'src', schema, rows: [] });
    expect(live.length).toBe(0);
  });

  it('forwards retention.maxBytes', () => {
    // 1024 bytes is plenty for a few small events; this just pins
    // that the option reaches the constructor.
    const live = LiveSeries.fromJSON(
      { name: 'src', schema, rows: [[0, 0.5, 'api-1']] },
      { retention: { maxBytes: 1024 } },
    );
    expect(live.length).toBe(1);
  });

  it('forwards graceWindow + ordering=reorder', () => {
    const live = LiveSeries.fromJSON(
      { name: 'src', schema, rows: [[1000, 0.5, 'api-1']] },
      { ordering: 'reorder', graceWindow: '5s' },
    );
    expect(live.length).toBe(1);
    expect(live.graceWindowMs).toBe(5000);
    // A late event within grace lands at its insertion point.
    live.push([500, 0.6, 'api-1']);
    expect(live.length).toBe(2);
    expect(live.at(0)!.get('cpu')).toBe(0.6);
  });
});

describe('Event.toRow / Event.toJsonRow', () => {
  it('toRow(schema) returns a typed tuple in column order', () => {
    const live = makeLive();
    live.pushMany([[0, 0.5, 'api-1']]);
    const event = live.at(0)!;
    const row = event.toRow(live.schema);
    expect(row[0]).toBeInstanceOf(Time);
    expect(row[1]).toBe(0.5);
    expect(row[2]).toBe('api-1');
  });

  it('toJsonRow(schema) emits numeric key + null for undefined', () => {
    const live = makeLive();
    live.pushMany([[0, 0.5, undefined]]);
    const event = live.at(0)!;
    const row = event.toJsonRow(live.schema) as ReadonlyArray<unknown>;
    expect(row[0]).toBe(0);
    expect(row[1]).toBe(0.5);
    expect(row[2]).toBeNull();
  });

  it('toJsonRow with rowFormat: object emits schema-keyed record', () => {
    const live = makeLive();
    live.pushMany([[0, 0.5, 'api-1']]);
    const event = live.at(0)!;
    const row = event.toJsonRow(live.schema, {
      rowFormat: 'object',
    });
    expect(row).toEqual({ time: 0, cpu: 0.5, host: 'api-1' });
  });

  it('roundtrip via toJsonRow → pushJson preserves payload', () => {
    const live = makeLive();
    live.pushMany([[0, 0.5, 'api-1']]);
    const event = live.at(0)!;
    const wireRow = event.toJsonRow(live.schema);

    const dest = makeLive();
    dest.pushJson([wireRow]);
    expect(dest.at(0)!.get('cpu')).toBe(0.5);
    expect(dest.at(0)!.get('host')).toBe('api-1');
    expect(dest.at(0)!.begin()).toBe(0);
  });

  it("on('batch') fanout pattern: events.map(e => e.toJsonRow(schema)) round-trips", () => {
    const source = makeLive();
    const sink = makeLive();
    source.on('batch', (events) => {
      const rows = events.map((e) => e.toJsonRow(source.schema));
      sink.pushJson(rows);
    });
    source.pushMany([
      [0, 0.5, 'api-1'],
      [1000, 0.6, 'api-2'],
    ]);
    expect(sink.length).toBe(2);
    expect(sink.at(0)!.get('cpu')).toBe(0.5);
    expect(sink.at(1)!.get('host')).toBe('api-2');
  });

  it('Event.toRow with timeRange-keyed schema preserves the key extent', () => {
    const trSchema = [
      { name: 'tr', kind: 'timeRange' },
      { name: 'v', kind: 'number' },
    ] as const;
    const event = new Event(new TimeRange({ start: 0, end: 5000 }), {
      v: 0.42,
    });
    const row = event.toRow(trSchema);
    expect(row[0]).toBeInstanceOf(TimeRange);
    expect((row[0] as TimeRange).begin()).toBe(0);
    expect((row[0] as TimeRange).end()).toBe(5000);
    expect(row[1]).toBe(0.42);
  });

  it('Event.toJsonRow with interval-keyed schema emits [value, start, end]', () => {
    const intSchema = [
      { name: 'i', kind: 'interval' },
      { name: 'v', kind: 'number' },
    ] as const;
    const event = new Event(
      new Interval({ value: 'b1', start: 0, end: 1000 }),
      { v: 0.42 },
    );
    const row = event.toJsonRow(intSchema) as ReadonlyArray<unknown>;
    expect(row[0]).toEqual(['b1', 0, 1000]);
    expect(row[1]).toBe(0.42);
  });
});

describe("schema-evolution self-test (mirrors the experiment's)", () => {
  it('pushJson rejects a JsonRowForSchema<S> shape that is the wrong length', () => {
    const live = makeLive();
    expect(() =>
      live.pushJson([
        // @ts-expect-error column-count mismatch — pushJson validates shape
        [0, 0.5],
      ]),
    ).toThrow();
  });

  it('pushMany rejects a row tuple of the wrong length', () => {
    const live = makeLive();
    expect(() =>
      live.pushMany([
        // @ts-expect-error column-count mismatch
        [0, 0.5],
      ]),
    ).toThrow();
  });
});
