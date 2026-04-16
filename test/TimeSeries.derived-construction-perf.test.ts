import { describe, expect, it } from 'vitest';
import { TimeSeries } from '../src/index.js';

describe('TimeSeries derived construction performance regression coverage', () => {
  it('preserves behavior across event-based derived transforms', () => {
    const schema = [
      { name: 'time', kind: 'time' },
      { name: 'cpu', kind: 'number' },
      { name: 'requests', kind: 'number' },
      { name: 'host', kind: 'string' },
      { name: 'active', kind: 'boolean' },
    ] as const;

    const series = new TimeSeries({
      name: 'cpu',
      schema,
      rows: [
        [0, 1, 10, 'a', true],
        [10, 2, 20, 'b', false],
        [20, 3, 30, 'c', true],
        [30, 4, 40, 'd', true],
      ],
    });

    const selected = series
      .filter((event) => event.get('active'))
      .select('cpu', 'requests', 'host');
    const renamed = selected.rename({ host: 'server' });
    const collapsed = renamed.collapse(
      ['cpu', 'requests'],
      'score',
      ({ cpu, requests }) => (cpu ?? 0) + (requests ?? 0),
      { append: true },
    );
    const mapped = collapsed.map(
      [
        { name: 'time', kind: 'time' },
        { name: 'score', kind: 'number' },
        { name: 'server', kind: 'string' },
      ] as const,
      (event) => event.select('score', 'server'),
    );

    expect(mapped.length).toBe(3);
    expect(mapped.at(0)?.get('score')).toBe(11);
    expect(mapped.at(1)?.get('score')).toBe(33);
    expect(mapped.at(2)?.get('score')).toBe(44);
    expect(mapped.at(0)?.get('server')).toBe('a');
    expect(mapped.at(2)?.get('server')).toBe('d');
    expect(mapped.rows[1]?.[0]?.begin?.()).toBe(20);
    expect(mapped.rows[1]?.slice(1)).toEqual([33, 'c']);
  });
});
