import { describe, expect, it } from 'vitest';
import { TimeSeries } from '../src/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number' },
  { name: 'other', kind: 'number' },
  { name: 'label', kind: 'string' },
] as const;

function makeSeries() {
  return new TimeSeries({
    name: 'pct',
    schema,
    rows: [
      [0, 100, 10, 'a'],
      [1000, 110, 20, 'b'],
      [2000, 132, 15, 'c'],
      [3000, 99, 30, 'd'],
    ],
  });
}

describe('TimeSeries.pctChange', () => {
  it('computes (curr - prev) / prev for a single column', () => {
    const p = makeSeries().pctChange('value');
    expect(p.at(0)?.get('value')).toBeUndefined();
    expect(p.at(1)?.get('value')).toBeCloseTo(0.1, 10);
    expect(p.at(2)?.get('value')).toBeCloseTo(0.2, 10);
    expect(p.at(3)?.get('value')).toBeCloseTo(-0.25, 10);
  });

  it('preserves non-target columns', () => {
    const p = makeSeries().pctChange('value');
    expect(p.at(1)?.get('other')).toBe(20);
    expect(p.at(1)?.get('label')).toBe('b');
  });

  it('supports multiple columns', () => {
    const p = makeSeries().pctChange(['value', 'other']);
    expect(p.at(1)?.get('value')).toBeCloseTo(0.1, 10);
    expect(p.at(1)?.get('other')).toBeCloseTo(1.0, 10);
  });

  it('drops first event with { drop: true }', () => {
    const p = makeSeries().pctChange('value', { drop: true });
    expect(p.length).toBe(3);
    expect(p.at(0)?.get('value')).toBeCloseTo(0.1, 10);
  });

  it('returns undefined when prev is zero', () => {
    const s = new TimeSeries({
      name: 'zero-prev',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'value', kind: 'number' },
      ] as const,
      rows: [
        [0, 0],
        [1000, 10],
        [2000, 20],
      ],
    });
    const p = s.pctChange('value');
    expect(p.at(1)?.get('value')).toBeUndefined();
    expect(p.at(2)?.get('value')).toBeCloseTo(1.0, 10);
  });

  it('handles empty series', () => {
    const s = new TimeSeries({
      name: 'empty',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'value', kind: 'number' },
      ] as const,
      rows: [],
    });
    expect(s.pctChange('value').length).toBe(0);
  });

  it('handles single-event series', () => {
    const s = new TimeSeries({
      name: 'single',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'value', kind: 'number' },
      ] as const,
      rows: [[0, 42]],
    });
    const p = s.pctChange('value');
    expect(p.length).toBe(1);
    expect(p.at(0)?.get('value')).toBeUndefined();
  });

  it('composes with groupBy', () => {
    const s = new TimeSeries({
      name: 'grouped',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'value', kind: 'number' },
        { name: 'host', kind: 'string' },
      ] as const,
      rows: [
        [0, 100, 'a'],
        [1000, 200, 'a'],
        [2000, 50, 'b'],
        [3000, 75, 'b'],
      ],
    });
    const groups = s.groupBy('host', (g) => g.pctChange('value'));
    expect(groups.get('a')!.at(1)?.get('value')).toBeCloseTo(1.0, 10);
    expect(groups.get('b')!.at(1)?.get('value')).toBeCloseTo(0.5, 10);
  });
});
