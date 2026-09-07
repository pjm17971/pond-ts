import { describe, expect, it } from 'vitest';
import type { Session } from '../src/index.js';
import {
  previousSessionHlcValues,
  sessionIdValues,
} from '../src/kernels/session.js';
import { anchoredVwapValues } from '../src/kernels/anchored-vwap.js';
import { pivotLevelValues } from '../src/kernels/pivot.js';

/*
 * The three loops behind the session-anchored studies (assessment 6.9, G4),
 * tested on their own because each is SHARED:
 *
 *  - `sessionIdValues` is the one walk behind BOTH session doors —
 *    `TradingCalendar.tagSessions` and the studies' `sessions` option.
 *  - `anchoredVwapValues` is the one accumulation behind BOTH anchored VWAPs
 *    (`anchoredVwap` passes a single group, `sessionVwap` a session per bar).
 *  - `pivotLevelValues` holds the four formula sets, which is where a
 *    transposed constant would hide.
 */

const arr = (...xs: number[]) => Float64Array.from(xs);
const read = (x: Float64Array) =>
  Array.from(x, (v) => (Number.isNaN(v) ? undefined : v));

const H = 3_600_000;
const D0 = Date.UTC(2021, 0, 4); // Mon
const D2 = Date.UTC(2021, 0, 6); // Wed — Tuesday is a gap

const sessions: Session[] = [
  { date: '2021-01-04', open: D0 + 9 * H, close: D0 + 12 * H },
  { date: '2021-01-06', open: D2 + 9 * H, close: D2 + 12 * H },
];
const MON = D0 + 9 * H;
const WED = D2 + 9 * H;

describe('sessionIdValues', () => {
  const keys = [
    D0 + 8 * H, // before Monday's open
    D0 + 9 * H, // Monday's open exactly
    D0 + 11 * H, // mid-session
    D0 + 12 * H, // Monday's close exactly
    Date.UTC(2021, 0, 5) + 10 * H, // Tuesday — no session at all
    D2 + 9 * H, // Wednesday's open
    D2 + 20 * H, // after Wednesday's close
  ];

  it('open-stamped: a session owns [open, close)', () => {
    expect(read(sessionIdValues(keys, sessions))).toEqual([
      undefined,
      MON,
      MON,
      undefined,
      undefined,
      WED,
      undefined,
    ]);
  });

  it('close-stamped: a session owns (open, close]', () => {
    expect(read(sessionIdValues(keys, sessions, 'close'))).toEqual([
      undefined,
      // The open instant is the PREVIOUS bar's close under this convention,
      // and nothing precedes it here, so it reads closed.
      undefined,
      MON,
      MON,
      undefined,
      undefined,
      undefined,
    ]);
  });

  it('an empty schedule leaves every bar in closed time', () => {
    expect(read(sessionIdValues(keys, []))).toEqual(keys.map(() => undefined));
  });

  it('a schedule that does not reach the bars leaves them all closed', () => {
    const later: Session[] = [
      {
        date: '2021-02-01',
        open: Date.UTC(2021, 1, 1),
        close: Date.UTC(2021, 1, 1) + H,
      },
    ];
    expect(read(sessionIdValues(keys, later))).toEqual(
      keys.map(() => undefined),
    );
    const earlier: Session[] = [
      {
        date: '2020-12-01',
        open: Date.UTC(2020, 11, 1),
        close: Date.UTC(2020, 11, 1) + H,
      },
    ];
    expect(read(sessionIdValues(keys, earlier))).toEqual(
      keys.map(() => undefined),
    );
  });

  it('the cursor only moves forward — O(N + sessions), not O(N · sessions)', () => {
    // 5000 back-to-back sessions and 5000 bars, one per session. A per-bar
    // scan would be 25M comparisons; the merge walk is 10k. The assertion is
    // the ANSWER (every bar in its own session), and the size is what makes a
    // quadratic implementation visible as a timeout rather than as a wrong
    // number.
    const many: Session[] = Array.from({ length: 5000 }, (_, i) => ({
      date: new Date(Date.UTC(2000, 0, 1) + i * 86_400_000)
        .toISOString()
        .slice(0, 10),
      open: i * 1000,
      close: i * 1000 + 500,
    }));
    const bars = Array.from({ length: 5000 }, (_, i) => i * 1000 + 100);
    const ids = sessionIdValues(bars, many);
    expect(Array.from(ids)).toEqual(many.map((s) => s.open));
  });
});

describe('previousSessionHlcValues', () => {
  //     ids:   A    A    A   gap   B    B    C
  const ids = arr(1, 1, 1, NaN, 2, 2, 3);
  const high = arr(12, 14, 16, 99, 10, 13, 11);
  const low = arr(10, 10, 14, 98, 8, 9, 7);
  const close = arr(11, 12, 15, 98.5, 9, 10, 8);

  it('holds the previous session aggregate across the current one', () => {
    const p = previousSessionHlcValues(ids, high, low, close);
    // Session A: high 16, low 10, last close 15. Session B: high 13, low 8,
    // last close 10. The closed-time bar contributes to neither.
    expect(read(p.high)).toEqual([
      undefined,
      undefined,
      undefined,
      undefined,
      16,
      16,
      13,
    ]);
    expect(read(p.low)).toEqual([
      undefined,
      undefined,
      undefined,
      undefined,
      10,
      10,
      8,
    ]);
    expect(read(p.close)).toEqual([
      undefined,
      undefined,
      undefined,
      undefined,
      15,
      15,
      10,
    ]);
  });

  it('aggregates over the cells that are PRESENT, per column', () => {
    // The session's own high goes missing on its extreme bar: the aggregate
    // falls back to the next-highest present bar rather than blanking the
    // whole session — and the low, which is present on that bar, still counts.
    const p = previousSessionHlcValues(
      ids,
      arr(12, 14, NaN, 99, 10, 13, 11),
      low,
      close,
    );
    expect(read(p.high)[4]).toBe(14);
    expect(read(p.low)[4]).toBe(10);
  });

  it('a column with no present cell in a session blanks that column only', () => {
    const p = previousSessionHlcValues(
      ids,
      arr(NaN, NaN, NaN, 99, 10, 13, 11),
      low,
      close,
    );
    expect(read(p.high)[4]).toBeUndefined();
    expect(read(p.low)[4]).toBe(10);
    expect(read(p.close)[4]).toBe(15);
  });

  it('a closed-time bar INSIDE a run of one id does not split the session', () => {
    // Only reachable through the session-COLUMN door (a real schedule puts
    // closed time between sessions, never inside one). The rule is that a
    // session ends when the id CHANGES, not when a bar happens to carry
    // none — otherwise the second half of the run would read the first
    // half's aggregate as its "previous session".
    const split = arr(1, 1, NaN, 1, 2);
    const p = previousSessionHlcValues(
      split,
      arr(12, 14, 99, 16, 10),
      arr(10, 10, 98, 14, 8),
      arr(11, 12, 98.5, 15, 9),
    );
    expect(read(p.high)[3]).toBeUndefined();
    // Session 1 is all three of its bars: high 16, low 10, last close 15.
    expect(read(p.high)[4]).toBe(16);
    expect(read(p.low)[4]).toBe(10);
    expect(read(p.close)[4]).toBe(15);
  });

  it('the last PRESENT close is the session close, not the last row', () => {
    const p = previousSessionHlcValues(
      ids,
      high,
      low,
      arr(11, 12, NaN, 98.5, 9, 10, 8),
    );
    expect(read(p.close)[4]).toBe(12);
  });
});

describe('anchoredVwapValues', () => {
  const typical = arr(11, 12, 15, 9, 10, 20);
  const volume = arr(100, 200, 300, 400, 100, 100);

  it('one group is a running weighted mean', () => {
    const v = read(anchoredVwapValues(typical, volume, arr(0, 0, 0, 0, 0, 0)));
    expect(v[0]).toBeCloseTo(11, 12);
    expect(v[1]).toBeCloseTo((11 * 100 + 12 * 200) / 300, 12);
    expect(v[5]).toBeCloseTo(
      (11 * 100 + 12 * 200 + 15 * 300 + 9 * 400 + 10 * 100 + 20 * 100) / 1200,
      12,
    );
  });

  it('a change of id restarts BOTH sums', () => {
    const v = read(anchoredVwapValues(typical, volume, arr(0, 0, 0, 1, 1, 1)));
    expect(v[3]).toBeCloseTo(9, 12);
    expect(v[4]).toBeCloseTo((9 * 400 + 10 * 100) / 500, 12);
  });

  it('a NaN id contributes nothing and reads missing', () => {
    const v = read(
      anchoredVwapValues(typical, volume, arr(0, NaN, 0, 1, 1, 1)),
    );
    expect(v[1]).toBeUndefined();
    // Bar 1 is skipped entirely — bar 2 continues group 0 from bar 0 alone.
    expect(v[2]).toBeCloseTo((11 * 100 + 15 * 300) / 400, 12);
  });

  it('a leading gap shifts the start; an interior one ends the group', () => {
    const holed = arr(NaN, 12, NaN, 9, 10, 20);
    const v = read(anchoredVwapValues(holed, volume, arr(0, 0, 0, 1, 1, 1)));
    expect(v[0]).toBeUndefined();
    expect(v[1]).toBeCloseTo(12, 12);
    expect(v[2]).toBeUndefined();
    // The next GROUP re-seeds — the reset is the recovery.
    expect(v[3]).toBeCloseTo(9, 12);
  });

  it('a missing volume blanks the numerator too (no zero-bias)', () => {
    const v = read(
      anchoredVwapValues(
        typical,
        arr(100, NaN, 300, 400, 100, 100),
        arr(0, 0, 0, 0, 0, 0),
      ),
    );
    expect(v[0]).toBeCloseTo(11, 12);
    expect(v[1]).toBeUndefined();
    expect(v[2]).toBeUndefined();
  });

  it('zero accumulated volume reads missing, not zero', () => {
    const v = read(
      anchoredVwapValues(arr(11, 12, 15), arr(0, 0, 100), arr(0, 0, 0)),
    );
    expect(v[0]).toBeUndefined();
    expect(v[1]).toBeUndefined();
    expect(v[2]).toBeCloseTo(15, 12);
  });
});

describe('pivotLevelValues', () => {
  // H 110, L 90, C 105 — range 20, standard pivot (110+90+105)/3 = 101.6667.
  const h = arr(110);
  const l = arr(90);
  const c = arr(105);
  const at = (m: Parameters<typeof pivotLevelValues>[0]) =>
    pivotLevelValues(m, h, l, c);

  it('standard — the floor-trader set', () => {
    const v = at('standard');
    const p = (110 + 90 + 105) / 3;
    expect(v.pivot[0]).toBeCloseTo(p, 12);
    expect(v.r1[0]).toBeCloseTo(2 * p - 90, 12);
    expect(v.s1[0]).toBeCloseTo(2 * p - 110, 12);
    expect(v.r2[0]).toBeCloseTo(p + 20, 12);
    expect(v.s2[0]).toBeCloseTo(p - 20, 12);
    expect(v.r3[0]).toBeCloseTo(110 + 2 * (p - 90), 12);
    expect(v.s3[0]).toBeCloseTo(90 - 2 * (110 - p), 12);
    // The other spelling in circulation is algebraically the same numbers.
    expect(v.r3[0]).toBeCloseTo(v.r1[0]! + 20, 12);
    expect(v.s3[0]).toBeCloseTo(v.s1[0]! - 20, 12);
    expect(v.r4).toBeUndefined();
    expect(v.s4).toBeUndefined();
  });

  it('fibonacci — 38.2 / 61.8 / 100 % of the range about the same pivot', () => {
    const v = at('fibonacci');
    const p = (110 + 90 + 105) / 3;
    expect(v.pivot[0]).toBeCloseTo(p, 12);
    expect(v.r1[0]).toBeCloseTo(p + 0.382 * 20, 12);
    expect(v.r2[0]).toBeCloseTo(p + 0.618 * 20, 12);
    expect(v.r3[0]).toBeCloseTo(p + 20, 12);
    expect(v.s1[0]).toBeCloseTo(p - 0.382 * 20, 12);
    expect(v.s2[0]).toBeCloseTo(p - 0.618 * 20, 12);
    expect(v.s3[0]).toBeCloseTo(p - 20, 12);
  });

  it('woodie — the standard ladder over a close-weighted centre', () => {
    const v = at('woodie');
    const p = (110 + 90 + 2 * 105) / 4;
    expect(p).toBeCloseTo(102.5, 12);
    expect(v.pivot[0]).toBeCloseTo(p, 12);
    expect(v.r1[0]).toBeCloseTo(2 * p - 90, 12);
    expect(v.s1[0]).toBeCloseTo(2 * p - 110, 12);
    // The centre is the only difference from 'standard'.
    expect(v.pivot[0]).not.toBeCloseTo(at('standard').pivot[0]!, 6);
  });

  it('camarilla — four pairs measured from the CLOSE, not from the pivot', () => {
    const v = at('camarilla');
    expect(v.pivot[0]).toBeCloseTo((110 + 90 + 105) / 3, 12);
    expect(v.r1[0]).toBeCloseTo(105 + (20 * 1.1) / 12, 12);
    expect(v.r2[0]).toBeCloseTo(105 + (20 * 1.1) / 6, 12);
    expect(v.r3[0]).toBeCloseTo(105 + (20 * 1.1) / 4, 12);
    expect(v.r4![0]).toBeCloseTo(105 + (20 * 1.1) / 2, 12);
    expect(v.s1[0]).toBeCloseTo(105 - (20 * 1.1) / 12, 12);
    expect(v.s4![0]).toBeCloseTo(105 - (20 * 1.1) / 2, 12);
    // Symmetric about the close, NOT about the pivot — the delta the
    // docstring names.
    expect((v.r1[0]! + v.s1[0]!) / 2).toBeCloseTo(105, 12);
    expect((v.r1[0]! + v.s1[0]!) / 2).not.toBeCloseTo(v.pivot[0]!, 6);
  });

  it('a missing input blanks the whole ladder for that row', () => {
    const v = pivotLevelValues(
      'standard',
      arr(110, NaN),
      arr(90, 90),
      arr(105, 105),
    );
    for (const name of ['pivot', 'r1', 'r2', 'r3', 's1', 's2', 's3'] as const) {
      expect(Number.isNaN(v[name]![1]!), name).toBe(true);
      expect(Number.isNaN(v[name]![0]!), name).toBe(false);
    }
  });
});
