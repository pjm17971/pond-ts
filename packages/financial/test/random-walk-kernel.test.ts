import { describe, expect, it } from 'vitest';
import { randomWalkValues } from '../src/kernels/random-walk.js';

/*
 * Tested directly because the horizon sweep is the part `randomWalkIndex`
 * cannot show: the study reports only the maximum, so a build that computed
 * the wrong TERM at some horizon can still agree on bars where a different
 * horizon happens to win. These fixtures make one named horizon the winner.
 */

const arr = (...xs: number[]) => Float64Array.from(xs);
const read = (x: Float64Array) =>
  Array.from(x, (v) => (Number.isNaN(v) ? undefined : v));

/** Four bars: a steady 2-wide range, then one that jumps. */
const high = arr(12, 13, 14, 20);
const low = arr(10, 11, 12, 13);
const close = arr(11, 12, 13, 19);

describe('randomWalkValues', () => {
  it('is (high − low[−n]) / (meanTR(n)·√n) at a single horizon', () => {
    //   TR = [_, 2, 2], meanTR(2)[2] = 2 → denominator 2√2
    //   rwiHigh[2] = (14 − 10)/(2√2) = √2 ; rwiLow[2] = (12 − 12)/(2√2) = 0
    const { rwiHigh, rwiLow } = randomWalkValues(
      high.slice(0, 3),
      low.slice(0, 3),
      close.slice(0, 3),
      2,
    );
    expect(read(rwiHigh)[2]).toBeCloseTo(Math.SQRT2, 12);
    expect(read(rwiLow)[2]).toBeCloseTo(0, 12);
  });

  it('takes the maximum over 2 … period, not the longest horizon alone', () => {
    //   TR = [_, 2, 2, 7]
    //   n = 2: (20 − 11)/(4.5·√2)      = √2       = 1.41421…
    //   n = 3: (20 − 10)/((11/3)·√3)   = 30/(11√3) = 1.57459…  ← the winner
    const { rwiHigh } = randomWalkValues(high, low, close, 3);
    expect(read(rwiHigh)[3]).toBeCloseTo(30 / (11 * Math.sqrt(3)), 12);
  });

  it('warms up at exactly `period` — the STRICT rule over every horizon', () => {
    const { rwiHigh, rwiLow } = randomWalkValues(high, low, close, 3);
    expect(
      read(rwiHigh)
        .slice(0, 3)
        .every((x) => x === undefined),
    ).toBe(true);
    expect(
      read(rwiLow)
        .slice(0, 3)
        .every((x) => x === undefined),
    ).toBe(true);
    expect(read(rwiHigh)[3]).toBeDefined();
  });

  it('a period longer than the series leaves every row undefined', () => {
    const { rwiHigh, rwiLow } = randomWalkValues(high, low, close, 9);
    expect(read(rwiHigh).every((x) => x === undefined)).toBe(true);
    expect(read(rwiLow).every((x) => x === undefined)).toBe(true);
    expect(rwiHigh).toHaveLength(4);
  });

  it('a zero mean true range reads undefined rather than an infinity', () => {
    const flat = arr(10, 10, 10, 10);
    const { rwiHigh } = randomWalkValues(flat, flat, flat, 3);
    expect(read(rwiHigh).every((x) => x === undefined)).toBe(true);
  });

  it('one missing horizon blanks the whole bar', () => {
    // A NaN high on bar 1 blanks TR[1] and TR[2], so every window holding
    // either is missing and the max is missing with it.
    const holed = arr(12, NaN, 14, 20);
    const { rwiHigh } = randomWalkValues(holed, low, close, 2);
    expect(read(rwiHigh)[2]).toBeUndefined();
    expect(read(rwiHigh)[3]).toBeDefined();
  });

  it('goes negative when every horizon fell', () => {
    const fallingHigh = arr(100, 97, 94, 91);
    const fallingLow = arr(98, 95, 92, 89);
    const fallingClose = arr(99, 96, 93, 90);
    const { rwiHigh } = randomWalkValues(
      fallingHigh,
      fallingLow,
      fallingClose,
      3,
    );
    expect(read(rwiHigh)[3]!).toBeLessThan(0);
  });
});
