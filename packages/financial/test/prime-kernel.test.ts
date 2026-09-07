import { describe, expect, it } from 'vitest';
import {
  isPrime,
  nearestPrime,
  nearestPrimeDistanceValues,
  primeAtOrAbove,
  primeAtOrBelow,
  primeBandValues,
} from '../src/kernels/prime.js';

const arr = (...xs: number[]) => Float64Array.from(xs);
const read = (x: Float64Array) =>
  Array.from(x, (v) => (Number.isNaN(v) ? undefined : v));

describe('isPrime', () => {
  it('agrees with the primes below 50, one by one', () => {
    // The whole list, not a spot check: an off-by-one in the 6k ± 1 ladder
    // shows up on exactly one of these.
    const primes = new Set([
      2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47,
    ]);
    for (let n = 0; n < 50; n += 1) {
      expect(isPrime(n), `${n}`).toBe(primes.has(n));
    }
  });

  it('catches the squares of primes, which a √n bound gets wrong by one', () => {
    // 25, 49, 121, 169: the classic `f * f < n` vs `f * f <= n` bug — each
    // of these is composite only because the loop reaches its own root.
    for (const n of [25, 49, 121, 169, 289, 361]) {
      expect(isPrime(n), `${n}`).toBe(false);
    }
  });

  it('is false for anything that is not a non-negative integer in range', () => {
    for (const n of [
      -7,
      0,
      1,
      2.5,
      NaN,
      Infinity,
      -Infinity,
      Number.MAX_SAFE_INTEGER + 2,
    ]) {
      expect(isPrime(n), `${n}`).toBe(false);
    }
  });

  it('holds on a larger pair either side of a hard case', () => {
    // 1e6 + 3 is prime, 1e6 + 1 is 101 × 9901 — a composite whose smallest
    // factor is far enough up the ladder to exercise it.
    expect(isPrime(1_000_003)).toBe(true);
    expect(isPrime(1_000_001)).toBe(false);
    expect(isPrime(9_999_991)).toBe(true);
  });
});

describe('primeAtOrAbove / primeAtOrBelow', () => {
  it('are inclusive — a value that IS prime is its own answer', () => {
    expect(primeAtOrAbove(13)).toBe(13);
    expect(primeAtOrBelow(13)).toBe(13);
  });

  it('bracket a non-integer value from the two sides', () => {
    // 100.37: up from ceil(100.37) = 101 (prime), down from floor = 100 → 97.
    expect(primeAtOrAbove(100.37)).toBe(101);
    expect(primeAtOrBelow(100.37)).toBe(97);
  });

  it('cross the fixture’s widest local gap, 113 → 127', () => {
    expect(primeAtOrAbove(114)).toBe(127);
    expect(primeAtOrBelow(126)).toBe(113);
  });

  it('are NaN below 2 — the domain rule, shared by both studies', () => {
    // Two is the smallest prime; below it there is nothing to bracket with,
    // and calling the prime "above" −1000 two would be a distance measured
    // across an empty half-line.
    for (const x of [1.999, 1, 0, -1, -1000]) {
      expect(primeAtOrAbove(x), `above ${x}`).toBeNaN();
      expect(primeAtOrBelow(x), `below ${x}`).toBeNaN();
    }
    // …and 2 itself is in the domain.
    expect(primeAtOrAbove(2)).toBe(2);
    expect(primeAtOrBelow(2)).toBe(2);
  });

  it('are NaN for a gap and past the safe-integer range', () => {
    for (const x of [NaN, Infinity, Number.MAX_SAFE_INTEGER + 2]) {
      expect(primeAtOrAbove(x), `above ${x}`).toBeNaN();
      expect(primeAtOrBelow(x), `below ${x}`).toBeNaN();
    }
  });
});

describe('nearestPrime', () => {
  it('picks the nearer side', () => {
    expect(nearestPrime(100.1)).toBe(101); // 0.9 vs 3.1
    expect(nearestPrime(98.4)).toBe(97); // 1.4 vs 2.6
  });

  it('breaks a TIE to the lower prime — the pinned convention', () => {
    // 6 is one from both 5 and 7; 9 is two from both 7 and 11; 15 is two
    // from both 13 and 17. All three take the lower.
    expect(nearestPrime(6)).toBe(5);
    expect(nearestPrime(9)).toBe(7);
    expect(nearestPrime(15)).toBe(13);
  });

  it('is the value itself when the value is an integer prime', () => {
    expect(nearestPrime(97)).toBe(97);
  });

  it('is NaN below 2 and for a gap', () => {
    for (const x of [1.5, 0, -3, NaN, Infinity]) {
      expect(nearestPrime(x), `${x}`).toBeNaN();
    }
  });
});

describe('primeBandValues', () => {
  it('brackets each bar from its own two prices, in one pass', () => {
    const { upper, lower } = primeBandValues(
      arr(100.2, 114, 13),
      arr(99.5, 112, 13),
    );
    expect(read(upper)).toEqual([101, 127, 13]);
    expect(read(lower)).toEqual([97, 109, 13]);
  });

  it('is length-preserving and misses only the bars whose input is missing', () => {
    const { upper, lower } = primeBandValues(
      arr(NaN, 100.2, 100.2),
      arr(99.5, NaN, 99.5),
    );
    expect(upper).toHaveLength(3);
    expect(read(upper)).toEqual([undefined, 101, 101]);
    expect(read(lower)).toEqual([97, undefined, 97]);
  });
});

describe('nearestPrimeDistanceValues', () => {
  it('is value − nearestPrime(value), signed', () => {
    // 100.1 → 101 → −0.9;  98.4 → 97 → +1.4;  97 → 97 → 0.
    const v = read(nearestPrimeDistanceValues(arr(100.1, 98.4, 97)));
    expect(v[0]!).toBeCloseTo(-0.9, 12);
    expect(v[1]!).toBeCloseTo(1.4, 12);
    expect(v[2]).toBe(0);
  });

  it('is missing where the value is missing or outside the domain', () => {
    expect(read(nearestPrimeDistanceValues(arr(NaN, 1.5, 100.1)))).toEqual([
      undefined,
      undefined,
      100.1 - 101,
    ]);
  });
});
