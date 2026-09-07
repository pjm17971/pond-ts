import { describe, expect, it } from 'vitest';
import { rollingBivariateValues } from '../src/kernels/bivariate.js';

/*
 * The K8 rolling bivariate moments (`covariance`, `varianceX`, `varianceY`)
 * every two-series study divides. What these pin is what the studies inherit
 * and therefore cannot test for themselves: the STRICT pair window, the
 * exactly-zero covariance of a flat column (which is why no study writes a
 * zero-denominator guard), and the conditioning at price magnitudes where the
 * textbook `Σxy − ΣxΣy/n` has already lost the answer.
 */

const arr = (...xs: number[]) => Float64Array.from(xs);
const read = (x: Float64Array) =>
  Array.from(x, (v) => (Number.isNaN(v) ? undefined : v));

/** Exact per-window two-pass moments, shifted by the window's own first
 *  value first — the reference the kernel is measured against. At 1e12 the
 *  shift is what makes the reference itself trustworthy: computing the mean
 *  as `Σx / n` on raw 1e12 values is the ill-conditioned party, which is how
 *  a first version of this probe managed to accuse the kernel. */
function reference(
  x: Float64Array,
  y: Float64Array,
  period: number,
): { cov: Float64Array; vx: Float64Array; vy: Float64Array } {
  const cov = new Float64Array(x.length).fill(NaN);
  const vx = new Float64Array(x.length).fill(NaN);
  const vy = new Float64Array(x.length).fill(NaN);
  for (let i = period - 1; i < x.length; i += 1) {
    const lo = i - period + 1;
    const ax = x[lo]!;
    const ay = y[lo]!;
    let sx = 0;
    let sy = 0;
    for (let k = lo; k <= i; k += 1) {
      sx += x[k]! - ax;
      sy += y[k]! - ay;
    }
    const mx = sx / period;
    const my = sy / period;
    let c = 0;
    let cx = 0;
    let cy = 0;
    for (let k = lo; k <= i; k += 1) {
      const dx = x[k]! - ax - mx;
      const dy = y[k]! - ay - my;
      c += dx * dy;
      cx += dx * dx;
      cy += dy * dy;
    }
    cov[i] = c / period;
    vx[i] = cx / period;
    vy[i] = cy / period;
  }
  return { cov, vx, vy };
}

/** The arrangement the kernel deliberately does NOT use, kept here as the
 *  control that shows what it buys. */
function naive(
  x: Float64Array,
  y: Float64Array,
  period: number,
): { cov: Float64Array; vx: Float64Array; vy: Float64Array } {
  const cov = new Float64Array(x.length).fill(NaN);
  const vx = new Float64Array(x.length).fill(NaN);
  const vy = new Float64Array(x.length).fill(NaN);
  let sx = 0;
  let sy = 0;
  let sxx = 0;
  let syy = 0;
  let sxy = 0;
  for (let i = 0; i < x.length; i += 1) {
    sx += x[i]!;
    sy += y[i]!;
    sxx += x[i]! * x[i]!;
    syy += y[i]! * y[i]!;
    sxy += x[i]! * y[i]!;
    if (i >= period) {
      const j = i - period;
      sx -= x[j]!;
      sy -= y[j]!;
      sxx -= x[j]! * x[j]!;
      syy -= y[j]! * y[j]!;
      sxy -= x[j]! * y[j]!;
    }
    if (i >= period - 1) {
      cov[i] = sxy / period - (sx / period) * (sy / period);
      vx[i] = sxx / period - (sx / period) * (sx / period);
      vy[i] = syy / period - (sy / period) * (sy / period);
    }
  }
  return { cov, vx, vy };
}

function correlationOf(m: {
  cov: Float64Array;
  vx: Float64Array;
  vy: Float64Array;
}): Float64Array {
  const out = new Float64Array(m.cov.length);
  for (let i = 0; i < out.length; i += 1) {
    out[i] = m.cov[i]! / Math.sqrt(m.vx[i]! * m.vy[i]!);
  }
  return out;
}

function worstAbs(got: Float64Array, want: Float64Array): number {
  let worst = 0;
  for (let i = 0; i < want.length; i += 1) {
    if (!Number.isFinite(want[i]!)) continue;
    const d = Math.abs(got[i]! - want[i]!);
    if (d > worst) worst = d;
  }
  return worst;
}

/** `x` and `y` at a chosen magnitude, with ±3-scale structure on top and
 *  different periods on each side so they are neither identical nor
 *  independent. */
function structured(
  base: number,
  length: number,
): { x: Float64Array; y: Float64Array } {
  const x = new Float64Array(length);
  const y = new Float64Array(length);
  for (let i = 0; i < length; i += 1) {
    x[i] = base + ((i % 7) - 3) + Math.sin(i / 11);
    y[i] = base + ((i % 5) - 2) * 0.7 + Math.cos(i / 13);
  }
  return { x, y };
}

describe('rollingBivariateValues', () => {
  it('is the population covariance and both variances, warm-up preserved', () => {
    // x = 1..5, y = 2,4,5,4,2. Window 3 ending at bar 2: x = 1,2,3 (mean 2),
    // y = 2,4,5 (mean 11/3). cov = ((-1)(2−11/3) + 0 + (1)(5−11/3))/3
    //   = (5/3 + 4/3)/3 = 1. varX = (1+0+1)/3 = 2/3.
    // varY = ((−5/3)² + (1/3)² + (4/3)²)/3 = (25/9+1/9+16/9)/3 = 42/27.
    const m = rollingBivariateValues(arr(1, 2, 3, 4, 5), arr(2, 4, 5, 4, 2), 3);
    expect(read(m.covariance).slice(0, 3)).toEqual([undefined, undefined, 1]);
    expect(m.varianceX[2]).toBeCloseTo(2 / 3, 12);
    expect(m.varianceY[2]).toBeCloseTo(42 / 27, 12);
    expect(m.covariance).toHaveLength(5);
  });

  it('divides by `period`, not `period − 1` (ddof = 0)', () => {
    // Two points 1 apart: population variance 0.25, sample variance 0.5.
    const m = rollingBivariateValues(arr(0, 1), arr(0, 1), 2);
    expect(m.varianceX[1]).toBeCloseTo(0.25, 15);
    expect(m.covariance[1]).toBeCloseTo(0.25, 15);
  });

  it('the window is STRICT: one missing cell in EITHER column blanks it', () => {
    const x = arr(1, 2, NaN, 4, 5, 6, 7);
    const y = arr(1, 3, 2, 5, 4, 7, 6);
    const m = rollingBivariateValues(x, y, 3);
    // Bars 0,1 are warm-up; 2,3,4 all contain the missing x; 5 and 6 recover.
    expect(read(m.covariance).map((v) => v !== undefined)).toEqual([
      false,
      false,
      false,
      false,
      false,
      true,
      true,
    ]);
    // …and the same hole from the OTHER column blanks the same three rows,
    // which is what "a pair, not a value" means.
    const m2 = rollingBivariateValues(y, x, 3);
    expect(read(m2.covariance).map((v) => v !== undefined)).toEqual(
      read(m.covariance).map((v) => v !== undefined),
    );
  });

  it('a window over `period` rows with fewer finite pairs is NOT averaged', () => {
    // Core's count-window reducers would emit here over the two present
    // cells. A moment must not: three-of-five pairs is a different statistic
    // from the one `period` named.
    const m = rollingBivariateValues(
      arr(1, NaN, 3, 4, 5),
      arr(2, 2, 6, 8, 10),
      3,
    );
    expect(read(m.covariance)).toEqual([
      undefined,
      undefined,
      undefined,
      undefined,
      12 / 9 + 0, // bars 2..4 are the first clean window: cov(3,4,5; 6,8,10)
    ]);
  });

  it('a FLAT column gives an exactly zero covariance and variance — the reason no study writes a guard', () => {
    const length = 300;
    const x = new Float64Array(length);
    const y = new Float64Array(length);
    for (let i = 0; i < length; i += 1) {
      x[i] = 100 + 5 * Math.sin(i / 3) + i * 0.01;
      y[i] = 42;
    }
    const m = rollingBivariateValues(x, y, 20);
    for (let i = 19; i < length; i += 1) {
      // Exactly zero — not "close to". A residue here would turn every
      // consumer's `0/0` into a `±Infinity`, which `withColumn` throws on.
      expect(m.covariance[i]).toBe(0);
      expect(m.varianceY[i]).toBe(0);
      expect(
        m.covariance[i]! / Math.sqrt(m.varianceX[i]! * m.varianceY[i]!),
      ).toBeNaN();
      expect(m.covariance[i]! / m.varianceY[i]!).toBeNaN();
    }
  });

  it('a column that goes flat MID-WINDOW reads exact zeros too — the change counter, not the sums', () => {
    // Layer-2 review of #706: `close = 100 + ⌊i/97⌋` against a moving
    // benchmark at period 30 threw `±Infinity` into `withColumn` on 416 of
    // 4 971 rows. Between aligned rebuilds the reverse-Welford removal left a
    // ~2e-16 residue in `cxy` beside an `m2x` clamped to exact 0. The
    // globally-flat test above only exercises the rebuild path; this one
    // exercises every phase of the rebuild cycle.
    const length = 5000;
    const period = 30;
    const x = new Float64Array(length);
    const y = new Float64Array(length);
    for (let i = 0; i < length; i += 1) {
      x[i] = 100 + Math.floor(i / 97);
      y[i] = 50 + 3 * Math.sin(i / 5) + i * 0.001;
    }
    const m = rollingBivariateValues(x, y, period);
    let flatWindows = 0;
    for (let i = period - 1; i < length; i += 1) {
      const flat = Math.floor((i - period + 1) / 97) === Math.floor(i / 97);
      if (flat) {
        flatWindows += 1;
        expect(m.varianceX[i], `varianceX[${i}]`).toBe(0);
        expect(m.covariance[i], `covariance[${i}]`).toBe(0);
        expect(
          m.covariance[i]! / Math.sqrt(m.varianceX[i]! * m.varianceY[i]!),
        ).toBeNaN();
      } else {
        expect(m.varianceX[i], `varianceX[${i}]`).toBeGreaterThan(0);
        expect(
          Number.isFinite(
            m.covariance[i]! / Math.sqrt(m.varianceX[i]! * m.varianceY[i]!),
          ),
          `corr[${i}] finite`,
        ).toBe(true);
      }
      expect(m.varianceY[i]).toBeGreaterThan(0);
    }
    // The invariance is not vacuous: most windows sit inside a flat run.
    expect(flatWindows).toBeGreaterThan(3000);
  });

  it('holds its accuracy at 1e6 and 1e12 where the textbook form does not', () => {
    // The claim on the kernel's docstring, pinned. The bound is deliberately
    // loose (1e-12 against a measured 2.4e-15) so it tracks a REGRESSION in
    // the arrangement rather than the last two bits of a platform's `sqrt`.
    for (const base of [1e6, 1e12]) {
      const { x, y } = structured(base, 4_000);
      const ref = correlationOf(reference(x, y, 30));
      const k = rollingBivariateValues(x, y, 30);
      const mine = correlationOf({
        cov: k.covariance,
        vx: k.varianceX,
        vy: k.varianceY,
      });
      expect(worstAbs(mine, ref), `magnitude ${base}`).toBeLessThan(1e-12);
    }
    // …and the control: the naive sliding `Σxy − ΣxΣy/n` is not merely less
    // accurate at 1e12, it returns a NEGATIVE variance, so its correlation is
    // non-finite. This is the assertion that makes the arrangement a
    // requirement rather than a preference.
    const { x, y } = structured(1e12, 4_000);
    const control = correlationOf(naive(x, y, 30));
    expect(Array.from(control.slice(29)).some((v) => !Number.isFinite(v))).toBe(
      true,
    );
  });

  it('a ranged sweep is not a thing here, but the aligned rebuild is deterministic', () => {
    // The rebuild fires on `i % period === 0`, so the state at any row is a
    // function of the row index and not of how many rows have been consumed.
    // Running the same input at two lengths must therefore agree exactly on
    // the shared prefix.
    const { x, y } = structured(1_000, 500);
    const short = rollingBivariateValues(x.slice(0, 300), y.slice(0, 300), 30);
    const long = rollingBivariateValues(x, y, 30);
    for (let i = 0; i < 300; i += 1) {
      expect(short.covariance[i]).toBe(long.covariance[i]);
      expect(short.varianceX[i]).toBe(long.varianceX[i]);
    }
  });

  it('an all-missing column gives an all-missing answer, not a throw', () => {
    const m = rollingBivariateValues(
      Float64Array.from([NaN, NaN, NaN, NaN]),
      arr(1, 2, 3, 4),
      2,
    );
    expect(read(m.covariance).every((v) => v === undefined)).toBe(true);
    expect(read(m.varianceX).every((v) => v === undefined)).toBe(true);
  });

  it('a period longer than the input is all-missing, length kept', () => {
    const m = rollingBivariateValues(arr(1, 2, 3), arr(3, 2, 1), 5);
    expect(m.covariance).toHaveLength(3);
    expect(read(m.covariance).every((v) => v === undefined)).toBe(true);
  });

  it('rejects columns of different lengths', () => {
    expect(() => rollingBivariateValues(arr(1, 2, 3), arr(1, 2), 2)).toThrow(
      /row-aligned/,
    );
  });

  it('rejects invalid periods at the API boundary — it is a public kernel', () => {
    expect(() => rollingBivariateValues(arr(1, 2), arr(2, 3), 0)).toThrow(
      /positive integer/,
    );
    expect(() => rollingBivariateValues(arr(1, 2), arr(2, 3), -1)).toThrow(
      /positive integer/,
    );
    expect(() => rollingBivariateValues(arr(1, 2), arr(2, 3), 1.5)).toThrow(
      /positive integer/,
    );
    expect(() => rollingBivariateValues(arr(1, 2), arr(2, 3), 1)).toThrow(
      /period >= 2/,
    );
  });

  it('a column that CHANGES has a positive variance — near-flat is not flat (reviewed 2026-09-07)', () => {
    // The reverse-Welford removal can drive `m2` to 0 or below on a window
    // whose values differ by ulps; the clamp then read 0 and the studies
    // reported a false missing cell. The kernel now rebuilds such a window
    // on demand, so the invariant "changes > 0 ⇒ variance > 0" holds on
    // every complete window, and the correlation it feeds is finite.
    const nextUp = (v: number) => {
      const b = new Float64Array([v]);
      const u = new BigInt64Array(b.buffer);
      u[0] = u[0]! + 1n;
      return b[0]!;
    };
    const jitter = (length: number, base: number, seed: number) => {
      const out = new Float64Array(length);
      let s = seed;
      for (let i = 0; i < length; i += 1) {
        s = (Math.imul(s, 1103515245) + 12345) >>> 0;
        let v = base;
        for (let j = 0; j < (s >>> 16) % 4; j += 1) v = nextUp(v);
        out[i] = v;
      }
      return out;
    };
    let checked = 0;
    for (const base of [3.003, 100, 1e6, 1e12]) {
      for (const period of [2, 3, 5, 14]) {
        const x = jitter(600, base, period + 11);
        const y = jitter(600, base * 0.5, period + 29);
        const m = rollingBivariateValues(x, y, period);
        for (let i = period - 1; i < 600; i += 1) {
          let cx = 0;
          let cy = 0;
          for (let k = i - period + 2; k <= i; k += 1) {
            if (x[k] !== x[k - 1]) cx += 1;
            if (y[k] !== y[k - 1]) cy += 1;
          }
          if (cx > 0) expect(m.varianceX[i], `varX[${i}]`).toBeGreaterThan(0);
          else expect(m.varianceX[i], `varX[${i}]`).toBe(0);
          if (cy > 0) expect(m.varianceY[i], `varY[${i}]`).toBeGreaterThan(0);
          else expect(m.varianceY[i], `varY[${i}]`).toBe(0);
          if (cx > 0 && cy > 0) {
            checked += 1;
            const corr =
              m.covariance[i]! / Math.sqrt(m.varianceX[i]! * m.varianceY[i]!);
            expect(Number.isFinite(corr), `corr[${i}] finite`).toBe(true);
            expect(Math.abs(corr), `corr[${i}]`).toBeLessThanOrEqual(1 + 1e-9);
          }
        }
      }
    }
    expect(checked).toBeGreaterThan(5000);
  });
});
