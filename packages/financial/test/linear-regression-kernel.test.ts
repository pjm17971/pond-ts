import { describe, expect, it } from 'vitest';
import { exactRegression, lcg, plateaus } from './exact-rational.js';
import {
  linearRegressionAt,
  linearRegressionValues,
} from '../src/kernels/linear-regression.js';

/*
 * The K7 rolling-regression kernel, tested on its own. The oracle pins the
 * VALUES against TA-Lib on a clean 80-bar input; what it cannot reach is
 * everything this file covers — the shifted frame at price magnitudes no
 * fixture carries, the flat window that TA-Lib's own fixture never contains,
 * the rebuild alignment, and the strict-window gap rule.
 */

const arr = (...xs: number[]) => Float64Array.from(xs);
const read = (x: Float64Array) =>
  Array.from(x, (v) => (Number.isNaN(v) ? undefined : v));

/**
 * The naive two-pass reference: centre `x` and `y`, then
 * `slope = Σdx·dy / Σdx²`. O(N·period) and never forms a large intermediate,
 * so it is what "the right answer at this magnitude" means below.
 */
function reference(values: Float64Array, period: number) {
  const n = values.length;
  const slope = new Float64Array(n).fill(NaN);
  const intercept = new Float64Array(n).fill(NaN);
  const r2 = new Float64Array(n).fill(NaN);
  const mx = (period - 1) / 2;
  for (let i = period - 1; i < n; i += 1) {
    let finite = true;
    let my = 0;
    for (let k = i - period + 1; k <= i; k += 1) {
      if (!Number.isFinite(values[k]!)) finite = false;
      my += values[k]! / period;
    }
    if (!finite) continue;
    let sxy = 0;
    let sxx = 0;
    let syy = 0;
    for (let u = 0; u < period; u += 1) {
      const dx = u - mx;
      const dy = values[i - period + 1 + u]! - my;
      sxy += dx * dy;
      sxx += dx * dx;
      syy += dy * dy;
    }
    slope[i] = sxy / sxx;
    intercept[i] = my - (sxy / sxx) * mx;
    r2[i] = syy === 0 ? NaN : (sxy * sxy) / (sxx * syy);
  }
  return { slope, intercept, r2 };
}

describe('linearRegressionValues', () => {
  it('fits x = 0…period−1 with x = 0 the OLDEST bar (TA-Lib’s convention)', () => {
    // y = 10, 12, 11 at period 3: Σy = 33, Σxy = 34, n·Σxy − Σx·Σy = 3,
    // n·Σx² − (Σx)² = 6 → slope 0.5, intercept (the fit at the window's
    // FIRST bar) 10.5. Reversing x would give −0.5 and 11.5.
    const fit = linearRegressionValues(arr(10, 12, 11), 3);
    expect(read(fit.slope)).toEqual([undefined, undefined, 0.5]);
    expect(fit.intercept[2]).toBeCloseTo(10.5, 12);
  });

  it('is the two-pass reference, bar for bar, at every period', () => {
    const values = Float64Array.from({ length: 200 }, (_, i) =>
      Number((100 + 6 * Math.sin(i / 5) + 0.11 * i).toFixed(4)),
    );
    for (const period of [2, 3, 7, 14, 40]) {
      const fit = linearRegressionValues(values, period);
      const want = reference(values, period);
      for (let i = 0; i < values.length; i += 1) {
        if (Number.isNaN(want.slope[i]!)) {
          expect(fit.slope[i], `period ${period} bar ${i}`).toBeNaN();
          continue;
        }
        expect(fit.slope[i]!, `slope ${period}/${i}`).toBeCloseTo(
          want.slope[i]!,
          12,
        );
        expect(fit.intercept[i]!, `intercept ${period}/${i}`).toBeCloseTo(
          want.intercept[i]!,
          10,
        );
        expect(fit.r2[i]!, `r2 ${period}/${i}`).toBeCloseTo(want.r2[i]!, 12);
      }
    }
  });

  it('[PND-SHIFTFRAME] holds up at 1e6 and 1e12, where a raw accumulator does not', () => {
    // `n·Σxy − Σx·Σy` differences two O(n²·ȳ) quantities whose difference is
    // O(n²·σ). Shifting by a value inside the window is what keeps both
    // operands the size of the answer. Measured over 200k rows the raw frame
    // is off by 2.97e-5 (1e6) and 27.8 (1e12) on the SLOPE alone, and its r²
    // leaves [0, 1] entirely; the two-pass reference below is the control.
    for (const [base, slopeTol, r2Tol] of [
      [1e6, 1e-12, 1e-12],
      [1e12, 1e-9, 1e-5],
    ] as const) {
      const values = Float64Array.from(
        { length: 20_000 },
        (_, i) => base + i * 0.01 + 3 * Math.sin(i / 7),
      );
      const fit = linearRegressionValues(values, 20);
      const want = reference(values, 20);
      let worstSlope = 0;
      let worstR2 = 0;
      for (let i = 19; i < values.length; i += 1) {
        worstSlope = Math.max(
          worstSlope,
          Math.abs(fit.slope[i]! - want.slope[i]!),
        );
        worstR2 = Math.max(worstR2, Math.abs(fit.r2[i]! - want.r2[i]!));
        // The intercept carries the price's own magnitude, so it can only
        // ever be right to about one ulp of it — that is representation, not
        // arithmetic, and the reference is exposed to it identically.
        expect(
          Math.abs(fit.intercept[i]! - want.intercept[i]!),
          `intercept at ${base} bar ${i}`,
        ).toBeLessThan(4 * Number.EPSILON * base);
        expect(
          fit.r2[i]!,
          `r2 range at ${base} bar ${i}`,
        ).toBeGreaterThanOrEqual(0);
        expect(fit.r2[i]!, `r2 range at ${base} bar ${i}`).toBeLessThanOrEqual(
          1,
        );
      }
      expect(worstSlope, `slope at ${base}`).toBeLessThan(slopeTol);
      expect(worstR2, `r2 at ${base}`).toBeLessThan(r2Tol);
    }
  });

  it('reads an exact line back exactly — slope b, r² 1, at any period', () => {
    // The shifted frame makes every accumulator exact on a line, so this is
    // `toBe` rather than close-to (measured over 1000 bars at 5 / 14 / 200).
    const line = Float64Array.from({ length: 400 }, (_, i) => 40 + 0.25 * i);
    for (const period of [2, 5, 14, 200]) {
      const fit = linearRegressionValues(line, period);
      for (let i = period - 1; i < line.length; i += 1) {
        expect(fit.slope[i], `slope ${period}/${i}`).toBe(0.25);
        expect(fit.r2[i], `r2 ${period}/${i}`).toBe(1);
        expect(linearRegressionAt(fit, period)[i], `tsf ${period}/${i}`).toBe(
          40 + 0.25 * (i + 1),
        );
      }
    }
  });

  it('a flat window is slope 0 EXACTLY and r² undefined, not a residue ratio', () => {
    // Without the change counter the accumulator residue reads
    // slope = −2.1e-14 and r² = −13.5 here — the second being outside the
    // statistic's own range, which is why the counter is not cosmetic.
    const fit = linearRegressionValues(
      arr(186.6, 154.81, 103.74, 193.5, 193.5, 193.5, 193.5),
      3,
    );
    expect(fit.slope[5]).toBe(0);
    expect(fit.slope[6]).toBe(0);
    expect(fit.intercept[5]).toBe(193.5);
    expect(fit.r2[5]).toBeNaN();
    expect(fit.r2[6]).toBeNaN();
    // The bar where the window is only PARTLY flat still fits a line.
    expect(fit.slope[4]).not.toBe(0);
  });

  it('the strict window: a gap blanks period bars and then recovers', () => {
    const fit = linearRegressionValues(
      arr(10, 11, 13, 12, NaN, 15, 14, 16, 18, 17),
      3,
    );
    const slope = read(fit.slope);
    // Bars 0–1 warm up, 2–3 fit, 4–6 hold the gap, 7 onward recover.
    expect(slope.map((v) => v !== undefined)).toEqual([
      false,
      false,
      true,
      true,
      false,
      false,
      false,
      true,
      true,
      true,
    ]);
  });

  it('a LEADING run of gaps shifts the start rather than emptying the fit', () => {
    const fit = linearRegressionValues(arr(NaN, NaN, 10, 11, 13, 12, 15), 3);
    const slope = read(fit.slope);
    expect(slope.slice(0, 4).every((v) => v === undefined)).toBe(true);
    expect(slope[4]).toBeCloseTo(1.5, 12);
    expect(slope[6]).toBeDefined();
  });

  it('rejects a period that is not an integer ≥ 2', () => {
    for (const bad of [0, -3, 2.5, NaN]) {
      expect(() => linearRegressionValues(arr(1, 2, 3), bad), `${bad}`).toThrow(
        /positive integer/,
      );
    }
    // period 1 passes `assertPeriod` and is still no regression: one point
    // does not determine a line, and the denominator n²(n²−1)/12 is 0.
    expect(() => linearRegressionValues(arr(1, 2, 3), 1)).toThrow(
      /linearRegressionValues period must be at least 2/,
    );
  });

  it('is all-missing when the period outruns the input, length kept', () => {
    const fit = linearRegressionValues(arr(1, 2, 3), 5);
    expect(fit.slope).toHaveLength(3);
    expect(read(fit.slope).every((v) => v === undefined)).toBe(true);
    expect(read(fit.r2).every((v) => v === undefined)).toBe(true);
  });

  it('is all-missing over an all-missing input', () => {
    const fit = linearRegressionValues(arr(NaN, NaN, NaN, NaN, NaN), 2);
    expect(read(fit.slope).every((v) => v === undefined)).toBe(true);
    expect(read(fit.intercept).every((v) => v === undefined)).toBe(true);
    expect(read(fit.r2).every((v) => v === undefined)).toBe(true);
  });
});

describe('linearRegressionAt', () => {
  it('reads the fit at the x the caller names', () => {
    const fit = linearRegressionValues(arr(10, 12, 11), 3);
    // slope 0.5, intercept 10.5.
    expect(linearRegressionAt(fit, 0)[2]).toBeCloseTo(10.5, 12);
    expect(linearRegressionAt(fit, 2)[2]).toBeCloseTo(11.5, 12); // LINEARREG
    expect(linearRegressionAt(fit, 3)[2]).toBeCloseTo(12, 12); // TSF
  });

  it('keeps the fit’s own warm-up rather than inventing one', () => {
    const fit = linearRegressionValues(arr(10, 12, 11), 3);
    expect(read(linearRegressionAt(fit, 3))).toEqual([
      undefined,
      undefined,
      12,
    ]);
  });

  describe('numerically degenerate windows — near-flat, not flat', () => {
    // Reviewed 2026-09-07: a window that CHANGES (the counter is > 0) but by
    // ulps left the rolling `n·Σz² − (Σz)²` at −1e-24, and the closed-form
    // ratio put a NEGATIVE r² into a column whose contract is 0 … 1. The
    // exact-flat counter does not see this: it is numerical, not
    // mathematical, degeneracy. The kernel now recomputes such a window
    // two-pass and centred, and pins r² to its bound.
    const nextUp = (v: number) => {
      const b = new Float64Array([v]);
      const u = new BigInt64Array(b.buffer);
      u[0] = u[0]! + 1n;
      return b[0]!;
    };
    const nextDown = (v: number) => {
      const b = new Float64Array([v]);
      const u = new BigInt64Array(b.buffer);
      u[0] = u[0]! - 1n;
      return b[0]!;
    };
    const jitter = (
      length: number,
      base: number,
      ulps: number,
      seed: number,
    ) => {
      const out = new Float64Array(length);
      let s = seed;
      for (let i = 0; i < length; i += 1) {
        // A 32-bit LCG via Math.imul: `s * a` in doubles loses the low bits
        // above 2^53 and the modulus then reads a constant.
        s = (Math.imul(s, 1103515245) + 12345) >>> 0;
        const k = ((s >>> 16) % (2 * ulps + 1)) - ulps;
        let v = base;
        for (let j = 0; j < Math.abs(k); j += 1)
          v = k > 0 ? nextUp(v) : nextDown(v);
        out[i] = v;
      }
      return out;
    };

    it("the reviewer's window — 3.0029989989969996 then two of 3.0029989989979997 at period 3 — reads r² in [0, 1]", () => {
      const v = arr(
        3.0029989989969996,
        3.0029989989979997,
        3.0029989989979997,
        3.0029989989979997,
        3.0029989989969996,
        3.0029989989979997,
        3.0029989989979997,
      );
      const { r2 } = linearRegressionValues(v, 3);
      let finite = 0;
      for (let i = 2; i < v.length; i += 1) {
        const r = r2[i]!;
        if (Number.isNaN(r)) continue;
        finite += 1;
        expect(r, `r2[${i}]`).toBeGreaterThanOrEqual(0);
        expect(r, `r2[${i}]`).toBeLessThanOrEqual(1);
      }
      expect(finite).toBeGreaterThan(0);
    });

    it('every finite r² over ulp-jittered near-flat input is within [0, 1], at several periods and magnitudes', () => {
      let checked = 0;
      let degenerate = 0;
      for (const base of [3.003, 100, 1e6, 1e12]) {
        for (const period of [2, 3, 5, 14]) {
          const v = jitter(600, base, 3, period * 7 + 1);
          const { r2, slope } = linearRegressionValues(v, period);
          for (let i = period - 1; i < v.length; i += 1) {
            let changes = 0;
            for (let k = i - period + 2; k <= i; k += 1)
              if (v[k] !== v[k - 1]) changes += 1;
            const r = r2[i]!;
            if (changes === 0) {
              expect(slope[i], `slope[${i}] flat`).toBe(0);
              expect(r, `r2[${i}] flat`).toBeNaN();
              continue;
            }
            checked += 1;
            if (Number.isNaN(r)) {
              degenerate += 1;
              continue;
            }
            expect(r, `r2[${i}] @${base}/${period}`).toBeGreaterThanOrEqual(0);
            expect(r, `r2[${i}] @${base}/${period}`).toBeLessThanOrEqual(1);
          }
        }
      }
      expect(checked).toBeGreaterThan(5000);
      // The centred recompute answers almost every degenerate window; a
      // residual NaN is allowed only where the centred variance is itself 0.
      expect(degenerate).toBeLessThan(checked / 100);
    });
  });

  describe('against an exact rational reference on plateau-stepped, ulp-jittered input', () => {
    // Reviewed 2026-09-07 (Layer-2 on #707): the first fix triggered on the
    // SIGN of the rolling spread, and a positive residue sailed through —
    // r² = 5.8e-11 where the exact answer was 0.75, 3.0 (pinned to 1.0)
    // where it was 0.43. This test computes the true r², slope and
    // intercept of every changing window with BigInt rationals and pins the
    // kernel to them, so neither the trigger nor the pin can hide a wrong
    // value. It fails with the fix reverted.
    it('every changing window reads within 1e-12 of the exact r² and intercept and relatively within 1e-9 of the exact slope', () => {
      const rnd = lcg(4242);
      let windows = 0;
      let unresolved = 0;
      for (const magnitude of [1e-3, 1, 1e6, 1e12, 3.002998998997]) {
        for (const step of [0, 1e-3, 1e-6, 1e-9, 1e-12]) {
          for (const period of [2, 3, 5, 8, 13, 21, 30]) {
            for (let trial = 0; trial < 1; trial += 1) {
              const v = plateaus(period + 40, magnitude, step, rnd);
              const { slope, intercept, r2 } = linearRegressionValues(
                v,
                period,
              );
              for (let i = period - 1; i < v.length; i += 1) {
                const w = v.subarray(i - period + 1, i + 1);
                let changes = 0;
                for (let k = 1; k < period; k += 1)
                  if (w[k] !== w[k - 1]) changes += 1;
                if (changes === 0) {
                  expect(slope[i], `flat slope[${i}]`).toBe(0);
                  expect(r2[i], `flat r2[${i}]`).toBeNaN();
                  continue;
                }
                windows += 1;
                const exact = exactRegression(w);
                const tag = `@${magnitude}/${step}/${period} bar ${i}`;
                // Relative, with a floor of ε times the window's own spread over the
                // period (an exactly-zero exact slope on a symmetric window still
                // leaves the kernel a rounding-sized value): no absolute 1e-9 — a
                // near-flat window's exact slope is ~1e-19 and an absolute clause pinned
                // nothing there (the pre-fix kernel passed one at 5e-4 of tolerance
                // while 1477× off relative — second-pass review of #707).
                let lo = w[0]!;
                let hi = w[0]!;
                for (let k = 1; k < period; k += 1) {
                  if (w[k]! < lo) lo = w[k]!;
                  if (w[k]! > hi) hi = w[k]!;
                }
                expect(
                  Math.abs(slope[i]! - exact.slope),
                  `slope ${tag}`,
                ).toBeLessThanOrEqual(
                  1e-9 * Math.abs(exact.slope) +
                    (Number.EPSILON * (hi - lo)) / period,
                );
                expect(
                  Math.abs(intercept[i]! - exact.intercept),
                  `intercept ${tag}`,
                ).toBeLessThanOrEqual(1e-12 * Math.abs(exact.intercept));
                if (Number.isNaN(r2[i])) {
                  unresolved += 1;
                  continue;
                }
                expect(r2[i], `r2 range ${tag}`).toBeGreaterThanOrEqual(0);
                expect(r2[i], `r2 range ${tag}`).toBeLessThanOrEqual(1);
                expect(
                  Math.abs(r2[i]! - exact.r2),
                  `r2 ${tag}`,
                ).toBeLessThanOrEqual(1e-12);
              }
            }
          }
        }
      }
      expect(windows).toBeGreaterThan(5000);
      expect(unresolved).toBe(0);
    });
  });
});
