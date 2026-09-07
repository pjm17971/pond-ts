/**
 * Exact rational arithmetic over the doubles' own values, for pinning a
 * kernel against the TRUE answer on numerically degenerate input. A double
 * is a dyadic rational, so every window statistic here is computed with
 * BigInt numerators and denominators and rounded once, at the end.
 */
export interface Q {
  n: bigint;
  d: bigint;
}

export function fromDouble(x: number): Q {
  const view = new DataView(new ArrayBuffer(8));
  view.setFloat64(0, x);
  const hi = view.getUint32(0);
  const lo = view.getUint32(4);
  const sign = hi >>> 31 ? -1n : 1n;
  const exp = (hi >>> 20) & 0x7ff;
  let mant = (BigInt(hi & 0xfffff) << 32n) | BigInt(lo);
  let e: number;
  if (exp === 0) e = -1074;
  else {
    mant |= 1n << 52n;
    e = exp - 1075;
  }
  let n = sign * mant;
  let d = 1n;
  if (e >= 0) n <<= BigInt(e);
  else d = 1n << BigInt(-e);
  return { n, d };
}

export const q = (i: number): Q => ({ n: BigInt(i), d: 1n });
export const add = (a: Q, b: Q): Q => ({
  n: a.n * b.d + b.n * a.d,
  d: a.d * b.d,
});
export const sub = (a: Q, b: Q): Q => ({
  n: a.n * b.d - b.n * a.d,
  d: a.d * b.d,
});
export const mul = (a: Q, b: Q): Q => ({ n: a.n * b.n, d: a.d * b.d });
export const div = (a: Q, b: Q): Q => ({ n: a.n * b.d, d: a.d * b.n });

/** Round a rational to the nearest double (to ~60 bits, then one rounding). */
export function toNumber(v: Q): number {
  if (v.n === 0n) return 0;
  const neg = v.n < 0n !== v.d < 0n;
  let n = v.n < 0n ? -v.n : v.n;
  let d = v.d < 0n ? -v.d : v.d;
  let shift = 0;
  while (n / d < 1n << 60n) {
    n <<= 8n;
    shift += 8;
  }
  while (n / d >= 1n << 68n) {
    d <<= 8n;
    shift -= 8;
  }
  const out = Number(n / d) * 2 ** -shift;
  return neg ? -out : out;
}

/** Exact OLS of a window against the bar index 0 … n−1. `r2` is NaN when the
 *  window's own variance is exactly zero. */
export function exactRegression(window: ArrayLike<number>): {
  slope: number;
  intercept: number;
  r2: number;
} {
  const n = window.length;
  const z = Array.from(window, fromDouble);
  let sz = q(0);
  let sxz = q(0);
  let szz = q(0);
  for (let k = 0; k < n; k += 1) {
    sz = add(sz, z[k]!);
    sxz = add(sxz, mul(q(k), z[k]!));
    szz = add(szz, mul(z[k]!, z[k]!));
  }
  const sumX = q((n * (n - 1)) / 2);
  const sumXX = q(((n - 1) * n * (2 * n - 1)) / 6);
  const den = sub(mul(q(n), sumXX), mul(sumX, sumX));
  const num = sub(mul(q(n), sxz), mul(sumX, sz));
  const spread = sub(mul(q(n), szz), mul(sz, sz));
  const slopeQ = div(num, den);
  const interceptQ = div(sub(sz, mul(slopeQ, sumX)), q(n));
  const r2 =
    spread.n === 0n ? NaN : toNumber(div(mul(num, num), mul(den, spread)));
  return { slope: toNumber(slopeQ), intercept: toNumber(interceptQ), r2 };
}

/** Exact population covariance, variances and correlation of two windows.
 *  `corr` is NaN when either variance is exactly zero. */
export function exactBivariate(
  wx: ArrayLike<number>,
  wy: ArrayLike<number>,
): { covariance: number; varianceX: number; varianceY: number; corr: number } {
  const n = wx.length;
  const xs = Array.from(wx, fromDouble);
  const ys = Array.from(wy, fromDouble);
  let sx = q(0);
  let sy = q(0);
  for (let k = 0; k < n; k += 1) {
    sx = add(sx, xs[k]!);
    sy = add(sy, ys[k]!);
  }
  const mx = div(sx, q(n));
  const my = div(sy, q(n));
  let cxy = q(0);
  let mxx = q(0);
  let myy = q(0);
  for (let k = 0; k < n; k += 1) {
    const dx = sub(xs[k]!, mx);
    const dy = sub(ys[k]!, my);
    cxy = add(cxy, mul(dx, dy));
    mxx = add(mxx, mul(dx, dx));
    myy = add(myy, mul(dy, dy));
  }
  const covariance = toNumber(div(cxy, q(n)));
  const varianceX = toNumber(div(mxx, q(n)));
  const varianceY = toNumber(div(myy, q(n)));
  let corr = NaN;
  if (mxx.n !== 0n && myy.n !== 0n) {
    // corr² exactly, then one square root; sign from the covariance.
    const c2 = toNumber(div(mul(cxy, cxy), mul(mxx, myy)));
    // The sign from the rational itself: `toNumber(cxy)` underflows to 0
    // below ~1e-308 and `Math.sign` then reads 0 (Codex review of #707).
    const negative = cxy.n < 0n !== cxy.d < 0n;
    corr = (cxy.n === 0n ? 0 : negative ? -1 : 1) * Math.sqrt(c2);
  }
  return { covariance, varianceX, varianceY, corr };
}

/** Deterministic 32-bit LCG (Math.imul — a double-precision `s * a` loses
 *  its low bits past 2^53 and the modulus then reads a constant). */
export function lcg(seed: number): () => number {
  let s = seed >>> 0;
  return () => {
    s = (Math.imul(s, 1664525) + 1013904223) >>> 0;
    return s >>> 9;
  };
}

export function nextUp(x: number, ulps = 1): number {
  const b = new Float64Array([x]);
  const u = new BigInt64Array(b.buffer);
  u[0] = u[0]! + BigInt(ulps);
  return b[0]!;
}

/**
 * Plateaus separated by a relative step, each plateau ulp-jittered — the
 * input that makes a shifted-frame accumulator's anchor go stale across a
 * step and then asks it about a window that differs only by ulps.
 */
export function plateaus(
  length: number,
  magnitude: number,
  relativeStep: number,
  rnd: () => number,
): Float64Array {
  const out = new Float64Array(length);
  let base = magnitude;
  for (let i = 0; i < length; i += 1) {
    if (rnd() % 11 === 0) base *= 1 + relativeStep * ((rnd() % 7) - 3);
    out[i] = nextUp(base, rnd() % 4);
  }
  return out;
}
