/** The four pivot formula sets, in menu order. */
export const PIVOT_METHODS = [
  'standard',
  'fibonacci',
  'woodie',
  'camarilla',
] as const;

/**
 * Which pivot formula set to use. All four read the **same three inputs** —
 * one period's high, low and close — and produce the same ladder of a central
 * pivot with support and resistance either side; they differ in the constants
 * and (for Woodie's) in how the centre is weighted. See
 * {@link pivotLevelValues} for each set written out.
 */
export type PivotMethod = (typeof PIVOT_METHODS)[number];

/** Row-aligned pivot levels. `r4` / `s4` are present **only** for
 *  `'camarilla'`, which is the one set that defines a fourth pair. */
export interface PivotLevels {
  readonly pivot: Float64Array;
  readonly r1: Float64Array;
  readonly r2: Float64Array;
  readonly r3: Float64Array;
  readonly s1: Float64Array;
  readonly s2: Float64Array;
  readonly s3: Float64Array;
  readonly r4?: Float64Array;
  readonly s4?: Float64Array;
}

/** Throw unless `method` names one of {@link PIVOT_METHODS}. */
export function assertPivotMethod(method: string): void {
  if (!(PIVOT_METHODS as readonly string[]).includes(method)) {
    throw new TypeError(
      `pivot method must be one of ${PIVOT_METHODS.join(' | ')}; got ${JSON.stringify(method)}`,
    );
  }
}

/**
 * **The four pivot formula sets**, evaluated row-wise over one period's
 * `high` / `low` / `close` (for {@link pivotPoints} these are the *previous
 * session's* aggregates, already broadcast onto each row).
 *
 * Writing `R = high − low` throughout:
 *
 * **`'standard'`** — the floor-trader / classic set (the one every vendor
 * means by an unqualified "pivot points"):
 *
 * ```
 * P  = (H + L + C) / 3
 * R1 = 2P − L            S1 = 2P − H
 * R2 = P + R             S2 = P − R
 * R3 = H + 2(P − L)      S3 = L − 2(H − P)
 * ```
 *
 * (`R3 = R1 + R` and `S3 = S1 − R` are the same two numbers written the other
 * common way — they are algebraically identical, so the two spellings in the
 * wild do not fork the definition.)
 *
 * **`'fibonacci'`** — the same centre, with the range scaled by the retracement
 * ratios 38.2 % / 61.8 % / 100 %:
 *
 * ```
 * P  = (H + L + C) / 3
 * R1 = P + 0.382R        S1 = P − 0.382R
 * R2 = P + 0.618R        S2 = P − 0.618R
 * R3 = P + 1.000R        S3 = P − 1.000R
 * ```
 *
 * **`'woodie'`** — the standard ladder over a **close-weighted** centre, which
 * pulls the pivot toward where the period actually finished:
 *
 * ```
 * P  = (H + L + 2C) / 4
 * R1 = 2P − L            S1 = 2P − H
 * R2 = P + R             S2 = P − R
 * R3 = H + 2(P − L)      S3 = L − 2(H − P)
 * ```
 *
 * **`'camarilla'`** (Nick Scott, 1989) — four pairs at `1.1/12`, `1.1/6`,
 * `1.1/4` and `1.1/2` of the range, measured from the **close** rather than
 * from the pivot:
 *
 * ```
 * P  = (H + L + C) / 3
 * R1 = C + R·1.1/12      S1 = C − R·1.1/12
 * R2 = C + R·1.1/6       S2 = C − R·1.1/6
 * R3 = C + R·1.1/4       S3 = C − R·1.1/4
 * R4 = C + R·1.1/2       S4 = C − R·1.1/2
 * ```
 *
 * ## Missing cells
 *
 * Every level of every set reads all three inputs (`P` needs all three, and
 * Camarilla's levels need `C` and the range), so a missing input blanks the
 * **whole ladder** for that row rather than part of it — the columns have one
 * warm-up between them, not seven. `NaN` propagates through the arithmetic on
 * its own; there is no guard here because there is no division.
 *
 * O(N) per call, one allocation per output column.
 */
export function pivotLevelValues(
  method: PivotMethod,
  high: Float64Array,
  low: Float64Array,
  close: Float64Array,
): PivotLevels {
  const length = high.length;
  const pivot = new Float64Array(length);
  const r1 = new Float64Array(length);
  const r2 = new Float64Array(length);
  const r3 = new Float64Array(length);
  const s1 = new Float64Array(length);
  const s2 = new Float64Array(length);
  const s3 = new Float64Array(length);
  const camarilla = method === 'camarilla';
  const r4 = camarilla ? new Float64Array(length) : undefined;
  const s4 = camarilla ? new Float64Array(length) : undefined;

  for (let i = 0; i < length; i += 1) {
    const h = high[i]!;
    const l = low[i]!;
    const c = close[i]!;
    const range = h - l;
    const p = method === 'woodie' ? (h + l + 2 * c) / 4 : (h + l + c) / 3;
    pivot[i] = p;
    if (method === 'fibonacci') {
      r1[i] = p + 0.382 * range;
      r2[i] = p + 0.618 * range;
      r3[i] = p + range;
      s1[i] = p - 0.382 * range;
      s2[i] = p - 0.618 * range;
      s3[i] = p - range;
    } else if (camarilla) {
      r1[i] = c + (range * 1.1) / 12;
      r2[i] = c + (range * 1.1) / 6;
      r3[i] = c + (range * 1.1) / 4;
      r4![i] = c + (range * 1.1) / 2;
      s1[i] = c - (range * 1.1) / 12;
      s2[i] = c - (range * 1.1) / 6;
      s3[i] = c - (range * 1.1) / 4;
      s4![i] = c - (range * 1.1) / 2;
    } else {
      // 'standard' and 'woodie' share the ladder and differ only in `p`.
      r1[i] = 2 * p - l;
      r2[i] = p + range;
      r3[i] = h + 2 * (p - l);
      s1[i] = 2 * p - h;
      s2[i] = p - range;
      s3[i] = l - 2 * (h - p);
    }
  }

  return camarilla
    ? { pivot, r1, r2, r3, r4: r4!, s1, s2, s3, s4: s4! }
    : { pivot, r1, r2, r3, s1, s2, s3 };
}
