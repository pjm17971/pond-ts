/**
 * **Wilder's Swing Index term** — the per-bar swing, shared by the two
 * studies built on it ({@link swingIndex} appends it, and
 * {@link accumulativeSwingIndex} is the running total of *this array*, not of
 * a second derivation that could drift from it).
 *
 * ```
 * SI = 50 · ( (C − C₁) + 0.5·(C − O) + 0.25·(C₁ − O₁) ) / R · (K / T)
 * ```
 *
 * with, writing `A = |H − C₁|`, `B = |L − C₁|`, `D = |H − L|`:
 *
 * ```
 * K = max(A, B)                        the larger gap against yesterday's close
 * R = A − 0.5·B + 0.25·|C₁ − O₁|       when A is the largest of A, B, D
 *   = B − 0.5·A + 0.25·|C₁ − O₁|       when B is
 *   = D + 0.25·|C₁ − O₁|               when D is
 * T = the limit move (the caller's `limit`)
 * ```
 *
 * `C`/`O`/`H`/`L` are today's close, open, high and low; `C₁`/`O₁` are
 * yesterday's close and open. Every term is named on the study's docstring
 * as well, because the formula is famously opaque — the short reading is
 * that the numerator is the bar's net move plus half its own body plus a
 * quarter of yesterday's, `R` is a true-range-like normaliser that leans on
 * whichever gap dominated, and `K/T` scales the whole thing by how large
 * today's gap was relative to the instrument's limit move.
 *
 * ## Bar 0 is `NaN`
 *
 * Every term reads yesterday, so the first bar has no swing — a
 * length-preserving warm-up of exactly one row, the same one
 * {@link trueRangeValues} has and for the same reason.
 *
 * ## `R === 0` is `undefined`, not `0`
 *
 * `R` is zero only when `A = B = D = 0` **and** `|C₁ − O₁| = 0`, i.e. today's
 * high, today's low, yesterday's close and yesterday's open are all the same
 * number — a tape that has not moved for two bars. The numerator is *not*
 * forced to zero by that condition (it also reads today's close and open,
 * which this kernel never checks against today's range — a `close`
 * redirected at a smoothed column is outside it by construction) — but `K`
 * *is* zero on exactly the same bars, so the whole expression is
 * `50·N·0/(0·T)`: a `0/0` however it is grouped, with no value it takes in
 * the limit. `undefined` is the answer, per the flat-window rule in the
 * studies README (the study's docstring says the same).
 *
 * Note the consequence for {@link accumulativeSwingIndex}: a running sum has
 * no local answer after an unknown term ({@link cumulativeValues}), so a
 * halted bar pair ends the ASI rather than being skipped. That is the same
 * rule OBV and the A/D line already follow, and it is stated on the study.
 *
 * `NaN` marks a gap ([PND-STUDYBOX]) and propagates on its own: a missing
 * price makes `A`, `B` and `D` all `NaN`, every `>=` comparison against one
 * is false, so the third branch is taken and `R` is `NaN` — which is not
 * `=== 0` and therefore falls through to the division. A bar with any of its
 * six inputs missing has no swing, and so does the bar after it.
 *
 * ## The order of the three `R` branches is UNOBSERVABLE, and that is a proof
 *
 * The tests are written as `A >= B && A >= D`, then `B >= A && B >= D`, then
 * `D` — so a tie between `A` and `B` takes the first. Swapping the first two
 * branches changes **no output**, and a mutation of the order survives every
 * test (measured, not assumed). That is not a coverage gap: the two branches
 * are *equal* wherever both conditions hold. `A = B` means the previous
 * close is equidistant from today's high and low, which is either
 * `H − C₁ = t` and `L − C₁ = −t` (so `D = 2t`, and `D ≤ A = t` forces
 * `t = 0`) or `H − C₁ = L − C₁` (so `H = L` and `D = 0`). In both cases
 * `A − 0.5·B = B − 0.5·A`, so the two expressions are the same number. The
 * order stays as written because it is Wilder's own listing; it is recorded
 * here so the surviving mutation reads as a proof rather than an untested
 * branch.
 *
 * O(N), one pass, one allocation. `limit` is validated by the studies.
 */
export function swingIndexValues(
  open: Float64Array,
  high: Float64Array,
  low: Float64Array,
  close: Float64Array,
  limit: number,
): Float64Array {
  const length = close.length;
  const out = new Float64Array(length);
  if (length === 0) return out;

  out[0] = NaN;
  for (let i = 1; i < length; i += 1) {
    const prevClose = close[i - 1]!;
    const prevOpen = open[i - 1]!;
    const c = close[i]!;
    const o = open[i]!;
    const h = high[i]!;
    const l = low[i]!;

    const a = Math.abs(h - prevClose);
    const b = Math.abs(l - prevClose);
    const d = Math.abs(h - l);
    const yesterdayBody = 0.25 * Math.abs(prevClose - prevOpen);

    // Ties go to the earlier branch (A, then B, then D), which matters only
    // when two of the three are exactly equal — on a flat bar all three are
    // zero and every branch gives the same `R`.
    const r =
      a >= b && a >= d
        ? a - 0.5 * b + yesterdayBody
        : b >= a && b >= d
          ? b - 0.5 * a + yesterdayBody
          : d + yesterdayBody;

    const k = Math.max(a, b);
    const numerator =
      c - prevClose + 0.5 * (c - o) + 0.25 * (prevClose - prevOpen);
    // `r === 0` is the fully halted two-bar case (see above); NaN is not
    // `=== 0`, so a gap falls to the division and propagates.
    out[i] = r === 0 ? NaN : ((50 * numerator) / r) * (k / limit);
  }
  return out;
}
