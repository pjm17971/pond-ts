/**
 * **Wilder's directional movement** — the two legs of the Directional
 * Movement System (Wilder 1978), and the per-bar term every study in the
 * `ADX` family starts from:
 *
 * ```
 * up = high[i] − high[i−1]      dn = low[i−1] − low[i]
 * +DM = up  when up > dn and up > 0, else 0
 * −DM = dn  when dn > up and dn > 0, else 0
 * ```
 *
 * The rule is "how far did this bar move *outside* the previous bar's range,
 * on the side that moved further" — an inside bar (both extremes contained)
 * moves in neither direction, and an outside bar counts only its larger
 * excursion. **At most one leg is non-zero on any bar**, and a tie (`up ===
 * dn`, including the `0 === 0` of an inside bar) makes both zero: the two
 * strict comparisons are what makes the split exhaustive without needing a
 * third case.
 *
 * ## Why a kernel and not a loop inside the study
 *
 * The studies README asks for the loop to live here (a study is
 * options-validation plus kernel calls), and the split earns it on the same
 * grounds {@link upDownLegValues} did: the obvious one-liners are wrong in a
 * case each. `Math.max(up, 0)` ignores the *comparison between the legs*, so
 * an outside bar would count in both directions and `+DI + −DI` would no
 * longer be the total directional movement it is defined to be. And a
 * `>= 0` anywhere would make a flat pair report movement.
 *
 * ## Missing cells
 *
 * `NaN` marks a gap ([PND-STUDYBOX]). A comparison against `NaN` is `false`,
 * so an unguarded version would map an **unknown** move to a flat one —
 * "zero movement" and "no answer" are exactly the two readings the family's
 * gap behaviour depends on. Both legs are therefore explicitly `NaN`
 * whenever either bar's extreme is missing, which costs **two** bars for one
 * missing cell: the bar itself and its successor, for which it is `prev`.
 *
 * `DM[0]` is `NaN`: the first bar has no predecessor, so its directional
 * movement is undefined rather than zero. Callers smoothing this should pass
 * `start = 1`, as {@link atrValues} does with `TR[0]`.
 *
 * O(N), one pass, two allocations.
 */
export function directionalMovementValues(
  high: Float64Array,
  low: Float64Array,
): { plus: Float64Array; minus: Float64Array } {
  const length = high.length;
  const plus = new Float64Array(length);
  const minus = new Float64Array(length);
  if (length === 0) return { plus, minus };

  plus[0] = NaN;
  minus[0] = NaN;
  for (let i = 1; i < length; i += 1) {
    const up = high[i]! - high[i - 1]!;
    const dn = low[i - 1]! - low[i]!;
    if (Number.isNaN(up) || Number.isNaN(dn)) {
      plus[i] = NaN;
      minus[i] = NaN;
      continue;
    }
    plus[i] = up > dn && up > 0 ? up : 0;
    minus[i] = dn > up && dn > 0 ? dn : 0;
  }
  return { plus, minus };
}

/**
 * **Vortex movement** — the two legs of Botes & Siepman's Vortex Indicator
 * (2010), which measure the same thing Wilder's directional movement does
 * and measure it **across** the bars rather than along them:
 *
 * ```
 * +VM = |high[i] − low[i−1]|      −VM = |low[i] − high[i−1]|
 * ```
 *
 * The contrast with {@link directionalMovementValues} is the reason the two
 * share a file. Wilder compares each extreme with the *same* extreme of the
 * previous bar and reports the excursion beyond it, so at most one leg is
 * non-zero and an inside bar reports nothing. The vortex crosses them —
 * today's high against yesterday's low, today's low against yesterday's high
 * — so **both legs are always positive**, and it is their *ratio* over a
 * window, not their individual sizes, that carries the signal. Their authors
 * named the two distances for the way price circulates around the previous
 * bar; the absolute values are in the published definition and matter only
 * for the (rare, gapped) bar where the crossing distance would come out
 * negative.
 *
 * ## Missing cells
 *
 * `NaN` propagates through the subtraction and `Math.abs` on its own, so no
 * guard is needed here (unlike the DM split, which decides between two
 * finite answers and therefore has to test). One missing extreme costs two
 * bars, its own and its successor's, for the same reason.
 *
 * `VM[0]` is `NaN`: no previous bar.
 *
 * O(N), one pass, two allocations.
 */
export function vortexMovementValues(
  high: Float64Array,
  low: Float64Array,
): { plus: Float64Array; minus: Float64Array } {
  const length = high.length;
  const plus = new Float64Array(length);
  const minus = new Float64Array(length);
  if (length === 0) return { plus, minus };

  plus[0] = NaN;
  minus[0] = NaN;
  for (let i = 1; i < length; i += 1) {
    plus[i] = Math.abs(high[i]! - low[i - 1]!);
    minus[i] = Math.abs(low[i]! - high[i - 1]!);
  }
  return { plus, minus };
}
