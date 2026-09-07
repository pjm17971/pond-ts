/**
 * **The anchored-VWAP arithmetic**, generalized over an *anchor group*:
 *
 * ```
 * out[i] = Σ typical·volume / Σ volume     over the bars of anchors[i] up to i
 * ```
 *
 * `anchors` is one id per row — `NaN` for a bar that belongs to no group (it
 * contributes nothing and reads `NaN`), and a **change of id restarts both
 * sums**. That one parameter is the whole difference between the package's
 * two anchored VWAPs, which is why they share this loop rather than two:
 *
 * - `anchoredVwap` passes a single group (`0` from the anchor bar onward,
 *   `NaN` before it), so the sums run to the end of the series.
 * - `sessionVwap` passes the **session id** of each bar, so the sums reset at
 *   every session open and a bar in closed time reads `NaN`.
 *
 * Ids are expected **contiguous** (a run of one id is one group), which they
 * are for any ordered series tagged against a non-overlapping schedule.
 *
 * ## The rules, and why each is what it is
 *
 * - **Both sums consume the same bars.** The contribution is blanked when
 *   *either* `typical` or `volume` is missing, before anything is
 *   accumulated. A bar with a volume but a missing `high` would otherwise
 *   feed `Σ volume` and not `Σ price·volume`, which is a VWAP quietly biased
 *   toward zero — the #710 rule (two accumulations must consume the same
 *   bars).
 * - **A leading run of gaps shifts the group's start.** The sums begin at the
 *   first complete bar of the group; earlier rows read `NaN`. That is
 *   `cumulativeValues`' documented rule, which this replaces in place.
 * - **An interior gap ENDS the group's line.** Both sums are running sums, so
 *   after an unknown term every later level is a known sum plus an unknown.
 *   That is `obv`'s rule and the A/D line's: the reading is a *level*, and
 *   skipping the bar would report an average price that silently excludes
 *   volume that traded. The **next group re-seeds** — for `sessionVwap` the
 *   reset at the next session open is the recovery, where `anchoredVwap`
 *   needs the caller to re-anchor.
 * - **Zero accumulated volume → `NaN`.** Reachable when a group opens on a
 *   run of zero-volume bars: there is nothing to weight by, so there is no
 *   average price — not `0`, and not the plain mean of typical price. The
 *   guard is **live and at the output**; without it the reading would be
 *   `±Infinity`, which `withColumn` rejects outright.
 *
 * O(N), one pass, one allocation. The fused form replaces the two
 * `cumulativeValues` passes and their two blanked inputs (four arrays) that
 * `anchoredVwap` allocated before this kernel existed.
 */
export function anchoredVwapValues(
  typical: Float64Array,
  volume: Float64Array,
  anchors: Float64Array,
): Float64Array {
  const length = typical.length;
  const out = new Float64Array(length);

  let currentId = NaN;
  let started = false;
  let flow = 0;
  let weight = 0;

  for (let i = 0; i < length; i += 1) {
    const id = anchors[i]!;
    if (Number.isNaN(id)) {
      out[i] = NaN;
      continue;
    }
    if (id !== currentId) {
      currentId = id;
      started = false;
      flow = 0;
      weight = 0;
    }
    const v = volume[i]!;
    const f = typical[i]! * v;
    if (!started) {
      // The leading-gap rule: the group's sums begin at its first complete
      // bar. `f` is NaN when either input is missing, so the two sums are
      // blanked together by construction.
      if (Number.isNaN(f)) {
        out[i] = NaN;
        continue;
      }
      started = true;
    }
    // Once started, a NaN poisons both accumulators for the rest of the
    // group — the interior-gap rule, IEEE arithmetic doing it for free.
    flow += f;
    weight += v;
    out[i] = weight === 0 ? NaN : flow / weight;
  }
  return out;
}
