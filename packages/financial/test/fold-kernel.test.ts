import { describe, expect, it } from 'vitest';
import { foldRows } from '../src/index.js';

/* -------------------------------------------------------------------------- */
/* [PND-SFOLD] — the K6 stateful row fold.                                     */
/*                                                                             */
/* The kernel owns exactly two things: the loop and the GAP RULE. These pin    */
/* both directly, without a study in the way — the studies' own tests then     */
/* only have to pin their arithmetic.                                          */
/* -------------------------------------------------------------------------- */

const f = (values: number[]) => Float64Array.from(values);

/** A step that just records what it was called with and echoes `run`. */
const recorder = () => {
  const calls: Array<{ i: number; run: number }> = [];
  const step = (
    _state: null,
    i: number,
    run: number,
    _inputs: readonly Float64Array[],
    outputs: readonly Float64Array[],
  ): void => {
    calls.push({ i, run });
    outputs[0]![i] = run;
  };
  return { calls, step };
};

describe('foldRows', () => {
  it('calls the step once per row and counts the run from 1', () => {
    const { calls, step } = recorder();
    const [out] = foldRows([f([1, 2, 3, 4])], 1, null, step);
    expect(calls).toEqual([
      { i: 0, run: 1 },
      { i: 1, run: 2 },
      { i: 2, run: 3 },
      { i: 3, run: 4 },
    ]);
    expect(Array.from(out!)).toEqual([1, 2, 3, 4]);
  });

  it('a missing cell in ANY input skips the row and RESETS the run', () => {
    // The gap rule, in one assertion: bar 2 is incomplete (the second column
    // is NaN there), so the step is never offered it, and bar 3 comes back as
    // run 1 rather than run 4.
    const { calls, step } = recorder();
    const [out] = foldRows(
      [f([1, 2, 3, 4, 5]), f([9, 9, NaN, 9, 9])],
      1,
      null,
      step,
    );
    expect(calls).toEqual([
      { i: 0, run: 1 },
      { i: 1, run: 2 },
      { i: 3, run: 1 },
      { i: 4, run: 2 },
    ]);
    // …and the skipped row keeps the NaN the kernel pre-filled, which is what
    // `withColumn` turns into a missing cell.
    expect(Number.isNaN(out![2]!)).toBe(true);
    expect(Array.from(out!.subarray(3))).toEqual([1, 2]);
  });

  it('leading and trailing gaps are the same rule, not special cases', () => {
    const { calls, step } = recorder();
    foldRows([f([NaN, NaN, 3, 4, NaN])], 1, null, step);
    expect(calls).toEqual([
      { i: 2, run: 1 },
      { i: 3, run: 2 },
    ]);
  });

  it('every output starts NaN-filled, so a step that writes nothing warms up', () => {
    // A step that only writes from run 3 on: the kernel's own fill is what
    // makes the first two rows read as a warm-up rather than as zeros (a
    // zero-filled Float64Array would publish 0, which is a price).
    const [a, b] = foldRows(
      [f([1, 2, 3, 4])],
      2,
      null,
      (_s, i, run, _in, out) => {
        if (run < 3) return;
        out[0]![i] = i;
        out[1]![i] = -i;
      },
    );
    expect(Array.from(a!).map((x) => (Number.isNaN(x) ? null : x))).toEqual([
      null,
      null,
      2,
      3,
    ]);
    expect(Array.from(b!).map((x) => (Number.isNaN(x) ? null : x))).toEqual([
      null,
      null,
      -2,
      -3,
    ]);
  });

  it('mutates the caller’s state in place and never re-creates it', () => {
    const state = { seen: 0, runs: [] as number[] };
    foldRows([f([1, NaN, 3, 4])], 1, state, (s, _i, run) => {
      s.seen += 1;
      s.runs.push(run);
    });
    expect(state.seen).toBe(3);
    expect(state.runs).toEqual([1, 1, 2]);
  });

  it('an infinite cell is a number the machine acts on, not a gap', () => {
    // Only NaN marks a gap ([PND-STUDYBOX]); ±Infinity is arithmetic the
    // study is free to produce and must decide about itself.
    const { calls, step } = recorder();
    foldRows([f([1, Infinity, 3])], 1, null, step);
    expect(calls.map((c) => c.i)).toEqual([0, 1, 2]);
  });

  it('an empty series produces empty outputs rather than throwing', () => {
    const outputs = foldRows([new Float64Array(0)], 2, null, () => {
      throw new Error('the step must never be called');
    });
    expect(outputs).toHaveLength(2);
    expect(outputs[0]!).toHaveLength(0);
  });

  it('rejects no inputs, ragged inputs and a bad output count', () => {
    const noop = () => {};
    expect(() => foldRows([], 1, null, noop)).toThrow(
      /at least one input column/,
    );
    expect(() => foldRows([f([1, 2]), f([1])], 1, null, noop)).toThrow(
      /same length/,
    );
    expect(() => foldRows([f([1])], 0, null, noop)).toThrow(/positive integer/);
    expect(() => foldRows([f([1])], 1.5, null, noop)).toThrow(
      /positive integer/,
    );
  });
});
