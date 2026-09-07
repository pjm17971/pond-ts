import { describe, expect, it } from 'vitest';
import {
  trueRangeBoundsValues,
  trueRangeValues,
} from '../src/kernels/true-range.js';

/*
 * Tested directly, because `atr` alone cannot discriminate these: a Layer-2
 * review of PR #684 found that the ATR unit fixture (a constant 2-wide range
 * with no gaps) passes under EVERY true-range mutation, including dropping a
 * term outright — the same value-blindness a monotonic series gave the RSI
 * chaining test. Each of the three terms needs a bar where it is the strict
 * winner, which is what these provide.
 */

const arr = (...xs: number[]) => Float64Array.from(xs);
const read = (x: Float64Array) =>
  Array.from(x, (v) => (Number.isNaN(v) ? undefined : v));

describe('trueRangeValues', () => {
  it('takes high − low when the bar contains the previous close', () => {
    //         prevClose 100 sits inside bar 1's [99, 103] range.
    const tr = read(trueRangeValues(arr(101, 103), arr(99, 99), arr(100, 100)));
    expect(tr[1]).toBeCloseTo(4, 12); // 103-99=4 beats |103-100|=3, |99-100|=1
  });

  it('takes |high − prevClose| on a gap UP', () => {
    // Bar 1's whole range sits above the previous close, so the span from
    // that close up to the new high is wider than the bar itself.
    const tr = read(
      trueRangeValues(arr(101, 120), arr(99, 118), arr(100, 119)),
    );
    expect(tr[1]).toBeCloseTo(20, 12); // |120-100|=20 beats 120-118=2
  });

  it('takes |low − prevClose| on a gap DOWN', () => {
    // The term no ATR unit test reached before this: only the oracle caught
    // its removal.
    const tr = read(trueRangeValues(arr(101, 82), arr(99, 80), arr(100, 81)));
    expect(tr[1]).toBeCloseTo(20, 12); // |80-100|=20 beats 82-80=2, |82-100|=18
  });

  it('has no true range for the first bar', () => {
    // Not its plain range — there is no previous close, so it is unknown.
    const tr = read(trueRangeValues(arr(101, 102), arr(99, 98), arr(100, 100)));
    expect(tr[0]).toBeUndefined();
  });

  it('a missing close costs the NEXT bar, not its own', () => {
    // True range reads only the PREVIOUS close, so a bar with a missing close
    // of its own is unaffected — it is the following bar, for which that
    // close is `prevClose`, that has no answer. Worth pinning because the
    // intuition runs the other way.
    const tr = read(
      trueRangeValues(
        arr(101, 102, 103, 104),
        arr(99, 98, 97, 96),
        arr(100, NaN, 100, 100),
      ),
    );
    expect(tr[1]).toBeCloseTo(4, 12); // its own close is missing; irrelevant
    expect(tr[2]).toBeUndefined(); // prevClose is the missing one
    expect(tr[3]).toBeDefined();
  });

  it('is unknown where the high or the low is missing', () => {
    const noHigh = read(
      trueRangeValues(arr(101, NaN), arr(99, 98), arr(100, 100)),
    );
    expect(noHigh[1]).toBeUndefined();
    const noLow = read(
      trueRangeValues(arr(101, 102), arr(99, NaN), arr(100, 100)),
    );
    expect(noLow[1]).toBeUndefined();
  });

  it('preserves length, allocates one output, and leaves inputs alone', () => {
    const h = arr(101, 102, 103);
    const l = arr(99, 98, 97);
    const c = arr(100, 100, 100);
    const tr = trueRangeValues(h, l, c);
    expect(tr).toHaveLength(3);
    expect(Array.from(h)).toEqual([101, 102, 103]);
    expect(Array.from(l)).toEqual([99, 98, 97]);
    expect(Array.from(c)).toEqual([100, 100, 100]);
  });

  it('handles an empty input', () => {
    expect(trueRangeValues(arr(), arr(), arr())).toHaveLength(0);
  });
});

describe('trueRangeBoundsValues', () => {
  it('widens the bar to the previous close on whichever side gapped', () => {
    // Bar 1 gaps UP away from a close of 100: its low (105) is above it, so
    // the true low is the previous close and the true high is its own high.
    const { trueHigh, trueLow } = trueRangeBoundsValues(
      arr(101, 108),
      arr(99, 105),
      arr(100, 106),
    );
    expect(read(trueHigh)).toEqual([undefined, 108]);
    expect(read(trueLow)).toEqual([undefined, 100]);
  });

  it('leaves a bar that contains the previous close alone', () => {
    const { trueHigh, trueLow } = trueRangeBoundsValues(
      arr(101, 103),
      arr(99, 99),
      arr(100, 100),
    );
    expect(read(trueHigh)).toEqual([undefined, 103]);
    expect(read(trueLow)).toEqual([undefined, 99]);
  });

  it('is exactly the two halves of trueRangeValues', () => {
    // The claim that makes this a re-naming rather than a second definition,
    // pinned on bars that gap both ways and on one that does not.
    const high = arr(101, 108, 96, 104);
    const low = arr(99, 105, 92, 95);
    const close = arr(100, 106, 94, 103);
    const { trueHigh, trueLow } = trueRangeBoundsValues(high, low, close);
    const tr = trueRangeValues(high, low, close);
    for (let i = 1; i < tr.length; i += 1) {
      expect(trueHigh[i]! - trueLow[i]!, `bar ${i}`).toBeCloseTo(tr[i]!, 12);
    }
  });

  it('bar 0 is missing in both, and a missing close costs the NEXT bar', () => {
    const { trueHigh, trueLow } = trueRangeBoundsValues(
      arr(101, 103, 105),
      arr(99, 99, 101),
      arr(100, NaN, 104),
    );
    expect(read(trueHigh)).toEqual([undefined, 103, undefined]);
    expect(read(trueLow)).toEqual([undefined, 99, undefined]);
  });

  it('an empty input gives two empty arrays', () => {
    const { trueHigh, trueLow } = trueRangeBoundsValues(arr(), arr(), arr());
    expect(trueHigh).toHaveLength(0);
    expect(trueLow).toHaveLength(0);
  });
});
