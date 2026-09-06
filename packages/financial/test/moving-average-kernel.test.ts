/*
 * The K2 moving-average engine, tested per type on a **wavy** fixture.
 *
 * The fixture matters more than the assertions. A monotonic or constant
 * series is value-BLIND for a moving average: on `100, 101, 102, …` an SMA,
 * a WMA and a TRIMA all sit on the same straight line, so a dispatch that
 * returned the wrong type — or weights that were reversed, or a window off by
 * one — passes every bar. (A Layer-2 review found exactly that shape on RSI:
 * a monotonic fixture sent every value through one branch.) So the input
 * turns, the ten types are pairwise separated on it (the oracle generator
 * asserts that separation on its own fixture too), and each type is pinned
 * bar-for-bar with the arithmetic spelled out for at least one bar.
 */
import { describe, expect, it } from 'vitest';
import { TimeSeries } from 'pond-ts';
import {
  MA_TYPES,
  assertMaType,
  movingAverageColumn,
  movingAverageValues,
} from '../src/kernels/moving-average.js';
import type { MaType } from '../src/kernels/moving-average.js';
import {
  columnValues,
  emaValues,
  rollingValues,
} from '../src/kernels/rolling.js';
import { wilderValues } from '../src/kernels/wilder.js';
import { envelope } from '../src/index.js';

/** The wavy fixture: it goes up, down, up, so no two MA types agree on it. */
const WAVY = [10, 13, 11, 15, 12, 16, 14, 18, 15, 19];
const values = () => Float64Array.from(WAVY);

/** A longer version, for the gap tests — long enough that every type's
 *  recovery (or its refusal to recover) is visible before the array ends. */
const LONG = [10, 13, 11, 15, 12, 16, 14, 18, 15, 19, 16, 20, 17, 21, 18, 22];

const read = (x: Float64Array): Array<number | undefined> =>
  Array.from(x, (v) => (Number.isNaN(v) ? undefined : v));

const firstValid = (x: Float64Array): number =>
  read(x).findIndex((v) => v !== undefined);

/** Assert a row-aligned result against an expected list, `undefined` for the
 *  warm-up — the mask is checked as a mask (an off-by-one there is invisible
 *  to a magnitude comparison, which skips any index where only one side is
 *  missing). */
function expectSeries(
  actual: Float64Array,
  expected: Array<number | undefined>,
): void {
  expect(read(actual).map((v) => v === undefined)).toEqual(
    expected.map((v) => v === undefined),
  );
  for (let i = 0; i < expected.length; i += 1) {
    const want = expected[i];
    if (want !== undefined) expect(actual[i], `bar ${i}`).toBeCloseTo(want, 10);
  }
}

const _ = undefined;

describe('movingAverageValues — one type at a time, period 4', () => {
  it('sma: the plain mean of the window', () => {
    // bar 3 = (10 + 13 + 11 + 15) / 4 = 12.25
    expectSeries(movingAverageValues(values(), 4, 'sma'), [
      _,
      _,
      _,
      12.25,
      12.75,
      13.5,
      14.25,
      15,
      15.75,
      16.5,
    ]);
  });

  it('ema: α = 2/(period+1), seeded on the first sample', () => {
    // α = 0.4. 10 → .4·13+.6·10 = 11.2 → .4·11+.6·11.2 = 11.12
    //        → .4·15+.6·11.12 = 12.672, the first bar emitted.
    expectSeries(movingAverageValues(values(), 4, 'ema'), [
      _,
      _,
      _,
      12.672,
      12.4032,
      13.84192,
      13.905152,
      15.5430912,
      15.32585472,
      16.795512832,
    ]);
  });

  it('wma: linear weights, heaviest on the newest bar', () => {
    // bar 3 = (1·10 + 2·13 + 3·11 + 4·15) / 10 = 129/10 = 12.9
    // bar 4 = (1·13 + 2·11 + 3·15 + 4·12) / 10 = 128/10 = 12.8
    // Reversed weights would give 13.6 and 12.4 here, so the fixture
    // discriminates the direction — a rising fixture would not.
    expectSeries(movingAverageValues(values(), 4, 'wma'), [
      _,
      _,
      _,
      12.9,
      12.8,
      14.1,
      14.3,
      15.8,
      15.8,
      17.1,
    ]);
  });

  it('smma: Wilder, SMA-seeded', () => {
    // seed bar 3 = mean(10,13,11,15) = 12.25
    // bar 4 = (12.25·3 + 12) / 4 = 48.75/4 = 12.1875
    expectSeries(movingAverageValues(values(), 4, 'smma'), [
      _,
      _,
      _,
      12.25,
      12.1875,
      13.140625,
      13.35546875,
      14.5166015625,
      14.637451171875,
      15.72808837890625,
    ]);
  });

  it('dema: 2·EMA − EMA(EMA), warming up at 2·period − 2', () => {
    // EMA(EMA) runs over the EMA's own output and steps over its warm-up, so
    // it needs 4 more bars: 3 + 3 = 6.
    // bar 6 = 2·13.905152 − 13.4073344 = 14.4029696
    expectSeries(movingAverageValues(values(), 4, 'dema'), [
      _,
      _,
      _,
      _,
      _,
      _,
      14.4029696,
      16.82454528,
      15.96438528,
      18.0604260352,
    ]);
  });

  it('tema: 3·EMA − 3·EMA(EMA) + EMA³, warming up at 3·period − 3', () => {
    expectSeries(movingAverageValues(values(), 4, 'tema'), [
      _,
      _,
      _,
      _,
      _,
      _,
      _,
      _,
      _,
      18.4815972352,
    ]);
  });

  it('trima (even period): SMA(3) then SMA(2) — lengths summing to period+1', () => {
    // bar 3 = (SMA3[2] + SMA3[3]) / 2 = (34/3 + 13) / 2 = 12.1666…
    // The other even split (SMA(2) then SMA(3)) would put the peak weight on
    // a different bar and give 12.4166… here.
    expectSeries(movingAverageValues(values(), 4, 'trima'), [
      _,
      _,
      _,
      12.166666666666668,
      12.833333333333332,
      13.5,
      14.166666666666668,
      15,
      15.833333333333332,
      16.5,
    ]);
  });

  it('trima (odd period): SMA(3) twice', () => {
    // bar 4 = (SMA3[2] + SMA3[3] + SMA3[4]) / 3 = (34/3 + 13 + 38/3)/3 = 37/3
    expectSeries(movingAverageValues(values(), 5, 'trima'), [
      _,
      _,
      _,
      _,
      12.333333333333334,
      13.333333333333334,
      13.666666666666666,
      14.777777777777779,
      15.222222222222221,
      16.333333333333332,
    ]);
  });

  it('hull: WMA(2·WMA(n/2) − WMA(n), round(√n))', () => {
    // half = 2, root = 2. WMA2[3] = (11 + 2·15)/3 = 41/3; WMA4[3] = 12.9, so
    // raw[3] = 2·41/3 − 12.9 = 14.4333…; raw[4] = 2·13 − 12.8 = 13.2, and
    // bar 4 = (14.4333… + 2·13.2)/3 = 13.6111…
    expectSeries(movingAverageValues(values(), 4, 'hull'), [
      _,
      _,
      _,
      _,
      13.611111111111109,
      14.555555555555555,
      15.1,
      16.7,
      16.644444444444446,
      17.555555555555557,
    ]);
  });

  it('hull rounds its √period rather than flooring it', () => {
    // period 8 is where the two differ: √8 = 2.83, so the outer WMA is 3 bars
    // and the first value lands on 8 − 2 + 3 = 9. Flooring would put it on 8.
    // (period 4 cannot see this — √4 is exactly 2 either way.)
    expect(firstValid(movingAverageValues(values(), 8, 'hull'))).toBe(9);
  });

  it('kama: TA-Lib fast 2 / slow 30, seeded on x[period−1]', () => {
    // bar 4: change = |12 − 10| = 2, path = 3+2+4+3 = 12, ER = 1/6,
    // SC = (ER·(2/3 − 2/31) + 2/31)² = 0.170608…² = 0.029107…,
    // KAMA = 15 + SC·(12 − 15) = 14.91844914…  (seeded on x[3] = 15)
    expectSeries(movingAverageValues(values(), 4, 'kama'), [
      _,
      _,
      _,
      _,
      14.918449146336764,
      14.963227132409159,
      14.923347943212013,
      15.050726403949056,
      15.048626247306546,
      15.212219626448778,
    ]);
  });

  it('kama: a flat window keeps smoothing rather than dividing 0 by 0', () => {
    // The efficiency ratio is `change / path` and a window that has not moved
    // makes both zero. TA-Lib's answer there is ER = 1 (smooth as fast as
    // possible), and it is load-bearing rather than cosmetic: KAMA is a
    // recursion, so an unguarded 0/0 would put NaN in `previous` and empty
    // the column from that bar to the end.
    //
    // Values are TA-Lib's own for this input (`MA(x, 4, matype=6)`), and the
    // series goes flat while KAMA is still well below it, so the flat bars
    // are the ones doing the work: 18.96 → 19.42 → 19.68 → 19.82 is the
    // recursion closing 4/9 of the remaining gap to 20 each bar.
    const flatTail = Float64Array.from([
      10, 13, 11, 15, 12, 16, 14, 18, 20, 20, 20, 20, 20, 20, 20,
    ]);
    const out = movingAverageValues(flatTail, 4, 'kama');
    expect(out[11]).toBeCloseTo(18.963933915284247, 10);
    expect(out[12]).toBeCloseTo(19.42440773071347, 10);
    expect(out[13]).toBeCloseTo(19.680226517063037, 10);
    expect(out[14]).toBeCloseTo(19.82234806503502, 10);
  });

  it('zlema: EMA(2x − x[i−lag]) with lag = ⌊(period−1)/2⌋', () => {
    // period 4 → lag 1, so the de-lagged series starts at bar 1 and the EMA
    // needs 4 of them: first value on bar 4.
    // de-lagged[1..4] = 16, 9, 19, 9 → EMA seeded at 16 →
    // .4·9+.6·16 = 13.2 → .4·19+.6·13.2 = 15.52 → .4·9+.6·15.52 = 12.912
    expectSeries(movingAverageValues(values(), 4, 'zlema'), [
      _,
      _,
      _,
      _,
      12.912,
      15.7472,
      14.24832,
      17.348992,
      15.2093952,
      18.32563712,
    ]);
  });
});

describe('movingAverageValues — shape rules that hold for every type', () => {
  it('is length-preserving on every type', () => {
    for (const type of MA_TYPES) {
      expect(movingAverageValues(values(), 4, type).length, type).toBe(
        WAVY.length,
      );
    }
  });

  it('warms up where its definition says, on every type', () => {
    const expected: Record<MaType, number> = {
      sma: 3,
      ema: 3,
      wma: 3,
      smma: 3,
      trima: 3,
      dema: 6, // 2n − 2
      tema: 9, // 3n − 3
      hull: 4, // n − 2 + round(√n)
      kama: 4, // n (the efficiency ratio needs n differences)
      zlema: 4, // ⌊(n−1)/2⌋ + n − 1
    };
    for (const type of MA_TYPES) {
      expect(firstValid(movingAverageValues(values(), 4, type)), type).toBe(
        expected[type],
      );
    }
  });

  it('period 1 is the identity for every type but kama', () => {
    for (const type of MA_TYPES) {
      const out = read(movingAverageValues(values(), 1, type));
      if (type === 'kama') {
        // KAMA at period 1 still has to see one difference before it can
        // measure efficiency, so it starts on bar 1 rather than bar 0.
        expect(out[0], type).toBeUndefined();
        continue;
      }
      expect(out, type).toEqual(WAVY);
    }
  });

  it('rejects a type outside the menu, rather than defaulting silently', () => {
    expect(() => assertMaType('vidya')).toThrow(/unknown moving-average type/);
    expect(() => movingAverageValues(values(), 4, 'wilder' as MaType)).toThrow(
      /unknown moving-average type/,
    );
  });

  it('rejects a period that is not a positive integer', () => {
    // The kernel validates as well as the study, because it is exported: a
    // `period` of 0 divides the weighted sum by zero rather than throwing.
    for (const bad of [0, -3, 2.5, NaN]) {
      expect(() => movingAverageValues(values(), bad, 'wma'), `${bad}`).toThrow(
        /positive integer/,
      );
    }
  });

  it('emits an all-missing column when the period outruns the input', () => {
    for (const type of MA_TYPES) {
      const out = read(movingAverageValues(values(), 40, type));
      expect(
        out.every((v) => v === undefined),
        type,
      ).toBe(true);
    }
  });
});

describe('movingAverageValues — one definition, not two', () => {
  const series = () =>
    new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'close', kind: 'number' },
      ] as const,
      rows: WAVY.map((c, i) => [i, c]) as Array<[number, number]>,
    }) as never;

  it('sma is bit-for-bit the rolling kernel `sma()` runs on', () => {
    const viaKernel = movingAverageValues(
      columnValues(series(), 'close'),
      4,
      'sma',
    );
    const viaRolling = rollingValues(series(), 'close', 'avg', 4);
    // Bit-for-bit, not toBeCloseTo: two implementations that agreed "to
    // rounding" would be two definitions.
    expect(Array.from(viaKernel)).toEqual(Array.from(viaRolling));
  });

  it("ema is bit-for-bit core's smooth('ema')", () => {
    const viaKernel = movingAverageValues(
      columnValues(series(), 'close'),
      4,
      'ema',
    );
    const viaSmooth = emaValues(series(), 'close', 4);
    expect(Array.from(viaKernel)).toEqual(Array.from(viaSmooth));
  });

  it('smma is bit-for-bit the Wilder kernel rsi and atr run on', () => {
    const viaKernel = movingAverageValues(values(), 4, 'smma');
    expect(Array.from(viaKernel)).toEqual(
      Array.from(wilderValues(values(), 4)),
    );
  });

  it('movingAverageColumn routes sma and ema to those same calls', () => {
    expect(
      Array.from(movingAverageColumn(series(), 'close', 4, 'sma')),
    ).toEqual(Array.from(rollingValues(series(), 'close', 'avg', 4)));
    expect(
      Array.from(movingAverageColumn(series(), 'close', 4, 'ema')),
    ).toEqual(Array.from(emaValues(series(), 'close', 4)));
  });
});

describe('movingAverageValues — gaps', () => {
  const long = () => Float64Array.from(LONG);
  /** The same series with bar 5 missing. */
  const gapped = () => {
    const v = Float64Array.from(LONG);
    v[5] = NaN;
    return v;
  };
  /** The same series with bars 0–2 missing — another study's warm-up. */
  const leading = () => {
    const v = Float64Array.from(LONG);
    v[0] = NaN;
    v[1] = NaN;
    v[2] = NaN;
    return v;
  };

  it('steps a leading run of gaps over rather than emptying the column', () => {
    // Three missing bars shift each type's first value by exactly three —
    // the `rsi(sma(...))` failure mode, where a leading NaN in a recursion's
    // seed window returned an entirely empty column.
    for (const type of MA_TYPES) {
      if (type === 'sma') continue; // see below
      const clean = firstValid(movingAverageValues(long(), 4, type));
      expect(firstValid(movingAverageValues(leading(), 4, type)), type).toBe(
        clean + 3,
      );
    }
  });

  it('sma alone does not shift — its window counts rows, not contributors', () => {
    // Deliberate, and the one place the engine is not uniform. `sma` is
    // `sma()`, whose count window emits once it SPANS `period` rows and
    // averages whichever of them are finite — the contract `sma∘sma` pins in
    // study-missing-cells.test.ts. Giving the engine's `sma` the step-over
    // would make `movingAverage({ type: 'sma' })` a second, differing SMA.
    const out = read(movingAverageValues(leading(), 4, 'sma'));
    expect(out[2]).toBeUndefined();
    expect(out[3]).toBeCloseTo(15, 10); // the one finite cell in rows 0–3
  });

  it('window types recover once the gap leaves the window', () => {
    // `sma` averages the finite cells of a window that still spans `period`
    // rows (its documented contract), so it never goes missing; `wma`,
    // `trima` and `hull` are pinned by where their missing rows are.
    const smaGap = read(movingAverageValues(gapped(), 4, 'sma'));
    expect(smaGap[5]).toBeCloseTo((11 + 15 + 12) / 3, 10);
    const wmaGap = read(movingAverageValues(gapped(), 4, 'wma'));
    expect(wmaGap.slice(5, 9).every((v) => v === undefined)).toBe(true);
    expect(wmaGap[9]).not.toBeUndefined();
    const hullGap = read(movingAverageValues(gapped(), 4, 'hull'));
    expect(hullGap.slice(5, 10).every((v) => v === undefined)).toBe(true);
    expect(hullGap[10]).not.toBeUndefined();

    // And every window type agrees with the gap-free answer again as soon as
    // the gap has left every window it feeds — bar 11 clears all of them.
    for (const type of ['sma', 'wma', 'trima', 'hull'] as const) {
      const clean = movingAverageValues(long(), 4, type);
      const gap = movingAverageValues(gapped(), 4, type);
      for (let i = 11; i < LONG.length; i += 1) {
        expect(gap[i], `${type} bar ${i}`).toBeCloseTo(clean[i]!, 10);
      }
    }
  });

  it('the ema family skips the missing bar and carries on', () => {
    for (const type of ['ema', 'dema', 'tema', 'zlema'] as const) {
      const out = read(movingAverageValues(gapped(), 4, type));
      expect(out[5], type).toBeUndefined();
      expect(out.at(-1), type).not.toBeUndefined();
    }
  });

  it('smma and kama propagate an interior gap to the end', () => {
    // The Wilder asymmetry: a recursion that CONSUMES every bar has no state
    // to carry across a hole, where the ema family simply skips it.
    for (const type of ['smma', 'kama'] as const) {
      const out = read(movingAverageValues(gapped(), 4, type));
      expect(out[4], type).not.toBeUndefined();
      expect(
        out.slice(5).every((v) => v === undefined),
        type,
      ).toBe(true);
    }
  });
});

describe('envelope on the widened maType', () => {
  const bars = () =>
    new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'close', kind: 'number' },
      ] as const,
      rows: WAVY.map((c, i) => [i, c]) as Array<[number, number]>,
    });

  const cells = (s: unknown, name: string): Array<number | undefined> =>
    (
      s as { events: ReadonlyArray<{ data(): Record<string, unknown> }> }
    ).events.map((e) => {
      const v = e.data()[name];
      return typeof v === 'number' ? v : undefined;
    });

  it("the sma and ema centre lines are the kernels' own, unchanged", () => {
    // Routing `envelope` through the MA engine must not have moved a single
    // number on the two types it already supported. Compared against the
    // kernel calls the study made directly before the engine existed.
    for (const [maType, expected] of [
      ['sma', rollingValues(bars() as never, 'close', 'avg', 4)],
      ['ema', emaValues(bars() as never, 'close', 4)],
    ] as const) {
      const out = envelope(bars(), { period: 4, percent: 2, maType });
      expect(cells(out, 'envMiddle'), maType).toEqual(
        read(expected as Float64Array),
      );
    }
  });

  it('accepts the rest of the menu on its centre line', () => {
    const out = envelope(bars(), { period: 4, percent: 2, maType: 'hull' });
    const middle = cells(out, 'envMiddle');
    expect(middle).toEqual(read(movingAverageValues(values(), 4, 'hull')));
    expect(cells(out, 'envUpper')[9]).toBeCloseTo(middle[9]! * 1.02, 10);
    expect(cells(out, 'envLower')[9]).toBeCloseTo(middle[9]! * 0.98, 10);
  });

  it('rejects a maType outside the menu', () => {
    expect(() =>
      envelope(bars(), { period: 4, maType: 'vwma' as MaType }),
    ).toThrow(/unknown moving-average type/);
  });
});
