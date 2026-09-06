/**
 * Cross-validation: every shipped study is checked against a **pandas** oracle.
 *
 * `scripts/oracle/generate.py` computes reference values with pandas (an
 * independent implementation, conventions pinned to match ours — see that file)
 * and commits them to `fixtures/study-oracle.json`. Here we run our TypeScript
 * studies over the *same* input and assert bar-for-bar agreement. CI needs no
 * Python — the JSON is the committed oracle; regenerate it only when a study's
 * definition changes (and expect the diff to be reviewed).
 */
import { readFileSync } from 'node:fs';
import { describe, it, expect } from 'vitest';
import { TimeSeries } from 'pond-ts';
import type { MaType } from '../src/index.js';
import {
  sma,
  ema,
  movingAverage,
  bollinger,
  rollingStdev,
  rollingMin,
  rollingMax,
  rollingPercentile,
  zScore,
  envelope,
  percentChange,
  rsi,
  macd,
  atr,
  momentum,
  historicalVolatility,
  stochastic,
  williamsR,
  donchian,
  obv,
  vwap,
  keltner,
  atrBands,
  qstick,
  trix,
  coppock,
} from '../src/index.js';

interface OracleCase {
  study: string;
  params: {
    period?: number;
    stdDev?: number;
    q?: number;
    percent?: number;
    periods?: number;
    type?: MaType;
    maType?: MaType;
    fastPeriod?: number;
    slowPeriod?: number;
    signalPeriod?: number;
    annualize?: number;
    kPeriod?: number;
    slowing?: number;
    dPeriod?: number;
    atrPeriod?: number;
    multiplier?: number;
    longPeriod?: number;
    shortPeriod?: number;
    wmaPeriod?: number;
  };
  expected: Record<string, Array<number | null>>;
}
interface Oracle {
  meta: { oracle: string };
  input: {
    closes: number[];
    opens: number[];
    highs: number[];
    lows: number[];
    volumes: number[];
  };
  cases: OracleCase[];
}

const oracle = JSON.parse(
  readFileSync(
    new URL('./fixtures/study-oracle.json', import.meta.url),
    'utf8',
  ),
) as Oracle;

/** Build the close series the oracle computed over (index as the time key). */
function series(): TimeSeries<never> {
  return new TimeSeries({
    name: 'oracle',
    schema: [
      { name: 'time', kind: 'time' },
      { name: 'close', kind: 'number' },
    ],
    rows: oracle.input.closes.map((c, i) => [i, c]),
  }) as unknown as TimeSeries<never>;
}

/** The same bars with open/high/low/volume, for the studies that read a
 *  whole bar. Kept separate so the close-only cases stay on exactly the
 *  series they were generated against. The volume and open columns ride
 *  along on every bar study — an extra column is invisible to one that does
 *  not name it. */
function ohlcSeries(): TimeSeries<never> {
  return new TimeSeries({
    name: 'oracle',
    schema: [
      { name: 'time', kind: 'time' },
      { name: 'open', kind: 'number' },
      { name: 'high', kind: 'number' },
      { name: 'low', kind: 'number' },
      { name: 'close', kind: 'number' },
      { name: 'volume', kind: 'number' },
    ],
    rows: oracle.input.closes.map((c, i) => [
      i,
      oracle.input.opens[i]!,
      oracle.input.highs[i]!,
      oracle.input.lows[i]!,
      c,
      oracle.input.volumes[i]!,
    ]),
  }) as unknown as TimeSeries<never>;
}

function run(c: OracleCase): unknown {
  const p = c.params;
  switch (c.study) {
    case 'sma':
      return sma(series(), p as { period: number });
    case 'ema':
      return ema(series(), p as { period: number });
    case 'movingAverage':
      return movingAverage(series(), p as { period: number; type?: MaType });
    case 'bollinger':
      return bollinger(series(), p as { period: number; stdDev?: number });
    case 'rollingStdev':
      return rollingStdev(series(), p as { period: number });
    case 'rollingMin':
      return rollingMin(series(), p as { period: number });
    case 'rollingMax':
      return rollingMax(series(), p as { period: number });
    case 'rollingPercentile':
      return rollingPercentile(series(), p as { period: number; q: number });
    case 'zScore':
      return zScore(series(), p as { period: number });
    case 'envelope':
      return envelope(
        series(),
        p as { period: number; percent?: number; maType?: MaType },
      );
    case 'percentChange':
      return percentChange(series(), p as { periods?: number });
    case 'rsi':
      return rsi(series(), p as { period?: number });
    case 'atr':
      return atr(ohlcSeries(), p as { period?: number });
    case 'obv':
      return obv(ohlcSeries());
    case 'vwap':
      return vwap(ohlcSeries(), p as { period: number });
    case 'macd':
      return macd(
        series(),
        p as {
          fastPeriod?: number;
          slowPeriod?: number;
          signalPeriod?: number;
        },
      );
    case 'momentum':
      return momentum(series(), p as { period?: number });
    case 'historicalVolatility':
      return historicalVolatility(
        series(),
        p as { period?: number; annualize?: number },
      );
    case 'stochastic':
      return stochastic(
        ohlcSeries(),
        p as { kPeriod?: number; slowing?: number; dPeriod?: number },
      );
    case 'williamsR':
      return williamsR(ohlcSeries(), p as { period?: number });
    case 'donchian':
      return donchian(ohlcSeries(), p as { period?: number });
    case 'keltner':
      return keltner(
        ohlcSeries(),
        p as {
          period?: number;
          atrPeriod?: number;
          multiplier?: number;
          maType?: MaType;
        },
      );
    case 'atrBands':
      return atrBands(
        ohlcSeries(),
        p as { period?: number; multiplier?: number },
      );
    case 'qstick':
      return qstick(ohlcSeries(), p as { period?: number; maType?: MaType });
    case 'trix':
      return trix(series(), p as { period?: number; signalPeriod?: number });
    case 'coppock':
      return coppock(
        series(),
        p as { longPeriod?: number; shortPeriod?: number; wmaPeriod?: number },
      );
    default:
      // A fixture case whose study has no dispatch here must fail loudly, not
      // silently skip — the guard for future fan-out studies.
      throw new Error(`no dispatch for oracle study '${String(c.study)}'`);
  }
}

/** Read an appended column as (number | null)[] — null for a missing cell, so
 *  it lines up with the oracle's JSON `null`. */
function colValues(result: unknown, name: string): Array<number | null> {
  const events = (
    result as { events: ReadonlyArray<{ data(): Record<string, unknown> }> }
  ).events;
  return events.map((e) => {
    const v = e.data()[name];
    return typeof v === 'number' ? v : null;
  });
}

describe(`studies match the ${oracle.meta.oracle} oracle`, () => {
  for (const c of oracle.cases) {
    const label = `${c.study}(${JSON.stringify(c.params)})`;
    it(`${label} agrees bar-for-bar`, () => {
      const result = run(c);
      for (const [column, expected] of Object.entries(c.expected)) {
        const actual = colValues(result, column);
        expect(actual).toHaveLength(expected.length);
        for (let i = 0; i < expected.length; i += 1) {
          const exp = expected[i]!;
          if (exp === null) {
            expect(actual[i], `${column}[${i}] should be missing`).toBeNull();
          } else {
            // pandas + our incremental reducer are both IEEE754 doubles but sum
            // in a different order; 1e-9 absolute is comfortably inside that.
            expect(actual[i], `${column}[${i}]`).toBeCloseTo(exp, 9);
          }
        }
      }
    });
  }
});
