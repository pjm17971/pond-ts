import {
  Baseline,
  Candlestick,
  ChartContainer,
  ChartRow,
  Layers,
  LineChart,
  YAxis,
} from '@pond-ts/charts';
import { macd, rsi } from '@pond-ts/financial';
import { useSiteChartTheme } from '@site/src/theme/useSiteChartTheme';
import { dailyCandles, dailyCandlesRange } from './lib/gallery-fixtures';

/** Oscillators live in their own rows. `rsi` and `macd` append columns to the
 *  same bar series the candles read, so each `<ChartRow>` below is just a
 *  different column of one `TimeSeries` on its own y-axis — RSI on a fixed
 *  0–100 axis with its 30/70 baselines, MACD line + signal on an auto-scaled
 *  one. The warm-up rows are `undefined`, which the line renders as a clean
 *  gap at the left rather than a spike from zero. */
export default function ChartsFinancialOscillators({
  width,
}: {
  width: number;
}) {
  const theme = useSiteChartTheme();
  const bars = macd(rsi(dailyCandles(80), { period: 14 }), {
    fastPeriod: 12,
    slowPeriod: 26,
    signalPeriod: 9,
  });

  return (
    <ChartContainer
      range={dailyCandlesRange(80)}
      width={width}
      theme={theme}
      cursor="crosshair"
    >
      <ChartRow height={170}>
        <YAxis id="price" side="right" format="$,.0f" />
        <Layers>
          <Candlestick series={bars} as="ACME" showOHLC />
        </Layers>
      </ChartRow>
      <ChartRow height={90}>
        <YAxis id="rsi" side="right" format=".0f" min={0} max={100} />
        <Layers>
          <Baseline value={70} axis="rsi" label="70" />
          <Baseline value={30} axis="rsi" label="30" />
          <LineChart series={bars} column="rsi" axis="rsi" as="rsi" />
        </Layers>
      </ChartRow>
      <ChartRow height={90}>
        <YAxis id="macd" side="right" format=".2f" />
        <Layers>
          <Baseline value={0} axis="macd" />
          <LineChart series={bars} column="macdLine" axis="macd" as="macd" />
          <LineChart
            series={bars}
            column="macdSignal"
            axis="macd"
            as="secondary"
          />
        </Layers>
      </ChartRow>
    </ChartContainer>
  );
}
