import {
  BandChart,
  Candlestick,
  ChartContainer,
  ChartRow,
  Layers,
  LineChart,
  YAxis,
} from '@pond-ts/charts';
import { bollinger, ema } from '@pond-ts/financial';
import { useSiteChartTheme } from '@site/src/theme/useSiteChartTheme';
import { marketBars, sessionWindow } from './lib/financial-fixtures';

/** Studies are pure `(series, options) => series` functions that **append**
 *  columns to a bar `TimeSeries` — so they compose, and you draw their output
 *  as ordinary chart layers. Here `bollinger` adds `bbUpper`/`bbMiddle`/
 *  `bbLower` and `ema` adds `ema`; the band and line just read those columns
 *  over the same candles.
 *
 *  Prices are **modelled**, not measured — see `lib/financial-fixtures.ts`.
 *  The window is the last 120 sessions of that year, cropped *before* the
 *  studies run so the warm-up rows are the window's own. */
export default function ChartsFinancialStudies({ width }: { width: number }) {
  const theme = useSiteChartTheme();
  const set = marketBars();
  const { range, bars } = sessionWindow(set, 120);
  const study = ema(bollinger(bars, { period: 20 }), { period: 10 });

  return (
    <ChartContainer
      range={range}
      width={width}
      theme={theme}
      calendar={set.calendar}
      cursor="crosshair"
    >
      <ChartRow height={240}>
        <YAxis id="price" side="right" format={set.priceFormat} width={62} />
        <Layers>
          <BandChart
            series={study}
            lower="bbLower"
            upper="bbUpper"
            axis="price"
            as="inner"
          />
          <LineChart series={study} column="ema" axis="price" as="secondary" />
          <Candlestick series={bars} as={set.symbol} showOHLC gap={1} />
        </Layers>
      </ChartRow>
    </ChartContainer>
  );
}
