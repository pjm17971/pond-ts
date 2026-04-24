# References

Working bibliography for pond-ts docs authoring. Not user-facing — this file
lives in the repo root and is consulted while writing. Entries marked
**[Cite in docs]** will appear in user-facing "Further reading" blocks on
specific pages; the rest is author calibration (voice, structure, terminology
alignment).

Update this file when a reference gets retired, a URL moves, or a new
source proves useful during writing. The goal is a curated shelf, not an
exhaustive link dump.

## 1. Time-series / DataFrame library docs (voice + depth)

- **Pandas — Time series / date functionality**
  https://pandas.pydata.org/docs/user_guide/timeseries.html
  The reference for "this concept explained well." Our `resample` /
  `rolling` / `groupby().agg()` analogues should read like the pandas
  equivalents: one-sentence intro, minimal example, common gotcha.

- **Polars — Time series operations**
  https://docs.pola.rs/user-guide/transformations/time-series/
  Terser and more formal. Second opinion on the same concepts; useful
  when pandas's prose gets too conversational.

- **Arquero — API reference**
  https://uwdata.github.io/arquero/
  JS-native, smaller scope — our closest peer in the JS ecosystem.
  Structurally the best match for pond-ts's "composable dataframe
  primitives in TypeScript" positioning.

## 2. Lineage (what pond-ts replaces)

- **pondjs docs**
  https://software.es.net/pond/
  The predecessor site. Readers migrating from pondjs will search for
  its vocabulary — `IndexedEvent`, `Collection`, `Pipeline`, `TimeRangeEvent`.
  The migration guide will need a rename table keyed off these terms.

- **react-timeseries-charts**
  https://software.es.net/react-timeseries-charts/
  The shape the eventual `@pond-ts/charts` package will echo. The
  pondjs-era API was tightly coupled to its data model; ours won't be,
  but the charting vocabulary ("TimeRangeChart", "AreaChart") is worth
  preserving for familiarity.

## 3. React hooks libraries (pattern calibration)

- **TanStack Query — Overview**
  https://tanstack.com/query/latest/docs/framework/react/overview
  "A few hooks, real stakes, ref stability matters." Closest structural
  match for our hooks docs. Note how they separate _concept_ pages
  (caching, invalidation) from _hook_ pages (`useQuery`, `useMutation`) —
  the same split we're planning for `@pond-ts/react`.

- **SWR**
  https://swr.vercel.app/
  Simpler voice. Good calibration for terse hook pages like `useLatest`
  where there isn't much to explain.

- **Zustand — README**
  https://github.com/pmndrs/zustand
  Lean "here's one concept, here's a hook" rhythm. Reference for when a
  hook page can be 40 lines rather than 200.

## 4. Observability / streaming explainers

- **PromQL — Query basics**
  https://prometheus.io/docs/prometheus/latest/querying/basics/
  How rate / avg / stddev / windowing get explained for an ops audience.
  Our users overlap significantly with ops readers, so matching
  vocabulary reduces translation cost.

- **Grafana — Transform data**
  https://grafana.com/docs/grafana/latest/panels-visualizations/query-transform-data/transform-data/
  Direct competitor in "let me reshape this time series." If a Grafana
  user thinks in terms of Grafana transformations, our docs should name
  the equivalents (or explicitly say we don't have one).

- **InfluxDB — Flux language basics**
  https://docs.influxdata.com/flux/
  Pipeline-style, worth contrasting. Flux's `|>` operator is a useful
  point of comparison when explaining pond-ts's method-chaining style.

## 5. Chart-library glossaries (interop)

- **Recharts — API**
  https://recharts.org/en-US/api
  Common target for `toPoints(col)`. Their `{ x, y }` convention is why
  `toPoints` emits `{ ts, value }`.

- **Observable Plot**
  https://observablehq.com/plot/
  Terminology for "marks," "channels," "scales." Worth aligning where we
  overlap — users coming from Plot should recognize our chart adapters.

- **Visx — Gallery**
  https://airbnb.io/visx/
  Lower-level, for users building custom charts. Reference for the shape
  a future `@pond-ts/charts` might expose.

## 6. Docs-as-a-craft

- **Diátaxis framework** — _Daniele Procida_
  https://diataxis.fr/
  **[Cite in contributing guide]** Resolves "is this a recipe, a concept,
  or a reference page?" Four quadrants: tutorial, how-to, reference,
  explanation. Our top-level IA already roughly maps: _Start here_ →
  tutorial, _Recipes_ → how-to, _Concepts_ → explanation, _API reference_
  → reference. Worth being explicit about the mapping so new authors
  know where content belongs.

- **Divio — "The documentation system"** — _the original before Diátaxis_
  https://docs.divio.com/documentation-system/
  Denser prose, same core model. Reference for the provenance of the
  ideas in Diátaxis.

## 7. Concept primers

### Streaming & windowing

- **Tyler Akidau — "The world beyond batch: Streaming 101"**
  https://www.oreilly.com/radar/the-world-beyond-batch-streaming-101/
  **[Cite in docs: windowing concepts page]** The canonical taxonomy of
  tumbling / sliding / session windows and event-time vs
  processing-time. pond-ts is event-time windowing over in-order data
  with no late-arrival handling — we should say that using Akidau's
  terms so readers calibrate expectations correctly. Diagram-heavy;
  worth re-reading before drafting our own windowing page.

- **Tyler Akidau — "The world beyond batch: Streaming 102"**
  https://www.oreilly.com/radar/the-world-beyond-batch-streaming-102/
  **[Cite in docs: live section intro]** Watermarks, triggers, and
  out-of-order correctness — everything `LiveSeries` deliberately does
  _not_ do. Cite this to set the "what you're not getting here"
  expectation honestly. Framing `LiveSeries` as a bounded in-order
  buffer (rather than a streaming engine) is the correct positioning.

### Smoothing primitives

- **Wikipedia — Moving average**
  https://en.wikipedia.org/wiki/Moving_average
  Comparison for `smooth('ema' | 'movingAverage' | 'weighted')`. Covers
  the classical intuitions we can lean on instead of re-deriving.

- **Wikipedia — Local regression (LOESS / LOWESS)**
  https://en.wikipedia.org/wiki/Local_regression
  For the `smooth('loess')` page. Users will want to know what makes it
  different from a moving average and when each is appropriate.

### Statistics sanity

- **Wikipedia — Standard deviation**
  https://en.wikipedia.org/wiki/Standard_deviation
  Cite for "why `sd === 0` on a flat run" — the v0.5.10 fix to
  `baseline()` hinges on this. The stdev-of-constant is zero, and a
  zero-width band flags every non-equal point, so we emit `undefined`.
  A one-line footnote on the `baseline()` page is probably enough.
