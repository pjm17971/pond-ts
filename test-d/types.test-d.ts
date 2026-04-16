import {
  BoundedSequence,
  type CalendarOptions,
  type CalendarUnit,
  Interval,
  type JsonObjectRowForSchema,
  type JsonRowForSchema,
  type JsonTimeRangeInput,
  type JsonTimestampInput,
  type JsonValueForKind,
  type NumericColumnNameForSchema,
  type RollingAlignment,
  type RollingSchema,
  type SmoothMethod,
  type SmoothAppendSchema,
  type SmoothSchema,
  Sequence,
  Time,
  TimeRange,
  TimeSeries,
  type TimeZoneOptions,
  type AggregateSchema,
  type AlignSchema,
  type EventForSchema,
  type IntervalKeyedSchema,
  type JoinConflictMode,
  type JoinManySchema,
  type JoinSchema,
  type JoinType,
  type PrefixedJoinManySchema,
  type PrefixedJoinSchema,
  type RowForSchema,
  type TimeRangeKeyedSchema,
} from '../src/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number' },
  { name: 'label', kind: 'string' },
] as const;

type Row = RowForSchema<typeof schema>;
const validRow: Row = [Date.now(), 42, 'ok'];
void validRow;
const calendarOptions: CalendarOptions = { timeZone: 'UTC', weekStartsOn: 1 };
const timeZoneOptions: TimeZoneOptions = { timeZone: 'Europe/Madrid' };
const calendarUnitDay: CalendarUnit = 'day';
const calendarUnitWeek: CalendarUnit = 'week';
const calendarUnitMonth: CalendarUnit = 'month';
const rollingAlignmentTrailing: RollingAlignment = 'trailing';
const rollingAlignmentLeading: RollingAlignment = 'leading';
const rollingAlignmentCentered: RollingAlignment = 'centered';
const smoothMethodEma: SmoothMethod = 'ema';
const smoothMethodMovingAverage: SmoothMethod = 'movingAverage';
const smoothMethodLoess: SmoothMethod = 'loess';
void calendarOptions;
void timeZoneOptions;
void calendarUnitDay;
void calendarUnitWeek;
void calendarUnitMonth;
void rollingAlignmentTrailing;
void rollingAlignmentLeading;
void rollingAlignmentCentered;
void smoothMethodEma;
void smoothMethodMovingAverage;
void smoothMethodLoess;

new TimeSeries({
  name: 'valid',
  schema,
  rows: [
    [new Date(), 1, 'x'],
    [new Time(Date.now()), 2, 'y'],
  ],
});

const jsonTimestamp: JsonTimestampInput = '2025-01-01T09:00';
const jsonTimeRange: JsonTimeRangeInput = {
  start: '2025-01-01',
  end: '2025-01-02',
};
const jsonValueTime: JsonValueForKind<'time'> = '2025-01-01T09:00';
const jsonRow: JsonRowForSchema<typeof schema> = ['2025-01-01T09:00', 1, 'ok'];
const jsonObjectRow: JsonObjectRowForSchema<typeof schema> = {
  time: '2025-01-01T09:00',
  value: 1,
  label: 'ok',
};
void jsonTimestamp;
void jsonTimeRange;
void jsonValueTime;
void jsonRow;
void jsonObjectRow;

const jsonSeries = TimeSeries.fromJSON({
  name: 'json',
  schema,
  rows: [jsonRow, jsonObjectRow],
  parse: { timeZone: 'UTC' },
});
const jsonSeriesEvent = jsonSeries.first();
if (!jsonSeriesEvent) {
  throw new Error('missing json event');
}
const jsonSeriesValue: number = jsonSeriesEvent.get('value');
const jsonSeriesLabel: string = jsonSeriesEvent.get('label');
void jsonSeriesValue;
void jsonSeriesLabel;

const parsedTime = Time.parse('2025-01-01T09:00', {
  timeZone: 'Europe/Madrid',
});
const parsedDayRange = TimeRange.fromDate('2025-01-01', { timeZone: 'UTC' });
const parsedWeekRange = TimeRange.fromCalendar('week', '2025-01-01', {
  timeZone: 'UTC',
  weekStartsOn: 1,
});
const parsedMonthInterval = Interval.fromCalendar('month', '2025-01', {
  timeZone: 'UTC',
  value: 'jan-2025',
});
void parsedTime;
void parsedDayRange;
void parsedWeekRange;
void parsedMonthInterval;

const rangeSchema = [
  { name: 'timeRange', kind: 'timeRange' },
  { name: 'value', kind: 'number' },
] as const;

new TimeSeries({
  name: 'range',
  schema: rangeSchema,
  rows: [[new TimeRange({ start: new Date(), end: Date.now() }), 1]],
});

const indexSchema = [
  { name: 'interval', kind: 'interval' },
  { name: 'value', kind: 'number' },
] as const;

new TimeSeries({
  name: 'interval',
  schema: indexSchema,
  rows: [[new Interval({ value: 'a', start: 0, end: 1 }), 1]],
});

const cpuSchema = [
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'number' },
  { name: 'host', kind: 'string' },
  { name: 'healthy', kind: 'boolean' },
] as const;
type CpuNumericColumn = NumericColumnNameForSchema<typeof cpuSchema>;
const cpuNumericColumn: CpuNumericColumn = 'cpu';
void cpuNumericColumn;

const cpuSeries = new TimeSeries({
  name: 'cpu-usage',
  schema: cpuSchema,
  rows: [
    [new Date('2025-01-01T00:00:00.000Z'), 0.42, 'api-1', true],
    [new Date('2025-01-01T00:01:00.000Z'), 0.51, 'api-2', true],
  ],
});

const nth = cpuSeries.at(1);
if (!nth) {
  throw new Error('missing event');
}

const first = cpuSeries.first();
const last = cpuSeries.last();
if (!first || !last) {
  throw new Error('missing boundary events');
}

const typedNth: EventForSchema<typeof cpuSchema> = nth;
void typedNth;
const typedFirst: EventForSchema<typeof cpuSchema> = first;
const typedLast: EventForSchema<typeof cpuSchema> = last;
void typedFirst;
void typedLast;

const cpuValue: number = nth.data().cpu;
const hostValue: string = nth.data().host;
const healthyValue: boolean = nth.data().healthy;
const cpuValueFromGet: number = nth.get('cpu');
const hostValueFromGet: string = nth.get('host');
const keyTime: Time = nth.key();
const eventRange: TimeRange = nth.timeRange();
const eventType: 'time' = nth.type();
const eventOverlaps: boolean = nth.overlaps(
  new TimeRange({ start: 1735689599000, end: 1735689601000 }),
);
const eventContains: boolean = nth.contains(new Time(1735689600000));
const eventBefore: boolean = nth.isBefore(new Time(1735689700000));
const eventAfter: boolean = nth.isAfter(new Time(1735689500000));
const eventIntersection: TimeRange | undefined = nth.intersection(
  new TimeRange({ start: 1735689599000, end: 1735689601000 }),
);
const eventTrimmed = nth.trim(
  new TimeRange({ start: 1735689599000, end: 1735689601000 }),
);
const asTimeCenter = nth.asTime({ at: 'center' });
const asTimeRangeEvent = nth.asTimeRange();
const asIntervalEvent = nth.asInterval('cpu');
const updatedNth = nth.set('cpu', 0.75);
const updatedCpuValue: number = updatedNth.get('cpu');
const mergedNth = nth.merge({ source: 'derived', healthy: false });
const mergedSource: string = mergedNth.get('source');
const mergedHealthy: boolean = mergedNth.get('healthy');
const renamedNth = nth.rename({ cpu: 'usage', host: 'server' });
const renamedUsage: number = renamedNth.get('usage');
const renamedServer: string = renamedNth.get('server');
const selectedNth = nth.select('cpu', 'healthy');
const selectedCpuValue: number = selectedNth.get('cpu');
const selectedHealthyValue: boolean = selectedNth.get('healthy');
const collapsedEvent = nth.collapse(
  ['cpu', 'healthy'],
  'score',
  ({ cpu, healthy }) => {
    return healthy ? cpu : 0;
  },
);
const collapsedScore: number = collapsedEvent.get('score');
const asTimeCenterKey: Time = asTimeCenter.key();
const asTimeRangeKey: TimeRange = asTimeRangeEvent.key();
const asIntervalKey: Interval = asIntervalEvent.key();
void cpuValue;
void hostValue;
void healthyValue;
void cpuValueFromGet;
void hostValueFromGet;
void keyTime;
void eventRange;
void eventType;
void eventOverlaps;
void eventContains;
void eventBefore;
void eventAfter;
void eventIntersection;
void eventTrimmed;
void asTimeCenter;
void asTimeRangeEvent;
void asIntervalEvent;
void updatedNth;
void updatedCpuValue;
void mergedNth;
void mergedSource;
void mergedHealthy;
void renamedNth;
void renamedUsage;
void renamedServer;
void selectedNth;
void selectedCpuValue;
void selectedHealthyValue;
void collapsedEvent;
void collapsedScore;
void asTimeCenterKey;
void asTimeRangeKey;
void asIntervalKey;

// @ts-expect-error cpu is number
const badCpuText: string = nth.data().cpu;
void badCpuText;

// @ts-expect-error host is string
const badHostFlag: boolean = nth.data().host;
void badHostFlag;

// @ts-expect-error cpu is number
const badCpuFromGet: string = nth.get('cpu');
void badCpuFromGet;

// @ts-expect-error cpu only accepts numbers
const badUpdatedNth = nth.set('cpu', 'high');
void badUpdatedNth;

// @ts-expect-error selected event no longer has host
const badSelectedHost = selectedNth.get('host');
void badSelectedHost;

// @ts-expect-error renamed event no longer has cpu
const badRenamedCpu = renamedNth.get('cpu');
void badRenamedCpu;

const trafficSchema = [
  { name: 'time', kind: 'time' },
  { name: 'in', kind: 'number' },
  { name: 'out', kind: 'number' },
] as const;

const trafficSeries = new TimeSeries({
  name: 'traffic',
  schema: trafficSchema,
  rows: [[new Date('2025-01-01T00:00:00.000Z'), 10, 20]],
});

const collapsedSeries = trafficSeries.collapse(
  ['in', 'out'],
  'avg',
  ({ in: inValue, out }) => {
    return (inValue + out) / 2;
  },
);
const collapsedSeriesEvent = collapsedSeries.at(0);
if (!collapsedSeriesEvent) {
  throw new Error('missing collapsed event');
}
const collapsedAvg: number = collapsedSeriesEvent.get('avg');
void collapsedAvg;

const selectedSeries = cpuSeries.select('host', 'healthy');
const selectedSeriesEvent = selectedSeries.at(0);
if (!selectedSeriesEvent) {
  throw new Error('missing selected event');
}
const selectedSeriesHost: string = selectedSeriesEvent.get('host');
const selectedSeriesHealthy: boolean = selectedSeriesEvent.get('healthy');
void selectedSeriesHost;
void selectedSeriesHealthy;

const filteredSeries = cpuSeries.filter((event) => event.get('healthy'));
const filteredEvent = filteredSeries.first();
if (!filteredEvent) {
  throw new Error('missing filtered event');
}
const filteredCpu: number = filteredEvent.get('cpu');
const filteredHost: string = filteredEvent.get('host');
void filteredCpu;
void filteredHost;

const foundCpuEvent = cpuSeries.find((event) => event.get('cpu') > 0.5);
if (!foundCpuEvent) {
  throw new Error('missing found event');
}
const foundCpuValue: number = foundCpuEvent.get('cpu');
const foundHostValue: string = foundCpuEvent.get('host');
const hasHealthyCpu: boolean = cpuSeries.some((event) => event.get('healthy'));
const allHealthyCpu: boolean = cpuSeries.every((event) => event.get('healthy'));
void foundCpuValue;
void foundHostValue;
void hasHealthyCpu;
void allHealthyCpu;

const slicedSeries = cpuSeries.slice(0, 1);
const slicedEvent = slicedSeries.last();
if (!slicedEvent) {
  throw new Error('missing sliced event');
}
const slicedHealthy: boolean = slicedEvent.get('healthy');
void slicedHealthy;

const withinSeries = cpuSeries.within(
  new Date('2025-01-01T00:00:00.000Z'),
  new Date('2025-01-01T00:01:00.000Z'),
);
const withinEvent = withinSeries.first();
if (!withinEvent) {
  throw new Error('missing within event');
}
const withinCpu: number = withinEvent.get('cpu');
void withinCpu;

const beforeSeries = cpuSeries.before(new Date('2025-01-01T00:01:00.000Z'));
const afterSeries = cpuSeries.after(new Time(1735689600000));
const cpuSeriesRange: TimeRange | undefined = cpuSeries.timeRange();
const cpuSeriesOverlaps: boolean = cpuSeries.overlaps(
  new TimeRange({ start: 1735689600000, end: 1735689700000 }),
);
const cpuSeriesContains: boolean = cpuSeries.contains(
  new TimeRange({ start: 1735689600000, end: 1735689660000 }),
);
const cpuSeriesIntersection: TimeRange | undefined = cpuSeries.intersection(
  new TimeRange({ start: 1735689600000, end: 1735689630000 }),
);
const overlappingCpuSeries = cpuSeries.overlapping(
  new TimeRange({ start: 1735689599000, end: 1735689601000 }),
);
const containedCpuSeries = cpuSeries.containedBy(
  new TimeRange({ start: 1735689599000, end: 1735689661000 }),
);
const trimmedCpuSeries = cpuSeries.trim(
  new TimeRange({ start: 1735689599000, end: 1735689601000 }),
);
const alignedCpuSeries = cpuSeries.align(Sequence.every('1m'), {
  method: 'hold',
  range: new TimeRange({ start: 1735689600000, end: 1735689660000 }),
});
const cpuSeriesAsTimeRange = cpuSeries.asTimeRange();
const cpuSeriesAsInterval = cpuSeries.asInterval((event) => event.begin());
const boundedMinuteSequence = new BoundedSequence([
  new Interval({
    value: 1735689600000,
    start: 1735689600000,
    end: 1735689660000,
  }),
]);
const defaultCalendarDaySequence = Sequence.calendar('day');
const calendarDaySequence = Sequence.calendar('day', { timeZone: 'UTC' });
const calendarDayBounded = calendarDaySequence.bounded(
  new TimeRange({ start: 1735689600000, end: 1735776000000 }),
);
const alignedCpuSeriesFromBounded = cpuSeries.align(boundedMinuteSequence, {
  method: 'hold',
});
void beforeSeries;
void afterSeries;
void cpuSeriesRange;
void cpuSeriesOverlaps;
void cpuSeriesContains;
void cpuSeriesIntersection;
void overlappingCpuSeries;
void containedCpuSeries;
void trimmedCpuSeries;
void alignedCpuSeries;
void cpuSeriesAsTimeRange;
void cpuSeriesAsInterval;
void alignedCpuSeriesFromBounded;
void defaultCalendarDaySequence;
void calendarDayBounded;

type AlignedCpuSchema = AlignSchema<typeof cpuSchema>;
type CpuAsTimeRangeSchema = TimeRangeKeyedSchema<typeof cpuSchema>;
type CpuAsIntervalSchema = IntervalKeyedSchema<typeof cpuSchema>;
const alignedCpuEvent = alignedCpuSeries.first();
const cpuSeriesAsTimeRangeEvent = cpuSeriesAsTimeRange.first();
const cpuSeriesAsIntervalEvent = cpuSeriesAsInterval.first();
if (!alignedCpuEvent) {
  throw new Error('missing aligned event');
}
if (!cpuSeriesAsTimeRangeEvent || !cpuSeriesAsIntervalEvent) {
  throw new Error('missing rekeyed event');
}
const alignedCpuKey: Interval = alignedCpuEvent.key();
const cpuSeriesAsTimeRangeKey: TimeRange = cpuSeriesAsTimeRangeEvent.key();
const cpuSeriesAsIntervalKey: Interval = cpuSeriesAsIntervalEvent.key();
const alignedCpuValue: number | undefined = alignedCpuEvent.get('cpu');
const cpuSeriesAsTimeRangeCpu: number = cpuSeriesAsTimeRangeEvent.get('cpu');
const cpuSeriesAsIntervalCpu: number = cpuSeriesAsIntervalEvent.get('cpu');
const alignedCpuHost: string | undefined = alignedCpuEvent.get('host');
const alignedCpuHealthy: boolean | undefined = alignedCpuEvent.get('healthy');
const alignedTypedSchemaEvent: EventForSchema<AlignedCpuSchema> =
  alignedCpuEvent;
const cpuSeriesAsTimeRangeTypedEvent: EventForSchema<CpuAsTimeRangeSchema> =
  cpuSeriesAsTimeRangeEvent;
const cpuSeriesAsIntervalTypedEvent: EventForSchema<CpuAsIntervalSchema> =
  cpuSeriesAsIntervalEvent;
void alignedCpuKey;
void cpuSeriesAsTimeRangeKey;
void cpuSeriesAsIntervalKey;
void alignedCpuValue;
void cpuSeriesAsTimeRangeCpu;
void cpuSeriesAsIntervalCpu;
void alignedCpuHost;
void alignedCpuHealthy;
void alignedTypedSchemaEvent;
void cpuSeriesAsTimeRangeTypedEvent;
void cpuSeriesAsIntervalTypedEvent;

const aggregatedCpuSeries = cpuSeries.aggregate(
  Sequence.every('1m'),
  { cpu: 'avg', host: 'first', healthy: 'last' },
  { range: new TimeRange({ start: 1735689600000, end: 1735689660000 }) },
);
type AggregatedCpuSchema = AggregateSchema<
  typeof cpuSchema,
  {
    readonly cpu: 'avg';
    readonly host: 'first';
    readonly healthy: 'last';
  }
>;
const aggregatedCpuEvent = aggregatedCpuSeries.first();
if (!aggregatedCpuEvent) {
  throw new Error('missing aggregated event');
}
const aggregatedCpuKey: Interval = aggregatedCpuEvent.key();
const aggregatedCpuValue: number | undefined = aggregatedCpuEvent.get('cpu');
const aggregatedCpuHost: string | undefined = aggregatedCpuEvent.get('host');
const aggregatedCpuHealthy: boolean | undefined =
  aggregatedCpuEvent.get('healthy');
const aggregatedTypedSchemaEvent: EventForSchema<AggregatedCpuSchema> =
  aggregatedCpuEvent;
void aggregatedCpuKey;
void aggregatedCpuValue;
void aggregatedCpuHost;
void aggregatedCpuHealthy;
void aggregatedTypedSchemaEvent;

const rolledCpuSeries = cpuSeries.rolling('1m', {
  cpu: 'avg',
  host: 'last',
  healthy: 'last',
});
type RolledCpuSchema = RollingSchema<
  typeof cpuSchema,
  {
    readonly cpu: 'avg';
    readonly host: 'last';
    readonly healthy: 'last';
  }
>;
const rolledCpuEvent = rolledCpuSeries.first();
if (!rolledCpuEvent) {
  throw new Error('missing rolled event');
}
const rolledCpuKey: Time = rolledCpuEvent.key();
const rolledCpuValue: number | undefined = rolledCpuEvent.get('cpu');
const rolledCpuHost: string | undefined = rolledCpuEvent.get('host');
const rolledCpuHealthy: boolean | undefined = rolledCpuEvent.get('healthy');
const rolledCpuTypedEvent: EventForSchema<RolledCpuSchema> = rolledCpuEvent;
void rolledCpuKey;
void rolledCpuValue;
void rolledCpuHost;
void rolledCpuHealthy;
void rolledCpuTypedEvent;

const rolledCpuOnSequence = cpuSeries.rolling(
  Sequence.every('1m'),
  '5m',
  { cpu: 'avg', host: 'last', healthy: 'last' },
  { range: new TimeRange({ start: 1735689600000, end: 1735689660000 }) },
);
type RolledCpuSequenceSchema = AggregateSchema<
  typeof cpuSchema,
  {
    readonly cpu: 'avg';
    readonly host: 'last';
    readonly healthy: 'last';
  }
>;
const rolledCpuOnSequenceEvent = rolledCpuOnSequence.first();
if (!rolledCpuOnSequenceEvent) {
  throw new Error('missing rolled sequence event');
}
const rolledCpuOnSequenceKey: Interval = rolledCpuOnSequenceEvent.key();
const rolledCpuOnSequenceValue: number | undefined =
  rolledCpuOnSequenceEvent.get('cpu');
const rolledCpuOnSequenceHost: string | undefined =
  rolledCpuOnSequenceEvent.get('host');
const rolledCpuOnSequenceHealthy: boolean | undefined =
  rolledCpuOnSequenceEvent.get('healthy');
const rolledCpuOnSequenceTypedEvent: EventForSchema<RolledCpuSequenceSchema> =
  rolledCpuOnSequenceEvent;
void rolledCpuOnSequenceKey;
void rolledCpuOnSequenceValue;
void rolledCpuOnSequenceHost;
void rolledCpuOnSequenceHealthy;
void rolledCpuOnSequenceTypedEvent;

const smoothedCpuSeries = cpuSeries.smooth('cpu', 'ema', { alpha: 0.5 });
type SmoothedCpuSchema = SmoothSchema<typeof cpuSchema, 'cpu'>;
const smoothedCpuEvent = smoothedCpuSeries.first();
if (!smoothedCpuEvent) {
  throw new Error('missing smoothed cpu event');
}
const smoothedCpuKey: Time = smoothedCpuEvent.key();
const smoothedCpuValue: number | undefined = smoothedCpuEvent.get('cpu');
const smoothedCpuHost: string = smoothedCpuEvent.get('host');
const smoothedCpuHealthy: boolean = smoothedCpuEvent.get('healthy');
const smoothedCpuTypedEvent: EventForSchema<SmoothedCpuSchema> =
  smoothedCpuEvent;
void smoothedCpuKey;
void smoothedCpuValue;
void smoothedCpuHost;
void smoothedCpuHealthy;
void smoothedCpuTypedEvent;

const appendedSmoothedCpuSeries = cpuSeries.smooth('cpu', 'ema', {
  alpha: 0.5,
  output: 'cpuEma',
});
type AppendedSmoothedCpuSchema = SmoothAppendSchema<typeof cpuSchema, 'cpuEma'>;
const appendedSmoothedCpuEvent = appendedSmoothedCpuSeries.first();
if (!appendedSmoothedCpuEvent) {
  throw new Error('missing appended smoothed cpu event');
}
const appendedSmoothedCpuValue: number = appendedSmoothedCpuEvent.get('cpu');
const appendedSmoothedCpuOutput: number | undefined =
  appendedSmoothedCpuEvent.get('cpuEma');
const appendedSmoothedCpuHost: string = appendedSmoothedCpuEvent.get('host');
const appendedSmoothedCpuTypedEvent: EventForSchema<AppendedSmoothedCpuSchema> =
  appendedSmoothedCpuEvent;
void appendedSmoothedCpuValue;
void appendedSmoothedCpuOutput;
void appendedSmoothedCpuHost;
void appendedSmoothedCpuTypedEvent;

const loessSmoothedCpuSeries = cpuSeries.smooth('cpu', 'loess', {
  span: 0.75,
  output: 'cpuLoess',
});
type LoessSmoothedCpuSchema = SmoothAppendSchema<typeof cpuSchema, 'cpuLoess'>;
const loessSmoothedCpuEvent = loessSmoothedCpuSeries.first();
if (!loessSmoothedCpuEvent) {
  throw new Error('missing loess smoothed cpu event');
}
const loessSmoothedCpuValue: number = loessSmoothedCpuEvent.get('cpu');
const loessSmoothedCpuOutput: number | undefined =
  loessSmoothedCpuEvent.get('cpuLoess');
const loessSmoothedCpuTypedEvent: EventForSchema<LoessSmoothedCpuSchema> =
  loessSmoothedCpuEvent;
void loessSmoothedCpuValue;
void loessSmoothedCpuOutput;
void loessSmoothedCpuTypedEvent;

// @ts-expect-error host is not a numeric smoothing target
const badSmoothedHostSeries = cpuSeries.smooth('host', 'ema', { alpha: 0.5 });
void badSmoothedHostSeries;

const hostSchema = [
  { name: 'time', kind: 'time' },
  { name: 'host', kind: 'string' },
] as const;

const hostSeries = new TimeSeries({
  name: 'hosts',
  schema: hostSchema,
  rows: [
    [new Date('2025-01-01T00:00:00.000Z'), 'api-1'],
    [new Date('2025-01-01T00:01:00.000Z'), 'api-2'],
  ],
});

const alignedHostSeries = hostSeries.align(Sequence.every('1m'), {
  method: 'hold',
  range: new TimeRange({ start: 1735689600000, end: 1735689660000 }),
});

const joinedAlignedSeries = alignedCpuSeries.join(alignedHostSeries);
const joinedLeftSeries = alignedCpuSeries.join(alignedHostSeries, {
  type: 'left',
});
const joinedInnerSeries = alignedCpuSeries.join(alignedHostSeries, {
  type: 'inner',
});
const prefixedJoinedAlignedSeries = alignedCpuSeries.join(alignedHostSeries, {
  onConflict: 'prefix',
  prefixes: ['cpu', 'host'] as const,
});
type JoinedAlignedSchema = JoinSchema<
  AlignSchema<typeof cpuSchema>,
  AlignSchema<typeof hostSchema>
>;
type PrefixedJoinedAlignedSchema = PrefixedJoinSchema<
  AlignSchema<typeof cpuSchema>,
  AlignSchema<typeof hostSchema>,
  readonly ['cpu', 'host']
>;
const joinedAlignedEvent = joinedAlignedSeries.first();
if (!joinedAlignedEvent) {
  throw new Error('missing joined aligned event');
}
const prefixedJoinedAlignedEvent = prefixedJoinedAlignedSeries.first();
if (!prefixedJoinedAlignedEvent) {
  throw new Error('missing prefixed joined aligned event');
}
const joinedAlignedKey: Interval = joinedAlignedEvent.key();
const joinedAlignedCpu: number | undefined = joinedAlignedEvent.get('cpu');
const joinedAlignedHost: string | undefined = joinedAlignedEvent.get('host');
const joinedAlignedHealthy: boolean | undefined =
  joinedAlignedEvent.get('healthy');
const joinedAlignedTypedEvent: EventForSchema<JoinedAlignedSchema> =
  joinedAlignedEvent;
void joinedAlignedKey;
void joinedAlignedCpu;
void joinedAlignedHost;
void joinedAlignedHealthy;
void joinedAlignedTypedEvent;
const prefixedJoinedAlignedCpu: number | undefined =
  prefixedJoinedAlignedEvent.get('cpu');
const prefixedJoinedAlignedHost: string | undefined =
  prefixedJoinedAlignedEvent.get('host_host');
const prefixedJoinedAlignedHealthy: boolean | undefined =
  prefixedJoinedAlignedEvent.get('healthy');
const prefixedJoinedAlignedTypedEvent: EventForSchema<PrefixedJoinedAlignedSchema> =
  prefixedJoinedAlignedEvent;
void prefixedJoinedAlignedCpu;
void prefixedJoinedAlignedHost;
void prefixedJoinedAlignedHealthy;
void prefixedJoinedAlignedTypedEvent;
const joinedLeftEvent = joinedLeftSeries.first();
const joinedInnerEvent = joinedInnerSeries.first();
void joinedLeftEvent;
void joinedInnerEvent;

const joinConflictError: JoinConflictMode = 'error';
const joinConflictPrefix: JoinConflictMode = 'prefix';
const joinTypeLeft: JoinType = 'left';
const joinTypeRight: JoinType = 'right';
const joinTypeInner: JoinType = 'inner';
const joinTypeOuter: JoinType = 'outer';
void joinConflictError;
void joinConflictPrefix;
void joinTypeLeft;
void joinTypeRight;
void joinTypeInner;
void joinTypeOuter;

const statusSchema = [
  { name: 'interval', kind: 'interval' },
  { name: 'status', kind: 'string' },
] as const;

const statusSeries = new TimeSeries({
  name: 'status',
  schema: statusSchema,
  rows: [
    [
      new Interval({
        value: 1735689600000,
        start: 1735689600000,
        end: 1735689660000,
      }),
      'ok',
    ],
  ],
});

const joinedManySeries = TimeSeries.joinMany([
  alignedCpuSeries,
  alignedHostSeries,
  statusSeries,
]);
const prefixedJoinedManySeries = TimeSeries.joinMany(
  [alignedCpuSeries, alignedHostSeries, statusSeries],
  {
    onConflict: 'prefix',
    prefixes: ['cpu', 'host', 'status'] as const,
  },
);
type JoinedManyAlignedSchema = JoinManySchema<
  readonly [
    AlignSchema<typeof cpuSchema>,
    AlignSchema<typeof hostSchema>,
    typeof statusSchema,
  ]
>;
type PrefixedJoinedManyAlignedSchema = PrefixedJoinManySchema<
  readonly [
    AlignSchema<typeof cpuSchema>,
    AlignSchema<typeof hostSchema>,
    typeof statusSchema,
  ],
  readonly ['cpu', 'host', 'status']
>;
const joinedManyEvent = joinedManySeries.first();
if (!joinedManyEvent) {
  throw new Error('missing joinMany event');
}
const prefixedJoinedManyEvent = prefixedJoinedManySeries.first();
if (!prefixedJoinedManyEvent) {
  throw new Error('missing prefixed joinMany event');
}
const joinedManyKey: Interval = joinedManyEvent.key();
const joinedManyCpu: number | undefined = joinedManyEvent.get('cpu');
const joinedManyHost: string | undefined = joinedManyEvent.get('host');
const joinedManyHealthy: boolean | undefined = joinedManyEvent.get('healthy');
const joinedManyStatus: string | undefined = joinedManyEvent.get('status');
const joinedManyTypedEvent: EventForSchema<JoinedManyAlignedSchema> =
  joinedManyEvent;
void joinedManyKey;
void joinedManyCpu;
void joinedManyHost;
void joinedManyHealthy;
void joinedManyStatus;
void joinedManyTypedEvent;
const prefixedJoinedManyCpu: number | undefined =
  prefixedJoinedManyEvent.get('cpu');
const prefixedJoinedManyHost: string | undefined =
  prefixedJoinedManyEvent.get('host_host');
const prefixedJoinedManyHealthy: boolean | undefined =
  prefixedJoinedManyEvent.get('healthy');
const prefixedJoinedManyStatus: string | undefined =
  prefixedJoinedManyEvent.get('status');
const prefixedJoinedManyTypedEvent: EventForSchema<PrefixedJoinedManyAlignedSchema> =
  prefixedJoinedManyEvent;
void prefixedJoinedManyCpu;
void prefixedJoinedManyHost;
void prefixedJoinedManyHealthy;
void prefixedJoinedManyStatus;
void prefixedJoinedManyTypedEvent;

// @ts-expect-error prefixed join renames duplicate host columns
const badPrefixedJoinedHost = prefixedJoinedAlignedEvent.get('host');
void badPrefixedJoinedHost;

const hasFirstCpuKey: boolean = cpuSeries.includesKey(new Time(1735689600000));
const cpuInsertIndex: number = cpuSeries.bisect(new Time(1735689630000));
const cpuAtOrBefore = cpuSeries.atOrBefore(new Time(1735689630000));
const cpuAtOrAfter = cpuSeries.atOrAfter(new Time(1735689630000));
void hasFirstCpuKey;
void cpuInsertIndex;
if (!cpuAtOrBefore || !cpuAtOrAfter) {
  throw new Error('missing bisected events');
}
const beforeCpuValue: number = cpuAtOrBefore.get('cpu');
const afterCpuHost: string = cpuAtOrAfter.get('host');
void beforeCpuValue;
void afterCpuHost;

const appendedSeries = trafficSeries.collapse(
  ['in', 'out'],
  'avg',
  ({ in: inValue, out }) => (inValue + out) / 2,
  { append: true },
);
const appendedEvent = appendedSeries.at(0);
if (!appendedEvent) {
  throw new Error('missing appended event');
}
const appendedIn: number = appendedEvent.get('in');
const appendedOut: number = appendedEvent.get('out');
const appendedAvg: number = appendedEvent.get('avg');
void appendedIn;
void appendedOut;
void appendedAvg;

const avgSchema = [
  { name: 'time', kind: 'time' },
  { name: 'avg', kind: 'number' },
] as const;

const mappedSeries = trafficSeries.map(avgSchema, (event) =>
  event.collapse(
    ['in', 'out'],
    'avg',
    ({ in: inValue, out }) => (inValue + out) / 2,
  ),
);
const mappedEvent = mappedSeries.at(0);
if (!mappedEvent) {
  throw new Error('missing mapped event');
}
const mappedAvg: number = mappedEvent.get('avg');
void mappedAvg;

const enrichedCpuSchema = [
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'number' },
  { name: 'host', kind: 'string' },
  { name: 'healthy', kind: 'boolean' },
] as const;

const enrichedCpuSeries = cpuSeries.map(enrichedCpuSchema, (event) =>
  event.merge({ healthy: event.get('cpu') < 0.9 }),
);
const enrichedCpuEvent = enrichedCpuSeries.at(0);
if (!enrichedCpuEvent) {
  throw new Error('missing enriched event');
}
const enrichedHealthy: boolean = enrichedCpuEvent.get('healthy');
void enrichedHealthy;

const renamedCpuSeries = cpuSeries.rename({ cpu: 'usage', host: 'server' });
const renamedCpuEvent = renamedCpuSeries.at(0);
if (!renamedCpuEvent) {
  throw new Error('missing renamed event');
}
const renamedCpuUsage: number = renamedCpuEvent.get('usage');
const renamedCpuServer: string = renamedCpuEvent.get('server');
void renamedCpuUsage;
void renamedCpuServer;

// @ts-expect-error collapsed event no longer has "in"
const badCollapsedIn = collapsedSeriesEvent.get('in');
void badCollapsedIn;

// @ts-expect-error selected series event no longer has cpu
const badSelectedSeriesCpu = selectedSeriesEvent.get('cpu');
void badSelectedSeriesCpu;

// @ts-expect-error renamed series event no longer has host
const badRenamedSeriesHost = renamedCpuEvent.get('host');
void badRenamedSeriesHost;

// @ts-expect-error mapped event no longer has "in"
const badMappedIn = mappedEvent.get('in');
void badMappedIn;

// @ts-expect-error merged source is string
const badMergedSource: number = mergedNth.get('source');
void badMergedSource;

// @ts-expect-error - wrong first column type for "time"
const badTime: Row = ['not-a-time', 10, 'bad'];
void badTime;

// @ts-expect-error - wrong second column type for "number"
const badValue: Row = [Date.now(), 'NaN', 'bad'];
void badValue;

new TimeSeries({
  name: 'bad-shape',
  schema,
  // @ts-expect-error - wrong row shape (missing label)
  rows: [[Date.now(), 1]],
});
