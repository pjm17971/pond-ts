/**
 * Terminal accessors on `LiveView` for the windowed-count case
 * surfaced by the gRPC experiment's M1 friction notes — the
 * `useCurrent(live, { cpu: 'count' }, { tail: '1m' }).cpu / 60`
 * boilerplate collapses to `live.window('1m').eventRate()`.
 *
 * `eventRate` is the events-per-second-from-window operator,
 * deliberately distinct from `LiveView.rate(columns)` which is the
 * per-column rate-of-change derivative.
 */
import { describe, expect, it } from 'vitest';
import { LiveSeries } from '../src/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'number' },
  { name: 'host', kind: 'string' },
] as const;

function makeLive(): LiveSeries<typeof schema> {
  return new LiveSeries({ name: 'test', schema });
}

describe('LiveView.count', () => {
  it('returns the current view buffer length', () => {
    const live = makeLive();
    const win = live.window('1m');
    expect(win.count()).toBe(0);

    live.push([Date.now(), 0.5, 'a']);
    expect(win.count()).toBe(1);

    live.push([Date.now() + 1, 0.6, 'b']);
    live.push([Date.now() + 2, 0.7, 'c']);
    expect(win.count()).toBe(3);
  });

  it('matches view.length exactly', () => {
    const live = makeLive();
    const win = live.window('1m');
    for (let i = 0; i < 10; i += 1) {
      live.push([Date.now() + i, i * 0.1, 'a']);
    }
    expect(win.count()).toBe(win.length);
  });

  it('drops as time-window evicts old events', () => {
    const live = makeLive();
    const win = live.window('1s');
    const t0 = Date.now();
    live.push([t0, 0.1, 'a']);
    live.push([t0 + 100, 0.2, 'a']);
    live.push([t0 + 200, 0.3, 'a']);
    expect(win.count()).toBe(3);

    // Push an event 5 seconds later; window drops everything before
    // (t - 1s) = t + 4000.
    live.push([t0 + 5000, 0.4, 'a']);
    expect(win.count()).toBe(1);
  });
});

describe('LiveView.eventRate', () => {
  it('returns count / windowSeconds for a time-based window', () => {
    const live = makeLive();
    const win = live.window('1m');
    // 60 events evenly spaced across 1 minute → 1 event/sec
    const t0 = Date.now();
    for (let i = 0; i < 60; i += 1) {
      live.push([t0 + i * 1000, 0.1, 'a']);
    }
    expect(win.eventRate()).toBeCloseTo(60 / 60, 6);
    expect(win.eventRate()).toBeCloseTo(1, 6);
  });

  it('matches the dashboard scenario (60s × 8 events/sec → 8.0/s)', () => {
    const live = makeLive();
    const win = live.window('1m');
    // Simulate 60 seconds of 8 events/sec — two ticks/sec × 4 hosts.
    // Push tick A (all 4 hosts), then tick B (all 4 hosts), so events
    // stay non-decreasing in time.
    const t0 = Date.now();
    for (let s = 0; s < 60; s += 1) {
      for (let h = 0; h < 4; h += 1) {
        live.push([t0 + s * 1000 + h, 0.5, `host-${h}`]);
      }
      for (let h = 0; h < 4; h += 1) {
        live.push([t0 + s * 1000 + 500 + h, 0.5, `host-${h}`]);
      }
    }
    expect(win.eventRate()).toBeCloseTo(8.0, 1);
  });

  it('throws on a count-based window (no denominator)', () => {
    const live = makeLive();
    const win = live.window(10);
    live.push([Date.now(), 0.5, 'a']);
    expect(() => win.eventRate()).toThrow(/time-based window/);
  });

  it('throws on a view that was not windowed (filter has no duration)', () => {
    const live = makeLive();
    const view = live.filter((e) => e.get('cpu') > 0);
    live.push([Date.now(), 0.5, 'a']);
    expect(() => view.eventRate()).toThrow(/time-based window/);
  });

  it('updates as the window evicts', () => {
    const live = makeLive();
    const win = live.window('1s');
    const t0 = Date.now();
    // First second: 4 events
    live.push([t0, 0.1, 'a']);
    live.push([t0 + 250, 0.2, 'a']);
    live.push([t0 + 500, 0.3, 'a']);
    live.push([t0 + 750, 0.4, 'a']);
    expect(win.eventRate()).toBeCloseTo(4.0, 6);

    // Push 5 seconds later — window drops everything older than (t - 1s)
    live.push([t0 + 5000, 0.5, 'a']);
    expect(win.eventRate()).toBeCloseTo(1.0, 6);
  });

  it('coexists with LiveView.rate(columns) — distinct operators with distinct returns', () => {
    // `rate(columns)` is the per-column derivative LiveView (one
    // event per source event with the rate-of-change of `cpu`).
    // `eventRate()` is events-per-second-from-window (one number).
    // Different operations, different return shapes.
    const live = makeLive();
    const win = live.window('1m');
    const rateView = win.rate(['cpu']);
    const t0 = Date.now();
    live.push([t0, 1.0, 'a']);
    live.push([t0 + 1000, 2.0, 'a']); // delta of 1.0 over 1s = rate of 1.0/s
    // rate(columns) returns a LiveView with a derivative event per source event
    expect(rateView.length).toBeGreaterThan(0);
    expect(typeof rateView.at(rateView.length - 1)).toBe('object');
    // eventRate() returns a number derived from view.length / windowSec
    expect(typeof win.eventRate()).toBe('number');
    expect(win.eventRate()).toBeCloseTo(2 / 60, 6);
  });
});

describe('LiveView.window construction surfaces', () => {
  it('LiveSeries.window(duration) produces a windowed LiveView', () => {
    const live = makeLive();
    const win = live.window('30s');
    live.push([Date.now(), 0.5, 'a']);
    expect(win.count()).toBe(1);
    // count/30s = 1/30 ≈ 0.0333/s
    expect(win.eventRate()).toBeCloseTo(1 / 30, 6);
  });

  it('LiveView.window(duration) chained off a filter also propagates', () => {
    const live = makeLive();
    const win = live.filter((e) => e.get('cpu') > 0).window('1m');
    live.push([Date.now(), 0.5, 'a']); // passes filter
    live.push([Date.now() + 1, 0, 'a']); // fails filter (cpu === 0)
    live.push([Date.now() + 2, 0.5, 'b']); // passes
    expect(win.count()).toBe(2);
    expect(win.eventRate()).toBeCloseTo(2 / 60, 6);
  });

  it('windowMs propagates through filter / map / select on a windowed view', () => {
    // `window('1m').filter(...).eventRate()` — the windowed view
    // is the SOURCE; the filter creates a child view that should
    // inherit the same time denominator. Without propagation this
    // would throw; with it, eventRate is meaningful (count divided
    // by the same 60s).
    const live = makeLive();
    const t0 = Date.now();
    live.push([t0, 0.5, 'a']);
    live.push([t0 + 1, 0, 'b']);
    live.push([t0 + 2, 0.7, 'c']);

    const filtered = live.window('1m').filter((e) => e.get('cpu') > 0);
    expect(filtered.count()).toBe(2);
    expect(filtered.eventRate()).toBeCloseTo(2 / 60, 6);

    const mapped = live.window('1m').map((e) => e);
    expect(mapped.count()).toBe(3);
    expect(mapped.eventRate()).toBeCloseTo(3 / 60, 6);

    const selected = live.window('1m').select('cpu', 'host');
    expect(selected.count()).toBe(3);
    expect(selected.eventRate()).toBeCloseTo(3 / 60, 6);
  });
});
