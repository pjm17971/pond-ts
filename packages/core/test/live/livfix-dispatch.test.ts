import { describe, expect, it } from 'vitest';

import { LiveSeries } from '../../src/live/live-series.js';

/* -------------------------------------------------------------------------- */
/* [PND-LIVFIX] — the dispatch cluster: listener error isolation (audit §4.1), */
/* re-entrancy (§4.2) and chained dispose (§4.4). Each test here reproduces   */
/* one of the audit's confirmed wrong answers, on BOTH backings where the    */
/* backing matters (strict+time → chunked columnar; reorder → Event[]).       */
/* -------------------------------------------------------------------------- */

const SCHEMA = [
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number' },
] as const;

type Backing = 'chunked' | 'array';

function make(
  backing: Backing,
  retention?: { maxEvents?: number; maxAge?: string },
) {
  return backing === 'chunked'
    ? new LiveSeries({ name: 'l', schema: SCHEMA, retention })
    : new LiveSeries({
        name: 'l',
        schema: SCHEMA,
        retention,
        ordering: 'reorder',
        graceWindow: '1h',
      });
}

const rows = (from: number, n: number) =>
  Array.from({ length: n }, (_, i) => [from + i, from + i] as const);

describe.each<Backing>(['chunked', 'array'])(
  '[PND-LIVFIX] listener error isolation (%s backing)',
  (backing) => {
    it('a throwing listener does not skip retention', () => {
      const live = make(backing, { maxEvents: 3 });
      live.on('event', () => {
        throw new Error('boom');
      });
      expect(() => live.pushMany(rows(0, 5))).toThrow('boom');
      // Audit: buffer sat at 5 with maxEvents 3 until the next push.
      expect(live.length).toBe(3);
      expect(live.stats().evicted).toBe(2);
    });

    it('a throwing listener does not starve the listeners after it', () => {
      const live = make(backing);
      const seen: number[] = [];
      live.on('event', () => {
        throw new Error('boom');
      });
      live.on('event', (e) => seen.push(e.get('value') as number));
      expect(() => live.pushMany(rows(0, 3))).toThrow('boom');
      expect(seen).toEqual([0, 1, 2]);
    });

    it("'batch' and 'evict' still fire after a throwing 'event' listener", () => {
      const live = make(backing, { maxEvents: 2 });
      let batches = 0;
      let evicted = 0;
      live.on('event', () => {
        throw new Error('boom');
      });
      live.on('batch', (b) => {
        batches += b.length;
      });
      live.on('evict', (e) => {
        evicted += e.length;
      });
      expect(() => live.pushMany(rows(0, 4))).toThrow('boom');
      expect(batches).toBe(4);
      expect(evicted).toBe(2);
    });

    it('a derived filter() view stays in sync when an earlier listener throws', () => {
      const live = make(backing);
      live.on('event', () => {
        throw new Error('boom');
      });
      const view = live.filter(() => true);
      expect(() => live.pushMany(rows(0, 3))).toThrow('boom');
      // Audit: the view was permanently desynced and never healed.
      expect(view.length).toBe(live.length);
      expect(() => live.pushMany(rows(3, 2))).toThrow('boom');
      expect(view.length).toBe(live.length);
      expect(view.length).toBe(5);
    });

    it('the error surfaces once, after the push has fully committed', () => {
      const live = make(backing);
      let calls = 0;
      live.on('event', () => {
        calls += 1;
        throw new Error(`boom ${calls}`);
      });
      // Every event still reaches the listener (3 throws); the caller sees
      // the first one, after all three rows are in the buffer.
      expect(() => live.pushMany(rows(0, 3))).toThrow('boom 1');
      expect(calls).toBe(3);
      expect(live.length).toBe(3);
      expect(live.stats().ingested).toBe(3);
    });
  },
);

describe.each<Backing>(['chunked', 'array'])(
  '[PND-LIVFIX] re-entrancy (%s backing)',
  (backing) => {
    it('a listener removing a LATER listener during dispatch does not rob it of the current event', () => {
      // Set iteration skips an element deleted before it is visited, so
      // on main `b` never saw event 0. The snapshot delivers the event to
      // everyone who was subscribed when it arrived.
      const live = make(backing);
      const seen: string[] = [];
      let offB: (() => void) | undefined;
      live.on('event', () => {
        seen.push('a');
        offB?.();
      });
      offB = live.on('event', () => seen.push('b'));
      live.pushMany(rows(0, 2));
      expect(seen).toEqual(['a', 'b', 'a']);
    });

    it('queued pushes drain strictly FIFO, even when a queued push queues more', () => {
      // Review repro: listener on 1 queues 10 then 20; listener on 10
      // queues 30. Draining from every guard level ran 30 before 20 and
      // strict ordering rejected 20. Only the outermost guard drains.
      const live = make(backing);
      const seen: number[] = [];
      live.on('event', (e) => {
        const v = e.get('value') as number;
        seen.push(v);
        if (v === 1) {
          live.push([10, 10]);
          live.push([20, 20]);
        }
        if (v === 10) live.push([30, 30]);
      });
      live.push([1, 1]);
      expect(seen).toEqual([1, 10, 20, 30]);
      expect(live.stats().rejected).toBe(0);
      expect(live.length).toBe(4);
    });

    it('a re-entrant push on the trusted path is queued on the chunked backing too', () => {
      // `_pushTrustedEvents` is the partition router's entry; its chunked
      // branch used to bypass the guard.
      const live = make(backing);
      const seen: number[] = [];
      live.on('event', (e) => {
        const v = e.get('value') as number;
        seen.push(v);
        if (v === 0) {
          // A schema-identical event built on a scratch series, as the
          // partition router would hand one through.
          const scratch = make(backing);
          scratch.push([10, 10]);
          (
            live as unknown as {
              _pushTrustedEvents(evs: readonly unknown[]): void;
            }
          )._pushTrustedEvents([scratch.at(0)!]);
        }
      });
      live.pushMany(rows(0, 3));
      expect(seen).toEqual([0, 1, 2, 10]);
    });

    it('a listener adding a listener during dispatch does not fire it for the current event', () => {
      const live = make(backing);
      const seen: number[] = [];
      live.on('event', (e) => {
        if ((e.get('value') as number) === 0) {
          live.on('event', (x) => seen.push(x.get('value') as number));
        }
      });
      live.pushMany(rows(0, 3));
      expect(seen).toEqual([1, 2]);
    });

    it('a re-entrant push from a listener is delivered after the outer batch, in order', () => {
      const live = make(backing);
      const seen: number[] = [];
      live.on('event', (e) => {
        const v = e.get('value') as number;
        seen.push(v);
        // Audit: on the chunked backing this was spuriously rejected as
        // out-of-order; on the array backing emission went non-monotonic.
        if (v === 0) live.push([10, 10]);
      });
      live.pushMany(rows(0, 3));
      expect(seen).toEqual([0, 1, 2, 10]);
      expect(live.length).toBe(4);
      expect(live.stats().rejected).toBe(0);
      expect(live.stats().ingested).toBe(4);
    });

    it('a view over a series whose listener pushes re-entrantly does not break', () => {
      const live = make(backing);
      const view = live.filter(() => true);
      live.on('event', (e) => {
        if ((e.get('value') as number) === 0) live.push([10, 10]);
      });
      // Audit: `[object Object]` ValidationError from the view.
      expect(() => live.pushMany(rows(0, 3))).not.toThrow();
      expect(view.length).toBe(4);
      expect(view.last()!.get('value')).toBe(10);
    });
  },
);

describe.each<Backing>(['chunked', 'array'])(
  '[PND-LIVFIX] isolation on the other fan-outs (%s backing)',
  (backing) => {
    it("a throwing view subscriber does not starve the view's other subscribers", () => {
      const live = make(backing);
      const view = live.filter(() => true);
      const seen: number[] = [];
      view.on('event', () => {
        throw new Error('view boom');
      });
      view.on('event', (e) => seen.push(e.get('value') as number));
      expect(() => live.pushMany(rows(0, 2))).toThrow('view boom');
      expect(seen).toEqual([0, 1]);
      expect(view.length).toBe(2);
    });

    it("clear(): a throwing 'evict' listener neither stops the clear nor the other listeners", () => {
      const live = make(backing);
      live.pushMany(rows(0, 3));
      let seen = 0;
      live.on('evict', () => {
        throw new Error('evict boom');
      });
      live.on('evict', (e) => {
        seen += e.length;
      });
      expect(() => live.clear()).toThrow('evict boom');
      expect(live.length).toBe(0);
      expect(seen).toBe(3);
      expect(live.stats().evicted).toBe(3);
    });
  },
);

describe('[PND-LIVFIX] chained dispose', () => {
  it('disposing the final view of live.filter().map() stops the intermediate too', () => {
    const live = make('chunked');
    const out = live.filter(() => true).map((e) => e);
    live.pushMany(rows(0, 3));
    expect(out.length).toBe(3);
    out.dispose();
    live.pushMany(rows(3, 1000));
    // Audit: the unreachable intermediate kept processing every push into
    // an unbounded buffer. With no live listeners at all, nothing should
    // be materialised for the row path: the chunked backing only builds
    // Events when a row listener exists.
    expect(out.length).toBe(3);
    expect(
      (live as unknown as { _rowListenerCount: number })._rowListenerCount,
    ).toBe(0);
  });

  it('does not cascade into a source view that still has other subscribers', () => {
    const live = make('chunked');
    const mid = live.filter(() => true);
    const a = mid.map((e) => e);
    const b = mid.map((e) => e);
    a.dispose();
    live.pushMany(rows(0, 3));
    expect(mid.length).toBe(3);
    expect(b.length).toBe(3);
  });

  it('dispose is idempotent and clears the view own listeners', () => {
    const live = make('chunked');
    const view = live.filter(() => true);
    let seen = 0;
    view.on('event', () => {
      seen += 1;
    });
    view.dispose();
    view.dispose();
    live.pushMany(rows(0, 3));
    expect(seen).toBe(0);
    expect(view.length).toBe(0);
  });
});
