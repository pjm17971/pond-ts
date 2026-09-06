/**
 * [PND-LIVFIX] A listener set whose iteration snapshot is cached: `add` /
 * `delete` invalidate it, `snapshot()` rebuilds it lazily. Dispatch
 * iterates the snapshot, so a listener that subscribes or unsubscribes
 * mid-fan-out neither disturbs the others nor fires for the event it was
 * added during — without allocating an array per event on the hot path
 * (subscriptions change rarely; events arrive at kHz).
 */
export class ListenerSet<F extends (...args: never[]) => void> {
  readonly #set = new Set<F>();
  #snapshot: readonly F[] | null = null;
  get size(): number {
    return this.#set.size;
  }
  add(fn: F): void {
    this.#set.add(fn);
    this.#snapshot = null;
  }
  delete(fn: F): void {
    if (this.#set.delete(fn)) this.#snapshot = null;
  }
  clear(): void {
    this.#set.clear();
    this.#snapshot = null;
  }
  snapshot(): readonly F[] {
    return (this.#snapshot ??= Array.from(this.#set));
  }
  [Symbol.iterator](): Iterator<F> {
    return this.snapshot()[Symbol.iterator]();
  }
}
