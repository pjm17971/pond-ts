import type { ScalarValue } from '../types.js';
import type { ReducerDef } from './types.js';

export const keep: ReducerDef = {
  outputKind: 'source',
  reduce(defined) {
    if (defined.length === 0) return undefined;
    const ref = defined[0]!;
    for (let i = 1; i < defined.length; i++) {
      if (defined[i] !== ref) return undefined;
    }
    return ref;
  },
  bucketState() {
    let ref: ScalarValue | undefined;
    let hasDefined = false;
    let allSame = true;
    return {
      add(v) {
        if (v === undefined) return;
        if (!hasDefined) {
          ref = v;
          hasDefined = true;
        } else if (v !== ref) allSame = false;
      },
      snapshot() {
        return hasDefined && allSame ? ref : undefined;
      },
    };
  },
  rollingState() {
    const counts = new Map<ScalarValue, number>();
    return {
      add(_i, v) {
        if (v === undefined) return;
        counts.set(v, (counts.get(v) ?? 0) + 1);
      },
      remove(_i, v) {
        if (v === undefined) return;
        const c = counts.get(v)! - 1;
        if (c === 0) counts.delete(v);
        else counts.set(v, c);
      },
      snapshot() {
        return counts.size === 1 ? counts.keys().next().value : undefined;
      },
    };
  },
};
