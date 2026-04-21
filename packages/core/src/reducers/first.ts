import type { ScalarValue } from '../types.js';
import type { ReducerDef } from './types.js';
import { rollingOrderedEntries } from './rolling.js';

export const first: ReducerDef = {
  outputKind: 'source',
  reduce(defined) {
    return defined[0];
  },
  bucketState() {
    let val: ScalarValue | undefined;
    return {
      add(v) {
        if (val === undefined && v !== undefined) val = v;
      },
      snapshot() {
        return val;
      },
    };
  },
  rollingState() {
    return rollingOrderedEntries((entries, head) => entries[head]?.value);
  },
};
