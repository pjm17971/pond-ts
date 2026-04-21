import type { ScalarValue } from '../types.js';

export type AggregateBucketState = {
  add(value: ScalarValue | undefined): void;
  snapshot(): ScalarValue | undefined;
};

export type RollingReducerState = {
  add(index: number, value: ScalarValue | undefined): void;
  remove(index: number, value: ScalarValue | undefined): void;
  snapshot(): ScalarValue | undefined;
};

export type ReducerDef = {
  outputKind: 'number' | 'source';
  reduce(
    defined: ReadonlyArray<ScalarValue>,
    numeric: ReadonlyArray<number>,
  ): ScalarValue | undefined;
  bucketState(): AggregateBucketState;
  rollingState(): RollingReducerState;
};
