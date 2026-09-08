/**
 * `@pond-ts/financial/catalog` — every study, described at runtime.
 *
 * ```ts
 * import { STUDIES } from '@pond-ts/financial/catalog';
 * for (const study of STUDIES) registry.declare(toOpDef(study));
 * ```
 *
 * The descriptors are the facts a registry or a picker needs — inputs,
 * params with defaults and ranges, output columns with units — that the
 * package otherwise carries only in types TypeScript erases. Each one is
 * checked against its study's options interface at compile time
 * (`defineStudy`) and run against the study in `test/catalog.test.ts`, so a
 * study cannot change its defaults or its columns without the catalog
 * following. A study that is missing from the catalog fails that test too:
 * the catalog is exactly the set of fluent methods.
 *
 * A separate subpath so the main entry's tree-shaking is untouched —
 * importing the catalog pulls in every study, which is what a registry
 * wants and a one-study consumer does not.
 */
export type {
  StudyDescriptor,
  StudyFamily,
  StudyUnit,
  StudyInput,
  StudyParam,
  StudyNumberParam,
  StudyEnumParam,
  StudyOutput,
  StudyNaming,
  StudyRun,
} from './types.js';
export { STUDY_FAMILIES } from './types.js';
export type { StudySpec } from './define.js';
export { defineStudy } from './define.js';

import type { StudyDescriptor } from './types.js';
import { MOVING_AVERAGE_STUDIES } from './moving-average.js';
import { BANDS_STUDIES } from './bands.js';
import { MOMENTUM_STUDIES } from './momentum.js';
import { TREND_STUDIES } from './trend.js';
import { VOLATILITY_STUDIES } from './volatility.js';
import { VOLUME_STUDIES } from './volume.js';
import { STATISTICAL_STUDIES } from './statistical.js';
import { PRICE_STUDIES } from './price.js';
import { SESSION_STUDIES } from './session.js';

/** Every study, in family order (`STUDY_FAMILIES`) and menu order within a family. */
export const STUDIES: readonly StudyDescriptor[] = [
  ...MOVING_AVERAGE_STUDIES,
  ...BANDS_STUDIES,
  ...MOMENTUM_STUDIES,
  ...TREND_STUDIES,
  ...VOLATILITY_STUDIES,
  ...VOLUME_STUDIES,
  ...STATISTICAL_STUDIES,
  ...PRICE_STUDIES,
  ...SESSION_STUDIES,
];

/** Look a study up by its exported name; `undefined` when it is not in the catalog. */
export function studyDescriptor(name: string): StudyDescriptor | undefined {
  return STUDIES.find((d) => d.name === name);
}
