/**
 * Wiring test for the `pond-ts/types` subpath export.
 *
 * The type-test in `test-d/types-public.test-d.ts` pins what
 * `src/types-public.ts` exposes. This test pins the
 * `package.json#exports."./types"` key that makes those types
 * resolvable through the published subpath. If either side
 * regresses, downstream consumers using
 * `import type ... from 'pond-ts/types'` break silently — TS
 * doesn't run npm-resolution checks against the real exports map.
 */
import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

const PKG_DIR = join(__dirname, '..');
const pkg = JSON.parse(readFileSync(join(PKG_DIR, 'package.json'), 'utf8')) as {
  exports: Record<string, { types?: string; import?: string } | undefined>;
};

describe('pond-ts/types subpath export', () => {
  it('package.json declares `./types` with both `types` and `import`', () => {
    const entry = pkg.exports['./types'];
    expect(entry, 'package.json exports."./types" missing').toBeDefined();
    expect(entry?.types).toBe('./dist/types-public.d.ts');
    expect(entry?.import).toBe('./dist/types-public.js');
  });

  it('points at files that build emits (dist/types-public.{d.ts,js})', () => {
    const entry = pkg.exports['./types']!;
    const types = join(PKG_DIR, entry.types!);
    const runtime = join(PKG_DIR, entry.import!);
    expect(existsSync(types), `${types} missing — run \`npm run build\``).toBe(
      true,
    );
    expect(
      existsSync(runtime),
      `${runtime} missing — run \`npm run build\``,
    ).toBe(true);
  });

  it('runtime payload is empty (the entry ships no JS code)', () => {
    const entry = pkg.exports['./types']!;
    const runtime = readFileSync(join(PKG_DIR, entry.import!), 'utf8').trim();
    // Tolerant match: tsc emits `export {};` for type-only modules
    // and may append a sourceMappingURL comment.
    expect(runtime.startsWith('export {};')).toBe(true);
  });
});
