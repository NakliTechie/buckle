/**
 * Fixture generator, not a test of Buckle: runs the NATIVE breakscale TS
 * (vendor-src clone at the pinned commit, untranspiled) and records the
 * snapshot for PRESETS[0], seed 42, 600 ticks at 60fps. gate-c1 then demands
 * the inlined engine.esm.js reproduce it byte-for-byte.
 *
 * Needs vendor-src/ present (see engine/VENDOR.md). Run:
 *   npx vitest run tools/gen-native-fixture.test.ts
 */
import { writeFileSync } from 'node:fs';
import { it } from 'vitest';
import { Engine } from '../vendor-src/src/sim/engine';
import { PRESETS } from '../vendor-src/src/sim/presets';

it('writes the native snapshot fixture', () => {
  const engine = new Engine(structuredClone(PRESETS[0].topology), 42);
  for (let i = 0; i < 600; i += 1) engine.advance(1000 / 60);
  writeFileSync(
    new URL('./fixtures/snapshot-preset0-seed42-600.json', import.meta.url),
    JSON.stringify(engine.snapshot()),
  );
});
