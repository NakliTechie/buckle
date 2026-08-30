/**
 * Gate C1, determinism half: the bundled engine.esm.js must reproduce the
 * native-breakscale snapshot fixture byte-for-byte.
 * PRESETS[0], seed 42, 600 ticks at 60fps. Exit 0/1.
 */
import { readFileSync } from 'node:fs';
import { Engine, PRESETS } from '../engine.esm.js';

const fixture = readFileSync(
  new URL('./fixtures/snapshot-preset0-seed42-600.json', import.meta.url),
  'utf8',
);

const engine = new Engine(structuredClone(PRESETS[0].topology), 42);
for (let i = 0; i < 600; i += 1) engine.advance(1000 / 60);
const actual = JSON.stringify(engine.snapshot());

if (actual === fixture) {
  console.log(`snapshot byte-identical to native fixture (${fixture.length} bytes)`);
  process.exit(0);
}
console.error(
  `snapshot MISMATCH: fixture ${fixture.length} bytes, actual ${actual.length} bytes`,
);
process.exit(1);
