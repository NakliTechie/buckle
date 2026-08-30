/**
 * Parity lint (D10, rule ⑥): every command registered on the bus must have a
 * manifest entry — manifest ⊇ bus. Also checks the C1 scaffold trio exists
 * on both sides. Exit 0/1.
 */
import * as engine from '../engine.esm.js';
import { MANIFEST } from '../app/manifest.js';
import { createRuntime } from '../app/runtime.js';

const bus = createRuntime(engine).bus;
const manifestNames = new Set(MANIFEST.map((t) => t.name));
const busNames = bus.names();

const missing = busNames.filter((n) => !manifestNames.has(n));
if (missing.length) {
  console.error(`bus commands missing from manifest: ${missing.join(', ')}`);
  process.exit(1);
}

const required = ['load_preset', 'get_snapshot', 'set_load'];
const absent = required.filter((n) => !busNames.includes(n) || !manifestNames.has(n));
if (absent.length) {
  console.error(`C1 scaffold requires on both bus and manifest: ${absent.join(', ')}`);
  process.exit(1);
}

console.log(
  `parity: manifest ${manifestNames.size} tools ⊇ bus ${busNames.length} commands`,
);
process.exit(0);
