/**
 * validateTopology sanity: all 23 bundled presets pass the one ingress (I3);
 * a deliberately mangled topology fails with actionable errors; NODE_KINDS
 * covers every kind the presets actually use (guards the hand-mirrored list
 * in app/validate.js against upstream drift on re-vendor). Exit 0/1.
 */
import { PRESETS } from '../engine.esm.js';
import { validateTopology, NODE_KINDS } from '../app/validate.js';

let failed = 0;
const fail = (msg) => { console.error(msg); failed = 1; };

for (const preset of PRESETS) {
  const r = validateTopology(preset.topology);
  if (!r.ok) fail(`preset ${preset.id} rejected: ${r.errors.join('; ')}`);
}

const usedKinds = new Set(PRESETS.flatMap((p) => p.topology.nodes.map((n) => n.kind)));
for (const kind of usedKinds) {
  if (!NODE_KINDS.includes(kind)) fail(`preset kind "${kind}" missing from NODE_KINDS`);
}

const bad = {
  nodes: [
    { id: 'a', kind: 'flooble', label: 1, x: NaN, y: 0, config: { capacity: -1 } },
    { id: 'a', kind: 'service', label: 'dup', x: 0, y: 0, config: {} },
  ],
  edges: [{ id: 'e', from: 'a', to: 'ghost', weight: -2 }],
};
const r = validateTopology(bad);
if (r.ok) fail('mangled topology was accepted');
else if (r.errors.length < 5) fail(`expected >= 5 errors on mangled topology, got ${r.errors.length}`);

if (!failed) console.log(`validateTopology: ${PRESETS.length} presets pass, ${usedKinds.size} kinds covered, mangled input rejected`);
process.exit(failed);
