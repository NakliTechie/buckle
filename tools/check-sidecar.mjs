/**
 * Gate C5 (sidecar): the deterministic compose→topology heuristic yields a
 * valid graph with resolving READ provenance; the model path validates a
 * FakeTransport response through the one ingress; and the BYOK key is passed
 * per-call, never handed to any storage function (I6/D6, checked at the source
 * level since there is no DOM here). Exit 0/1.
 */
import { readFileSync } from 'node:fs';
import { heuristicExtract } from '../app/extract-heuristic.js';
import { validateTopology } from '../app/validate.js';
import { extractWithModel, makeFakeTransport } from '../app/extract-model.js';

let failed = 0;
const check = (name, cond, detail = '') => { console.log(`  ${cond ? 'ok  ' : 'FAIL'} ${name}${detail ? ' — ' + detail : ''}`); if (!cond) failed = 1; };

// A compose-carrying analyze result (chatwoot-shaped).
const analyze = {
  repo: 'demo/app', resolvedSha: 'abc1234',
  selected: [{
    path: 'docker-compose.yaml', tier: 1, label: 'orchestration', size: 300,
    content: `services:
  web:
    image: rails
  sidekiq:
    command: bundle exec sidekiq
  postgres:
    image: postgres:15
  redis:
    image: redis:7
`,
  }],
};

const ex = heuristicExtract(analyze);
check('heuristic returns a topology', !!ex);
if (ex) {
  const v = validateTopology(ex.topology);
  check('heuristic topology validates', v.ok, v.ok ? '' : v.errors.slice(0, 3).join('; '));
  const kinds = new Set(ex.topology.nodes.map((n) => n.kind));
  for (const k of ['service', 'worker', 'db', 'cache']) check(`has a ${k} node`, kinds.has(k));
  // Every service node carries a resolving READ tag (file present, line in range).
  const composeLines = analyze.selected[0].content.split('\n').length;
  let readOk = true;
  for (const [id, p] of Object.entries(ex.provenance)) {
    if (id === 'client') continue;
    if (p.kind?.src !== 'READ' || p.kind.file !== 'docker-compose.yaml' || p.kind.line < 1 || p.kind.line > composeLines) readOk = false;
  }
  check('every service node has a resolving READ tag', readOk);
  // Every numeric field is provenance-tagged (I2).
  let tagged = true;
  for (const p of Object.values(ex.provenance)) for (const t of Object.values(p.config || {})) if (t.src !== 'READ' && t.src !== 'ASSUMED') tagged = false;
  check('every numeric field tagged READ or ASSUMED', tagged);
}

// Model path through FakeTransport: a recorded response validates.
const recorded = JSON.stringify({
  nodes: [
    { id: 'client', kind: 'client', label: 'Client', x: 60, y: 200, config: { capacity: 1, serviceMs: 0, serviceCv: 0, queueLimit: 0, hitRate: 0, errorRate: 0, timeoutMs: 2000, retries: 0, rps: 50 } },
    { id: 'web', kind: 'service', label: 'web', x: 260, y: 200, config: { capacity: 16, serviceMs: 40, serviceCv: 0.5, queueLimit: 200, hitRate: 0, errorRate: 0, timeoutMs: 2000, retries: 0 } },
  ],
  edges: [{ id: 'client->web', from: 'client', to: 'web', weight: 1 }],
});
const res = await extractWithModel(analyze, { transport: makeFakeTransport(recorded), apiKey: 'unused' });
check('FakeTransport response validates through one ingress', !!res.topology && res.topology.nodes.length === 2);

// Source-level key-safety: no storage module is imported by the extractors,
// so a key can't be persisted from there.
const modelSrc = readFileSync(new URL('../app/extract-model.js', import.meta.url), 'utf8');
const heurSrc = readFileSync(new URL('../app/extract-heuristic.js', import.meta.url), 'utf8');
const uiSrc = readFileSync(new URL('../app/ui.js', import.meta.url), 'utf8');
check('extractors never import the storage module', !/from ['"]\.\/storage/.test(modelSrc) && !/from ['"]\.\/storage/.test(heurSrc));
check('key never written to localStorage/storage tiers', !/localStorage\.setItem\([^)]*apiKey|saveDesign\([^)]*apiKey|ghToken[^\n]*localStorage/.test(uiSrc));

process.exit(failed);
