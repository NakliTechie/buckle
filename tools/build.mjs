/**
 * Build the single ship artifact dist/index.html (D1: one HTML file at ship;
 * inlining is the ship step, the checkpoint tests the inlined artifact).
 *   1. transpile is assumed done (engine.esm.js present) — we bundle from TS anyway
 *   2. precompute the cold-open worked example (D8: findings ready at open)
 *   3. bundle the sweep Worker (iife) and the app (esm) with esbuild
 *   4. inject version + both bundles into app/index.html → dist/index.html
 */
import { build } from 'esbuild';
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { execSync } from 'node:child_process';
import { Engine, PRESETS } from '../engine.esm.js';
import { sweep } from '../app/sweep-core.js';
import { heuristicExtract } from '../app/extract-heuristic.js';

const root = new URL('..', import.meta.url).pathname;
mkdirSync(root + 'dist', { recursive: true });

// --- version ---
let version = 'v1.0.0';
try {
  const sha = execSync('git rev-parse --short HEAD', { cwd: root }).toString().trim();
  const date = execSync('git show -s --format=%cs HEAD', { cwd: root }).toString().trim();
  version = `v1.0.0 · ${sha} · ${date}`;
} catch { /* not a git repo */ }

// --- cold open: the bundled Chatwoot worked example (D8/D12) ---
// Read the real chatwoot slice (pinned fixture), extract its topology with the
// same deterministic heuristic the live app uses, precompute the findings.
const chatwoot = JSON.parse(readFileSync(root + 'tools/fixtures/chatwoot-analyze.json', 'utf8'));
const cw = heuristicExtract(chatwoot);
if (!cw) throw new Error('cold-open: chatwoot fixture yielded no topology');
console.log(`[build] precomputing cold-open findings for chatwoot (${cw.topology.nodes.length} nodes)…`);
const coldResult = sweep(Engine, cw.topology, { seed: 42 });
const coldLoad = coldResult.findings.find((f) => f.kind === 'knee')?.at_rps || coldResult.system.peakRps || 50;
writeFileSync(root + 'app/cold-open.json', JSON.stringify({
  topology: cw.topology,
  provenance: cw.provenance,
  findings: coldResult,
  load: coldLoad,
  source: cw.source,
  note: cw.note,
}));

// --- bundle the sweep worker (iife, self-contained) ---
const sweepBundle = await build({
  entryPoints: [root + 'app/sweep-entry.js'],
  bundle: true, format: 'iife', write: false, logLevel: 'warning',
  target: 'es2022',
});
const sweepSrc = sweepBundle.outputFiles[0].text;

// --- bundle the app (esm module) ---
const appBundle = await build({
  entryPoints: [root + 'app/ui.js'],
  bundle: true, format: 'esm', write: false, logLevel: 'warning',
  target: 'es2022', loader: { '.json': 'json' },
});
const appSrc = appBundle.outputFiles[0].text;

// --- assemble ---
let html = readFileSync(root + 'app/index.html', 'utf8');
html = html.replaceAll('__VERSION__', version);
// Use function replacers so `$` in bundle text is not treated as a replacement pattern.
html = html.replace('__SWEEP_WORKER__', () => sweepSrc);
html = html.replace('__APP_BUNDLE__', () => appSrc);

writeFileSync(root + 'dist/index.html', html);
const kb = (html.length / 1024).toFixed(0);
console.log(`[build] dist/index.html written — ${kb} KB (sweep ${(sweepSrc.length/1024).toFixed(0)}KB, app ${(appSrc.length/1024).toFixed(0)}KB) · ${version}`);
