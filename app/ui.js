/**
 * Buckle UI controller. Vanilla; no framework. Owns the canvas, inspector,
 * sliders, findings, ingest and sidecar flow, keyboard, and the sweep Worker.
 * Every number on screen is the engine's own snapshot (D: nothing derived by
 * the UI). Ported mechanics from breakscale are logged in CRIB.md.
 */
import { Engine, PRESETS } from '../engine/entry';
import { createRuntime } from './runtime.js';
import { createAgentFace } from './manifest.js';
import {
  KIND_ICON, KIND_NAME, groupOfKind,
  formatMs, formatRate, formatRateBare, formatCount, formatPct,
  healthOfLoad, healthOfErr, healthOfLatency,
} from './visuals.js';
import { explain } from './explanations.js';
import { analyzeRepo } from './ingest.js';
import { heuristicExtract } from './extract-heuristic.js';
import { extractWithModel, extractWithNano, anthropicTransport, openaiTransport, makeGeminiNanoTransport } from './extract-model.js';
import { saveDesign, openDesign, downloadText } from './storage.js';
import COLD_OPEN from './cold-open.json' with { type: 'json' };

const $ = (id) => document.getElementById(id);
const currentTopo = () => (ui.mode === 'staged' ? (rt.state.staged?.topology || rt.state.topology) : (rt.state.topology || rt.state.staged?.topology));
const svgNS = 'http://www.w3.org/2000/svg';
const GROUP_COLOR = { traffic: 'var(--g-traffic)', compute: 'var(--g-compute)', data: 'var(--g-data)', stores: 'var(--g-stores)', messaging: 'var(--g-messaging)', control: 'var(--g-control)' };

const rt = createRuntime({ Engine, PRESETS });
createAgentFace(rt.bus, window);

const ui = {
  selected: null,
  paused: false,
  mode: 'live', // 'live' animates the active engine; 'staged' freezes on the dimmed preview
  keys: { apiKey: '', ghToken: '', provider: 'anthropic', model: '' }, // memory only (D6)
  layout: { x: 0, y: 0, w: 1000, h: 600 },
  lastSnap: null,
  sweepWorker: null,
  sweeping: false,
};

/* ---- small helpers ---------------------------------------------------- */
function toast(msg, ms = 2600) {
  const t = $('toast'); t.textContent = msg; t.classList.add('show');
  clearTimeout(toast._t); toast._t = setTimeout(() => t.classList.remove('show'), ms);
}
function modal(html) {
  $('modal').innerHTML = html; $('modal-bg').classList.add('show');
}
function closeModal() { $('modal-bg').classList.remove('show'); }
$('modal-bg').addEventListener('click', (e) => { if (e.target === $('modal-bg')) closeModal(); });

function svgEl(tag, attrs = {}) {
  const e = document.createElementNS(svgNS, tag);
  for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v);
  return e;
}

/* ---- sweep worker (from the inlined blob) ----------------------------- */
function ensureWorker() {
  if (ui.sweepWorker) return ui.sweepWorker;
  const src = document.querySelector('script[type="buckle/sweep"]').textContent;
  const url = URL.createObjectURL(new Blob([src], { type: 'text/javascript' }));
  ui.sweepWorker = new Worker(url);
  return ui.sweepWorker;
}
function runSweep() {
  if (!rt.state.topology || ui.sweeping) return;
  ui.sweeping = true;
  setLeftHead('Sweeping…');
  renderStage(`Sweeping ${rt.state.topology.nodes.length} nodes across 20 load steps…`);
  const w = ensureWorker();
  const onMsg = (e) => {
    if (e.data.type === 'done') {
      w.removeEventListener('message', onMsg);
      ui.sweeping = false;
      rt.state.findings = e.data.result;
      renderFindings(e.data.result);
      toast(`${e.data.result.findings.length} findings · peak goodput ${formatRateBare(e.data.result.system.goodputPeak)}/s`);
    } else if (e.data.type === 'error') {
      w.removeEventListener('message', onMsg);
      ui.sweeping = false;
      renderStage(`Sweep failed: ${e.data.message}`);
    }
  };
  w.addEventListener('message', onMsg);
  w.postMessage({ topology: rt.state.topology, seed: rt.state.seed, steps: 20 });
}

/* ---- canvas ----------------------------------------------------------- */
function computeLayout() {
  const nodes = (ui.mode === 'staged' ? (rt.state.staged?.topology || rt.state.topology) : (rt.state.topology || rt.state.staged?.topology))?.nodes || [];
  if (!nodes.length) return;
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  for (const n of nodes) { minX = Math.min(minX, n.x); minY = Math.min(minY, n.y); maxX = Math.max(maxX, n.x); maxY = Math.max(maxY, n.y); }
  const pad = 90;
  ui.layout = { x: minX - pad, y: minY - pad, w: (maxX - minX) + pad * 2, h: (maxY - minY) + pad * 2 };
  $('canvas').setAttribute('viewBox', `${ui.layout.x} ${ui.layout.y} ${Math.max(ui.layout.w, 400)} ${Math.max(ui.layout.h, 300)}`);
}

const NODE_W = 96, NODE_H = 52;

function renderCanvas(staged = false) {
  const topo = staged ? (rt.state.staged?.topology || rt.state.topology) : (rt.state.topology || rt.state.staged?.topology);
  const canvas = $('canvas');
  canvas.innerHTML = '';
  if (!topo) return;
  const snap = ui.lastSnap;
  const nodeById = Object.fromEntries(topo.nodes.map((n) => [n.id, n]));

  // Edges first (under nodes).
  const gEdges = svgEl('g');
  for (const e of topo.edges) {
    const a = nodeById[e.from]; const b = nodeById[e.to];
    if (!a || !b) continue;
    const st = snap?.edgeState?.[e.id];
    let stroke = 'var(--line-hi)'; let dash = '';
    if (st === 'cut') { stroke = 'var(--accent)'; dash = '4 3'; }
    else if (st === 'blocked') { stroke = 'var(--accent)'; dash = '2 4'; }
    else if (st === 'live') { stroke = 'var(--ink-dim)'; }
    else if (st === 'standby') { stroke = 'var(--ink-faint)'; dash = '2 5'; }
    const path = svgEl('line', { x1: a.x, y1: a.y, x2: b.x, y2: b.y, stroke, 'stroke-width': e.control ? 0.6 : 1, 'stroke-dasharray': e.control ? '1 4' : dash });
    gEdges.appendChild(path);
  }
  canvas.appendChild(gEdges);

  for (const n of topo.nodes) {
    const g = svgEl('g', { class: `node-g${staged ? ' staged' : ''}`, transform: `translate(${n.x - NODE_W / 2}, ${n.y - NODE_H / 2})` });
    g.dataset.id = n.id;
    const gc = GROUP_COLOR[groupOfKind(n.kind)] || 'var(--ink-dim)';
    const st = snap?.nodes?.[n.id];
    const util = st ? st.utilization : 0;
    const health = st ? (healthOfLoad(util) === 'danger' || (st.shedRate > 0) ? 'danger' : healthOfLoad(util)) : 'ok';
    const selected = ui.selected === n.id;
    const border = health === 'danger' ? 'var(--accent)' : selected ? 'var(--ink)' : 'var(--line-hi)';

    g.appendChild(svgEl('rect', { x: 0, y: 0, width: NODE_W, height: NODE_H, rx: 3, fill: 'var(--panel-2)', stroke: border, 'stroke-width': selected ? 1.5 : 1 }));
    // group tab (left edge)
    g.appendChild(svgEl('rect', { x: 0, y: 0, width: 3, height: NODE_H, rx: 1, fill: gc }));
    // utilisation fill (subtle, from bottom)
    if (util > 0) {
      const h = Math.min(util, 1) * NODE_H;
      g.appendChild(svgEl('rect', { x: NODE_W - 4, y: NODE_H - h, width: 3, height: h, fill: health === 'danger' ? 'var(--accent)' : health === 'warn' ? 'var(--warn)' : 'var(--ink-faint)' }));
    }
    // glyph
    const icon = KIND_ICON[n.kind];
    if (icon) {
      const gi = svgEl('g', { transform: `translate(10, 8) scale(0.7)`, stroke: health === 'danger' ? 'var(--accent)' : 'var(--ink-dim)', fill: 'none', 'stroke-width': 2, 'stroke-linecap': 'round', 'stroke-linejoin': 'round' });
      for (const [tag, at] of icon) {
        const p = svgEl(tag, {});
        for (const [k, v] of Object.entries(at)) { if (k !== 'key') p.setAttribute(k, v); }
        if (at.fill === 'currentColor') p.setAttribute('fill', 'var(--ink-dim)');
        gi.appendChild(p);
      }
      g.appendChild(gi);
    }
    // label
    const label = svgEl('text', { x: 30, y: 20, fill: 'var(--ink-hi)', 'font-size': 10, 'font-family': 'var(--mono)' });
    label.textContent = (n.label || n.id).slice(0, 12); g.appendChild(label);
    const kindT = svgEl('text', { x: 30, y: 33, fill: 'var(--ink-faint)', 'font-size': 8, 'font-family': 'var(--mono)' });
    kindT.textContent = n.kind; g.appendChild(kindT);
    // live stat
    if (st) {
      const s = svgEl('text', { x: 30, y: 45, fill: health === 'danger' ? 'var(--accent)' : 'var(--ink-dim)', 'font-size': 8.5, 'font-family': 'var(--mono)' });
      s.textContent = st.shedRate > 0 ? `shed ${formatRateBare(st.shedRate)}/s` : `${formatPct(util)} · ${formatMs(st.p99)}`;
      g.appendChild(s);
    }
    g.addEventListener('click', () => selectNode(n.id));
    canvas.appendChild(g);
  }
}

/* ---- live loop -------------------------------------------------------- */
let lastT = 0;
function loop(t) {
  requestAnimationFrame(loop);
  if (!rt.state.engine || ui.paused || ui.mode === 'staged') { lastT = t; return; }
  const dt = Math.min(t - lastT, 100); lastT = t;
  if (dt > 0) rt.advance(dt);
  ui.lastSnap = rt.snapshot();
  renderDynamic();
}
let frameSkip = 0;
function renderDynamic() {
  const snap = ui.lastSnap; if (!snap) return;
  // readouts every frame (cheap)
  $('ro-good').textContent = formatRateBare(snap.system.goodputRps);
  $('ro-p99').textContent = formatMs(snap.system.p99);
  const shed = Object.values(snap.nodes).reduce((s, n) => s + (n.shedRate || 0), 0);
  $('ro-shed').textContent = formatRateBare(shed);
  $('ro-shed-wrap').classList.toggle('bad', shed > 0);
  // bottom strip
  $('m-p50').textContent = formatMs(snap.system.p50);
  $('m-p95').textContent = formatMs(snap.system.p95);
  $('m-p99').textContent = formatMs(snap.system.p99);
  $('m-good').textContent = formatRate(snap.system.goodputRps);
  $('m-off').textContent = formatRate(snap.system.offeredRps);
  // canvas every ~4 frames
  if ((frameSkip = (frameSkip + 1) % 4) === 0) { renderCanvas(false); if (ui.selected) renderInspector(); }
}

/* ---- slider ----------------------------------------------------------- */
const traffic = $('traffic');
traffic.addEventListener('input', () => {
  const rps = Number(traffic.value);
  $('ro-rps').textContent = formatRateBare(rps);
  rt.bus.dispatch('set_load', { rps }, 'window');
});
function setLoad(rps) { traffic.value = rps; $('ro-rps').textContent = formatRateBare(rps); rt.bus.dispatch('set_load', { rps }, 'window'); }

/* ---- findings & left rail --------------------------------------------- */
function setLeftHead(t) { $('left-head').textContent = t; }
function renderStage(text) { $('left-body').innerHTML = `<div class="stage-row">${text}</div>`; }
function renderFindings(result) {
  setLeftHead(`Findings · ${result.findings.length}`);
  const body = $('left-body');
  if (!result.findings.length) { body.innerHTML = '<div class="stage-row">No bottleneck found in range. Raise the ceiling or add load.</div>'; return; }
  body.innerHTML = '';
  const topo = rt.state.topology;
  for (const f of result.findings) {
    const ex = explain(f.kind);
    const sev = (f.kind === 'retry_amplification' || f.kind === 'collapse' || f.kind === 'spof') ? 'sev-high' : '';
    const nodeLabels = f.nodes.map((id) => topo.nodes.find((n) => n.id === id)?.label || id).join(' → ');
    const row = document.createElement('div');
    row.className = `finding ${sev}`;
    row.innerHTML = `<div class="kind">${ex.title}</div>
      <div class="meta">${nodeLabels ? nodeLabels + ' · ' : ''}at <span class="num">${formatRateBare(f.at_rps)}</span>/s · <span class="ev">${f.evidence.metric} ${f.evidence.value}</span></div>`;
    row.addEventListener('click', () => {
      setLoad(f.at_rps);
      if (f.nodes[0]) selectNode(f.nodes[0]);
    });
    body.appendChild(row);
  }
}

/* ---- inspector -------------------------------------------------------- */
function selectNode(id) { ui.selected = id; renderInspector(); renderCanvas(!!rt.state.staged && !rt.state.topology); }
const EDIT_FIELDS = [
  ['capacity', 'capacity'], ['instances', 'instances'], ['serviceMs', 'service ms'], ['serviceCv', 'service cv'],
  ['queueLimit', 'queue limit'], ['timeoutMs', 'timeout ms'], ['retries', 'retries'], ['hitRate', 'hit rate'], ['errorRate', 'error rate'], ['rps', 'rps'],
];
function renderInspector() {
  const insp = $('insp');
  const topo = currentTopo();
  const n = topo?.nodes.find((x) => x.id === ui.selected);
  if (!n) { insp.innerHTML = '<div class="rail-head">Inspector</div><div class="stage-row">Select a node.</div>'; return; }
  const prov = rt.state.provenance?.[n.id] || {};
  const snap = ui.lastSnap;
  const st = snap?.nodes?.[n.id];
  let html = `<div class="insp-title"><span>${n.label}</span><span class="dim">${KIND_NAME[n.kind]}</span></div>`;
  if (st) {
    html += `<div class="insp-field"><label>utilisation</label><span class="num ${healthOfLoad(st.utilization) !== 'ok' ? 'warn' : ''}" style="text-align:right">${formatPct(st.utilization)}</span></div>`;
    html += `<div class="insp-field"><label>p99 / queued</label><span class="num" style="text-align:right">${formatMs(st.p99)} · ${formatCount(st.queued)}</span></div>`;
  }
  for (const [field, label] of EDIT_FIELDS) {
    if (n.config[field] === undefined) continue;
    const p = prov.config?.[field];
    const chip = p ? (p.src === 'READ' ? `<span class="prov read" title="${p.file}:${p.line}">READ</span>` : `<span class="prov assumed" title="default ${p.default}">ASSUMED</span>`) : '';
    html += `<div class="insp-field"><label>${label} ${chip}</label><input data-field="${field}" value="${n.config[field]}" /></div>`;
  }
  html += `<div class="chaos"><div class="dim" style="font-size:var(--f-sm);margin-bottom:4px">chaos</div>
    <button data-chaos="crash">crash</button>
    <button data-chaos="slow">slow ×3</button>
    <button data-chaos="errors">errors 30%</button>
    <button data-chaos="clear">clear</button></div>`;
  insp.innerHTML = html;
  insp.querySelectorAll('input[data-field]').forEach((inp) => {
    inp.addEventListener('change', () => {
      const v = Number(inp.value);
      if (!Number.isFinite(v)) return;
      try { rt.bus.dispatch('set_param', { node: n.id, field: inp.dataset.field, value: v }, 'window'); toast(`${inp.dataset.field} = ${v} · press s to re-sweep`); }
      catch (e) { toast(e.message); }
    });
  });
  insp.querySelectorAll('button[data-chaos]').forEach((b) => {
    b.addEventListener('click', () => { try { rt.bus.dispatch('apply_chaos', { node: n.id, kind: b.dataset.chaos }, 'window'); toast(`${b.dataset.chaos} on ${n.label}`); } catch (e) { toast(e.message); } });
  });
}

/* ---- run flow --------------------------------------------------------- */
async function runRepo() {
  const repo = $('repo').value.trim();
  if (!repo) { openPresets(); return; }
  const ref = $('ref').value.trim();
  setLeftHead('Ingesting');
  renderStage(`Pulling <b>${repo}</b> through the edge…`);
  let analyze;
  try {
    analyze = await analyzeRepo(repo, { ref, token: ui.keys.ghToken, base: '' });
  } catch (e) { renderStage(`Ingest failed: ${e.message}`); toast(e.message); return; }
  renderStage(`Read ${analyze.selected.length} files (${(analyze.selectedBytes / 1024).toFixed(0)} KB) of ${analyze.totalFiles}. Extracting topology…`);

  // Extract via the ladder: on-device Gemini Nano or BYOK model when chosen,
  // else the deterministic compose heuristic (D4).
  let extracted = null;
  const useNano = ui.keys.provider === 'gemini-nano';
  if (useNano || ui.keys.apiKey) {
    try {
      const label = useNano ? 'gemini-nano' : ui.keys.provider;
      let res;
      if (useNano) {
        renderStage(`Extracting with on-device Gemini Nano… (first run downloads the model)`);
        const transport = makeGeminiNanoTransport({ onProgress: (p) => renderStage(`Downloading Gemini Nano… ${Math.round(p * 100)}%`) });
        res = await extractWithNano(analyze, { transport });
      } else {
        const transport = ui.keys.provider === 'anthropic' ? anthropicTransport
          : ui.keys.provider === 'deepseek' ? openaiTransport('https://api.deepseek.com/v1')
          : openaiTransport();
        res = await extractWithModel(analyze, { transport, apiKey: ui.keys.apiKey, model: ui.keys.model || undefined });
      }
      extracted = { topology: res.topology, provenance: {}, source: `model:${label}`, stats: { nodes: res.topology.nodes.length, read: 0, assumed: 0 }, note: `Extracted by ${label} in ${res.rounds} repair round(s). The model proposed the graph shape; the engine grades it, and every number is a labelled default.` };
    } catch (e) {
      toast(`Model extraction failed (${e.message}); falling back to compose read.`);
    }
  }
  if (!extracted) extracted = heuristicExtract(analyze);
  if (!extracted) { renderStage(`No docker-compose or model key. Add a BYOK key, or paste topology JSON.`); return; }

  stageGraph(extracted);
}

function stageGraph(extracted) {
  try {
    rt.bus.dispatch('load_topology', { topology: extracted.topology, source: extracted.source, provenance: extracted.provenance || {} }, 'window');
  } catch (e) { renderStage(`Extraction invalid: ${e.message}`); return; }
  ui.mode = 'staged';
  ui.selected = null;
  computeLayout();
  renderCanvas(true); // dimmed
  const s = extracted.stats || {};
  setLeftHead('Staged');
  $('left-body').innerHTML = `<div class="stage-row"><b>${extracted.topology.nodes.length} nodes</b> · ${s.read ?? 0} READ · ${s.assumed ?? 0} ASSUMED<br><span class="dim">${extracted.note || ''}</span></div>
    <div style="padding:10px 12px"><button class="primary" id="accept-btn">Accept &amp; sweep</button></div>`;
  $('accept-btn').addEventListener('click', acceptAndSweep);
}

function acceptAndSweep() {
  try { rt.bus.dispatch('accept_graph', {}, 'window'); } catch (e) { toast(e.message); return; }
  ui.mode = 'live';
  computeLayout();
  setLoad(50);
  renderCanvas(false);
  runSweep();
}

/* ---- presets / paste / export ----------------------------------------- */
function openPresets() {
  const items = PRESETS.map((p) => `<div class="finding" data-preset="${p.id}"><div class="kind">${p.name}</div><div class="meta">${(p.tagline || '').slice(0, 60)}</div></div>`).join('');
  modal(`<h2>Presets</h2><div style="max-height:60vh;overflow:auto">${items}</div>`);
  $('modal').querySelectorAll('[data-preset]').forEach((el) => el.addEventListener('click', () => {
    closeModal();
    rt.bus.dispatch('load_preset', { name: el.dataset.preset }, 'window');
    stageGraph({ topology: rt.state.staged.topology, source: rt.state.staged.source, stats: {}, note: 'Bundled preset.' });
  }));
}
function openPaste() {
  modal(`<h2>Paste topology JSON</h2><textarea id="paste-ta" style="width:100%;height:280px;background:var(--bg);color:var(--ink);border:1px solid var(--line-hi);font-family:var(--mono);font-size:11px"></textarea><div style="margin-top:10px"><button class="primary" id="paste-load">Load</button></div>`);
  $('paste-load').addEventListener('click', () => {
    try {
      const topo = JSON.parse($('paste-ta').value);
      rt.bus.dispatch('load_topology', { topology: topo, source: 'paste' }, 'window');
      closeModal();
      stageGraph({ topology: rt.state.staged.topology, source: 'paste', stats: {}, note: 'Pasted topology.' });
    } catch (e) { toast(`Invalid: ${e.message}`); }
  });
}
function doExport() {
  const data = rt.bus.dispatch('export', {}, 'window');
  const name = (rt.state.topology ? 'buckle' : 'buckle');
  downloadText(JSON.stringify(data, null, 2), `${name}.buckle.json`);
  toast('Exported closure JSON (topology · provenance · findings · seed)');
}

/* ---- key modal -------------------------------------------------------- */
function openKeys() {
  modal(`<h2>Keys · memory only, never stored</h2>
    <div class="dim" style="font-size:var(--f-sm);margin-bottom:10px">Local model unavailable → BYOK is the honest default. Nothing here is written to disk (D6).</div>
    <div class="insp-field" style="grid-template-columns:1fr 2fr"><label>provider</label>
      <select id="k-prov" style="background:var(--panel-2);color:var(--ink);border:1px solid var(--line-hi)">
        <option value="gemini-nano">Gemini Nano (on-device, no key)</option><option value="anthropic">Anthropic</option><option value="openai">OpenAI</option><option value="deepseek">DeepSeek</option></select></div>
    <div class="insp-field" style="grid-template-columns:1fr 2fr"><label>API key</label><input id="k-api" type="password" value="${ui.keys.apiKey}" placeholder="sk-…" /></div>
    <div class="insp-field" style="grid-template-columns:1fr 2fr"><label>model</label><input id="k-model" value="${ui.keys.model}" placeholder="default" /></div>
    <div class="insp-field" style="grid-template-columns:1fr 2fr"><label>GitHub token</label><input id="k-gh" type="password" value="${ui.keys.ghToken}" placeholder="for private / rate limits" /></div>
    <div style="margin-top:12px"><button class="primary" id="k-save">Keep for this session</button></div>`);
  $('k-prov').value = ui.keys.provider;
  $('k-save').addEventListener('click', () => {
    ui.keys.provider = $('k-prov').value; ui.keys.apiKey = $('k-api').value.trim();
    ui.keys.model = $('k-model').value.trim(); ui.keys.ghToken = $('k-gh').value.trim();
    closeModal(); detectLadder();
    toast(ui.keys.provider === 'gemini-nano' ? 'Gemini Nano selected · on-device, no key'
      : ui.keys.apiKey ? `BYOK ${ui.keys.provider} set (memory only)` : 'Keys cleared');
  });
}
function detectLadder() {
  // Ladder detect (§4.3): on-device model → WebGPU → BYOK. Detect, don't ask.
  let tier = 'compose-read (no key)';
  if (ui.keys.provider === 'gemini-nano') tier = 'Gemini Nano · on-device';
  else if (ui.keys.apiKey) tier = `BYOK ${ui.keys.provider}`;
  else if ('LanguageModel' in window) tier = 'Gemini Nano available · compose-read default';
  else if (navigator.gpu) tier = 'WebGPU available · compose-read default';
  $('ladder-chip').textContent = tier;
}

/* ---- guide ------------------------------------------------------------ */
function openGuide() {
  modal(`<h2>Buckle — an architecture linter with numbers</h2>
    <p class="dim">Point it at a repo. It pulls the code, reads the shape, builds a queueing model, and loads it until it buckles — then tells you which component gave way first and why.</p>
    <p><b>Teaching-grade fidelity.</b> The sim is a queueing model (finite server slots, gamma service times, FIFO queues, real timeouts and retries), not a capacity planner. Every service-time number without telemetry is a labelled default.</p>
    <p><b>Findings come from the engine's sweep, never a model.</b> Load is swept 1→10⁴ rps across 20 steps; each finding is deterministic and replayable from the export.</p>
    <table style="font-size:var(--f-sm);border-collapse:collapse">
      <tr><td class="kbd"><kbd>/</kbd></td><td>focus repo</td></tr>
      <tr><td><kbd>space</kbd></td><td>pause / run the live engine</td></tr>
      <tr><td><kbd>[</kbd> <kbd>]</kbd></td><td>traffic down / up</td></tr>
      <tr><td><kbd>s</kbd></td><td>re-sweep</td></tr>
      <tr><td><kbd>e</kbd></td><td>export closure JSON</td></tr>
      <tr><td><kbd>?</kbd></td><td>this guide</td></tr>
    </table>
    <p class="dim" style="font-size:var(--f-sm)">Engine vendored from breakscale (MIT). Agent face: <span class="kbd">window.buckle</span> — ${Object.keys(window.buckle).length - 1} tools.</p>`);
}

/* ---- wiring ----------------------------------------------------------- */
$('run-btn').addEventListener('click', runRepo);
$('key-btn').addEventListener('click', openKeys);
$('preset-btn').addEventListener('click', openPresets);
$('paste-btn').addEventListener('click', openPaste);
$('export-btn').addEventListener('click', doExport);
$('guide-btn').addEventListener('click', openGuide);
// Theme: default is light (Chirag's default); dark is the toggle / system-dark.
function initTheme() {
  let saved = null;
  try { saved = localStorage.getItem('buckle-theme'); } catch { /* private mode */ }
  if (saved === 'dark' || saved === 'light') document.documentElement.setAttribute('data-theme', saved);
}
function toggleTheme() {
  const cur = document.documentElement.getAttribute('data-theme');
  const isDark = cur ? cur === 'dark' : matchMedia('(prefers-color-scheme: dark)').matches;
  const next = isDark ? 'light' : 'dark';
  document.documentElement.setAttribute('data-theme', next);
  try { localStorage.setItem('buckle-theme', next); } catch { /* ignore */ }
  toast(`${next} mode`);
}
$('theme-btn').addEventListener('click', toggleTheme);
initTheme();
$('bottom-bar').addEventListener('click', () => $('bottom').classList.toggle('collapsed'));
$('repo').addEventListener('keydown', (e) => { if (e.key === 'Enter') runRepo(); });

document.addEventListener('keydown', (e) => {
  if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.tagName === 'SELECT') {
    if (e.key === 'Escape') e.target.blur();
    return;
  }
  if (e.key === '/') { e.preventDefault(); $('repo').focus(); }
  else if (e.key === ' ') { e.preventDefault(); ui.paused = !ui.paused; toast(ui.paused ? 'paused' : 'running'); }
  else if (e.key === '[') setLoad(Math.max(0, Number(traffic.value) - Math.ceil(Number(traffic.value) * 0.15) - 1));
  else if (e.key === ']') setLoad(Math.min(10000, Number(traffic.value) + Math.ceil(Number(traffic.value) * 0.15) + 1));
  else if (e.key === 's') runSweep();
  else if (e.key === 'e') doExport();
  else if (e.key === '?') openGuide();
  else if (e.key === 'Escape') closeModal();
});

/* ---- cold open (D8): a worked example already running, findings ready -- */
function coldOpen() {
  rt.bus.dispatch('load_topology', { topology: COLD_OPEN.topology, source: COLD_OPEN.source || 'cold-open', provenance: COLD_OPEN.provenance || {} }, 'window');
  rt.bus.dispatch('accept_graph', {}, 'window');
  computeLayout();
  setLoad(COLD_OPEN.load || 50);
  rt.state.findings = COLD_OPEN.findings;
  renderCanvas(false);
  renderFindings(COLD_OPEN.findings);
  renderInspector();
  detectLadder();
}
coldOpen();
requestAnimationFrame(loop);
