/**
 * Model extraction path (C5). Every model call goes through one transport
 * interface; FakeTransport is a first-class implementation, not a test hack.
 * The BYOK key is passed per-call and never stored (D6, I6).
 *
 * transport(prompt, { apiKey, model }) → Promise<string>  (raw model text)
 */
import { validateTopology } from './validate.js';
import { KIND_NAME, KIND_GROUPS } from './visuals.js';
import { DEFAULTS } from './defaults.js';
import { configForKind } from './defaults.js';

const CATALOGUE = KIND_GROUPS.map(
  (g) => `${g.title}: ${g.kinds.map((k) => `${k} (${KIND_NAME[k]})`).join(', ')}`,
).join('\n');

const SCHEMA = `{
  "nodes": [{ "id": string, "kind": <one of the catalogue kinds>, "label": string,
              "x": number, "y": number,
              "config": { "capacity": number, "serviceMs": number, "serviceCv": number,
                          "queueLimit": number, "hitRate": number, "errorRate": number,
                          "timeoutMs": number, "retries": number } }],
  "edges": [{ "id": string, "from": <node id>, "to": <node id>, "weight": number,
              "control": boolean (optional, true for an autoscaler→target edge) }]
}`;

// A tiny prompt for on-device models: only the compose file, and the model is
// asked for the graph SHAPE only (id/kind/label + edges). We fill every config
// number from DEFAULTS ourselves — the model proposes, the engine grades (D3).
export function buildNanoPrompt(analyze) {
  const compose = (analyze.selected || []).find((f) => /compose[^/]*\.ya?ml$/i.test(f.path) && typeof f.content === 'string')
    || (analyze.selected || []).find((f) => typeof f.content === 'string');
  const body = (compose?.content || '').slice(0, 1200);
  const prompt = `From this docker-compose, list the services as a JSON graph. Reply with ONLY JSON, no prose.
Format: {"nodes":[{"id":"web","kind":"service","label":"web"}],"edges":[["web","db"]]}
kind is one of: client, service, worker, db, cache, queue, lb, searchindex, objectstore.
Map: rails/web/app/api/node=service; sidekiq/worker/celery=worker; postgres/mysql/mongo=db; redis/memcached=cache; nginx/traefik=lb; elasticsearch/opensearch=searchindex; rabbitmq/kafka=queue.
Add one "client" node with an edge to the main web service.

${body}`;
  return prompt;
}

// Turn a shape-only graph ({nodes:[{id,kind,label}], edges:[[from,to]]|[{from,to}]})
// into a full engine topology: fill config from defaults, lay out, ensure a client.
export function normalizeShape(shape) {
  const nodes = (shape.nodes || []).map((n, i) => ({
    id: String(n.id || `n${i}`).replace(/[^A-Za-z0-9_-]/g, '_'),
    kind: KIND_NAME[n.kind] ? n.kind : 'service',
    label: n.label || n.id || `node ${i}`,
    x: 60 + (1 + (i % 3)) * 200,
    y: 120 + Math.floor(i / 3) * 130,
    config: configForKind(KIND_NAME[n.kind] ? n.kind : 'service'),
  }));
  const ids = new Set(nodes.map((n) => n.id));
  if (!nodes.some((n) => n.kind === 'client')) {
    nodes.unshift({ id: 'client', kind: 'client', label: 'Client', x: 60, y: 200, config: configForKind('client') });
    ids.add('client');
  }
  const edges = [];
  const raw = shape.edges || [];
  for (const e of raw) {
    const from = String((Array.isArray(e) ? e[0] : e.from) || '').replace(/[^A-Za-z0-9_-]/g, '_');
    const to = String((Array.isArray(e) ? e[1] : e.to) || '').replace(/[^A-Za-z0-9_-]/g, '_');
    if (ids.has(from) && ids.has(to) && from !== to && !edges.some((x) => x.id === `${from}->${to}`)) {
      edges.push({ id: `${from}->${to}`, from, to, weight: 1 });
    }
  }
  // Ensure the client reaches something.
  if (!edges.some((e) => e.from === 'client')) {
    const target = nodes.find((n) => n.kind === 'service') || nodes.find((n) => n.kind !== 'client');
    if (target) edges.push({ id: `client->${target.id}`, from: 'client', to: target.id, weight: 1 });
  }
  return { nodes, edges };
}

export function buildExtractPrompt(analyze, { compact = false } = {}) {
  // On-device models (Gemini Nano) have a tiny context, so the compact prompt
  // feeds only the orchestration/config files, hard-trimmed.
  let picked = (analyze.selected || []).filter((f) => typeof f.content === 'string');
  if (compact) {
    picked = picked.filter((f) => f.tier <= 2).slice(0, 4);
  }
  const perFile = compact ? 1500 : 8000;
  const files = picked
    .map((f) => `# FILE: ${f.path}\n${f.content.slice(0, perFile)}`)
    .join('\n\n');
  const defaults = Object.entries(DEFAULTS)
    .map(([k, d]) => `${k}: serviceMs=${d.serviceMs}, capacity=${d.capacity}, timeoutMs=${d.timeoutMs}, retries=${d.retries}`)
    .join('\n');
  const system = `You are a runtime-topology extractor. From the repository files you are given, output ONLY a JSON object describing the system's request/queue topology as an engine graph. No prose, no markdown fences.

Component kinds (use these exactly):
${CATALOGUE}

Output schema:
${SCHEMA}

Rules:
- Emit a "client" node as the traffic origin, wired to the entry point.
- Only create a component the files actually evidence. Do not invent services.
- For any number you cannot read from the files, use these per-kind defaults:
${defaults}
- Lay nodes left-to-right by request flow: client at x≈60, each downstream tier +200 in x.
- Return exactly one JSON object and nothing else.`;
  const user = `Repository: ${analyze.repo} @ ${analyze.resolvedSha || analyze.ref}\n\n${files}`;
  return { system, user };
}

/** Strip fences / prose and parse the first JSON object. */
export function parseModelJson(text) {
  if (!text) throw new Error('empty model response');
  let s = text.trim();
  const fence = s.match(/```(?:json)?\s*([\s\S]*?)```/);
  if (fence) s = fence[1].trim();
  const start = s.indexOf('{');
  const end = s.lastIndexOf('}');
  if (start < 0 || end < 0) throw new Error('no JSON object in model response');
  return JSON.parse(s.slice(start, end + 1));
}

/**
 * Run extraction with up to 2 repair rounds (§4.3). Returns { topology } or
 * throws with the last validator message (the UI then shows the raw output).
 */
export async function extractWithModel(analyze, { transport, apiKey, model, compact = false }) {
  const { system, user } = buildExtractPrompt(analyze, { compact });
  let convo = `${system}\n\n${user}`;
  let lastErr = '';
  let raw = '';
  for (let round = 0; round <= 2; round += 1) {
    raw = await transport(convo, { apiKey, model });
    let parsed;
    try {
      parsed = parseModelJson(raw);
    } catch (e) {
      lastErr = e.message;
      convo = `${system}\n\n${user}\n\nYour previous answer could not be parsed: ${e.message}\nReturn ONLY the JSON object.`;
      continue;
    }
    const result = validateTopology(parsed);
    if (result.ok) return { topology: result.topology, raw, rounds: round };
    lastErr = result.errors.join('; ');
    convo = `${system}\n\n${user}\n\nYour previous answer failed validation:\n${result.errors.slice(0, 20).join('\n')}\nFix these and return ONLY the corrected JSON object.`;
  }
  const err = new Error(lastErr || 'extraction failed');
  err.raw = raw;
  throw err;
}

/**
 * On-device extraction: the model proposes the graph shape, we fill numbers
 * from defaults, the engine grades it. Up to 2 repair rounds. Returns
 * { topology, raw, rounds } or throws.
 */
export async function extractWithNano(analyze, { transport }) {
  let prompt = buildNanoPrompt(analyze);
  let lastErr = '';
  let raw = '';
  for (let round = 0; round <= 2; round += 1) {
    raw = await transport(prompt);
    let shape;
    try { shape = parseModelJson(raw); } catch (e) { lastErr = e.message; prompt = `${buildNanoPrompt(analyze)}\n\nYour last reply was not valid JSON (${e.message}). Reply with ONLY the JSON object.`; continue; }
    const topology = normalizeShape(shape);
    const v = validateTopology(topology);
    if (v.ok) return { topology: v.topology, raw, rounds: round };
    lastErr = v.errors.join('; ');
    prompt = `${buildNanoPrompt(analyze)}\n\nThe graph was invalid: ${v.errors.slice(0, 6).join('; ')}. Reply with ONLY corrected JSON.`;
  }
  const err = new Error(lastErr || 'nano extraction failed'); err.raw = raw; throw err;
}

/* ---- transports ------------------------------------------------------- */

/** Anthropic Messages API, browser BYOK (direct-access header). */
export async function anthropicTransport(prompt, { apiKey, model = 'claude-sonnet-4-5' }) {
  const resp = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-api-key': apiKey,
      'anthropic-version': '2023-06-01',
      'anthropic-dangerous-direct-browser-access': 'true',
    },
    body: JSON.stringify({ model, max_tokens: 4096, messages: [{ role: 'user', content: prompt }] }),
  });
  const data = await resp.json();
  if (!resp.ok) throw new Error(data.error?.message || `anthropic ${resp.status}`);
  return data.content?.map((c) => c.text || '').join('') || '';
}

/** OpenAI-compatible (OpenAI, DeepSeek, together, …) via a configurable base. */
export function openaiTransport(base = 'https://api.openai.com/v1') {
  return async (prompt, { apiKey, model = 'gpt-4o-mini' }) => {
    const resp = await fetch(`${base}/chat/completions`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${apiKey}` },
      body: JSON.stringify({ model, messages: [{ role: 'user', content: prompt }], temperature: 0 }),
    });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.error?.message || `openai ${resp.status}`);
    return data.choices?.[0]?.message?.content || '';
  };
}

/**
 * On-device Gemini Nano via Chrome's built-in Prompt API (the WebGPU/local
 * tier of the Edge-First ladder — no key, nothing leaves the machine). The
 * model is small, so it is paired with the compact prompt. Downloads on first
 * use; onProgress reports 0..1.
 */
export function makeGeminiNanoTransport({ onProgress } = {}) {
  let session = null;
  return async (prompt) => {
    if (!('LanguageModel' in self)) throw new Error('Prompt API (Gemini Nano) not available in this browser');
    if (!session) {
      const availability = await LanguageModel.availability();
      if (availability === 'unavailable') throw new Error('Gemini Nano unavailable on this device');
      session = await LanguageModel.create({
        temperature: 0,
        topK: 1,
        monitor(m) {
          m.addEventListener('downloadprogress', (e) => { if (onProgress) onProgress(e.loaded); });
        },
      });
    }
    return await session.prompt(prompt);
  };
}

/** FakeTransport: returns a recorded extraction. First-class (D: offline dev/test). */
export function makeFakeTransport(recorded) {
  return async () => (typeof recorded === 'string' ? recorded : JSON.stringify(recorded));
}
