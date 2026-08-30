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

export function buildExtractPrompt(analyze) {
  const files = (analyze.selected || [])
    .filter((f) => typeof f.content === 'string')
    .map((f) => `# FILE: ${f.path}\n${f.content.slice(0, 8000)}`)
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
export async function extractWithModel(analyze, { transport, apiKey, model }) {
  const { system, user } = buildExtractPrompt(analyze);
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

/** FakeTransport: returns a recorded extraction. First-class (D: offline dev/test). */
export function makeFakeTransport(recorded) {
  return async () => (typeof recorded === 'string' ? recorded : JSON.stringify(recorded));
}
