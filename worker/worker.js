/**
 * Buckle edge worker.
 *   GET /                     → the app (static asset, from dist/)
 *   GET /api/analyze?repo=…   → fetch the GitHub tarball, untar in the worker,
 *                               run the deterministic selector, return the
 *                               file tree + the selected slice with contents.
 *
 * The repo pull moves off the tab and into here (the architecture Chirag
 * asked for): public GitHub code, fetched by Buckle's own edge. The
 * sovereignty invariant that still holds is I6 in spirit — only the selected
 * slice is ever handed to the person's model, and the tab shows it first.
 */
import { selectFiles } from '../app/select.js';

const GH_API = 'https://api.github.com';
const MAX_DECOMPRESSED = 90 * 1024 * 1024; // hard OOM guard
const MAX_FILE_CONTENT = 128 * 1024;       // per-file text captured
const MAX_TOTAL_CONTENT = 24 * 1024 * 1024; // global content budget
const TEXT_EXT = /\.(rb|ts|tsx|js|jsx|mjs|cjs|py|go|java|kt|ex|exs|rs|php|c|h|cpp|yml|yaml|toml|env|json|md|txt|proto|tf|sh|cfg|ini|xml|gradle|lock|example|dockerfile)$/i;

const json = (obj, status = 200) =>
  new Response(JSON.stringify(obj), { status, headers: { 'content-type': 'application/json; charset=utf-8', 'access-control-allow-origin': '*' } });

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === '/api/analyze') return analyze(url, request, env);
    if (url.pathname === '/api/health') return json({ ok: true });
    // Everything else is a static asset (the app).
    return env.ASSETS.fetch(request);
  },
};

function parseRepo(raw) {
  if (!raw) return null;
  let s = raw.trim();
  const m = s.match(/github\.com\/([^/]+)\/([^/]+?)(?:\.git)?(?:\/tree\/([^/?#]+))?(?:[/?#]|$)/i);
  if (m) return { owner: m[1], repo: m[2], ref: m[3] || null };
  const parts = s.replace(/^\/+|\/+$/g, '').split('/');
  if (parts.length >= 2) return { owner: parts[0], repo: parts[1], ref: parts[3] || null };
  return null;
}

async function analyze(url, request, env) {
  const target = parseRepo(url.searchParams.get('repo'));
  if (!target) return json({ error: 'pass ?repo=owner/repo or a github.com URL' }, 400);
  const ref = url.searchParams.get('ref') || target.ref || '';
  const token = request.headers.get('x-github-token') || '';

  const cacheKey = new Request(`https://buckle.cache/${target.owner}/${target.repo}@${ref || 'default'}`);
  const cache = caches.default;
  if (!token) {
    const hit = await cache.match(cacheKey);
    if (hit) return hit;
  }

  const tarUrl = `${GH_API}/repos/${target.owner}/${target.repo}/tarball/${ref}`;
  const headers = { 'user-agent': 'buckle-edge', accept: 'application/vnd.github+json' };
  if (token) headers.authorization = `Bearer ${token}`;

  let resp;
  try {
    resp = await fetch(tarUrl, { headers, redirect: 'follow' });
  } catch (e) {
    return json({ error: `fetch failed: ${e.message}` }, 502);
  }
  if (!resp.ok) {
    const body = await resp.text().catch(() => '');
    return json({ error: `github ${resp.status}: ${body.slice(0, 200) || resp.statusText}` }, resp.status === 404 ? 404 : 502);
  }

  let files, resolvedSha;
  try {
    ({ files, resolvedSha } = await streamUntar(resp.body));
  } catch (e) {
    return json({ error: `unpack failed: ${e.message}` }, 502);
  }

  const selection = selectFiles(files);
  const contentByPath = Object.fromEntries(files.filter((f) => typeof f.content === 'string').map((f) => [f.path, f.content]));
  const selected = selection.selected.map((s) => ({ ...s, content: contentByPath[s.path] ?? null }));

  const out = json({
    repo: `${target.owner}/${target.repo}`,
    ref: ref || 'default',
    resolvedSha,
    totalFiles: selection.totalFiles,
    tree: selection.tree,
    selected,
    selectedBytes: selection.selectedBytes,
    truncated: selection.truncated,
  });
  if (!token) {
    out.headers.set('cache-control', 'public, max-age=3600');
    // Cache API needs a cloneable response with a cacheable request.
    try { await cache.put(cacheKey, out.clone()); } catch { /* best effort */ }
  }
  return out;
}

/**
 * Streaming untar over the gunzip stream. Never holds the whole decompressed
 * tar: it keeps a small rolling buffer, parses each 512-byte header as it
 * arrives, and captures file DATA only for wanted text files under the caps —
 * everything else (the bulk: images, node_modules that shipped, spec assets)
 * is counted for the tree and discarded. Peak memory is one file + the buffer,
 * so repo size does not bound it. ustar + pax/GNU long names.
 */
async function streamUntar(body) {
  const reader = body.pipeThrough(new DecompressionStream('gzip')).getReader();
  const dec = new TextDecoder('utf-8', { fatal: false });
  const files = [];
  let resolvedSha = null;
  let totalContent = 0;
  let processed = 0;
  let paxPath = null;
  const octal = (b) => { const s = dec.decode(b).replace(/\0.*$/, '').trim(); return s ? parseInt(s, 8) || 0 : 0; };

  // Rolling buffer of not-yet-consumed bytes. Kept small: we only ever buffer
  // a header (512) or a wanted file's data (≤128 KB); skipped data is drained.
  let buf = new Uint8Array(0);
  const pull = async () => {
    const { done, value } = await reader.read();
    if (done) return false;
    processed += value.byteLength;
    if (processed > 500 * 1024 * 1024) { reader.cancel(); throw new Error('repo too large (>500 MB uncompressed)'); }
    const next = new Uint8Array(buf.length + value.length);
    next.set(buf, 0); next.set(value, buf.length); buf = next;
    return true;
  };
  // Ensure ≥ n bytes buffered; returns false at clean EOF.
  const ensure = async (n) => { while (buf.length < n) { if (!(await pull())) return false; } return true; };
  const take = (n) => { const out = buf.subarray(0, n); buf = buf.subarray(n); return out; };
  // Discard n bytes without ever holding more than one chunk of them.
  const drain = async (n) => {
    let remaining = n;
    const fromBuf = Math.min(remaining, buf.length);
    buf = buf.subarray(fromBuf); remaining -= fromBuf;
    while (remaining > 0) {
      if (!(await pull())) return;
      const d = Math.min(remaining, buf.length);
      buf = buf.subarray(d); remaining -= d;
    }
  };

  // High-value config/orchestration files (the ones the selector's top tiers
  // match) are always captured — they are small and few. The generic text
  // budget applies only to other source, so a repo's huge app/ tree can never
  // starve its docker-compose.yaml of content.
  const HIGH_VALUE = /(^|\/)(docker-compose|compose)[^/]*\.ya?ml$|(^|\/)dockerfile[^/]*$|(^|\/)procfile[^/]*$|(^|\/)(k8s|helm)\/|\.tf$|(^|\/)(fly\.toml|render\.yaml|app\.json)$|(^|\/)config\/[^/]*\.(yml|rb)$|(^|\/)config\/initializers\/[^/]*$|(^|\/)config\/redis[^/]*$|\.env\.example$|(^|\/)(settings|urls)\.py$|(^|\/)celery[^/]*\.py$|(^|\/)application\.ya?ml$|(^|\/)routes\/[^/]+\.ts$|\.proto$/i;
  const isWanted = (rel, size) => {
    if (size > MAX_FILE_CONTENT) return false;
    if (HIGH_VALUE.test(rel)) return true;
    const text = TEXT_EXT.test(rel) || /(^|\/)(dockerfile|procfile|makefile|gemfile|rakefile)[^/]*$/i.test(rel);
    return text && totalContent + size <= MAX_TOTAL_CONTENT;
  };
  const relOf = (name) => { const i = name.indexOf('/'); return i < 0 ? '' : name.slice(i + 1); };

  for (;;) {
    if (!(await ensure(512))) break;
    const header = take(512);
    if (header.every((b) => b === 0)) break; // end marker

    let name = dec.decode(header.subarray(0, 100)).replace(/\0.*$/, '');
    const prefix = dec.decode(header.subarray(345, 500)).replace(/\0.*$/, '');
    if (prefix) name = `${prefix}/${name}`;
    const size = octal(header.subarray(124, 136));
    const type = String.fromCharCode(header[156]);
    const padded = Math.ceil(size / 512) * 512;
    const pad = padded - size;

    if (type === 'x' || type === 'g' || type === 'L') {
      if (!(await ensure(padded))) break;
      const data = take(padded);
      const rec = dec.decode(data.subarray(0, size));
      if (type === 'L') paxPath = rec.replace(/\0.*$/, '');
      else { const m = rec.match(/\d+ path=([^\n]+)\n/); if (m) paxPath = m[1]; }
      continue;
    }
    if (!(type === '0' || type === '\0' || type === '')) { await drain(padded); paxPath = null; continue; }

    if (paxPath) { name = paxPath; paxPath = null; }
    if (!resolvedSha) { const s = name.slice(0, Math.max(0, name.indexOf('/'))).match(/-([0-9a-f]{7,40})$/i); if (s) resolvedSha = s[1]; }
    const rel = relOf(name);
    if (!rel) { await drain(padded); continue; }

    if (isWanted(rel, size)) {
      if (!(await ensure(padded))) break;
      const region = take(padded);
      totalContent += size;
      files.push({ path: rel, size, content: dec.decode(region.subarray(0, size)) });
    } else {
      files.push({ path: rel, size, content: null }); // path + size for the tree
      await drain(padded);
    }
  }
  return { files, resolvedSha };
}
