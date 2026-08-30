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

  let tarBytes;
  try {
    tarBytes = await readGunzipCapped(resp.body, MAX_DECOMPRESSED);
  } catch (e) {
    return json({ error: `unpack failed: ${e.message}` }, 502);
  }

  let files, resolvedSha;
  try {
    ({ files, resolvedSha } = parseTar(tarBytes));
  } catch (e) {
    return json({ error: `tar parse failed: ${e.message}` }, 502);
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

/** Read a gzip stream, decompress, and concatenate — capped to guard memory. */
async function readGunzipCapped(body, cap) {
  const stream = body.pipeThrough(new DecompressionStream('gzip'));
  const reader = stream.getReader();
  const chunks = [];
  let total = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.byteLength;
    if (total > cap) { reader.cancel(); throw new Error('repo exceeds 90 MB uncompressed'); }
    chunks.push(value);
  }
  const out = new Uint8Array(total);
  let off = 0;
  for (const c of chunks) { out.set(c, off); off += c.byteLength; }
  return out;
}

/** Minimal tar reader: ustar + pax extended headers for long paths. */
function parseTar(buf) {
  const files = [];
  const dec = new TextDecoder('utf-8', { fatal: false });
  let resolvedSha = null;
  let totalContent = 0;
  let pos = 0;
  let paxPath = null;

  const octal = (bytes) => {
    const s = dec.decode(bytes).replace(/\0.*$/, '').trim();
    return s ? parseInt(s, 8) || 0 : 0;
  };

  while (pos + 512 <= buf.length) {
    const header = buf.subarray(pos, pos + 512);
    // Two zero blocks mark the end.
    if (header.every((b) => b === 0)) break;
    let name = dec.decode(header.subarray(0, 100)).replace(/\0.*$/, '');
    const size = octal(header.subarray(124, 136));
    const type = String.fromCharCode(header[156]);
    const prefix = dec.decode(header.subarray(345, 500)).replace(/\0.*$/, '');
    if (prefix) name = `${prefix}/${name}`;
    pos += 512;
    const dataStart = pos;
    pos += Math.ceil(size / 512) * 512;

    if (type === 'x' || type === 'g') {
      // pax extended header: parse "len key=value\n" records for path.
      const rec = dec.decode(buf.subarray(dataStart, dataStart + size));
      const m = rec.match(/\d+ path=([^\n]+)\n/);
      if (m) paxPath = m[1];
      continue;
    }
    if (type === 'L') { // GNU long name
      paxPath = dec.decode(buf.subarray(dataStart, dataStart + size)).replace(/\0.*$/, '');
      continue;
    }
    if (paxPath) { name = paxPath; paxPath = null; }
    if (type !== '0' && type !== '\0' && type !== '') continue; // only regular files

    // Strip the top-level "<owner>-<repo>-<sha>/" directory.
    const slash = name.indexOf('/');
    if (slash < 0) continue;
    const top = name.slice(0, slash);
    if (!resolvedSha) { const s = top.match(/-([0-9a-f]{7,40})$/i); if (s) resolvedSha = s[1]; }
    const rel = name.slice(slash + 1);
    if (!rel) continue;

    let content = null;
    const isText = TEXT_EXT.test(rel) || /(^|\/)(dockerfile|procfile|makefile|gemfile|rakefile)[^/]*$/i.test(rel);
    if (isText && size <= MAX_FILE_CONTENT && totalContent + size <= MAX_TOTAL_CONTENT) {
      content = dec.decode(buf.subarray(dataStart, dataStart + size));
      totalContent += size;
    }
    files.push({ path: rel, size, content });
  }
  return { files, resolvedSha };
}
