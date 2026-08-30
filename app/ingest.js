/**
 * Ingest client — talks to the edge worker's /api/analyze, which pulls the
 * GitHub tarball, untars it, and runs the deterministic selector. The tab
 * receives the file tree + the selected slice with contents. The GitHub token,
 * when present, is memory-only (D6) and sent as a header, never stored.
 */
export async function analyzeRepo(repo, { ref = '', token = '', base = '' } = {}) {
  const params = new URLSearchParams({ repo });
  if (ref) params.set('ref', ref);
  const headers = {};
  if (token) headers['x-github-token'] = token;
  const resp = await fetch(`${base}/api/analyze?${params}`, { headers });
  const data = await resp.json().catch(() => ({ error: `bad response ${resp.status}` }));
  if (!resp.ok || data.error) throw new Error(data.error || `analyze failed (${resp.status})`);
  return data;
}
