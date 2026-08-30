/**
 * Deterministic file selection (§4.2), no model. Scores files by path/name
 * rules into tiers, takes the highest tiers within the read budget (D7: ≤ 40
 * files, ≤ 200 KB). Runs in the Worker (where the tarball is untarred) and is
 * unit-tested from Node. Same file set in → same slice out.
 *
 * The rules live in one table so they extend without touching the walker.
 */
export const BUDGET = { maxFiles: 40, maxBytes: 200 * 1024 };

// Tier 1 (highest) → tier 5. First matching tier wins. Each entry is a
// predicate over the lowercased repo-relative path.
const TIERS = [
  {
    tier: 1,
    label: 'orchestration',
    test: (p) =>
      /(^|\/)docker-compose[^/]*\.ya?ml$/.test(p) ||
      /(^|\/)compose[^/]*\.ya?ml$/.test(p) ||
      /(^|\/)procfile[^/]*$/.test(p) ||
      /(^|\/)(k8s|helm)\//.test(p) ||
      /\.tf$/.test(p) ||
      /(^|\/)fly\.toml$/.test(p) ||
      /(^|\/)render\.yaml$/.test(p) ||
      /(^|\/)app\.json$/.test(p) ||
      /(^|\/)dockerfile[^/]*$/.test(p),
  },
  {
    tier: 2,
    label: 'service config',
    test: (p) =>
      /(^|\/)config\/(database|sidekiq|cable|queue|puma)\.(yml|rb)$/.test(p) ||
      /(^|\/)config\/redis[^/]*$/.test(p) ||
      /(^|\/)config\/initializers\/(sidekiq|redis|http|faraday)[^/]*$/.test(p) ||
      /\.env\.example$/.test(p) ||
      /(^|\/)ormconfig[^/]*$/.test(p) ||
      /(^|\/)settings\.py$/.test(p) ||
      /(^|\/)celery[^/]*\.py$/.test(p) ||
      /(^|\/)application\.ya?ml$/.test(p),
  },
  {
    tier: 3,
    label: 'routes',
    test: (p) =>
      /(^|\/)config\/routes\.rb$/.test(p) ||
      /(^|\/)routes\/[^/]+\.ts$/.test(p) ||
      /(^|\/)urls\.py$/.test(p) ||
      /\.proto$/.test(p),
  },
];

// Tier 4 (client init) and Tier 5 (retry/timeout) need file CONTENT, matched
// by these grep patterns. Applied only to source files, shortest path first,
// one file per distinct client pattern.
const TIER4_CLIENTS = [
  { key: 'redis', re: /Redis\.new|new Redis\(|redis\.createClient/ },
  { key: 'sidekiq', re: /Sidekiq|ActiveJob|perform_async/ },
  { key: 'faraday', re: /Faraday|HTTParty|Net::HTTP/ },
  { key: 'http', re: /axios|fetch\(|got\(|node-fetch/ },
  { key: 'pg', re: /new Pool\(|pg\.Pool|ActiveRecord::Base|Sequel\.connect/ },
  { key: 'celery', re: /celery|Celery\(/ },
  { key: 'boto3', re: /boto3/ },
  { key: 'grpc', re: /grpc|Grpc/ },
];
const TIER5_RE = /retry|timeout|circuit|backoff|rate_limit|rateLimit/;

const SOURCE_EXT = /\.(rb|ts|tsx|js|jsx|py|go|java|kt|ex|exs|rs|php|yml|yaml|toml|env)$/i;
const SKIP_DIR = /(^|\/)(node_modules|vendor|\.git|dist|build|tmp|log|test|spec|__tests__|fixtures|\.github\/workflows)\//i;

/**
 * @param files [{ path, size, content }] — the full untarred file list.
 * @returns { selected: [{path,size,tier,label}], tree: [{path,size}], truncated }
 */
export function selectFiles(files) {
  const tree = files.map((f) => ({ path: f.path, size: f.size })).sort((a, b) => a.path.localeCompare(b.path));
  const candidates = [];
  const seenTier4 = new Set();

  const sorted = [...files].sort((a, b) => a.path.length - b.path.length || a.path.localeCompare(b.path));

  for (const f of sorted) {
    const p = f.path.toLowerCase();
    if (SKIP_DIR.test('/' + p)) continue;
    let matched = null;
    for (const t of TIERS) {
      if (t.test(p)) { matched = { tier: t.tier, label: t.label }; break; }
    }
    if (!matched && SOURCE_EXT.test(p) && typeof f.content === 'string') {
      for (const c of TIER4_CLIENTS) {
        if (!seenTier4.has(c.key) && c.re.test(f.content)) {
          seenTier4.add(c.key);
          matched = { tier: 4, label: `client:${c.key}` };
          break;
        }
      }
      if (!matched && TIER5_RE.test(f.content) && f.content.length < 20000) {
        matched = { tier: 5, label: 'retry/timeout' };
      }
    }
    if (matched) candidates.push({ path: f.path, size: f.size, tier: matched.tier, label: matched.label });
  }

  candidates.sort((a, b) => a.tier - b.tier || a.path.localeCompare(b.path));

  const selected = [];
  let bytes = 0;
  let truncated = false;
  for (const c of candidates) {
    if (selected.length >= BUDGET.maxFiles || bytes + c.size > BUDGET.maxBytes) { truncated = true; break; }
    selected.push(c);
    bytes += c.size;
  }
  return { selected, tree, truncated, totalFiles: files.length, selectedBytes: bytes };
}
