/**
 * Cribbed from breakscale (D13, MIT): KIND_NAME, KIND_GROUPS, the Lucide
 * kind→icon primitives (extracted from nodeVisuals.ts + lucide-react@1.34.0),
 * and the number formatters from format.ts. Ported verbatim in behaviour;
 * the skin (colour, weight) is rebound to Buckle's Dense tokens in CSS.
 * See CRIB.md.
 */
import KIND_ICON from './kind-icons.json' with { type: 'json' };
export { KIND_ICON };

export const KIND_NAME = {
  client: 'Client', lb: 'Load balancer', service: 'Service', cache: 'Cache',
  db: 'Database', queue: 'Queue', worker: 'Worker', replica: 'Read replicas',
  shard: 'Sharded store', autoscaler: 'Autoscaler', region: 'Region', cdn: 'CDN',
  ratelimiter: 'Rate limiter', breaker: 'Circuit breaker', objectstore: 'Object storage',
  searchindex: 'Search index', timeseriesdb: 'Time-series store', graphdb: 'Graph database',
  coldstorage: 'Cold storage', vectordb: 'Vector database', streambroker: 'Stream broker',
  pubsub: 'Pub/sub topic', websocket: 'WebSocket gateway', apigateway: 'API gateway',
  sidecar: 'Sidecar proxy', lambda: 'Lambda', cron: 'Cron job', bulkhead: 'Bulkhead',
  retryqueue: 'Retry queue', transcoder: 'Transcoder', edgecompute: 'Edge compute',
  writebehind: 'Write-behind cache', loadshedder: 'Load shedder',
};

export const KIND_GROUPS = [
  { id: 'traffic', title: 'Traffic', kinds: ['client', 'lb', 'cdn', 'edgecompute'] },
  { id: 'compute', title: 'Compute', kinds: ['service', 'worker', 'queue', 'retryqueue', 'transcoder'] },
  { id: 'data', title: 'Data', kinds: ['db', 'cache', 'writebehind', 'replica', 'shard'] },
  { id: 'stores', title: 'Specialised stores', kinds: ['objectstore', 'searchindex', 'timeseriesdb', 'graphdb', 'vectordb', 'coldstorage'] },
  { id: 'messaging', title: 'Messaging', kinds: ['streambroker', 'pubsub', 'websocket', 'lambda', 'cron'] },
  { id: 'control', title: 'Control', kinds: ['ratelimiter', 'loadshedder', 'breaker', 'bulkhead', 'autoscaler', 'region', 'apigateway', 'sidecar'] },
];

const GROUP_OF_KIND = {};
for (const g of KIND_GROUPS) for (const k of g.kinds) GROUP_OF_KIND[k] = g.id;
export const groupOfKind = (kind) => GROUP_OF_KIND[kind] || 'compute';

/* ---- format.ts port (behaviour verbatim) ------------------------------ */
export const NA = 'n/a';
const finite = (v) => (typeof v === 'number' && Number.isFinite(v) ? v : null);
const trim = (s) => (s.endsWith('.0') ? s.slice(0, -2) : s);
const group = (n) => (n >= 10000 ? n.toLocaleString('en-US') : `${n}`);

export const healthOfLoad = (u) => { const v = finite(u); return v === null ? 'ok' : v >= 0.9 ? 'danger' : v >= 0.7 ? 'warn' : 'ok'; };
export const healthOfErr = (e) => { const v = finite(e); return v === null ? 'ok' : v >= 0.05 ? 'danger' : v >= 0.005 ? 'warn' : 'ok'; };
export const healthOfLatency = (ms) => { const v = finite(ms); return v === null ? 'ok' : v >= 1000 ? 'danger' : v >= 200 ? 'warn' : 'ok'; };

export function formatMs(ms) {
  const v = finite(ms);
  if (v === null || v < 0) return NA;
  if (v === 0) return '0ms';
  if (v < 0.1) return '<0.1ms';
  if (v < 10) return `${trim(v.toFixed(1))}ms`;
  if (v < 1000) return `${Math.round(v)}ms`;
  if (v < 10000) return `${trim((v / 1000).toFixed(1))}s`;
  return `${group(Math.round(v / 1000))}s`;
}
export function formatRateBare(rps) {
  const v = finite(rps);
  if (v === null || v < 0) return NA;
  if (v === 0) return '0';
  if (v < 0.1) return '<0.1';
  if (v < 10) return trim(v.toFixed(1));
  if (v < 1000) return `${Math.round(v)}`;
  if (v < 10000) return `${trim((v / 1000).toFixed(1))}k`;
  return `${Math.round(v / 1000)}k`;
}
export function formatRate(rps) { const b = formatRateBare(rps); return b === NA ? NA : `${b}/s`; }
export function formatCount(n) {
  const v = finite(n);
  if (v === null || v < 0) return NA;
  if (v === 0) return '0';
  if (v < 1) return '<1';
  if (v < 1000000) return group(Math.round(v));
  return `${trim((v / 1000000).toFixed(1))}M`;
}
export function formatPct(fraction) {
  const v = finite(fraction);
  if (v === null || v < 0) return NA;
  if (v === 0) return '0%';
  const pct = v * 100;
  if (pct < 0.1) return '<0.1%';
  if (pct < 10) return `${trim(pct.toFixed(1))}%`;
  if (pct < 100) {
    const r = Math.round(pct);
    if (r >= 100) return `${(Math.floor(pct * 10) / 10).toFixed(1)}%`;
    return `${r}%`;
  }
  return `${Math.round(pct)}%`;
}
