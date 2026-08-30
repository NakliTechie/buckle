/**
 * Deterministic, no-model extractor: reads docker-compose services (and a few
 * config signals) from the selected slice and builds an engine-valid topology
 * with READ provenance pointing at the file:line each node came from, plus
 * ASSUMED tags on every number it filled from DEFAULTS. This is the honest
 * no-key path (D4): a compose file IS evidence, so its nodes are READ, not
 * guessed. The model path (extract-model.js) is the richer, optional one.
 */
import { configForKind, DEFAULTS, DEFAULT_FALLBACK } from './defaults.js';

// image / service-name substring → engine kind. First match wins; order
// matters (more specific before generic).
const IMAGE_KIND = [
  [/postgres|postgis|pgvector|mysql|mariadb|cockroach|mongo|percona/, 'db'],
  [/\bredis\b|valkey|keydb|memcached/, 'cache'],
  [/rabbitmq|kafka|redpanda|nats|pulsar/, 'streambroker'],
  [/elasticsearch|opensearch|meilisearch|meili|solr|typesense/, 'searchindex'],
  [/clickhouse|influx|timescale|prometheus|victoria/, 'timeseriesdb'],
  [/minio|ceph|garage|seaweed/, 'objectstore'],
  [/nginx|traefik|haproxy|envoy|caddy|apache/, 'lb'],
  [/vault|consul|etcd/, 'apigateway'],
];
const NAME_KIND = [
  [/sidekiq|worker|celery|resque|delayed|beat|scheduler|cron/, 'worker'],
  [/postgres|\bdb\b|database|mysql|mariadb|mongo/, 'db'],
  [/\bredis\b|cache|valkey/, 'cache'],
  [/rabbit|kafka|broker|queue|nats/, 'streambroker'],
  [/elastic|opensearch|search|meili|solr/, 'searchindex'],
  [/minio|s3|storage|objectstore/, 'objectstore'],
  [/nginx|proxy|gateway|ingress|traefik|haproxy|\blb\b/, 'lb'],
  [/clickhouse|influx|timescale|metrics|prometheus/, 'timeseriesdb'],
  [/websocket|cable|ws\b/, 'websocket'],
  [/web|app|api|rails|puma|unicorn|gunicorn|node|frontend|backend|server/, 'service'],
];

function kindFor(name, image, command) {
  const img = (image || '').toLowerCase();
  const nm = (name || '').toLowerCase();
  const cmd = (command || '').toLowerCase();
  if (/sidekiq|celery|resque|worker|delayed|beat/.test(cmd)) return 'worker';
  for (const [re, k] of IMAGE_KIND) if (re.test(img)) return k;
  for (const [re, k] of NAME_KIND) if (re.test(nm)) return k;
  if (/bundle exec|rails|puma|unicorn|gunicorn|node |npm |yarn |python |flask|django|uvicorn/.test(cmd)) return 'service';
  return 'service';
}

/**
 * Very small docker-compose parser: enough for the `services:` map, each
 * service's `image`, `command`, `depends_on`, and the 1-based line the
 * service key sits on (for READ provenance). Not a general YAML parser.
 */
function parseCompose(text) {
  const lines = text.split(/\r?\n/);
  const services = [];
  let inServices = false;
  let servicesIndent = -1;
  let cur = null;
  const indentOf = (l) => l.match(/^\s*/)[0].length;
  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i];
    if (!line.trim() || line.trim().startsWith('#')) continue;
    const indent = indentOf(line);
    const keyMatch = line.match(/^(\s*)([A-Za-z0-9._-]+):\s*(.*)$/);
    if (!inServices) {
      if (/^services:\s*$/.test(line.trim()) && indent === 0) { inServices = true; servicesIndent = indent; }
      continue;
    }
    // A top-level (indent 0) non-services key ends the services block.
    if (indent === 0 && keyMatch && !/^services:/.test(line.trim())) { inServices = false; if (cur) services.push(cur); cur = null; continue; }
    if (!keyMatch) continue;
    const [, , key, rest] = keyMatch;
    // A service definition is the first indent level under services.
    if (cur === null || indent === cur.indent) {
      if (indent > servicesIndent && (cur === null || indent === cur.indent)) {
        if (cur) services.push(cur);
        cur = { name: key, line: i + 1, indent, image: '', command: '', dependsOn: [] };
        continue;
      }
    }
    if (cur && indent > cur.indent) {
      if (/^image$/.test(key)) cur.image = rest.replace(/["']/g, '').trim();
      else if (/^command$/.test(key)) cur.command = rest.replace(/["']/g, '').trim();
      else if (/^depends_on$/.test(key)) {
        const inline = rest.match(/\[([^\]]*)\]/);
        if (inline) cur.dependsOn = inline[1].split(',').map((s) => s.trim().replace(/["']/g, '')).filter(Boolean);
        else {
          for (let j = i + 1; j < lines.length; j += 1) {
            const dl = lines[j];
            if (!dl.trim()) continue;
            if (indentOf(dl) <= indent) break;
            const dm = dl.match(/^\s*-?\s*([A-Za-z0-9._-]+):?\s*$/);
            if (dm) cur.dependsOn.push(dm[1]);
          }
        }
      }
    }
  }
  if (cur) services.push(cur);
  return services;
}

const READ = (file, line) => ({ src: 'READ', file, line });
const ASSUMED = (kind) => ({ src: 'ASSUMED', default: (DEFAULTS[kind] || DEFAULT_FALLBACK).id });

/**
 * @param analyze the /api/analyze result (selected slice with contents).
 * @returns { topology, provenance, assumptions, note } or null if no compose.
 */
export function heuristicExtract(analyze) {
  const compose = (analyze.selected || []).find(
    (f) => /docker-compose[^/]*\.ya?ml$|(^|\/)compose[^/]*\.ya?ml$/i.test(f.path) && typeof f.content === 'string',
  );
  if (!compose) return null;

  const services = parseCompose(compose.content).filter((s) => s.image || s.command || /web|app|api|worker|db|redis|sidekiq|nginx|postgres/i.test(s.name));
  if (services.length === 0) return null;

  const nodes = [];
  const provenance = {};
  const idOf = new Map();
  services.forEach((s, i) => {
    const kind = kindFor(s.name, s.image, s.command);
    const id = s.name.replace(/[^A-Za-z0-9_-]/g, '_');
    idOf.set(s.name, id);
    const col = 1 + (i % 3);
    const row = Math.floor(i / 3);
    nodes.push({
      id, kind, label: s.name,
      x: 220 + col * 200, y: 120 + row * 130,
      config: configForKind(kind),
    });
    const cfgProv = {};
    for (const field of Object.keys(configForKind(kind))) cfgProv[field] = ASSUMED(kind);
    provenance[id] = { kind: READ(compose.path, s.line), label: READ(compose.path, s.line), config: cfgProv };
  });

  // Traffic origin.
  const clientId = 'client';
  nodes.unshift({ id: clientId, kind: 'client', label: 'Client', x: 60, y: 200, config: configForKind('client') });
  provenance[clientId] = { kind: ASSUMED('client'), config: { rps: ASSUMED('client') } };

  // Edges. Prefer depends_on; else wire by role.
  const edges = [];
  const addEdge = (from, to, weight = 1) => {
    if (from === to) return;
    const eid = `${from}->${to}`;
    if (edges.some((e) => e.id === eid)) return;
    edges.push({ id: eid, from, to, weight });
  };
  const byKind = (k) => nodes.filter((n) => n.kind === k).map((n) => n.id);
  const lbs = byKind('lb');
  const svcs = byKind('service');
  const workers = byKind('worker');
  const dataKinds = ['db', 'cache', 'streambroker', 'searchindex', 'objectstore', 'timeseriesdb'];
  const dataIds = dataKinds.flatMap(byKind);

  // client → lb (if any) → services; else client → services.
  const front = lbs.length ? lbs : svcs.length ? svcs : nodes.filter((n) => n.kind !== 'client').map((n) => n.id).slice(0, 1);
  for (const f of front) addEdge(clientId, f);
  if (lbs.length) for (const l of lbs) for (const s of svcs) addEdge(l, s);

  // depends_on refinement.
  let usedDepends = false;
  for (const s of services) {
    const from = idOf.get(s.name);
    for (const dep of s.dependsOn || []) {
      const to = idOf.get(dep);
      if (to) { addEdge(from, to); usedDepends = true; }
    }
  }
  // Fallback wiring: every service and worker reaches each data store.
  if (!usedDepends) {
    for (const s of [...svcs, ...workers]) for (const d of dataIds) addEdge(s, d);
  } else {
    // Still ensure workers reach data stores.
    for (const w of workers) for (const d of dataIds) addEdge(w, d);
  }
  // Services enqueue to workers via a broker if present, else directly.
  const brokers = byKind('streambroker');
  for (const s of svcs) for (const w of workers) addEdge(brokers[0] || s, w);

  const topology = { nodes, edges };
  const assumptionCount = Object.values(provenance).reduce(
    (n, p) => n + Object.values(p.config || {}).filter((t) => t.src === 'ASSUMED').length + (p.kind?.src === 'ASSUMED' ? 1 : 0), 0,
  );
  const readCount = Object.values(provenance).reduce(
    (n, p) => n + (p.kind?.src === 'READ' ? 1 : 0), 0,
  );
  return {
    topology, provenance,
    stats: { nodes: nodes.length, read: readCount, assumed: assumptionCount },
    note: `Read ${services.length} services from ${compose.path}. Every service-time and capacity number is a DEFAULT — edit and re-sweep.`,
    source: `heuristic:${compose.path}`,
  };
}
