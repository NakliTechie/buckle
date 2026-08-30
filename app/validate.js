/**
 * validateTopology — the one ingress (I3). Model output, pasted JSON, and
 * imported exports all pass through here before touching state.
 *
 * Written against the engine's own types (engine/sim/types.ts): SimNode,
 * SimEdge, Topology, NodeKind. Structural only — it checks shape, kinds,
 * reference integrity and numeric sanity; it does not fill defaults (the
 * defaults table owns that, with ASSUMED provenance, from C5).
 *
 * Returns { ok: true, topology } or { ok: false, errors: string[] }.
 * Errors are worded to be shown verbatim and to be actionable in the
 * C5 repair loop.
 */

// Mirror of NodeKind in engine/sim/types.ts:1-34 (a type, so erased at
// runtime; re-vendoring must keep this list in sync — gate-c1 checks it
// against the kinds PRESETS actually use).
export const NODE_KINDS = [
  'client', 'lb', 'service', 'cache', 'db', 'queue', 'worker', 'autoscaler',
  'region', 'cdn', 'ratelimiter', 'breaker', 'replica', 'shard', 'objectstore',
  'searchindex', 'timeseriesdb', 'graphdb', 'coldstorage', 'vectordb',
  'streambroker', 'pubsub', 'websocket', 'apigateway', 'sidecar', 'lambda',
  'cron', 'bulkhead', 'retryqueue', 'transcoder', 'edgecompute', 'writebehind',
  'loadshedder',
];

// NodeConfig fields the engine declares non-optional (engine/sim/types.ts).
const REQUIRED_CONFIG = [
  'capacity', 'serviceMs', 'serviceCv', 'queueLimit', 'hitRate', 'errorRate',
  'timeoutMs', 'retries',
];

export function validateTopology(input) {
  const errors = [];
  const err = (msg) => errors.push(msg);

  if (input === null || typeof input !== 'object' || Array.isArray(input)) {
    return { ok: false, errors: ['topology must be a JSON object with "nodes" and "edges" arrays'] };
  }
  if (!Array.isArray(input.nodes)) err('"nodes" must be an array');
  if (!Array.isArray(input.edges)) err('"edges" must be an array');
  if (errors.length) return { ok: false, errors };

  const ids = new Set();
  input.nodes.forEach((node, i) => {
    const at = `nodes[${i}]`;
    if (node === null || typeof node !== 'object') { err(`${at} must be an object`); return; }
    if (typeof node.id !== 'string' || node.id === '') err(`${at}.id must be a non-empty string`);
    else if (ids.has(node.id)) err(`${at}.id "${node.id}" is a duplicate`);
    else ids.add(node.id);
    if (!NODE_KINDS.includes(node.kind)) {
      err(`${at}.kind "${node.kind}" is not an engine kind (valid: ${NODE_KINDS.join(', ')})`);
    }
    if (typeof node.label !== 'string') err(`${at}.label must be a string`);
    if (!Number.isFinite(node.x) || !Number.isFinite(node.y)) err(`${at} needs finite x and y`);
    if (node.config === null || typeof node.config !== 'object') {
      err(`${at}.config must be an object`);
      return;
    }
    for (const field of REQUIRED_CONFIG) {
      const v = node.config[field];
      if (!Number.isFinite(v)) err(`${at}.config.${field} must be a finite number (got ${JSON.stringify(v)})`);
      else if (v < 0) err(`${at}.config.${field} must be >= 0 (got ${v})`);
    }
    for (const [field, v] of Object.entries(node.config)) {
      if (typeof v === 'number' && !Number.isFinite(v)) err(`${at}.config.${field} is ${v}`);
    }
  });

  const edgeIds = new Set();
  input.edges.forEach((edge, i) => {
    const at = `edges[${i}]`;
    if (edge === null || typeof edge !== 'object') { err(`${at} must be an object`); return; }
    if (typeof edge.id !== 'string' || edge.id === '') err(`${at}.id must be a non-empty string`);
    else if (edgeIds.has(edge.id)) err(`${at}.id "${edge.id}" is a duplicate`);
    else edgeIds.add(edge.id);
    for (const end of ['from', 'to']) {
      if (typeof edge[end] !== 'string' || !ids.has(edge[end])) {
        err(`${at}.${end} "${edge[end]}" does not name a node id`);
      }
    }
    if (!Number.isFinite(edge.weight) || edge.weight < 0) {
      err(`${at}.weight must be a finite number >= 0 (got ${JSON.stringify(edge.weight)})`);
    }
    if (edge.control !== undefined && typeof edge.control !== 'boolean') {
      err(`${at}.control must be a boolean when present`);
    }
    for (const field of ['latencyMs', 'bandwidthRps', 'lossRate']) {
      if (edge[field] !== undefined && (!Number.isFinite(edge[field]) || edge[field] < 0)) {
        err(`${at}.${field} must be a finite number >= 0 when present`);
      }
    }
  });

  if (errors.length) return { ok: false, errors };
  return { ok: true, topology: input };
}
