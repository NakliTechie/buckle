/**
 * DEFAULTS table (§4.3, editable in UI). Per component kind: the service-time
 * mean/CV, capacity, timeout, retries, queue limit a topology gets when the
 * files don't evidence a number. Each row has an id so an ASSUMED provenance
 * tag can point at exactly the default it used (D5, I2).
 *
 * Values follow breakscale's own per-kind defaults where it has them (O2:
 * verbatim, they are tuned to the engine); the rest are teaching-grade
 * round numbers, labelled as assumptions everywhere they surface.
 */
export const DEFAULTS = {
  client:     { id: 'def.client',     serviceMs: 0,   serviceCv: 0,   capacity: 1,  queueLimit: 0,    timeoutMs: 2000, retries: 0, rps: 50 },
  lb:         { id: 'def.lb',         serviceMs: 1,   serviceCv: 0.3, capacity: 256, queueLimit: 1000, timeoutMs: 1000, retries: 0 },
  service:    { id: 'def.service',    serviceMs: 40,  serviceCv: 0.5, capacity: 16, queueLimit: 200,  timeoutMs: 2000, retries: 0, instances: 1 },
  worker:     { id: 'def.worker',     serviceMs: 120, serviceCv: 0.8, capacity: 10, queueLimit: 5000, timeoutMs: 30000, retries: 2, instances: 1 },
  cache:      { id: 'def.cache',      serviceMs: 1,   serviceCv: 0.4, capacity: 64, queueLimit: 1000, timeoutMs: 500,  retries: 0, hitRate: 0.85 },
  db:         { id: 'def.db',         serviceMs: 8,   serviceCv: 0.6, capacity: 20, queueLimit: 500,  timeoutMs: 2000, retries: 0 },
  queue:      { id: 'def.queue',      serviceMs: 2,   serviceCv: 0.3, capacity: 100, queueLimit: 100000, timeoutMs: 0, retries: 0 },
  streambroker:{ id: 'def.streambroker', serviceMs: 2, serviceCv: 0.3, capacity: 200, queueLimit: 1000000, timeoutMs: 0, retries: 0 },
  searchindex:{ id: 'def.searchindex', serviceMs: 25, serviceCv: 0.7, capacity: 12, queueLimit: 300, timeoutMs: 3000, retries: 0 },
  objectstore:{ id: 'def.objectstore', serviceMs: 30, serviceCv: 0.9, capacity: 32, queueLimit: 500, timeoutMs: 5000, retries: 1 },
  timeseriesdb:{ id: 'def.timeseriesdb', serviceMs: 15, serviceCv: 0.6, capacity: 16, queueLimit: 400, timeoutMs: 3000, retries: 0 },
  websocket:  { id: 'def.websocket',  serviceMs: 5,   serviceCv: 0.5, capacity: 1000, queueLimit: 5000, timeoutMs: 60000, retries: 0 },
  apigateway: { id: 'def.apigateway', serviceMs: 3,   serviceCv: 0.3, capacity: 128, queueLimit: 2000, timeoutMs: 2000, retries: 0 },
  cdn:        { id: 'def.cdn',        serviceMs: 2,   serviceCv: 0.4, capacity: 512, queueLimit: 5000, timeoutMs: 1000, retries: 0, hitRate: 0.9 },
};

export const DEFAULT_FALLBACK = { id: 'def.service', serviceMs: 40, serviceCv: 0.5, capacity: 16, queueLimit: 200, timeoutMs: 2000, retries: 0 };

/** The full NodeConfig the engine needs, filled from a kind's default row. */
export function configForKind(kind, overrides = {}) {
  const d = DEFAULTS[kind] || DEFAULT_FALLBACK;
  const base = {
    capacity: d.capacity ?? 16,
    serviceMs: d.serviceMs ?? 20,
    serviceCv: d.serviceCv ?? 0.5,
    queueLimit: d.queueLimit ?? 200,
    hitRate: d.hitRate ?? 0,
    errorRate: 0,
    timeoutMs: d.timeoutMs ?? 2000,
    retries: d.retries ?? 0,
  };
  if (d.instances) base.instances = d.instances;
  if (d.rps != null) base.rps = d.rps;
  return { ...base, ...overrides };
}
