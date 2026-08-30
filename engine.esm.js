// engine/sim/behaviour-data.ts
var KEYSPACE = 64;
function clampInt(v, min) {
  const n = Math.floor(v);
  return n < min ? min : n;
}
function clamp01(v) {
  return v < 0 ? 0 : v > 1 ? 1 : v;
}
var scratch = [];
function instanceScratch(n) {
  if (scratch.length !== n) scratch.length = n;
  return scratch;
}
function replicaExt(state) {
  return state.ext;
}
function readCapacity(state) {
  return clampInt(state.config.capacity, 1) * clampInt(state.config.replicaCount, 1);
}
function writeCapacity(state) {
  return clampInt(state.config.capacity, 1);
}
var replica = {
  kind: "replica",
  servesRequests: true,
  generatesLoad: false,
  pullsFromQueues: false,
  buffersForConsumers: false,
  // Admission, queueing and slot release are all handled inside onAdmit and
  // the drain callback, because reads and writes draw from two different
  // pools. The engine's single busy/waiting pair cannot express that.
  pump: "none",
  creditsJoinCompletion: true,
  initState(_state) {
    const visibleAt = new Float64Array(KEYSPACE);
    visibleAt.fill(-Infinity);
    return {
      visibleAt,
      readBusy: 0,
      writeBusy: 0,
      readQueue: [],
      writeQueue: [],
      readHead: 0,
      writeHead: 0
    };
  },
  onAdmit(ctx, state, req) {
    const ext = replicaExt(state);
    const isWrite = ctx.roll() >= clamp01(state.config.readFraction);
    ctx.markWrite(req, isWrite);
    const queue2 = isWrite ? ext.writeQueue : ext.readQueue;
    const head = isWrite ? ext.writeHead : ext.readHead;
    const busy = isWrite ? ext.writeBusy : ext.readBusy;
    const capacity = isWrite ? writeCapacity(state) : readCapacity(state);
    if (busy < capacity) {
      startReplicaService(ctx, state, req, isWrite);
      return "handled";
    }
    if (queue2.length - head >= ctx.effectiveQueueLimit(state)) return "shed";
    queue2.push(req);
    return "handled";
  },
  // A replica set is a primary plus N read replicas, and drawing it as one
  // box hides the reason adding replicas does not fix a write bottleneck.
  instanceModel: "custom",
  /**
   * Unit 0 is the PRIMARY and reports the write pool's utilisation; units
   * 1..replicaCount are the read replicas.
   *
   * The read replicas all report the same number, and that is the honest
   * reading rather than a shortcut: the engine pools read slots across the
   * whole set, so a read is served by whichever replica is free and no
   * individual replica has a utilisation of its own. Splitting the pool's
   * load evenly across the drawn units says exactly that. What the vector
   * DOES separate -- the primary from the read set -- is the distinction that
   * carries the lesson, because it is where the two pools genuinely differ.
   */
  reportInstances(ctx, state) {
    const ext = replicaExt(state);
    const replicas = clampInt(state.config.replicaCount, 1);
    const out = instanceScratch(replicas + 1);
    const writeCap = writeCapacity(state);
    out[0] = writeCap > 0 ? Math.min(ext.writeBusy / writeCap, 1) : 0;
    const readCap = readCapacity(state);
    const readUtil = readCap > 0 ? Math.min(ext.readBusy / readCap, 1) : 0;
    for (let i = 1; i <= replicas; i++) out[i] = readUtil;
    ctx.reportInstances(state, out, 0);
  },
  onTick(ctx, state, _dtMs) {
    const ext = replicaExt(state);
    ctx.reportOccupancy(
      state,
      ext.readBusy + ext.writeBusy,
      readCapacity(state) + writeCapacity(state)
    );
  },
  decorateStats(ctx, state, stats) {
    const fresh = ctx.counterRate(state, "freshRead");
    const stale = ctx.counterRate(state, "staleRead");
    const total = fresh + stale;
    stats.staleReadRate = total > 0 ? stale / total : 0;
    const ext = replicaExt(state);
    stats.queued = ext.readQueue.length - ext.readHead + (ext.writeQueue.length - ext.writeHead);
    stats.inFlight = ext.readBusy + ext.writeBusy;
  }
};
function startReplicaService(ctx, state, req, isWrite) {
  const ext = replicaExt(state);
  const key = req.key % KEYSPACE;
  if (isWrite) {
    ext.writeBusy++;
  } else {
    ext.readBusy++;
    const stale = ctx.now < ext.visibleAt[key];
    ctx.countCustom(state, stale ? "staleRead" : "freshRead", 1);
  }
  if (isWrite) {
    const lag = state.config.replicationLagMs;
    const visible = ctx.now + (lag > 0 ? lag : 0);
    if (visible > ext.visibleAt[key]) ext.visibleAt[key] = visible;
  }
  ctx.serveWithin(state, req, onReplicaDrained);
}
function onReplicaDrained(ctx, state, req) {
  const ext = replicaExt(state);
  if (req.isWrite) {
    if (ext.writeBusy > 0) ext.writeBusy--;
    const lag = state.config.replicationLagMs;
    const key = req.key % KEYSPACE;
    const visible = ctx.now + (lag > 0 ? lag : 0);
    if (visible > ext.visibleAt[key]) ext.visibleAt[key] = visible;
  } else if (ext.readBusy > 0) {
    ext.readBusy--;
  }
  pumpReplica(ctx, state);
}
function pumpReplica(ctx, state) {
  const ext = replicaExt(state);
  while (ext.writeBusy < writeCapacity(state) && ext.writeHead < ext.writeQueue.length) {
    const req = ext.writeQueue[ext.writeHead];
    ext.writeQueue[ext.writeHead] = null;
    ext.writeHead++;
    compact(
      ext.writeQueue,
      () => ext.writeHead,
      (v) => ext.writeHead = v
    );
    startReplicaService(ctx, state, req, true);
  }
  while (ext.readBusy < readCapacity(state) && ext.readHead < ext.readQueue.length) {
    const req = ext.readQueue[ext.readHead];
    ext.readQueue[ext.readHead] = null;
    ext.readHead++;
    compact(
      ext.readQueue,
      () => ext.readHead,
      (v) => ext.readHead = v
    );
    startReplicaService(ctx, state, req, false);
  }
}
function shardExt(state) {
  return state.ext;
}
function makeShardExt(count) {
  const queues = [];
  for (let i = 0; i < count; i++) queues.push([]);
  return {
    busy: new Int32Array(count),
    queues,
    heads: new Int32Array(count),
    utilization: new Float64Array(count),
    report: new Array(count).fill(0),
    sized: count,
    lastIntegrateMs: 0
  };
}
function ensureSized(state, ext) {
  const want = clampInt(state.config.shardCount, 1);
  if (ext.sized === want) return ext;
  const orphans = [];
  for (let i = 0; i < ext.sized; i++) {
    const q = ext.queues[i];
    for (let j = ext.heads[i]; j < q.length; j++) {
      if (q[j]) orphans.push(q[j]);
    }
  }
  const next = makeShardExt(want);
  next.lastIntegrateMs = ext.lastIntegrateMs;
  for (let i = 0; i < Math.min(ext.sized, want); i++) next.busy[i] = ext.busy[i];
  for (const req of orphans) next.queues[req.key % want].push(req);
  Object.assign(ext, next);
  return ext;
}
function shardIndexFor(ctx, state, req, count) {
  const hot = clamp01(state.config.hotKeyFraction);
  const roll = ctx.roll();
  if (hot > 0 && roll < hot) return 0;
  return (req.key % count + count) % count;
}
var shard = {
  kind: "shard",
  servesRequests: true,
  generatesLoad: false,
  pullsFromQueues: false,
  buffersForConsumers: false,
  pump: "none",
  creditsJoinCompletion: true,
  // A shard serves from shardCapacity (per partition), never from `capacity`.
  // Without this an autoscaler pointed here writes a field this kind ignores.
  scaleField: "shardCapacity",
  // One unit per partition. This is the kind the instance model exists for:
  // the per-partition numbers are genuinely independent, and the gap between
  // one pinned at 1.0 and the rest near idle IS the sharding lesson.
  instanceModel: "custom",
  /**
   * Unit i is partition i, reporting that partition's own smoothed
   * utilisation -- the same vector already surfaced as `shardUtilization`,
   * published here through the general instance channel so a consumer can
   * draw any partitioned kind without knowing the word "shard".
   */
  reportInstances(ctx, state) {
    const ext = ensureSized(state, shardExt(state));
    const out = instanceScratch(ext.sized);
    for (let i = 0; i < ext.sized; i++) out[i] = ext.utilization[i];
    ctx.reportInstances(state, out, 0);
  },
  initState(state) {
    return makeShardExt(clampInt(state.config.shardCount, 1));
  },
  onAdmit(ctx, state, req) {
    const ext = ensureSized(state, shardExt(state));
    const count = ext.sized;
    const idx = shardIndexFor(ctx, state, req, count);
    const capacity = clampInt(state.config.shardCapacity, 1);
    if (ext.busy[idx] < capacity) {
      startShardService(ctx, state, ext, idx, req);
      return "handled";
    }
    const q = ext.queues[idx];
    if (q.length - ext.heads[idx] >= ctx.effectiveQueueLimit(state)) return "shed";
    q.push(req);
    return "handled";
  },
  onTick(ctx, state, _dtMs) {
    const ext = ensureSized(state, shardExt(state));
    const dt = ctx.now - ext.lastIntegrateMs;
    ext.lastIntegrateMs = ctx.now;
    if (dt <= 0) return;
    const capacity = clampInt(state.config.shardCapacity, 1);
    const alpha = 1 - Math.exp(-dt / 500);
    let busy = 0;
    for (let i = 0; i < ext.sized; i++) {
      const instant = Math.min(ext.busy[i] / capacity, 1);
      ext.utilization[i] += (instant - ext.utilization[i]) * alpha;
      busy += ext.busy[i];
    }
    ctx.reportOccupancy(state, busy, ext.sized * capacity);
  },
  decorateStats(ctx, state, stats) {
    const ext = ensureSized(state, shardExt(state));
    const n = ext.sized;
    let max = 0;
    let min = Infinity;
    let sum = 0;
    let busy = 0;
    let queued = 0;
    for (let i = 0; i < n; i++) {
      const u = ext.utilization[i];
      ext.report[i] = u;
      if (u > max) max = u;
      if (u < min) min = u;
      sum += u;
      busy += ext.busy[i];
      queued += ext.queues[i].length - ext.heads[i];
    }
    ext.report.length = n;
    stats.maxShardUtilization = max;
    stats.minShardUtilization = min === Infinity ? 0 : min;
    stats.utilization = n > 0 ? sum / n : 0;
    stats.inFlight = busy;
    stats.queued = queued;
    ctx.reportShardUtilization(state, ext.report);
    stats.shardUtilization = ext.report;
  }
};
function startShardService(ctx, state, ext, idx, req) {
  ext.busy[idx]++;
  shardOf.set(req, idx);
  ctx.serveWithin(state, req, onShardDrained);
}
function onShardDrained(ctx, state, req) {
  const ext = shardExt(state);
  const idx = shardOf.get(req);
  shardOf.delete(req);
  if (idx === void 0 || idx >= ext.sized) return;
  if (ext.busy[idx] > 0) ext.busy[idx]--;
  const capacity = clampInt(state.config.shardCapacity, 1);
  const q = ext.queues[idx];
  while (ext.busy[idx] < capacity && ext.heads[idx] < q.length) {
    const next = q[ext.heads[idx]];
    q[ext.heads[idx]] = null;
    ext.heads[idx]++;
    if (ext.heads[idx] > 64 && ext.heads[idx] * 2 >= q.length) {
      q.splice(0, ext.heads[idx]);
      ext.heads[idx] = 0;
    }
    if (!next) continue;
    startShardService(ctx, state, ext, idx, next);
  }
}
var shardOf = /* @__PURE__ */ new WeakMap();
function compact(q, getHead, setHead) {
  const head = getHead();
  if (head > 64 && head * 2 >= q.length) {
    q.splice(0, head);
    setHead(0);
  }
}
var DATA_BEHAVIOURS = [replica, shard];

// engine/sim/behaviour-edge.ts
var cdn = {
  kind: "cdn",
  servesRequests: true,
  generatesLoad: false,
  pullsFromQueues: false,
  buffersForConsumers: false,
  pump: "own",
  creditsJoinCompletion: true,
  onServiceComplete: (ctx, state, _req) => {
    if (ctx.roll() < clamp012(state.config.hitRate)) {
      ctx.countHit(state);
      return "complete";
    }
    ctx.countMiss(state);
    ctx.countCustom(state, "originFetch", 1);
    return state.out.length === 0 ? "complete" : "downstream";
  },
  decorateStats: (ctx, state, stats) => {
    stats.originFetchRate = ctx.counterRate(state, "originFetch");
  }
};
function limitRate(state) {
  const r = state.config.rateLimitRps;
  return r !== void 0 && r > 0 ? r : 0;
}
function limitBurst(state) {
  const b = state.config.burst;
  if (b !== void 0 && b > 0) return b;
  const r = limitRate(state);
  return r > 0 ? r : 1;
}
function refill(ctx, state, b) {
  const burst = limitBurst(state);
  if (burst !== b.lastBurst) {
    if (b.tokens > burst) b.tokens = burst;
    b.lastBurst = burst;
  }
  const elapsed = ctx.now - b.lastRefillMs;
  if (elapsed <= 0) return;
  b.lastRefillMs = ctx.now;
  const rate = limitRate(state);
  if (rate <= 0) return;
  b.tokens += elapsed / 1e3 * rate;
  if (b.tokens > burst) b.tokens = burst;
}
function projectedTokens(ctx, state, b) {
  const burst = limitBurst(state);
  let tokens = b.tokens > burst ? burst : b.tokens;
  const rate = limitRate(state);
  const elapsed = ctx.now - b.lastRefillMs;
  if (rate > 0 && elapsed > 0) {
    tokens += elapsed / 1e3 * rate;
    if (tokens > burst) tokens = burst;
  }
  return tokens;
}
var ratelimiter = {
  kind: "ratelimiter",
  // A limiter is a doorman, not a server: it holds no slots and its
  // utilisation would be meaningless, so it reports none.
  servesRequests: false,
  generatesLoad: false,
  pullsFromQueues: false,
  buffersForConsumers: false,
  pump: "none",
  creditsJoinCompletion: true,
  initState: (state) => ({
    // A fresh limiter starts FULL, so an idle system absorbs one full burst
    // instantly -- which is exactly the behaviour a token bucket is chosen
    // for, and the first thing worth demonstrating.
    tokens: limitBurst(state),
    lastRefillMs: 0,
    lastBurst: limitBurst(state)
  }),
  onAdmit: (ctx, state, req) => {
    const b = state.ext;
    refill(ctx, state, b);
    if (limitRate(state) <= 0) {
      ctx.countCustom(state, "admitted", 1);
      return "passthru";
    }
    if (b.tokens >= 1) {
      b.tokens -= 1;
      ctx.countCustom(state, "admitted", 1);
      return "passthru";
    }
    ctx.countCustom(state, "throttled", 1);
    ctx.reject(state, req, "throttled");
    return "handled";
  },
  decorateStats: (ctx, state, stats) => {
    const b = state.ext;
    stats.admittedRate = ctx.counterRate(state, "admitted");
    stats.throttledRate = ctx.counterRate(state, "throttled");
    stats.tokens = b ? projectedTokens(ctx, state, b) : 0;
  }
};
var BREAKER_BUCKETS = 10;
function cfgErrorThreshold(state) {
  const v = state.config.errorThreshold;
  return v !== void 0 ? clamp012(v) : 0.5;
}
function cfgWindowMs(state) {
  const v = state.config.windowMs;
  return v !== void 0 && v > 0 ? v : 5e3;
}
function cfgOpenMs(state) {
  const v = state.config.openMs;
  return v !== void 0 && v > 0 ? v : 3e3;
}
function cfgHalfOpenProbes(state) {
  const v = state.config.halfOpenProbes;
  return v !== void 0 && v >= 1 ? Math.floor(v) : 3;
}
function bucketOf(now, windowMs) {
  return Math.floor(now / (windowMs / BREAKER_BUCKETS));
}
function recordOutcome(b, now, windowMs, ok) {
  const stamp = bucketOf(now, windowMs);
  const idx = (stamp % BREAKER_BUCKETS + BREAKER_BUCKETS) % BREAKER_BUCKETS;
  if (b.stamps[idx] !== stamp) {
    b.stamps[idx] = stamp;
    b.total[idx] = 0;
    b.failed[idx] = 0;
  }
  b.total[idx] += 1;
  if (!ok) b.failed[idx] += 1;
}
function windowStats(b, now, windowMs) {
  const current = bucketOf(now, windowMs);
  let total = 0;
  let failed = 0;
  for (let i = 0; i < BREAKER_BUCKETS; i++) {
    const age = current - b.stamps[i];
    if (age >= 0 && age < BREAKER_BUCKETS) {
      total += b.total[i];
      failed += b.failed[i];
    }
  }
  return { rate: total > 0 ? failed / total : 0, total };
}
function clearWindow(b) {
  b.total.fill(0);
  b.failed.fill(0);
  b.stamps.fill(-1);
}
var BREAKER_MIN_SAMPLES = 5;
var breaker = {
  kind: "breaker",
  // Like the limiter, a pass-through gate rather than a server.
  servesRequests: false,
  generatesLoad: false,
  pullsFromQueues: false,
  buffersForConsumers: false,
  pump: "none",
  creditsJoinCompletion: true,
  // The whole point: it needs to see how its dependency is doing.
  observesOutcome: true,
  initState: () => ({
    phase: "closed",
    openedAtMs: 0,
    probesSent: 0,
    probesOk: 0,
    total: new Float64Array(BREAKER_BUCKETS),
    failed: new Float64Array(BREAKER_BUCKETS),
    stamps: new Float64Array(BREAKER_BUCKETS).fill(-1),
    trips: 0
  }),
  onAdmit: (ctx, state, req) => {
    const b = state.ext;
    if (b.phase === "open" && ctx.now - b.openedAtMs >= cfgOpenMs(state)) {
      b.phase = "half-open";
      b.probesSent = 0;
      b.probesOk = 0;
    }
    if (b.phase === "open") {
      ctx.countCustom(state, "rejected", 1);
      ctx.reject(state, req, "rejected");
      return "handled";
    }
    if (b.phase === "half-open") {
      if (b.probesSent >= cfgHalfOpenProbes(state)) {
        ctx.countCustom(state, "rejected", 1);
        ctx.reject(state, req, "rejected");
        return "handled";
      }
      b.probesSent++;
    }
    ctx.countCustom(state, "admitted", 1);
    return "passthru";
  },
  onDownstreamResult: (ctx, state, _req, ok, _reason) => {
    const b = state.ext;
    if (b.phase === "half-open") {
      if (!ok) {
        b.phase = "open";
        b.openedAtMs = ctx.now;
        b.trips++;
        clearWindow(b);
        return;
      }
      b.probesOk++;
      if (b.probesOk >= cfgHalfOpenProbes(state)) {
        b.phase = "closed";
        clearWindow(b);
      }
      return;
    }
    if (b.phase !== "closed") return;
    const windowMs = cfgWindowMs(state);
    recordOutcome(b, ctx.now, windowMs, ok);
    const { rate, total } = windowStats(b, ctx.now, windowMs);
    if (total >= BREAKER_MIN_SAMPLES && rate > cfgErrorThreshold(state)) {
      b.phase = "open";
      b.openedAtMs = ctx.now;
      b.trips++;
      clearWindow(b);
    }
  },
  /**
   * An OPEN breaker's downstream edge is carrying nothing, and it is carrying
   * nothing for a REASON -- the component is doing its job. Reporting it as
   * 'blocked' is what lets the canvas draw that wire severed instead of merely
   * quiet, which is the difference between a student seeing the breaker work
   * and seeing a link that looks identical to an idle one.
   *
   * HALF-OPEN is deliberately NOT blocked: a trickle of probes really is
   * crossing, and drawing it cut would contradict the traffic on it.
   */
  edgeStateFor: (ctx, state, _edge, _index) => {
    const b = state.ext;
    if (!b) return null;
    const open = b.phase === "open" && ctx.now - b.openedAtMs < cfgOpenMs(state);
    return open ? "blocked" : null;
  },
  decorateStats: (ctx, state, stats) => {
    const b = state.ext;
    if (!b) return;
    const phase = b.phase === "open" && ctx.now - b.openedAtMs >= cfgOpenMs(state) ? "half-open" : b.phase;
    stats.breakerState = phase;
    stats.breakerErrorRate = windowStats(b, ctx.now, cfgWindowMs(state)).rate;
    stats.rejectedRate = ctx.counterRate(state, "rejected");
    stats.breakerTrips = b.trips;
    stats.openRemainingMs = phase === "open" ? Math.max(0, cfgOpenMs(state) - (ctx.now - b.openedAtMs)) : 0;
  }
};
var EDGE_BEHAVIOURS = [cdn, ratelimiter, breaker];

// engine/sim/behaviour-control.ts
var DEAD_BAND = 0.1;
var DEFAULT_TARGET_UTIL = 0.7;
var DEFAULT_MIN_INSTANCES = 1;
var DEFAULT_MAX_INSTANCES = 64;
var DEFAULT_COOLDOWN_MS = 5e3;
var DEFAULT_STEP_PCT = 0.5;
var DEFAULT_WARMUP_MS = 0;
function asAutoscaler(state) {
  return state.ext ?? null;
}
var autoscaler = {
  kind: "autoscaler",
  // A controller is not in the request path: it serves nothing, holds nothing,
  // and reports no utilisation of its own. Marking it servesRequests:false is
  // what keeps its (permanently zero) slot count out of the utilisation
  // integration, so it renders as a control box rather than an idle server.
  servesRequests: false,
  generatesLoad: false,
  // Every edge out of an autoscaler names the node it drives, never a hop.
  // The engine reads this at wiring time and leaves those edges out of the
  // routing set, so "requests are never routed down a control edge" is a
  // structural property rather than something enforced request by request.
  controlsTarget: true,
  pullsFromQueues: false,
  buffersForConsumers: false,
  pump: "none",
  creditsJoinCompletion: false,
  initState: () => ({
    lastDecisionMs: -Infinity,
    targetInstances: 0,
    warmupDueMs: -1,
    pendingInstances: 0,
    watchedId: "",
    observeUntilMs: -1
  }),
  /**
   * Traffic reaching an autoscaler means the student wired requests INTO the
   * controller -- which now takes deliberate effort, since the edges the
   * controller itself owns are control edges and carry nothing. Refusing
   * explicitly is far more legible than serving it, which would make the
   * controller look like a hop that mysteriously adds latency.
   */
  onAdmit: (ctx, state, req) => {
    ctx.reject(state, req, "no-route");
    return "handled";
  },
  /**
   * `dtMs` is deliberately unused: every decision compares absolute simulated
   * timestamps, so the controller behaves identically at any frame rate.
   */
  onTick: (ctx, state) => {
    const st = asAutoscaler(state);
    if (!st) return;
    const watched = ctx.controlTargetOf(state);
    if (watched !== st.watchedId) {
      st.watchedId = watched;
      st.warmupDueMs = -1;
      st.targetInstances = 0;
      st.lastDecisionMs = -Infinity;
      st.observeUntilMs = ctx.now + Math.max(0, state.config.cooldownMs ?? DEFAULT_COOLDOWN_MS);
    }
    if (watched === "") return;
    const liveInstances = ctx.scaleOf(watched);
    if (liveInstances === null) return;
    if (st.targetInstances === 0) st.targetInstances = liveInstances;
    if (st.warmupDueMs >= 0 && ctx.now >= st.warmupDueMs) {
      st.warmupDueMs = -1;
      st.targetInstances = st.pendingInstances;
      ctx.setScale(watched, st.pendingInstances);
    }
    if (st.warmupDueMs >= 0) return;
    const cfg = state.config;
    const cooldown = Math.max(0, cfg.cooldownMs ?? DEFAULT_COOLDOWN_MS);
    if (st.observeUntilMs >= 0) {
      if (ctx.now < st.observeUntilMs) return;
      st.observeUntilMs = -1;
    }
    if (ctx.now - st.lastDecisionMs < cooldown) return;
    if (ctx.isCrashed(watched)) return;
    const util = ctx.utilizationOf(watched);
    if (util === null) return;
    const minInst = Math.max(1, Math.floor(cfg.minCapacity ?? DEFAULT_MIN_INSTANCES));
    const maxInst = Math.max(
      minInst,
      Math.floor(cfg.maxCapacity ?? DEFAULT_MAX_INSTANCES)
    );
    const target = clamp012(cfg.targetUtil ?? DEFAULT_TARGET_UTIL);
    const step = Math.max(0.01, cfg.scaleStepPct ?? DEFAULT_STEP_PCT);
    const delta = Math.max(1, Math.round(st.targetInstances * step));
    const busyInstances = util * st.targetInstances;
    const idealInstances = Math.max(
      1,
      Math.ceil(busyInstances / (target > 0 ? target : 1))
    );
    let want = st.targetInstances;
    if (util > target) {
      want = Math.min(
        st.targetInstances + delta,
        Math.max(idealInstances, st.targetInstances + 1)
      );
    } else if (util < target - DEAD_BAND) {
      want = Math.max(st.targetInstances - delta, idealInstances);
    }
    want = want < minInst ? minInst : want > maxInst ? maxInst : want;
    if (want === st.targetInstances) return;
    st.lastDecisionMs = ctx.now;
    if (want < st.targetInstances) {
      st.targetInstances = want;
      st.warmupDueMs = -1;
      st.pendingInstances = want;
      ctx.setScale(watched, want);
      return;
    }
    const warmup = Math.max(0, cfg.warmupMs ?? DEFAULT_WARMUP_MS);
    if (warmup === 0) {
      st.targetInstances = want;
      ctx.setScale(watched, want);
      return;
    }
    st.pendingInstances = want;
    st.warmupDueMs = ctx.now + warmup;
  },
  /**
   * Publish everything needed to say, in one sentence, what this controller
   * is doing and why. A student should never have to open the config panel to
   * find out that the box is sitting in a cooldown.
   */
  decorateStats: (ctx, state, stats) => {
    const st = asAutoscaler(state);
    if (!st) return;
    const scaling = st.warmupDueMs >= 0;
    const live = st.watchedId ? ctx.scaleOf(st.watchedId) ?? 0 : 0;
    const wanted = scaling ? st.pendingInstances : st.targetInstances;
    stats.watchedId = st.watchedId;
    stats.targetInstances = wanted;
    stats.watchedInstances = live;
    stats.pendingInstances = scaling ? Math.max(0, wanted - live) : 0;
    stats.scaling = scaling;
    stats.watchedUtil = st.watchedId ? ctx.utilizationOf(st.watchedId) ?? 0 : 0;
    stats.setpoint = clamp012(state.config.targetUtil ?? DEFAULT_TARGET_UTIL);
    const cooldown = Math.max(0, state.config.cooldownMs ?? DEFAULT_COOLDOWN_MS);
    if (scaling) {
      stats.scalePhase = "warming";
      stats.phaseRemainingMs = Math.max(0, st.warmupDueMs - ctx.now);
    } else if (st.observeUntilMs >= 0 && ctx.now < st.observeUntilMs) {
      stats.scalePhase = "observing";
      stats.phaseRemainingMs = Math.max(0, st.observeUntilMs - ctx.now);
    } else if (ctx.now - st.lastDecisionMs < cooldown) {
      stats.scalePhase = "cooldown";
      stats.phaseRemainingMs = Math.max(0, cooldown - (ctx.now - st.lastDecisionMs));
    } else {
      stats.scalePhase = "steady";
      stats.phaseRemainingMs = 0;
    }
  }
};
function asRegion(state) {
  return state.ext ?? null;
}
function regionCount(state) {
  const declared = Math.floor(state.config.regions ?? state.out.length);
  return Math.max(1, Math.min(declared, state.out.length));
}
function regionHealthy(ctx, state, i) {
  const edge2 = state.out[i];
  if (!edge2) return false;
  if (ctx.isEdgeCut(edge2.id)) return false;
  return !ctx.isCrashed(edge2.to);
}
var region = {
  kind: "region",
  // A pure switch, like an lb: it forwards without holding slots of its own.
  servesRequests: true,
  generatesLoad: false,
  pullsFromQueues: false,
  buffersForConsumers: false,
  pump: "own",
  creditsJoinCompletion: true,
  initState: () => ({
    active: -1,
    failoverDueMs: -1,
    failoverTarget: -1,
    lastConfigured: -1
  }),
  onAdmit: () => "passthru",
  route: () => "one",
  // pickEdge returning null here is not a wiring mistake: it means no region
  // is serving, either mid-failover or with every region down.
  noRouteReason: "region-down",
  /**
   * Returning null means "no region is serving", which the engine resolves as
   * a routing failure. The reason is mapped to 'region-down' by the engine's
   * no-route path for this kind, so a student sees WHY it failed rather than a
   * generic routing error.
   */
  pickEdge: (ctx, state, _req, out) => {
    const st = asRegion(state);
    if (!st) return null;
    const cfg = state.config;
    const count = regionCount(state);
    const configured = Math.floor(cfg.activeRegion ?? 0);
    const wanted = configured < 0 ? 0 : configured >= count ? count - 1 : configured;
    if (st.active === -1 || wanted !== st.lastConfigured) {
      st.lastConfigured = wanted;
      st.active = wanted;
      st.failoverDueMs = -1;
    }
    if (st.failoverDueMs >= 0) {
      if (ctx.now < st.failoverDueMs) return null;
      st.active = st.failoverTarget;
      st.failoverDueMs = -1;
    }
    if (regionHealthy(ctx, state, st.active)) return out[st.active];
    let next = -1;
    for (let i = 1; i <= count; i++) {
      const candidate = (st.active + i) % count;
      if (regionHealthy(ctx, state, candidate)) {
        next = candidate;
        break;
      }
    }
    if (next === -1) return null;
    const failoverMs = Math.max(0, cfg.failoverMs ?? 0);
    if (failoverMs === 0) {
      st.active = next;
      return out[next];
    }
    st.failoverTarget = next;
    st.failoverDueMs = ctx.now + failoverMs;
    return null;
  },
  /**
   * Which outgoing edge is live, and which are merely standing by.
   *
   * A standby region is wired, healthy and deliberately unused. Left to the
   * engine's flow-based fallback it would read 'idle' -- indistinguishable
   * from a dead link -- and the entire point of the component (there is a
   * second region sitting there ready) would be invisible on the canvas.
   *
   * Strictly a read: `pickEdge` is where the state machine advances, and
   * duplicating any of that here would let the number of times the UI polled
   * change the simulation.
   */
  edgeStateFor: (ctx, state, _edge, index) => {
    const st = asRegion(state);
    if (!st) return null;
    const count = regionCount(state);
    if (index >= count) return null;
    const live = liveRegionIndex(ctx, state, st);
    if (live === -1) return "standby";
    return index === live ? "live" : "standby";
  },
  decorateStats: (ctx, state, stats) => {
    const st = asRegion(state);
    if (!st) return;
    const failingOver = st.failoverDueMs >= 0 && ctx.now < st.failoverDueMs;
    stats.activeRegion = st.active < 0 ? 0 : st.active;
    stats.failingOver = failingOver;
    stats.failoverRemainingMs = failingOver ? Math.max(0, st.failoverDueMs - ctx.now) : 0;
    const count = regionCount(state);
    let healthy = 0;
    for (let i = 0; i < count; i++) {
      if (regionHealthy(ctx, state, i)) healthy += 1;
    }
    stats.regionsHealthy = healthy;
    stats.regionsTotal = count;
    const live = liveRegionIndex(ctx, state, st);
    stats.liveEdgeId = live === -1 ? void 0 : state.out[live]?.id;
  }
};
function liveRegionIndex(ctx, state, st) {
  const count = regionCount(state);
  if (st.failoverDueMs >= 0 && ctx.now < st.failoverDueMs) return -1;
  const active = st.failoverDueMs >= 0 ? st.failoverTarget : st.active;
  if (active < 0) {
    const configured = Math.floor(state.config.activeRegion ?? 0);
    const wanted = configured < 0 ? 0 : configured >= count ? count - 1 : configured;
    return regionHealthy(ctx, state, wanted) ? wanted : -1;
  }
  if (regionHealthy(ctx, state, active)) return active;
  let next = -1;
  for (let i = 1; i <= count; i++) {
    const candidate = (active + i) % count;
    if (regionHealthy(ctx, state, candidate)) {
      next = candidate;
      break;
    }
  }
  if (next === -1) return -1;
  return Math.max(0, state.config.failoverMs ?? 0) === 0 ? next : -1;
}
var CONTROL_BEHAVIOURS = [autoscaler, region];

// engine/sim/behaviour-store.ts
var KEYSPACE2 = 64;
function clamp013(v) {
  return v < 0 ? 0 : v > 1 ? 1 : v;
}
function num(v, fallback) {
  return typeof v === "number" && Number.isFinite(v) ? v : fallback;
}
var OBJECT_PREFIXES = 8;
function objectPrefixRate(state) {
  const r = state.config.prefixRps;
  return r !== void 0 && r > 0 ? r : 0;
}
function objectRefill(ext, prefix, rate, now) {
  if (rate !== ext.lastRate) {
    for (let p = 0; p < OBJECT_PREFIXES; p++) {
      if (ext.tokens[p] > rate) ext.tokens[p] = rate;
    }
    ext.lastRate = rate;
  }
  const elapsed = now - ext.lastRefillMs[prefix];
  if (elapsed <= 0) return;
  ext.lastRefillMs[prefix] = now;
  ext.tokens[prefix] += elapsed / 1e3 * rate;
  if (ext.tokens[prefix] > rate) ext.tokens[prefix] = rate;
}
var objectstore = {
  kind: "objectstore",
  servesRequests: true,
  generatesLoad: false,
  pullsFromQueues: false,
  buffersForConsumers: false,
  pump: "own",
  creditsJoinCompletion: true,
  initState(state) {
    const rate = objectPrefixRate(state);
    const tokens = new Float64Array(OBJECT_PREFIXES);
    tokens.fill(rate > 0 ? rate : 0);
    return { tokens, lastRefillMs: new Float64Array(OBJECT_PREFIXES), lastRate: rate };
  },
  onAdmit(ctx, state, req) {
    const rate = objectPrefixRate(state);
    if (rate <= 0) return "serve";
    const ext = state.ext;
    const prefix = (req.key % OBJECT_PREFIXES + OBJECT_PREFIXES) % OBJECT_PREFIXES;
    objectRefill(ext, prefix, rate, ctx.now);
    if (ext.tokens[prefix] < 1) {
      ctx.countCustom(state, "slowdown", 1);
      ctx.reject(state, req, "throttled");
      return "handled";
    }
    ext.tokens[prefix] -= 1;
    return "serve";
  },
  decorateStats(ctx, state, stats) {
    stats.slowdownRate = ctx.counterRate(state, "slowdown");
  }
};
var coldDrained = (_ctx, state, _req) => {
  const ext = state.ext;
  if (ext.jobs > 0) ext.jobs--;
};
var coldstorage = {
  kind: "coldstorage",
  servesRequests: true,
  generatesLoad: false,
  pullsFromQueues: false,
  buffersForConsumers: false,
  // No queue and no engine slots: jobs are tracked in ext, timed by
  // serveWithin, and the only admission outcomes are "job started" and
  // "refused". queueLimit is ignored, because a vault has no line to wait in.
  pump: "none",
  creditsJoinCompletion: true,
  initState: () => ({ jobs: 0 }),
  onAdmit(ctx, state, req) {
    const ext = state.ext;
    if (ext.jobs >= ctx.effectiveCapacity(state)) {
      ctx.countCustom(state, "restoreDenied", 1);
      ctx.reject(state, req, "throttled");
      return "handled";
    }
    ext.jobs++;
    ctx.serveWithin(state, req, coldDrained);
    return "handled";
  },
  onTick(ctx, state) {
    const ext = state.ext;
    ctx.reportOccupancy(state, ext.jobs, ctx.effectiveCapacity(state));
  },
  decorateStats(ctx, state, stats) {
    const ext = state.ext;
    stats.inFlight = ext ? ext.jobs : 0;
    stats.queued = 0;
    stats.throttledRate = ctx.counterRate(state, "restoreDenied");
  }
};
function poolExt(state) {
  return state.ext;
}
function makeDrained(onStart, onServed) {
  const drained = (ctx, state, req) => {
    const ext = poolExt(state);
    if (ext.busy > 0) ext.busy--;
    ctx.countCustom(state, "served", 1);
    ctx.countCustom(state, "servedMs", req.ownMs);
    if (onServed) onServed(ctx, state, req);
    const q = ext.queue;
    while (ext.busy < ctx.effectiveCapacity(state) && ext.head < q.length) {
      const next = q[ext.head];
      q[ext.head] = null;
      ext.head++;
      if (ext.head > 64 && ext.head * 2 >= q.length) {
        q.splice(0, ext.head);
        ext.head = 0;
      }
      if (!next) continue;
      startPoolService(ctx, state, next, onStart, drained);
    }
  };
  return drained;
}
function startPoolService(ctx, state, req, onStart, drained) {
  const ext = poolExt(state);
  ext.busy++;
  if (onStart) onStart(ctx, state, req);
  ctx.serveWithin(state, req, drained);
}
function poolAdmit(ctx, state, req, classify, onStart, drained) {
  const ext = poolExt(state);
  classify(ctx, state, req);
  if (ext.busy < ctx.effectiveCapacity(state)) {
    startPoolService(ctx, state, req, onStart, drained);
    return "handled";
  }
  if (ext.queue.length - ext.head >= ctx.effectiveQueueLimit(state)) return "shed";
  ext.queue.push(req);
  return "handled";
}
function initPool() {
  return { busy: 0, queue: [], head: 0 };
}
function reportPool(ctx, state) {
  ctx.reportOccupancy(state, poolExt(state).busy, ctx.effectiveCapacity(state));
}
function decoratePool(state, stats) {
  const ext = poolExt(state);
  stats.inFlight = ext.busy;
  stats.queued = ext.queue.length - ext.head;
}
function meanServedMs(ctx, state) {
  const served = ctx.counterRate(state, "served");
  if (!(served > 0)) return 0;
  const ms = ctx.counterRate(state, "servedMs") / served;
  return Number.isFinite(ms) && ms > 0 ? ms : 0;
}
function dbClassify(ctx, state, req) {
  const isWrite = ctx.roll() >= clamp013(num(state.config.readFraction, 0.9));
  ctx.markWrite(req, isWrite);
}
var dbStart = (ctx, state, req) => {
  ctx.countCustom(state, req.isWrite ? "write" : "read", 1);
  if (!req.isWrite) return;
  const ext = state.ext;
  const lockMs = Math.max(0, num(state.config.lockMs, 0));
  const wait = lockMs * ext.writers;
  if (wait > 0) {
    ctx.addServiceDelay(req, wait);
    ctx.countCustom(state, "lockWaitMs", wait);
  }
  ext.writers++;
};
var dbDrained = makeDrained(dbStart, (_ctx, state, req) => {
  if (!req.isWrite) return;
  const ext = state.ext;
  if (ext.writers > 0) ext.writers--;
});
var db = {
  kind: "db",
  servesRequests: true,
  instanceModel: "slots",
  scaleField: "instances",
  generatesLoad: false,
  pullsFromQueues: false,
  buffersForConsumers: false,
  pump: "none",
  creditsJoinCompletion: true,
  initState: () => ({ ...initPool(), writers: 0 }),
  onAdmit(ctx, state, req) {
    return poolAdmit(ctx, state, req, dbClassify, dbStart, dbDrained);
  },
  onTick: reportPool,
  decorateStats(ctx, state, stats) {
    decoratePool(state, stats);
    const writes = ctx.counterRate(state, "write");
    stats.readRate = ctx.counterRate(state, "read");
    stats.writeRate = writes;
    const waited = ctx.counterRate(state, "lockWaitMs");
    const mean = writes > 0 ? waited / writes : 0;
    stats.lockWaitMs = Number.isFinite(mean) && mean > 0 ? mean : 0;
  }
};
var searchDrained = makeDrained(
  // At service start: a SEARCH is judged against the index it is actually
  // reading right now. Judged here rather than at admission so a query
  // that waited in line long enough for the refresh to land counts fresh,
  // which is exactly how a real cluster behaves.
  (ctx, state, req) => {
    if (req.isWrite) return;
    const ext = state.ext;
    const stale = ctx.now < ext.visibleAt[req.key % KEYSPACE2];
    ctx.countCustom(state, stale ? "staleSearch" : "freshSearch", 1);
  },
  // At service end: the write has committed, so the refresh clock starts
  // HERE. The admission-time claim below was only a lower bound.
  (ctx, state, req) => {
    if (!req.isWrite) return;
    const ext = state.ext;
    const lag = Math.max(0, num(state.config.indexLagMs, 0));
    const key = req.key % KEYSPACE2;
    const visible = ctx.now + lag;
    if (visible > ext.visibleAt[key]) ext.visibleAt[key] = visible;
  }
);
var searchStart = (ctx, state, req) => {
  if (req.isWrite) return;
  const ext = state.ext;
  const stale = ctx.now < ext.visibleAt[req.key % KEYSPACE2];
  ctx.countCustom(state, stale ? "staleSearch" : "freshSearch", 1);
};
function searchClassify(ctx, state, req) {
  const isWrite = ctx.roll() >= clamp013(num(state.config.readFraction, 0.9));
  ctx.markWrite(req, isWrite);
  if (isWrite) {
    ctx.addServiceDelay(req, Math.max(0, num(state.config.indexMs, 0)));
    ctx.countCustom(state, "indexWrite", 1);
    const ext = state.ext;
    const lag = Math.max(0, num(state.config.indexLagMs, 0));
    const key = req.key % KEYSPACE2;
    const visible = ctx.now + lag;
    if (visible > ext.visibleAt[key]) ext.visibleAt[key] = visible;
  }
}
var searchindex = {
  kind: "searchindex",
  servesRequests: true,
  instanceModel: "slots",
  scaleField: "instances",
  generatesLoad: false,
  pullsFromQueues: false,
  buffersForConsumers: false,
  pump: "none",
  creditsJoinCompletion: true,
  initState() {
    const visibleAt = new Float64Array(KEYSPACE2);
    visibleAt.fill(-Infinity);
    return { ...initPool(), visibleAt };
  },
  onAdmit(ctx, state, req) {
    return poolAdmit(ctx, state, req, searchClassify, searchStart, searchDrained);
  },
  onTick: reportPool,
  decorateStats(ctx, state, stats) {
    decoratePool(state, stats);
    const fresh = ctx.counterRate(state, "freshSearch");
    const stale = ctx.counterRate(state, "staleSearch");
    const total = fresh + stale;
    stats.searchRate = total;
    stats.staleSearchRate = total > 0 ? stale / total : 0;
    stats.indexWriteRate = ctx.counterRate(state, "indexWrite");
  }
};
var tsdbStart = (ctx, state, req) => {
  ctx.countCustom(state, req.isWrite ? "append" : "rangeQuery", 1);
};
var tsdbDrained = makeDrained(tsdbStart, null);
function tsdbClassify(ctx, state, req) {
  const isRange = ctx.roll() < clamp013(num(state.config.rangeQueryFraction, 0));
  ctx.markWrite(req, !isRange);
  if (isRange) {
    ctx.addServiceDelay(req, Math.max(0, num(state.config.rangeQueryMs, 0)));
  }
}
var timeseriesdb = {
  kind: "timeseriesdb",
  servesRequests: true,
  instanceModel: "slots",
  scaleField: "instances",
  generatesLoad: false,
  pullsFromQueues: false,
  buffersForConsumers: false,
  pump: "none",
  creditsJoinCompletion: true,
  initState: initPool,
  onAdmit(ctx, state, req) {
    return poolAdmit(ctx, state, req, tsdbClassify, tsdbStart, tsdbDrained);
  },
  onTick: reportPool,
  decorateStats(ctx, state, stats) {
    decoratePool(state, stats);
    stats.appendRate = ctx.counterRate(state, "append");
    stats.rangeQueryRate = ctx.counterRate(state, "rangeQuery");
  }
};
var GRAPH_FANOUT = 3;
function graphClassify(ctx, state, req) {
  const depth = Math.max(1, Math.floor(num(state.config.traversalDepth, 1)));
  const base = Math.max(0, state.config.serviceMs);
  const mult = Math.pow(GRAPH_FANOUT, Math.min(depth, 8) - 1);
  ctx.addServiceDelay(req, base * (mult - 1));
}
var graphDrained = makeDrained(null, null);
var graphdb = {
  kind: "graphdb",
  servesRequests: true,
  instanceModel: "slots",
  scaleField: "instances",
  generatesLoad: false,
  pullsFromQueues: false,
  buffersForConsumers: false,
  pump: "none",
  creditsJoinCompletion: true,
  initState: initPool,
  onAdmit(ctx, state, req) {
    return poolAdmit(ctx, state, req, graphClassify, null, graphDrained);
  },
  onTick: reportPool,
  decorateStats(ctx, state, stats) {
    decoratePool(state, stats);
    stats.traversalCostMs = meanServedMs(ctx, state);
  }
};
function vectorClassify(ctx, state, req) {
  const sizeK = Math.max(0, num(state.config.indexSizeK, 0));
  const recall = Math.min(0.99, Math.max(0, num(state.config.recallTarget, 0)));
  const base = Math.max(0, state.config.serviceMs);
  const mult = Math.log2(2 + sizeK) / (1 - recall);
  if (mult > 1) ctx.addServiceDelay(req, base * (mult - 1));
}
var vectorDrained = makeDrained(null, null);
var vectordb = {
  kind: "vectordb",
  servesRequests: true,
  instanceModel: "slots",
  scaleField: "instances",
  generatesLoad: false,
  pullsFromQueues: false,
  buffersForConsumers: false,
  pump: "none",
  creditsJoinCompletion: true,
  initState: initPool,
  onAdmit(ctx, state, req) {
    return poolAdmit(ctx, state, req, vectorClassify, null, vectorDrained);
  },
  onTick: reportPool,
  decorateStats(ctx, state, stats) {
    decoratePool(state, stats);
    stats.queryCostMs = meanServedMs(ctx, state);
  }
};
var STORE_BEHAVIOURS = [
  db,
  objectstore,
  searchindex,
  timeseriesdb,
  graphdb,
  coldstorage,
  vectordb
];

// engine/sim/behaviour-messaging.ts
function clampInt2(v, min, fallback) {
  if (v === void 0 || Number.isNaN(v)) return fallback;
  const n = Math.floor(v);
  return n < min ? min : n;
}
var MIN_RETENTION_PER_PARTITION = 16;
var MAX_TOTAL_RETENTION = 65536;
function brokerPartitions(state) {
  return clampInt2(state.config.partitions, 1, 4);
}
function brokerRetention(state, partitions) {
  const total = Math.min(
    Math.max(1, Math.floor(state.config.queueLimit)),
    MAX_TOTAL_RETENTION
  );
  return Math.max(MIN_RETENTION_PER_PARTITION, Math.ceil(total / partitions));
}
function makeBrokerGroup(edge2, partitions, startAt) {
  const next = new Float64Array(partitions);
  next.set(startAt);
  return {
    edgeId: edge2.id,
    targetId: edge2.to,
    next,
    inflight: new Uint8Array(partitions),
    done: 0,
    skipped: 0,
    pumping: false
  };
}
function brokerExt(state) {
  return state.ext;
}
function ensureBroker(state, ext) {
  const partitions = brokerPartitions(state);
  if (partitions !== ext.partitions) {
    const retention2 = brokerRetention(state, partitions);
    ext.partitions = partitions;
    ext.retention = retention2;
    ext.rings = [];
    for (let p = 0; p < partitions; p++) ext.rings.push(new Int32Array(retention2));
    ext.head = new Float64Array(partitions);
    ext.groups = [];
    ext.groupSig = "\0stale";
  }
  const retention = brokerRetention(state, partitions);
  if (retention !== ext.retention) {
    ext.retention = retention;
    for (let p = 0; p < partitions; p++) ext.rings[p] = new Int32Array(retention);
    for (const g of ext.groups) {
      g.next.set(ext.head);
      g.inflight.fill(0);
    }
  }
  let sig = "";
  for (let i = 0; i < state.out.length; i++) sig += state.out[i].id + "\0";
  if (sig !== ext.groupSig) {
    const prev = /* @__PURE__ */ new Map();
    for (const g of ext.groups) prev.set(g.edgeId, g);
    const groups = [];
    for (let i = 0; i < state.out.length; i++) {
      const edge2 = state.out[i];
      const kept = prev.get(edge2.id);
      if (kept && kept.next.length === partitions) {
        kept.targetId = edge2.to;
        groups.push(kept);
      } else {
        groups.push(makeBrokerGroup(edge2, partitions, ext.head));
      }
    }
    ext.groups = groups;
    ext.groupSig = sig;
  }
  return ext;
}
function groupLag(ext, g) {
  let lag = 0;
  for (let p = 0; p < ext.partitions; p++) {
    lag += ext.head[p] - g.next[p];
    lag += g.inflight[p];
  }
  return lag;
}
function pumpGroup(ctx, state, ext, g, edge2) {
  if (g.pumping) return;
  g.pumping = true;
  let progress = true;
  while (progress) {
    progress = false;
    for (let p = 0; p < ext.partitions; p++) {
      if (g.inflight[p] !== 0) continue;
      const oldest = ext.head[p] - ext.retention;
      if (g.next[p] < oldest) {
        const skipped = oldest - g.next[p];
        g.skipped += skipped;
        ctx.countCustom(state, "retentionDrop", skipped);
        g.next[p] = oldest;
      }
      if (g.next[p] >= ext.head[p]) continue;
      const key = ext.rings[p][g.next[p] % ext.retention];
      g.inflight[p] = 1;
      g.next[p] += 1;
      if (!ctx.emitDetached(state, edge2, key)) {
        g.inflight[p] = 0;
        g.next[p] -= 1;
        g.pumping = false;
        return;
      }
      if (g.inflight[p] === 0) progress = true;
    }
  }
  g.pumping = false;
}
function pumpAllGroups(ctx, state, ext) {
  for (let i = 0; i < ext.groups.length; i++) {
    const edge2 = state.out[i];
    if (!edge2 || edge2.id !== ext.groups[i].edgeId) continue;
    pumpGroup(ctx, state, ext, ext.groups[i], edge2);
  }
}
var streambroker = {
  kind: "streambroker",
  // A broker is a buffer with cursors, not a server: its ack slots mean
  // nothing, and its meaningful backlog is lag, published via decorateStats.
  servesRequests: false,
  generatesLoad: false,
  pullsFromQueues: false,
  buffersForConsumers: false,
  pump: "none",
  // Deliveries join back through this node; crediting them would double-count
  // the broker's throughput, whose honest meaning is the PUBLISH rate booked
  // by the ack path.
  creditsJoinCompletion: false,
  // The whole pacing mechanism: a delivery resolving is what frees its
  // partition and triggers the next one.
  observesOutcome: true,
  // One unit per partition, like a shard: the per-partition backlog is where
  // a hot partition becomes visible.
  instanceModel: "custom",
  initState(state) {
    const partitions = brokerPartitions(state);
    const retention = brokerRetention(state, partitions);
    const rings = [];
    for (let p = 0; p < partitions; p++) rings.push(new Int32Array(retention));
    return {
      partitions,
      rings,
      head: new Float64Array(partitions),
      retention,
      groups: [],
      groupSig: "",
      report: []
    };
  },
  // A publish is acked after the broker's own (tiny) serviceMs, no matter
  // what the consumers are doing. That decoupling IS the product.
  onAdmit: () => "passthru",
  onServiceComplete(ctx, state, req) {
    const ext = ensureBroker(state, brokerExt(state));
    const p = (req.key % ext.partitions + ext.partitions) % ext.partitions;
    ext.rings[p][ext.head[p] % ext.retention] = req.key;
    ext.head[p] += 1;
    ctx.countCustom(state, "published", 1);
    pumpAllGroups(ctx, state, ext);
    return "complete";
  },
  onDownstreamResult(ctx, state, req, ok) {
    const ext = ensureBroker(state, brokerExt(state));
    const p = (req.key % ext.partitions + ext.partitions) % ext.partitions;
    for (let i = 0; i < ext.groups.length; i++) {
      const g = ext.groups[i];
      if (g.targetId !== req.nodeId || g.inflight[p] === 0) continue;
      g.inflight[p] = 0;
      g.done += 1;
      ctx.countCustom(state, "delivered", 1);
      if (!ok) ctx.countCustom(state, "deliveryFailed", 1);
      const edge2 = state.out[i];
      if (edge2 && edge2.id === g.edgeId) pumpGroup(ctx, state, ext, g, edge2);
      return;
    }
  },
  /**
   * Unit p is partition p, filled with the WORST group's backlog in that
   * partition against retention; the partition strip therefore shows where
   * in the keyspace the slow consumer is drowning.
   */
  reportInstances(ctx, state) {
    const ext = ensureBroker(state, brokerExt(state));
    const out = ext.report;
    out.length = ext.partitions;
    for (let p = 0; p < ext.partitions; p++) {
      let worst = 0;
      for (const g of ext.groups) {
        const behind = ext.head[p] - g.next[p] + g.inflight[p];
        if (behind > worst) worst = behind;
      }
      out[p] = clamp012(worst / ext.retention);
    }
    ctx.reportInstances(state, out, 0);
  },
  decorateStats(ctx, state, stats) {
    const ext = ensureBroker(state, brokerExt(state));
    let maxLag = 0;
    let inflight = 0;
    const byGroup = [];
    for (const g of ext.groups) {
      const lag = groupLag(ext, g);
      byGroup.push(lag);
      if (lag > maxLag) maxLag = lag;
      for (let p = 0; p < ext.partitions; p++) inflight += g.inflight[p];
    }
    stats.consumerLag = maxLag;
    stats.consumerLagByGroup = byGroup;
    stats.deliveryRate = ctx.counterRate(state, "delivered");
    stats.retentionDropRate = ctx.counterRate(state, "retentionDrop");
    stats.queued = maxLag;
    stats.inFlight = inflight;
  }
};
var pubsub = {
  kind: "pubsub",
  servesRequests: false,
  generatesLoad: false,
  pullsFromQueues: false,
  buffersForConsumers: false,
  pump: "none",
  creditsJoinCompletion: false,
  onAdmit: () => "passthru",
  onServiceComplete(ctx, state, req) {
    for (let i = 0; i < state.out.length; i++) {
      if (ctx.emitDetached(state, state.out[i], req.key)) {
        ctx.countCustom(state, "delivered", 1);
      }
    }
    return "complete";
  },
  decorateStats(ctx, state, stats) {
    stats.fanout = state.out.length;
    const delivered = ctx.counterRate(state, "delivered");
    stats.deliveryRate = delivered;
    stats.publishAmplification = delivered;
  }
};
function wsExt(state) {
  return state.ext;
}
function wsOpen(ext) {
  return ext.expiry.length - ext.head;
}
function wsExpire(ext, now) {
  const q = ext.expiry;
  while (ext.head < q.length && q[ext.head] <= now) ext.head++;
  if (ext.head > 64 && ext.head * 2 >= q.length) {
    q.splice(0, ext.head);
    ext.head = 0;
  }
}
function wsOpenProjected(ext, now) {
  const q = ext.expiry;
  let head = ext.head;
  while (head < q.length && q[head] <= now) head++;
  return q.length - head;
}
var websocket = {
  kind: "websocket",
  servesRequests: true,
  generatesLoad: false,
  pullsFromQueues: false,
  buffersForConsumers: false,
  // Slot bookkeeping is connection-based and lives in `ext`; the engine's
  // busy/waiting pair never sees it, so there is nothing for it to pump.
  pump: "none",
  creditsJoinCompletion: true,
  // One unit per instance; the waterline is connection occupancy, fed to the
  // engine through reportOccupancy below.
  instanceModel: "slots",
  scaleField: "instances",
  initState() {
    return { expiry: [], head: 0 };
  },
  onAdmit(ctx, state, req) {
    const ext = wsExt(state);
    wsExpire(ext, ctx.now);
    const cap = ctx.effectiveCapacity(state);
    if (wsOpen(ext) >= cap) {
      ctx.countCustom(state, "connRefused", 1);
      ctx.reject(state, req, "conn-refused");
      return "handled";
    }
    const holdMs = Math.max(0, state.config.connectionMs ?? 3e4);
    ext.expiry.push(ctx.now + holdMs);
    ctx.countCustom(state, "connected", 1);
    ctx.serveWithin(state, req, wsNoopDrain);
    return "handled";
  },
  onTick(ctx, state) {
    const ext = wsExt(state);
    wsExpire(ext, ctx.now);
    ctx.reportOccupancy(state, wsOpen(ext), ctx.effectiveCapacity(state));
  },
  decorateStats(ctx, state, stats) {
    const ext = wsExt(state);
    const open = wsOpenProjected(ext, ctx.now);
    stats.connectionsOpen = open;
    stats.maxConnections = ctx.effectiveCapacity(state);
    stats.connectRate = ctx.counterRate(state, "connected");
    stats.connectionRejectRate = ctx.counterRate(state, "connRefused");
    stats.inFlight = open;
  }
};
function wsNoopDrain() {
}
function gwRate(state) {
  const r = state.config.rateLimitRps;
  return r !== void 0 && r > 0 ? r : 0;
}
function gwBurst(state) {
  const b = state.config.burst;
  if (b !== void 0 && b > 0) return b;
  const r = gwRate(state);
  return r > 0 ? r : 1;
}
function gwRefill(ctx, state, b) {
  const burst = gwBurst(state);
  if (burst !== b.lastBurst) {
    if (b.tokens > burst) b.tokens = burst;
    b.lastBurst = burst;
  }
  const elapsed = ctx.now - b.lastRefillMs;
  if (elapsed <= 0) return;
  b.lastRefillMs = ctx.now;
  const rate = gwRate(state);
  if (rate <= 0) return;
  b.tokens += elapsed / 1e3 * rate;
  if (b.tokens > burst) b.tokens = burst;
}
function gwProjectedTokens(ctx, state, b) {
  const burst = gwBurst(state);
  let tokens = b.tokens > burst ? burst : b.tokens;
  const rate = gwRate(state);
  const elapsed = ctx.now - b.lastRefillMs;
  if (rate > 0 && elapsed > 0) {
    tokens += elapsed / 1e3 * rate;
    if (tokens > burst) tokens = burst;
  }
  return tokens;
}
var apigateway = {
  kind: "apigateway",
  servesRequests: true,
  generatesLoad: false,
  pullsFromQueues: false,
  buffersForConsumers: false,
  pump: "own",
  creditsJoinCompletion: true,
  instanceModel: "slots",
  scaleField: "instances",
  initState: (state) => ({
    tokens: gwBurst(state),
    lastRefillMs: 0,
    lastBurst: gwBurst(state)
  }),
  onAdmit(ctx, state, req) {
    const b = state.ext;
    gwRefill(ctx, state, b);
    if (gwRate(state) > 0) {
      if (b.tokens < 1) {
        ctx.countCustom(state, "throttled", 1);
        ctx.reject(state, req, "throttled");
        return "handled";
      }
      b.tokens -= 1;
    }
    const authFail = clamp012(state.config.authFailRate ?? 0);
    if (authFail > 0 && ctx.roll() < authFail) {
      ctx.countCustom(state, "authRejected", 1);
      ctx.reject(state, req, "unauthorized");
      return "handled";
    }
    ctx.countCustom(state, "admitted", 1);
    return "serve";
  },
  // The route table: exactly one backend per request, chosen by edge weight.
  route: () => "one",
  pickEdge: (ctx, _state, _req, out) => ctx.pickWeightedOrLeastLoaded(out),
  decorateStats(ctx, state, stats) {
    const b = state.ext;
    stats.admittedRate = ctx.counterRate(state, "admitted");
    stats.throttledRate = ctx.counterRate(state, "throttled");
    stats.authRejectRate = ctx.counterRate(state, "authRejected");
    stats.tokens = b ? gwProjectedTokens(ctx, state, b) : 0;
  }
};
function sidecarExt(state) {
  return state.ext;
}
function sidecarOutlierAfter(state) {
  return clampInt2(state.config.outlierAfter, 1, 5);
}
function sidecarOpenMs(state) {
  const v = state.config.openMs;
  return v !== void 0 && v > 0 ? v : 3e3;
}
var sidecar = {
  kind: "sidecar",
  servesRequests: true,
  generatesLoad: false,
  pullsFromQueues: false,
  buffersForConsumers: false,
  pump: "own",
  creditsJoinCompletion: true,
  observesOutcome: true,
  instanceModel: "slots",
  scaleField: "instances",
  initState: () => ({
    phase: "closed",
    openedAtMs: 0,
    consecutive: 0,
    probing: false,
    trips: 0
  }),
  onAdmit(ctx, state, req) {
    const st = sidecarExt(state);
    if (st.phase === "open" && ctx.now - st.openedAtMs >= sidecarOpenMs(state)) {
      st.phase = "half-open";
      st.probing = false;
    }
    if (st.phase === "open" || st.phase === "half-open" && st.probing) {
      ctx.countCustom(state, "rejected", 1);
      ctx.reject(state, req, "rejected");
      return "handled";
    }
    if (st.phase === "half-open") st.probing = true;
    return "serve";
  },
  onDownstreamResult(ctx, state, _req, ok) {
    const st = sidecarExt(state);
    if (!ok) ctx.countCustom(state, "upstreamFail", 1);
    if (st.phase === "half-open") {
      st.probing = false;
      if (ok) {
        st.phase = "closed";
        st.consecutive = 0;
      } else {
        st.phase = "open";
        st.openedAtMs = ctx.now;
        st.trips += 1;
      }
      return;
    }
    if (st.phase !== "closed") return;
    if (ok) {
      st.consecutive = 0;
      return;
    }
    st.consecutive += 1;
    if (st.consecutive >= sidecarOutlierAfter(state)) {
      st.phase = "open";
      st.openedAtMs = ctx.now;
      st.consecutive = 0;
      st.trips += 1;
    }
  },
  edgeStateFor(ctx, state, _edge, _index) {
    const st = state.ext;
    if (!st) return null;
    const open = st.phase === "open" && ctx.now - st.openedAtMs < sidecarOpenMs(state);
    return open ? "blocked" : null;
  },
  decorateStats(ctx, state, stats) {
    const st = state.ext;
    if (!st) return;
    const phase = st.phase === "open" && ctx.now - st.openedAtMs >= sidecarOpenMs(state) ? "half-open" : st.phase;
    stats.breakerState = phase;
    stats.breakerTrips = st.trips;
    stats.consecutiveFails = st.consecutive;
    stats.rejectedRate = ctx.counterRate(state, "rejected");
    stats.upstreamFailRate = ctx.counterRate(state, "upstreamFail");
  }
};
function lambdaExt(state) {
  return state.ext;
}
function lambdaLimit(state) {
  return clampInt2(state.config.maxConcurrency, 1, 40);
}
function lambdaReap(ext, now) {
  const q = ext.warmExpiry;
  while (ext.head < q.length && q[ext.head] <= now) ext.head++;
  if (ext.head > 64 && ext.head * 2 >= q.length) {
    q.splice(0, ext.head);
    ext.head = 0;
  }
}
function lambdaWarmProjected(ext, now) {
  const q = ext.warmExpiry;
  let head = ext.head;
  while (head < q.length && q[head] <= now) head++;
  return q.length - head;
}
var lambda = {
  kind: "lambda",
  servesRequests: true,
  generatesLoad: false,
  pullsFromQueues: false,
  buffersForConsumers: false,
  pump: "none",
  creditsJoinCompletion: true,
  // One unit per live instance, busy or warm; the stack GROWS WITH LOAD,
  // which is the one picture that says "serverless" truthfully.
  instanceModel: "custom",
  initState: () => ({ warmExpiry: [], head: 0, busy: 0 }),
  onAdmit(ctx, state, req) {
    const ext = lambdaExt(state);
    lambdaReap(ext, ctx.now);
    if (ext.busy >= lambdaLimit(state)) {
      ctx.countCustom(state, "throttled", 1);
      ctx.reject(state, req, "throttled");
      return "handled";
    }
    const warm = ext.warmExpiry.length - ext.head;
    if (warm > 0) {
      ext.warmExpiry.pop();
      ctx.countCustom(state, "warmStart", 1);
    } else {
      ctx.countCustom(state, "coldStart", 1);
      ctx.addServiceDelay(req, Math.max(0, state.config.coldStartMs ?? 350));
    }
    ext.busy += 1;
    ctx.serveWithin(state, req, onLambdaDrained);
    return "handled";
  },
  onTick(ctx, state) {
    const ext = lambdaExt(state);
    lambdaReap(ext, ctx.now);
    ctx.reportOccupancy(state, ext.busy, lambdaLimit(state));
  },
  reportInstances(ctx, state) {
    const ext = lambdaExt(state);
    const warm = lambdaWarmProjected(ext, ctx.now);
    const total = Math.min(ext.busy + warm, 64);
    const out = lambdaReport;
    out.length = total;
    for (let i = 0; i < total; i++) out[i] = i < ext.busy ? 1 : 0;
    ctx.reportInstances(state, out, 0);
  },
  decorateStats(ctx, state, stats) {
    const ext = lambdaExt(state);
    const cold = ctx.counterRate(state, "coldStart");
    const warmStarts = ctx.counterRate(state, "warmStart");
    const started = cold + warmStarts;
    stats.coldStartRate = started > 0 ? cold / started : 0;
    stats.coldStartsPerSec = cold;
    stats.warmIdle = lambdaWarmProjected(ext, ctx.now);
    stats.runningNow = ext.busy;
    stats.inFlight = ext.busy;
    stats.throttledRate = ctx.counterRate(state, "throttled");
  }
};
var lambdaReport = [];
function onLambdaDrained(ctx, state, _req) {
  const ext = lambdaExt(state);
  if (ext.busy > 0) ext.busy -= 1;
  const keep = Math.max(0, state.config.keepWarmMs ?? 12e3);
  ext.warmExpiry.push(ctx.now + keep);
}
function cronExt(state) {
  return state.ext;
}
function cronInterval(state) {
  const v = state.config.intervalMs;
  return v !== void 0 && v >= 250 ? Math.floor(v) : v !== void 0 && v > 0 ? 250 : 2e4;
}
function cronBatch(state) {
  return Math.min(clampInt2(state.config.batchSize, 1, 50), 2e3);
}
var cron = {
  kind: "cron",
  servesRequests: false,
  generatesLoad: false,
  pullsFromQueues: false,
  buffersForConsumers: false,
  pump: "none",
  creditsJoinCompletion: false,
  initState: () => ({ nextFireMs: -1, emitted: 0, keySeq: 0 }),
  // Traffic INTO a cron is a wiring mistake, same as into an autoscaler.
  onAdmit: (ctx, state, req) => {
    ctx.reject(state, req, "no-route");
    return "handled";
  },
  onTick(ctx, state) {
    const st = cronExt(state);
    const interval = cronInterval(state);
    if (st.nextFireMs < 0) {
      st.nextFireMs = ctx.now + interval;
      return;
    }
    if (ctx.now < st.nextFireMs) return;
    const batch = cronBatch(state);
    for (let i = 0; i < state.out.length; i++) {
      const edge2 = state.out[i];
      for (let n = 0; n < batch; n++) {
        if (!ctx.emitDetached(state, edge2, st.keySeq)) break;
        st.keySeq = (st.keySeq + 1) % 64;
        st.emitted += 1;
      }
    }
    ctx.countCustom(state, "fired", 1);
    st.nextFireMs = ctx.now + interval;
  },
  decorateStats(ctx, state, stats) {
    const st = state.ext;
    if (!st) return;
    stats.nextFireInMs = st.nextFireMs < 0 ? cronInterval(state) : Math.max(0, st.nextFireMs - ctx.now);
    stats.batchEmitted = st.emitted;
    stats.burstSize = cronBatch(state) * state.out.length;
  }
};
var MESSAGING_BEHAVIOURS = [
  streambroker,
  pubsub,
  websocket,
  apigateway,
  sidecar,
  lambda,
  cron
];

// engine/sim/behaviour-resilience.ts
function cfgBulkheadMax(state) {
  const v = state.config.bulkheadMax;
  return v !== void 0 && v >= 1 ? Math.floor(v) : 8;
}
var bulkhead = {
  kind: "bulkhead",
  // A gate, not a server: it holds no slots of its own and its utilisation
  // would be meaningless. The pool count is its real meter.
  servesRequests: false,
  generatesLoad: false,
  pullsFromQueues: false,
  buffersForConsumers: false,
  pump: "none",
  creditsJoinCompletion: true,
  // The whole component is a count of downstream outcomes.
  observesOutcome: true,
  initState: () => ({ inFlight: 0 }),
  onAdmit: (ctx, state, req) => {
    if (state.out.length === 0) return "passthru";
    const b = state.ext;
    if (b.inFlight >= cfgBulkheadMax(state)) {
      ctx.countCustom(state, "bulkheadRejected", 1);
      ctx.reject(state, req, "bulkhead-full");
      return "handled";
    }
    b.inFlight++;
    ctx.countCustom(state, "admitted", 1);
    return "passthru";
  },
  route: () => "one",
  pickEdge: (ctx, state, _req, out) => {
    const edge2 = ctx.pickWeightedOrLeastLoaded(out);
    if (edge2 === null) {
      const b = state.ext;
      if (b && b.inFlight > 0) b.inFlight--;
    }
    return edge2;
  },
  onDownstreamResult: (_ctx, state, _req, _ok, _reason) => {
    const b = state.ext;
    if (b.inFlight > 0) b.inFlight--;
  },
  decorateStats: (ctx, state, stats) => {
    const b = state.ext;
    stats.bulkheadInFlight = b ? b.inFlight : 0;
    stats.bulkheadLimit = cfgBulkheadMax(state);
    stats.bulkheadRejectedRate = ctx.counterRate(state, "bulkheadRejected");
    stats.inFlight = b ? b.inFlight : 0;
  }
};
var retryqueue = {
  kind: "retryqueue",
  // Delivery slots are real work: the meter reads delivery concurrency.
  servesRequests: true,
  instanceModel: "slots",
  scaleField: "instances",
  generatesLoad: false,
  pullsFromQueues: false,
  buffersForConsumers: false,
  pump: "own",
  creditsJoinCompletion: false,
  observesOutcome: true,
  initState: () => ({ deadLetters: 0 }),
  onAdmit: (ctx, state, req) => {
    if (ctx.queueDepth(state) >= ctx.effectiveQueueLimit(state)) return "shed";
    ctx.ackAndRelay(state, req);
    return "handled";
  },
  onDownstreamResult: (ctx, state, req, ok, reason) => {
    if (ok) {
      ctx.countCustom(state, "delivered", 1);
      return;
    }
    const retries = Math.max(0, Math.floor(state.config.retries));
    const final = req.attempt >= retries || reason === "depth" || reason === "no-route";
    if (final) {
      ctx.countCustom(state, "deadLetter", 1);
      state.ext.deadLetters++;
    } else {
      ctx.countCustom(state, "redelivery", 1);
    }
  },
  decorateStats: (ctx, state, stats) => {
    stats.deliveredRate = ctx.counterRate(state, "delivered");
    stats.redeliveryRate = ctx.counterRate(state, "redelivery");
    stats.deadLetterRate = ctx.counterRate(state, "deadLetter");
    stats.deadLetters = state.ext?.deadLetters ?? 0;
  }
};
function cfgRenditions(state) {
  const v = state.config.renditions;
  return v !== void 0 && v >= 1 ? Math.floor(v) : 3;
}
var transcoder = {
  kind: "transcoder",
  servesRequests: true,
  instanceModel: "slots",
  scaleField: "instances",
  generatesLoad: false,
  pullsFromQueues: true,
  buffersForConsumers: false,
  pump: "sources",
  // FALSE, and it must be: a finished job is booked here by the 'complete'
  // path below, and the detached uploads join back through this node -- were
  // joins credited too, every rendition landing on storage would count as a
  // second (and third, and fourth) completed job, and the store's latency
  // would pollute the farm's. Same reasoning as the stream broker.
  creditsJoinCompletion: false,
  onServiceComplete: (ctx, state, req) => {
    const renditions = cfgRenditions(state);
    for (let i = 0; i < state.out.length; i++) {
      const edge2 = state.out[i];
      for (let n = 0; n < renditions; n++) {
        if (!ctx.emitDetached(state, edge2, req.key)) break;
        ctx.countCustom(state, "output", 1);
      }
    }
    return "complete";
  },
  decorateStats: (ctx, state, stats) => {
    stats.outputRate = ctx.counterRate(state, "output");
  }
};
var edgecompute = {
  kind: "edgecompute",
  servesRequests: true,
  instanceModel: "slots",
  scaleField: "instances",
  generatesLoad: false,
  pullsFromQueues: false,
  buffersForConsumers: false,
  pump: "own",
  creditsJoinCompletion: true,
  onServiceComplete: (ctx, state, req) => {
    const canAnswer = ctx.roll() < clamp012(state.config.edgeShare ?? 0);
    if (canAnswer) {
      const cap = state.config.cpuMsCap;
      if (cap !== void 0 && cap > 0 && req.ownMs > cap) {
        ctx.countCustom(state, "cpuExceeded", 1);
        ctx.countCustom(state, "passedThrough", 1);
        return state.out.length === 0 ? "complete" : "downstream";
      }
      ctx.countCustom(state, "edgeHandled", 1);
      return "complete";
    }
    ctx.countCustom(state, "passedThrough", 1);
    return state.out.length === 0 ? "complete" : "downstream";
  },
  decorateStats: (ctx, state, stats) => {
    stats.edgeHandledRate = ctx.counterRate(state, "edgeHandled");
    stats.passedThroughRate = ctx.counterRate(state, "passedThrough");
    stats.cpuExceededRate = ctx.counterRate(state, "cpuExceeded");
  }
};
function cfgFlushDelayMs(state) {
  const v = state.config.flushDelayMs;
  return v !== void 0 && v > 0 ? v : 0;
}
var writebehind = {
  kind: "writebehind",
  servesRequests: true,
  generatesLoad: false,
  pullsFromQueues: false,
  buffersForConsumers: false,
  pump: "own",
  creditsJoinCompletion: false,
  observesOutcome: true,
  onAdmit: (ctx, state, req) => {
    if (ctx.queueDepth(state) >= ctx.effectiveQueueLimit(state)) return "shed";
    ctx.ackAndRelay(state, req, cfgFlushDelayMs(state));
    return "handled";
  },
  onDownstreamResult: (ctx, state, _req, ok, _reason) => {
    ctx.countCustom(state, ok ? "flushed" : "flushFailed", 1);
  },
  decorateStats: (ctx, state, stats) => {
    stats.dirtyWrites = stats.inFlight + stats.queued;
    stats.flushedRate = ctx.counterRate(state, "flushed");
    stats.flushFailRate = ctx.counterRate(state, "flushFailed");
  }
};
function shedRate(state) {
  const r = state.config.rateLimitRps;
  return r !== void 0 && r > 0 ? r : 0;
}
function shedBurst(state) {
  const b = state.config.burst;
  if (b !== void 0 && b > 0) return b;
  const r = shedRate(state);
  return r > 0 ? r : 1;
}
function shedRefill(ctx, state, b) {
  const burst = shedBurst(state);
  if (burst !== b.lastBurst) {
    if (b.tokens > burst) b.tokens = burst;
    b.lastBurst = burst;
  }
  const elapsed = ctx.now - b.lastRefillMs;
  if (elapsed <= 0) return;
  b.lastRefillMs = ctx.now;
  const rate = shedRate(state);
  if (rate <= 0) return;
  b.tokens += elapsed / 1e3 * rate;
  if (b.tokens > burst) b.tokens = burst;
}
function shedProjected(ctx, state, b) {
  const burst = shedBurst(state);
  let tokens = b.tokens > burst ? burst : b.tokens;
  const rate = shedRate(state);
  const elapsed = ctx.now - b.lastRefillMs;
  if (rate > 0 && elapsed > 0) {
    tokens += elapsed / 1e3 * rate;
    if (tokens > burst) tokens = burst;
  }
  return tokens;
}
function isLowPriority(state, req) {
  const share = clamp012(state.config.lowPriorityShare ?? 0);
  if (share <= 0) return false;
  const hashed = (Math.imul(req.key, 2654435761) >>> 0) / 4294967296;
  return hashed < share;
}
var loadshedder = {
  kind: "loadshedder",
  // A doorman, like the limiter: no slots, no meaningful utilisation.
  servesRequests: false,
  generatesLoad: false,
  pullsFromQueues: false,
  buffersForConsumers: false,
  pump: "none",
  creditsJoinCompletion: true,
  initState: (state) => ({
    // Starts full, like the limiter: an idle system absorbs a burst.
    tokens: shedBurst(state),
    lastRefillMs: 0,
    lastBurst: shedBurst(state)
  }),
  onAdmit: (ctx, state, req) => {
    const b = state.ext;
    shedRefill(ctx, state, b);
    const low = isLowPriority(state, req);
    if (shedRate(state) <= 0) {
      ctx.countCustom(state, low ? "lowAdmitted" : "highAdmitted", 1);
      return "passthru";
    }
    const floor = low ? 1 + clamp012(state.config.priorityReserve ?? 0.3) * shedBurst(state) : 1;
    if (b.tokens >= floor) {
      b.tokens -= 1;
      ctx.countCustom(state, low ? "lowAdmitted" : "highAdmitted", 1);
      return "passthru";
    }
    ctx.countCustom(state, low ? "lowShed" : "highShed", 1);
    ctx.reject(state, req, low ? "deprioritized" : "throttled");
    return "handled";
  },
  decorateStats: (ctx, state, stats) => {
    const b = state.ext;
    stats.highAdmittedRate = ctx.counterRate(state, "highAdmitted");
    stats.lowAdmittedRate = ctx.counterRate(state, "lowAdmitted");
    stats.highSheddedRate = ctx.counterRate(state, "highShed");
    stats.lowSheddedRate = ctx.counterRate(state, "lowShed");
    stats.admittedRate = stats.highAdmittedRate + stats.lowAdmittedRate;
    stats.tokens = b ? shedProjected(ctx, state, b) : 0;
  }
};
var RESILIENCE_BEHAVIOURS = [
  bulkhead,
  retryqueue,
  transcoder,
  edgecompute,
  writebehind,
  loadshedder
];

// engine/sim/behaviour.ts
var client = {
  kind: "client",
  servesRequests: true,
  generatesLoad: true,
  pullsFromQueues: false,
  buffersForConsumers: false,
  pump: "none",
  creditsJoinCompletion: false,
  onAdmit: () => "passthru"
};
var lb = {
  kind: "lb",
  servesRequests: true,
  instanceModel: "slots",
  scaleField: "instances",
  generatesLoad: false,
  pullsFromQueues: false,
  buffersForConsumers: false,
  pump: "own",
  creditsJoinCompletion: true,
  onAdmit: () => "passthru",
  route: () => "one",
  pickEdge: (ctx, _state, _req, out) => ctx.pickWeightedOrLeastLoaded(out)
};
var service = {
  kind: "service",
  servesRequests: true,
  instanceModel: "slots",
  scaleField: "instances",
  generatesLoad: false,
  pullsFromQueues: false,
  buffersForConsumers: false,
  pump: "own",
  creditsJoinCompletion: true
};
var cache = {
  kind: "cache",
  servesRequests: true,
  instanceModel: "slots",
  scaleField: "instances",
  generatesLoad: false,
  pullsFromQueues: false,
  buffersForConsumers: false,
  pump: "own",
  creditsJoinCompletion: true,
  onServiceComplete: (ctx, state, _req) => {
    if (ctx.roll() < clamp012(state.config.hitRate)) {
      ctx.countHit(state);
      return "complete";
    }
    ctx.countMiss(state);
    return state.out.length === 0 ? "complete" : "downstream";
  }
};
var queue = {
  kind: "queue",
  servesRequests: false,
  generatesLoad: false,
  pullsFromQueues: false,
  buffersForConsumers: true,
  pump: "none",
  creditsJoinCompletion: true,
  onAdmit: (ctx, state, req) => {
    if (ctx.queueDepth(state) >= ctx.effectiveQueueLimit(state)) return "shed";
    ctx.ackAndBuffer(state, req);
    return "handled";
  }
};
var worker = {
  kind: "worker",
  servesRequests: true,
  instanceModel: "slots",
  scaleField: "instances",
  generatesLoad: false,
  pullsFromQueues: true,
  buffersForConsumers: false,
  pump: "sources",
  creditsJoinCompletion: true
};
var ALL = [
  client,
  lb,
  service,
  cache,
  queue,
  worker,
  ...EDGE_BEHAVIOURS,
  ...CONTROL_BEHAVIOURS
];
ALL.push(...DATA_BEHAVIOURS);
ALL.push(...STORE_BEHAVIOURS);
ALL.push(...MESSAGING_BEHAVIOURS);
ALL.push(...RESILIENCE_BEHAVIOURS);
var BY_KIND = /* @__PURE__ */ new Map();
for (const b of ALL) BY_KIND.set(b.kind, b);
function behaviourFor(kind) {
  const b = BY_KIND.get(kind);
  if (b) return b;
  return service;
}
function clamp012(v) {
  return v < 0 ? 0 : v > 1 ? 1 : v;
}

// engine/sim/heap.ts
var MinHeap = class {
  items = [];
  get size() {
    return this.items.length;
  }
  push(item) {
    const a = this.items;
    a.push(item);
    let i = a.length - 1;
    while (i > 0) {
      const parent = i - 1 >> 1;
      if (less(a[i], a[parent])) {
        swap(a, i, parent);
        i = parent;
      } else break;
    }
  }
  pop() {
    const a = this.items;
    if (a.length === 0) return void 0;
    const top = a[0];
    const last = a.pop();
    if (a.length > 0) {
      a[0] = last;
      let i = 0;
      for (; ; ) {
        const l = 2 * i + 1;
        const r = l + 1;
        let smallest = i;
        if (l < a.length && less(a[l], a[smallest])) smallest = l;
        if (r < a.length && less(a[r], a[smallest])) smallest = r;
        if (smallest === i) break;
        swap(a, i, smallest);
        i = smallest;
      }
    }
    return top;
  }
  peek() {
    return this.items[0];
  }
  clear() {
    this.items.length = 0;
  }
};
function less(a, b) {
  return a.time !== b.time ? a.time < b.time : a.seq < b.seq;
}
function swap(a, i, j) {
  const t = a[i];
  a[i] = a[j];
  a[j] = t;
}

// engine/sim/random.ts
var Rng = class {
  s;
  constructor(seed) {
    this.s = seed >>> 0;
  }
  next() {
    this.s = this.s + 1831565813 >>> 0;
    let t = this.s;
    t = Math.imul(t ^ t >>> 15, t | 1);
    t ^= t + Math.imul(t ^ t >>> 7, t | 61);
    return ((t ^ t >>> 14) >>> 0) / 4294967296;
  }
  /** Exponential with the given mean. */
  exponential(mean) {
    if (mean <= 0) return 0;
    const u = 1 - this.next();
    return -Math.log(u) * mean;
  }
  /**
   * Service time with a target mean and coefficient of variation.
   * cv=0 -> deterministic, cv=1 -> exponential, cv>1 -> heavy tailed.
   * Implemented as a gamma draw with shape k = 1/cv^2.
   */
  serviceTime(mean, cv) {
    if (mean <= 0) return 0;
    if (cv <= 0.01) return mean;
    const shape = 1 / (cv * cv);
    const scale = mean / shape;
    return this.gamma(shape, scale);
  }
  /** Marsaglia-Tsang gamma sampler. */
  gamma(shape, scale) {
    if (shape < 1) {
      const u = 1 - this.next();
      return this.gamma(shape + 1, scale) * Math.pow(u, 1 / shape);
    }
    const d = shape - 1 / 3;
    const c = 1 / Math.sqrt(9 * d);
    for (; ; ) {
      let x;
      let v;
      do {
        x = this.normal();
        v = 1 + c * x;
      } while (v <= 0);
      v = v * v * v;
      const u = 1 - this.next();
      if (u < 1 - 0.0331 * x * x * x * x) return d * v * scale;
      if (Math.log(u) < 0.5 * x * x + d * (1 - v + Math.log(v))) {
        return d * v * scale;
      }
    }
  }
  /** Standard normal via Box-Muller. */
  normal() {
    const u = 1 - this.next();
    const v = this.next();
    return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
  }
};

// engine/sim/types.ts
var DEFAULT_TRAFFIC_PERIOD_S = 60;

// engine/sim/engine.ts
var MAX_DELTA_MS = 100;
var MAX_EVENTS_PER_ADVANCE = 6e4;
var MAX_HOP_DEPTH = 32;
var LATENCY_WINDOW_MS = 5e3;
var LATENCY_RING = 4096;
var PERCENTILE_SAMPLE_CAP = 512;
var SPIKE_QUIET_FRACTION = 0.9;
var SPIKE_PEAK = 4;
var SPIKE_TROUGH = 0.1;
var DIURNAL_TROUGH = 0.25;
var DIURNAL_PEAK = 2;
var RATE_WINDOW_MS = 1e3;
var RATE_BUCKETS = 10;
var HISTORY_INTERVAL_MS = 250;
var HISTORY_MAX = 240;
var RETRY_BASE_BACKOFF_MS = 25;
var MAX_LIVE_REQUESTS = 2e5;
var KEYSPACE3 = 64;
var EMPTY_SHARDS = [];
var EV_ARRIVAL = 0;
var EV_SERVICE_DONE = 1;
var EV_TIMEOUT = 2;
var EV_WORKER_POLL = 3;
var EV_RETRY = 4;
var EV_LINK_ARRIVE = 5;
var RateCounter = class {
  buckets = new Float64Array(RATE_BUCKETS);
  stamps = new Float64Array(RATE_BUCKETS).fill(-1);
  bucketMs = RATE_WINDOW_MS / RATE_BUCKETS;
  add(now, n) {
    const stamp = Math.floor(now / this.bucketMs);
    const idx = (stamp % RATE_BUCKETS + RATE_BUCKETS) % RATE_BUCKETS;
    if (this.stamps[idx] !== stamp) {
      this.stamps[idx] = stamp;
      this.buckets[idx] = 0;
    }
    this.buckets[idx] += n;
  }
  /**
   * Events per second over the trailing window.
   *
   * Every counter divides by the SAME fixed span, so rates from different
   * counters stay comparable and their ratios mean something -- goodput can
   * never exceed offered just because one of them happened to be idle for
   * part of the window. The in-progress bucket is excluded rather than
   * scaled: a partially elapsed bucket read as if it were whole is what
   * makes a live rate flicker.
   */
  rate(now) {
    const current = Math.floor(now / this.bucketMs);
    let total = 0;
    for (let i = 0; i < RATE_BUCKETS; i++) {
      const age = current - this.stamps[i];
      if (age > 0 && age < RATE_BUCKETS) {
        total += this.buckets[i];
      }
    }
    if (total === 0) return 0;
    const elapsedBuckets = Math.max(1, Math.min(RATE_BUCKETS - 1, current));
    return total * 1e3 / (elapsedBuckets * this.bucketMs);
  }
  reset() {
    this.buckets.fill(0);
    this.stamps.fill(-1);
  }
};
var LatencyRing = class {
  vals = new Float64Array(LATENCY_RING);
  times = new Float64Array(LATENCY_RING);
  head = 0;
  count = 0;
  scratch = new Float64Array(LATENCY_RING);
  /** Monotonic count of samples ever added; part of the memo key. */
  added = 0;
  cacheTime = -1;
  cacheAdded = -1;
  cache0 = 0;
  cache1 = 0;
  cache2 = 0;
  add(now, v) {
    this.vals[this.head] = v;
    this.times[this.head] = now;
    this.head = (this.head + 1) % LATENCY_RING;
    if (this.count < LATENCY_RING) this.count++;
    this.added++;
  }
  /**
   * Fills out[0..2] with p50/p95/p99 over the trailing window.
   *
   * Results are memoized per (time, sample count): snapshot() is polled at
   * 10Hz and asks every node for percentiles, but the underlying samples only
   * change when a request completes. Re-sorting an unchanged window would
   * dominate the frame budget.
   */
  percentiles(now, out) {
    if (now === this.cacheTime && this.added === this.cacheAdded) {
      out[0] = this.cache0;
      out[1] = this.cache1;
      out[2] = this.cache2;
      return;
    }
    const cutoff = now - LATENCY_WINDOW_MS;
    const s = this.scratch;
    let n = 0;
    const limit = this.count < PERCENTILE_SAMPLE_CAP ? this.count : PERCENTILE_SAMPLE_CAP;
    const stride = this.count > PERCENTILE_SAMPLE_CAP ? Math.floor(this.count / PERCENTILE_SAMPLE_CAP) : 1;
    for (let i = 0, taken = 0; taken < limit && i < this.count; i += stride) {
      const idx = (this.head - 1 - i + LATENCY_RING * 2) % LATENCY_RING;
      if (this.times[idx] < cutoff) break;
      s[n++] = this.vals[idx];
      taken++;
    }
    if (n === 0) {
      out[0] = 0;
      out[1] = 0;
      out[2] = 0;
    } else {
      const view = s.subarray(0, n);
      view.sort();
      out[0] = quantile(view, n, 0.5);
      out[1] = quantile(view, n, 0.95);
      out[2] = quantile(view, n, 0.99);
    }
    this.cacheTime = now;
    this.cacheAdded = this.added;
    this.cache0 = out[0];
    this.cache1 = out[1];
    this.cache2 = out[2];
  }
  reset() {
    this.head = 0;
    this.count = 0;
    this.added = 0;
    this.cacheTime = -1;
    this.cacheAdded = -1;
  }
};
function quantile(sorted, n, q) {
  if (n === 1) return sorted[0];
  const pos = q * (n - 1);
  const lo = Math.floor(pos);
  const hi = Math.ceil(pos);
  if (lo === hi) return sorted[lo];
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo);
}
var Engine = class {
  topology;
  seed;
  rng;
  /** Current simulated time in ms. Part of BehaviourCtx; never written from outside. */
  now = 0;
  seq = 0;
  heap = new MinHeap();
  nodes = /* @__PURE__ */ new Map();
  clientIds = [];
  /**
   * Nodes whose behaviour declares onTick. Kept as a separate list so the
   * per-advance walk costs nothing at all while no such kind exists.
   */
  tickNodes = [];
  edgeFlow = /* @__PURE__ */ new Map();
  freeReq = null;
  freeEv = [];
  liveRequests = 0;
  /** Root-level (end-to-end) measurements. */
  sysLatency = new LatencyRing();
  sysOffered = new RateCounter();
  sysGood = new RateCounter();
  sysFailed = new RateCounter();
  totalRequests = 0;
  totalFailed = 0;
  failures = {
    error: 0,
    shed: 0,
    timeout: 0,
    "no-route": 0,
    depth: 0,
    throttled: 0,
    rejected: 0,
    crashed: 0,
    partitioned: 0,
    "region-down": 0,
    "conn-refused": 0,
    unauthorized: 0,
    "bulkhead-full": 0,
    deprioritized: 0
  };
  /** Injected failures, keyed by node id. At most one fault per node. */
  faults = /* @__PURE__ */ new Map();
  /**
   * Every edge id cut by an active partition, flattened out of `faults` so the
   * send path tests a partition with one Set lookup instead of walking faults.
   * Rebuilt whenever faults or the topology change.
   */
  cutEdges = /* @__PURE__ */ new Set();
  /** Reused snapshot array for active failures. */
  snapFailures = [];
  history = [];
  lastHistoryMs = 0;
  pctScratch = new Float64Array(3);
  nodePctScratch = new Float64Array(3);
  /** Reused snapshot containers so 10Hz polling does not churn the heap. */
  snapNodes = {};
  snapEdges = {};
  snapEdgeState = {};
  /**
   * Capacity each node was authored with, by node id.
   *
   * A controller (the autoscaler) writes the capacity it has decided on back
   * into `this.topology`, so the Inspector shows the size the node actually
   * has rather than the one it started at. That write is correct for the live
   * run and wrong for reset(): rebuilding from the mutated topology would
   * restart the simulation at whatever capacity the controller happened to
   * have reached, so the same seed would not replay. Recording the authored
   * value at construction -- and at every setTopology, which is the only other
   * time the student states an intent -- lets reset() put it back.
   */
  authoredCapacity = /* @__PURE__ */ new Map();
  /** Same, for the per-shard knob a sharded store is scaled through. */
  authoredShardCapacity = /* @__PURE__ */ new Map();
  constructor(topology, seed = 1) {
    this.seed = seed >>> 0;
    this.rng = new Rng(this.seed);
    this.topology = cloneTopology(topology);
    this.recordAuthoredCapacity();
    this.buildNodes(null);
  }
  /**
   * Snapshot the authored capacity of every node, for reset() to restore.
   *
   * Both scalable knobs are recorded, because a controller may write either
   * one depending on the target kind's `capacityField`. Restoring only
   * `capacity` would let a controller-scaled sharded store carry its grown
   * `shardCapacity` across a reset, so the same seed would not replay.
   */
  recordAuthoredCapacity() {
    this.authoredCapacity.clear();
    this.authoredShardCapacity.clear();
    for (const n of this.topology.nodes) {
      this.authoredCapacity.set(n.id, n.config.capacity);
      this.authoredShardCapacity.set(n.id, n.config.shardCapacity);
    }
  }
  /* ---------------- public API ---------------- */
  setTopology(t) {
    const previous = this.nodes;
    this.topology = cloneTopology(t);
    this.recordAuthoredCapacity();
    this.buildNodes(previous);
    for (const nodeId of [...this.faults.keys()]) {
      if (!this.nodes.has(nodeId)) this.faults.delete(nodeId);
    }
    this.rebuildCutEdges();
  }
  updateNodeConfig(id, patch) {
    const node2 = this.topology.nodes.find((n) => n.id === id);
    if (node2) Object.assign(node2.config, patch);
    if (patch.capacity !== void 0) {
      this.authoredCapacity.set(id, Math.max(1, Math.floor(patch.capacity)));
    }
    if (patch.shardCapacity !== void 0) {
      this.authoredShardCapacity.set(id, Math.max(1, Math.floor(patch.shardCapacity)));
    }
    const state = this.nodes.get(id);
    if (!state) return;
    Object.assign(state.config, patch);
    this.pumpQueue(state);
  }
  /* ---------------- failure injection ---------------- *
   *
   * Chaos is a first-class engine operation rather than a component, so any
   * node in any topology can be faulted without rewiring anything. Each fault
   * intercepts at exactly one point in the request path, which is what makes
   * it compose with retries, timeouts and a circuit breaker for free:
   *
   *   crash      admit() refuses immediately, and in-flight work at the node
   *              is failed at injection time. The caller sees a failed call
   *              like any other, so its retry budget and its breaker's error
   *              window both observe it.
   *   slow       serviceTimeFor() multiplies the drawn service time. Nothing
   *              else changes, so the caller's timeout starts firing on its
   *              own -- which is the lesson.
   *   errors     onServiceComplete() rolls it after the node's own errorRate.
   *   partition  sendChild() refuses to cross a cut edge, so the CALLER sees
   *              the failure. That is what a network partition looks like
   *              from one side, and it leaves the target node untouched.
   * ------------------------------------------------------ */
  /**
   * Attach a fault to a node. One fault per node: injecting again replaces
   * whatever was there, so the UI never has to reason about stacking.
   */
  injectFailure(nodeId, kind, opts = {}) {
    if (!this.nodes.has(nodeId)) return;
    const fault = {
      kind,
      sinceMs: this.now,
      // A fault may not make a node faster, so a factor below 1 is clamped.
      factor: opts.factor !== void 0 && opts.factor > 1 ? opts.factor : 1,
      rate: opts.rate !== void 0 ? clamp014(opts.rate) : 1,
      edgeIds: opts.edgeIds ? opts.edgeIds.slice() : []
    };
    this.faults.set(nodeId, fault);
    this.rebuildCutEdges();
    if (kind === "crash") this.killInFlight(nodeId);
  }
  /** Heal a node. Work that already failed stays failed; new work succeeds. */
  clearFailure(nodeId) {
    if (!this.faults.delete(nodeId)) return;
    this.rebuildCutEdges();
    const state = this.nodes.get(nodeId);
    if (state) this.pumpQueue(state);
  }
  /** Every fault in force, for the UI. */
  activeFailures() {
    const out = [];
    for (const [nodeId, f] of this.faults) {
      out.push(describeFault(nodeId, f));
    }
    return out;
  }
  /**
   * Fail everything the crashed node is holding: requests in service, and
   * requests queued behind them. Iterating a copy of the waiting list matters
   * because resolve() can re-enter the engine through the parent's retry path.
   */
  killInFlight(nodeId) {
    const state = this.nodes.get(nodeId);
    if (!state) return;
    const waiting = [];
    for (let i = state.waitHead; i < state.waiting.length; i++) {
      const req = state.waiting[i];
      if (req) waiting.push(req);
    }
    state.waiting.length = 0;
    state.waitHead = 0;
    for (const req of waiting) {
      if (req.resolved) continue;
      state.totalFailed++;
      this.resolve(req, false, "crashed", 0);
    }
  }
  rebuildCutEdges() {
    this.cutEdges.clear();
    for (const [nodeId, fault] of this.faults) {
      if (fault.kind !== "partition") continue;
      if (fault.edgeIds.length > 0) {
        for (const id of fault.edgeIds) this.cutEdges.add(id);
        continue;
      }
      const state = this.nodes.get(nodeId);
      if (!state) continue;
      for (const edge2 of state.out) this.cutEdges.add(edge2.id);
      for (const edge2 of state.ctrl) this.cutEdges.add(edge2.id);
    }
  }
  advance(deltaMs) {
    if (!(deltaMs > 0)) return;
    const dt = Math.min(deltaMs, MAX_DELTA_MS);
    const target = this.now + dt;
    let budget = MAX_EVENTS_PER_ADVANCE;
    for (; ; ) {
      const next = this.heap.peek();
      if (!next || next.time > target) break;
      if (budget-- <= 0) break;
      const ev = this.heap.pop();
      this.now = ev.time;
      this.dispatch(ev);
      this.releaseEv(ev);
    }
    this.now = target;
    this.integrateAll();
    if (this.tickNodes.length > 0) this.runTicks(dt);
    this.maybeRecordHistory();
  }
  snapshot() {
    const now = this.now;
    this.sysLatency.percentiles(now, this.pctScratch);
    const system = {
      timeMs: now,
      offeredRps: this.sysOffered.rate(now),
      goodputRps: this.sysGood.rate(now),
      errorRate: 0,
      p50: this.pctScratch[0],
      p95: this.pctScratch[1],
      p99: this.pctScratch[2],
      totalRequests: this.totalRequests,
      totalFailed: this.totalFailed
    };
    const failRate = this.sysFailed.rate(now);
    const done = system.goodputRps + failRate;
    system.errorRate = done > 0 ? failRate / done : 0;
    const nodeOut = this.snapNodes;
    for (const key of Object.keys(nodeOut)) {
      if (!this.nodes.has(key)) delete nodeOut[key];
    }
    for (const state of this.nodes.values()) {
      state.latency.percentiles(now, this.nodePctScratch);
      const completions = state.completions.rate(now);
      const errorsPerSec = state.errors.rate(now);
      const shedRate2 = state.sheds.rate(now);
      const timeoutRate = state.timeouts.rate(now);
      const hits = state.hits.rate(now);
      const misses = state.misses.rate(now);
      const resolved = completions + errorsPerSec + shedRate2;
      const entry = {
        inFlight: 0,
        queued: 0,
        throughput: 0,
        arrivalRate: 0,
        utilization: 0,
        p50: 0,
        p95: 0,
        p99: 0,
        errorRate: 0,
        shedRate: 0,
        timeoutRate: 0,
        hitRate: 0,
        totalCompleted: 0,
        totalFailed: 0,
        queueLimit: this.effectiveQueueLimit(state),
        staleReadRate: 0,
        maxShardUtilization: 0,
        minShardUtilization: 0,
        shardUtilization: EMPTY_SHARDS
      };
      nodeOut[state.id] = entry;
      entry.inFlight = state.busy;
      entry.queued = state.waiting.length - state.waitHead;
      entry.throughput = completions;
      entry.arrivalRate = state.arrivals.rate(now);
      entry.utilization = state.utilization;
      entry.p50 = this.nodePctScratch[0];
      entry.p95 = this.nodePctScratch[1];
      entry.p99 = this.nodePctScratch[2];
      entry.errorRate = resolved > 0 ? (errorsPerSec + shedRate2) / resolved : 0;
      entry.shedRate = shedRate2;
      entry.timeoutRate = timeoutRate;
      entry.hitRate = hits + misses > 0 ? hits / (hits + misses) : 0;
      entry.totalCompleted = state.totalCompleted;
      entry.totalFailed = state.totalFailed;
      if (state.behaviour.decorateStats) {
        state.behaviour.decorateStats(this, state, entry);
      }
      const model = state.behaviour.instanceModel;
      if (model === "slots") {
        this.fillSlotInstances(state);
        this.finishInstances(state, entry);
      } else if (model === "custom") {
        state.instancePending = 0;
        if (state.behaviour.reportInstances)
          state.behaviour.reportInstances(this, state);
        this.finishInstances(state, entry);
      }
    }
    this.attachPendingInstances(nodeOut);
    const edgeOut = this.snapEdges;
    for (const key of Object.keys(edgeOut)) {
      if (!this.edgeFlow.has(key)) delete edgeOut[key];
    }
    for (const [id, counter] of this.edgeFlow) {
      edgeOut[id] = counter.rate(now);
    }
    return {
      system,
      nodes: nodeOut,
      history: this.history,
      edgeFlow: edgeOut,
      edgeState: this.snapshotEdgeState(edgeOut),
      failuresByReason: this.failures,
      activeFailures: this.snapshotFailures(),
      trace: this.lastTrace
    };
  }
  /**
   * Hand each autoscaler's booked-but-not-yet-live units to the node they were
   * booked FOR.
   *
   * The controller knows the number; the target is what the student is
   * looking at. Reporting it only on the autoscaler would leave the UI unable
   * to draw ghosted units on the stack that is about to grow, which is the
   * one place the warm-up lag is legible.
   *
   * `targetInstances` while scaling is the fleet size the target will reach,
   * and its live `instances` is the size it has, so the difference is exactly
   * what is still booting. Clamped at zero: a scale-up whose target was
   * manually enlarged past the booked figure in the meantime owes nothing.
   *
   * Driven off the behaviour's own control-target resolution rather than a
   * kind test, so the wiring rule lives in exactly one place.
   */
  attachPendingInstances(nodeOut) {
    for (const state of this.nodes.values()) {
      if (state.ctrl.length === 0) continue;
      const own = nodeOut[state.id];
      if (!own || own.scaling !== true) continue;
      const watched = this.controlTargetOf(state);
      const target = watched ? nodeOut[watched] : void 0;
      if (!target || target.instances === void 0) continue;
      const booked = own.targetInstances ?? 0;
      const pending = booked - target.instances;
      if (pending > 0) target.instancesPending = pending;
    }
  }
  /**
   * Classify every edge for the snapshot.
   *
   * Resolution order is fixed and matters: an injected cut is reported over a
   * breaker's refusal, because the fault is the more specific truth about that
   * particular wire. Below those, a kind that withholds traffic for a reason
   * of its own gets to say so through edgeStateFor(); everything else falls
   * back to whether the edge is actually moving requests.
   */
  snapshotEdgeState(rates) {
    const out = this.snapEdgeState;
    for (const key of Object.keys(out)) {
      if (!this.edgeFlow.has(key)) delete out[key];
    }
    for (const state of this.nodes.values()) {
      const hook = state.behaviour.edgeStateFor;
      for (let i = 0; i < state.out.length; i++) {
        const edge2 = state.out[i];
        if (this.cutEdges.size > 0 && this.cutEdges.has(edge2.id)) {
          out[edge2.id] = "cut";
          continue;
        }
        const declared = hook ? hook(this, state, edge2, i) : null;
        if (declared !== null) {
          out[edge2.id] = declared;
          continue;
        }
        out[edge2.id] = (rates[edge2.id] ?? 0) > 0 ? "live" : "idle";
      }
      for (const edge2 of state.ctrl) {
        out[edge2.id] = this.cutEdges.size > 0 && this.cutEdges.has(edge2.id) ? "cut" : "standby";
      }
    }
    for (const id of this.edgeFlow.keys()) {
      if (out[id] === void 0) out[id] = (rates[id] ?? 0) > 0 ? "live" : "idle";
    }
    return out;
  }
  /**
   * Injected failures, rebuilt into the reused array. Length is the number of
   * faulted nodes -- normally zero -- so this costs nothing on a healthy run.
   */
  snapshotFailures() {
    const out = this.snapFailures;
    out.length = 0;
    for (const [nodeId, fault] of this.faults) {
      out.push(describeFault(nodeId, fault));
    }
    return out;
  }
  reset() {
    this.rng = new Rng(this.seed);
    this.now = 0;
    this.seq = 0;
    this.heap.clear();
    this.freeReq = null;
    this.freeEv.length = 0;
    this.liveRequests = 0;
    this.sysLatency.reset();
    this.sysOffered.reset();
    this.sysGood.reset();
    this.sysFailed.reset();
    this.totalRequests = 0;
    this.totalFailed = 0;
    this.failures = {
      error: 0,
      shed: 0,
      timeout: 0,
      "no-route": 0,
      depth: 0,
      throttled: 0,
      rejected: 0,
      crashed: 0,
      partitioned: 0,
      "region-down": 0,
      "conn-refused": 0,
      unauthorized: 0,
      "bulkhead-full": 0,
      deprioritized: 0
    };
    this.history = [];
    this.lastHistoryMs = 0;
    this.snapNodes = {};
    this.snapEdges = {};
    for (const counter of this.edgeFlow.values()) counter.reset();
    this.faults.clear();
    this.cutEdges.clear();
    this.snapFailures.length = 0;
    this.tracing = null;
    this.lastTrace = null;
    for (const n of this.topology.nodes) {
      const authored = this.authoredCapacity.get(n.id);
      if (authored !== void 0) n.config.capacity = authored;
      const authoredShard = this.authoredShardCapacity.get(n.id);
      if (authoredShard !== void 0) n.config.shardCapacity = authoredShard;
    }
    this.buildNodes(null);
  }
  /* ---------------- topology wiring ---------------- */
  buildNodes(previous) {
    const next = /* @__PURE__ */ new Map();
    this.clientIds = [];
    for (const node2 of this.topology.nodes) {
      const kept = previous?.get(node2.id);
      let state;
      if (kept && kept.kind === node2.kind) {
        state = kept;
        state.config = { ...node2.config };
        state.out = [];
        state.ctrl = [];
        state.sources = [];
      } else {
        state = createNodeState(node2);
      }
      state.lastIntegrateMs = this.now;
      next.set(node2.id, state);
      if (state.behaviour.generatesLoad) this.clientIds.push(node2.id);
    }
    for (const edge2 of this.topology.edges) {
      const from = next.get(edge2.from);
      const to = next.get(edge2.to);
      if (!from || !to) continue;
      if (edge2.control === true || from.behaviour.controlsTarget === true) {
        from.ctrl.push(edge2);
        continue;
      }
      from.out.push(edge2);
      if (to.behaviour.pullsFromQueues && from.behaviour.buffersForConsumers) {
        to.sources.push(from.id);
      }
    }
    for (const state of next.values()) {
      if (!state.behaviour.pullsFromQueues) continue;
      for (const edge2 of state.out) {
        const target = next.get(edge2.to);
        if (target && target.behaviour.buffersForConsumers && !state.sources.includes(target.id)) {
          state.sources.push(target.id);
        }
      }
    }
    if (previous) {
      for (const [id, state] of previous) {
        if (next.get(id) === state) continue;
        this.discardNodeState(state);
      }
    }
    this.nodes = next;
    const flow = /* @__PURE__ */ new Map();
    for (const edge2 of this.topology.edges) {
      flow.set(edge2.id, this.edgeFlow.get(edge2.id) ?? new RateCounter());
    }
    this.edgeFlow = flow;
    for (const id of this.clientIds) {
      this.scheduleArrival(id);
    }
    this.tickNodes = [];
    for (const state of this.nodes.values()) {
      state.pollScheduled = false;
      if (state.ext === null && state.behaviour.initState) {
        state.ext = state.behaviour.initState(state);
      }
      this.pumpQueue(state);
      if (state.behaviour.onTick) this.tickNodes.push(state);
    }
  }
  discardNodeState(state) {
    for (let i = state.waitHead; i < state.waiting.length; i++) {
      const req = state.waiting[i];
      this.resolve(req, false, "no-route", 0);
    }
    state.waiting.length = 0;
    state.waitHead = 0;
    state.busy = 0;
  }
  /* ---------------- event dispatch ---------------- */
  dispatch(ev) {
    const state = this.nodes.get(ev.nodeId);
    if (!state) return;
    switch (ev.kind) {
      case EV_ARRIVAL:
        this.onClientArrival(state);
        break;
      case EV_SERVICE_DONE:
        if (ev.req && ev.req.token === ev.token) this.onServiceDone(state, ev.req);
        break;
      case EV_TIMEOUT:
        if (ev.req && ev.req.token === ev.token) this.onTimeout(ev.req);
        break;
      case EV_WORKER_POLL:
        state.pollScheduled = false;
        this.pumpWorker(state);
        break;
      case EV_RETRY:
        if (ev.req && ev.req.token === ev.token) this.onRetry(state, ev.req);
        break;
      case EV_LINK_ARRIVE:
        if (ev.req && ev.req.token === ev.token && !ev.req.resolved) {
          this.admit(state, ev.req);
        }
        break;
      default:
        break;
    }
  }
  /* ---------------- client ---------------- */
  scheduleArrival(nodeId) {
    const state = this.nodes.get(nodeId);
    if (!state || !state.behaviour.generatesLoad) return;
    const rps = this.effectiveRps(state);
    if (!(rps > 0)) {
      this.push(this.now + 50, EV_ARRIVAL, nodeId, null, 0);
      return;
    }
    const gap = this.rng.exponential(1e3 / rps);
    this.push(this.now + gap, EV_ARRIVAL, nodeId, null, 0);
  }
  onClientArrival(state) {
    this.scheduleArrival(state.id);
    if (this.effectiveRps(state) <= 0) return;
    if (this.liveRequests >= MAX_LIVE_REQUESTS) return;
    const root = this.acquireReq();
    root.nodeId = state.id;
    root.parent = null;
    root.rootStartMs = this.now;
    root.enterMs = this.now;
    root.hop = 0;
    root.attempt = 0;
    root.pending = 0;
    root.maxChildMs = 0;
    root.ownMs = 0;
    root.key = Math.floor(this.rng.next() * KEYSPACE3);
    if (this.tracing === null) {
      this.tracing = root;
      root.trace = [];
    }
    this.totalRequests++;
    this.sysOffered.add(this.now, 1);
    state.arrivals.add(this.now, 1);
    this.dispatchDownstream(state, root);
  }
  /* ---------------- routing ---------------- */
  /**
   * Issue this node's downstream calls. For an lb exactly one edge is chosen;
   * for anything else every distinct downstream is called and joined.
   */
  dispatchDownstream(state, req) {
    if (req.hop >= MAX_HOP_DEPTH) {
      this.resolve(req, false, "depth", req.ownMs);
      return;
    }
    const out = state.out;
    if (out.length === 0) {
      this.resolve(req, true, "error", req.ownMs);
      return;
    }
    const b = state.behaviour;
    const mode = b.route ? b.route(this, state, req) : "all";
    if (mode === "none") {
      this.completeNode(state, req, req.ownMs);
      return;
    }
    if (mode === "one") {
      const edge2 = b.pickEdge ? b.pickEdge(this, state, req, out) : this.pickWeightedOrLeastLoaded(out);
      if (!edge2) {
        const reason = state.behaviour.noRouteReason ?? "no-route";
        if (reason !== "no-route") {
          state.errors.add(this.now, 1);
          state.totalFailed++;
        }
        this.resolve(req, false, reason, req.ownMs);
        return;
      }
      req.pending = 1;
      this.sendChild(state, req, edge2, 0);
      return;
    }
    req.pending = out.length;
    for (let i = 0; i < out.length; i++) {
      if (req.resolved) return;
      this.sendChild(state, req, out[i], 0);
    }
  }
  /**
   * The shared edge-selection policy: weighted-random when the weights differ,
   * least-loaded when they are all equal. Exposed on BehaviourCtx so a
   * behaviour can reuse it instead of reimplementing the tie-break.
   */
  pickWeightedOrLeastLoaded(out) {
    const cut = this.cutEdges;
    const anyCut = cut.size > 0;
    if (out.length === 1) return anyCut && cut.has(out[0].id) ? null : out[0];
    let total = 0;
    let uniform = true;
    let firstLive = -1;
    for (let i = 0; i < out.length; i++) {
      if (anyCut && cut.has(out[i].id)) continue;
      const w = out[i].weight > 0 ? out[i].weight : 0;
      total += w;
      if (firstLive < 0) firstLive = i;
      else if (Math.abs(out[i].weight - out[firstLive].weight) > 1e-9) uniform = false;
    }
    if (firstLive < 0) return null;
    if (uniform) {
      let best = null;
      let bestLoad = Infinity;
      for (let i = 0; i < out.length; i++) {
        if (anyCut && cut.has(out[i].id)) continue;
        const target = this.nodes.get(out[i].to);
        if (!target) continue;
        const depth = target.busy + (target.waiting.length - target.waitHead);
        const cap = target.config.capacity > 0 ? target.config.capacity : 1;
        const load = depth / cap;
        if (load < bestLoad) {
          bestLoad = load;
          best = out[i];
        }
      }
      return best ?? out[firstLive];
    }
    if (total <= 0) return out[firstLive];
    let r = this.rng.next() * total;
    let last = out[firstLive];
    for (let i = 0; i < out.length; i++) {
      if (anyCut && cut.has(out[i].id)) continue;
      const w = out[i].weight > 0 ? out[i].weight : 0;
      last = out[i];
      r -= w;
      if (r <= 0) return out[i];
    }
    return last;
  }
  sendChild(parentState, parent, edge2, attempt) {
    const target = this.nodes.get(edge2.to);
    if (!target) {
      this.childResolved(parent, false, "no-route", 0);
      return;
    }
    if (this.cutEdges.size > 0 && this.cutEdges.has(edge2.id)) {
      parentState.errors.add(this.now, 1);
      parentState.totalFailed++;
      const stub = this.acquireReq();
      stub.attempt = attempt;
      stub.retryTarget = edge2.id;
      stub.resolved = true;
      this.childResolvedFrom(stub, parent, false, "partitioned", 0);
      this.recycle(stub);
      return;
    }
    const child = this.acquireReq();
    child.nodeId = target.id;
    child.parent = parent;
    child.rootStartMs = parent.rootStartMs;
    child.enterMs = this.now;
    child.hop = parent.hop + 1;
    child.attempt = attempt;
    child.viaEdge = edge2.id;
    child.retryTarget = edge2.id;
    child.detached = parent.detached;
    child.key = parent.key;
    child.isWrite = parent.isWrite;
    const counter = this.edgeFlow.get(edge2.id);
    if (counter) counter.add(this.now, 1);
    this.armTimeout(parentState, child);
    const linkMs = edge2.latencyMs;
    if (linkMs !== void 0 && linkMs > 0) {
      this.push(this.now + linkMs, EV_LINK_ARRIVE, target.id, child, child.token);
      return;
    }
    this.admit(target, child);
  }
  /* ---------------- admission ---------------- */
  /** Offer a request to a node: shed, buffer, or start service. */
  /**
   * Record this node's share of a traced request's latency.
   *
   * Called once per hop, at the moment the node's own work finishes.
   * `arriveMs` is when the request reached the node, `enterMs` when a slot
   * freed and service began, so the gap between them is the queue and the
   * gap from `enterMs` to now is the work. Splitting them here, rather than
   * reporting one "time at this node", is the entire point of the feature.
   */
  /** The request currently being traced, or null between samples. */
  tracing = null;
  /** The last completed trace, handed to every snapshot until replaced. */
  lastTrace = null;
  recordHop(req) {
    const traced = this.tracing;
    const trace = traced?.trace;
    if (!traced || !trace) return;
    let root = req;
    while (root && root.parent) root = root.parent;
    if (root !== traced) return;
    if (req.parent === null && req.hop === 0) return;
    const queuedMs = Math.max(0, req.enterMs - req.arriveMs);
    trace.push({
      nodeId: req.nodeId,
      depth: req.hop,
      queuedMs,
      // The node's OWN work, not its wall clock. A caller sits blocked while
      // its dependency runs, and charging that to the caller would make every
      // upstream node look slow when only the deepest one is: at 6x load the
      // api reads 344ms of wall clock against 22ms of actual work, and the
      // 322ms belongs to the database it was waiting on.
      serviceMs: req.ownMs
    });
  }
  admit(state, req) {
    state.arrivals.add(this.now, 1);
    req.arriveMs = this.now;
    const fault = this.faults.get(state.id);
    if (fault && fault.kind === "crash") {
      state.errors.add(this.now, 1);
      state.totalFailed++;
      this.resolve(req, false, "crashed", 0);
      return;
    }
    const b = state.behaviour;
    const action = b.onAdmit ? b.onAdmit(this, state, req) : "serve";
    if (action === "handled") return;
    if (action === "shed") {
      this.shed(state, req);
      return;
    }
    if (action === "passthru") {
      req.ownMs = 0;
      this.beginZeroService(state, req);
      return;
    }
    const capacity = this.effectiveCapacity(state);
    if (state.busy < capacity) {
      this.startService(state, req);
      return;
    }
    if (this.queueDepth(state) >= this.effectiveQueueLimit(state)) {
      this.shed(state, req);
      return;
    }
    state.waiting.push(req);
  }
  /**
   * Draw one service time for a node, with any injected 'slow' fault applied.
   *
   * Every service-time draw in the engine goes through here, so a slow fault
   * cannot be bypassed by whichever path a kind happens to take. The RNG is
   * consumed identically whether or not a fault is present -- the multiplier
   * scales the drawn value rather than changing the distribution's parameters
   * -- which keeps a faulted run's random stream aligned with a healthy one.
   */
  serviceTimeFor(state) {
    if (!(state.config.serviceMs > 0)) return 0;
    const ms = this.rng.serviceTime(state.config.serviceMs, state.config.serviceCv);
    const fault = this.faults.get(state.id);
    if (fault && fault.kind === "slow") return ms * fault.factor;
    return ms;
  }
  /** Book a shed against a node and fail the call. */
  shed(state, req) {
    state.sheds.add(this.now, 1);
    state.totalFailed++;
    this.resolve(req, false, "shed", 0);
  }
  /**
   * A buffering node acknowledges immediately: the caller's chain resolves as
   * a success right here, and the message is parked for the consumers. Called
   * by the queue behaviour, which has already checked the depth limit.
   */
  ackAndBuffer(stateLike, reqLike) {
    const state = stateLike;
    const req = reqLike;
    const ackMs = this.serviceTimeFor(state);
    const msg = this.acquireReq();
    msg.nodeId = state.id;
    msg.parent = null;
    msg.rootStartMs = this.now;
    msg.enterMs = this.now;
    msg.hop = req.hop;
    msg.detached = true;
    state.waiting.push(msg);
    state.completions.add(this.now, 1);
    state.totalCompleted++;
    state.latency.add(this.now, ackMs);
    this.resolve(req, true, "error", ackMs);
    this.wakeWorkersFor(state.id);
  }
  /**
   * Acknowledge the caller and RELAY a detached copy through this node's
   * own slot discipline.
   *
   * This is the delivery-side sibling of ackAndBuffer(): where a queue
   * parks its detached message for pull-based consumers, a relaying kind
   * (a retry queue, a write-behind cache) keeps the message and delivers
   * it downstream ITSELF -- the copy occupies this node's slots, draws the
   * node's service time, and then takes the ordinary completion path:
   * error roll, routing to the out edges, and the caller-side retry
   * machinery, so a failed delivery is re-issued with backoff by exactly
   * the code every other retry uses.
   *
   * `extraDeliveryMs` is added on top of the drawn service time of the
   * relayed copy (not of the ack), which is how a write-behind cache
   * models the interval a write sits dirty before its flush lands.
   *
   * The messages live in the engine's own waiting list, so `queueDepth`,
   * the snapshot's `queued`, and -- crucially -- killInFlight() all see
   * them: crashing the node loses the buffered messages instantly and
   * visibly, which is the write-behind lesson.
   *
   * The behaviour must check its own depth limit BEFORE calling this, the
   * same contract ackAndBuffer has.
   */
  ackAndRelay(stateLike, reqLike, extraDeliveryMs = 0) {
    const state = stateLike;
    const req = reqLike;
    const ackMs = this.serviceTimeFor(state);
    const msg = this.acquireReq();
    msg.nodeId = state.id;
    msg.parent = null;
    msg.rootStartMs = this.now;
    msg.enterMs = this.now;
    msg.hop = req.hop;
    msg.detached = true;
    msg.key = req.key;
    msg.isWrite = req.isWrite;
    if (extraDeliveryMs > 0) msg.extraServiceMs = extraDeliveryMs;
    if (state.busy < this.effectiveCapacity(state)) {
      this.startService(state, msg);
    } else {
      state.waiting.push(msg);
    }
    state.completions.add(this.now, 1);
    state.totalCompleted++;
    state.latency.add(this.now, ackMs);
    this.resolve(req, true, "error", ackMs);
  }
  wakeWorkersFor(queueId) {
    for (const state of this.nodes.values()) {
      if (!state.behaviour.pullsFromQueues) continue;
      if (!state.sources.includes(queueId)) continue;
      this.pumpWorker(state);
    }
  }
  /** lb / client style pass-through with a tiny (possibly zero) service time. */
  beginZeroService(state, req) {
    const ms = this.serviceTimeFor(state);
    req.ownMs = ms;
    if (ms > 0) {
      state.busy++;
      req.holdingSlot = true;
      this.push(this.now + ms, EV_SERVICE_DONE, state.id, req, req.token);
    } else {
      this.onServiceComplete(state, req);
    }
  }
  startService(state, req) {
    state.busy++;
    req.holdingSlot = true;
    req.enterMs = this.now;
    const extra = req.extraServiceMs;
    if (extra > 0) req.extraServiceMs = 0;
    const ms = this.serviceTimeFor(state) + extra;
    req.ownMs = ms;
    if (ms > 0) {
      this.push(this.now + ms, EV_SERVICE_DONE, state.id, req, req.token);
    } else {
      this.onServiceDone(state, req);
    }
  }
  onServiceDone(state, req) {
    this.releaseSlot(state, req);
    this.pumpQueue(state);
    const drained = req.onDrained;
    if (drained !== null) {
      req.onDrained = null;
      drained(this, state, req);
    }
    this.onServiceComplete(state, req);
  }
  releaseSlot(state, req) {
    if (!req.holdingSlot) return;
    req.holdingSlot = false;
    state.busy = Math.max(0, state.busy - 1);
  }
  /** Start serving whoever is next in line, if a slot is free. */
  pumpQueue(state) {
    const mode = state.behaviour.pump;
    if (mode === "none") return;
    if (mode === "sources") {
      this.pumpWorker(state);
      return;
    }
    const capacity = this.effectiveCapacity(state);
    while (state.busy < capacity && state.waitHead < state.waiting.length) {
      const req = state.waiting[state.waitHead];
      state.waiting[state.waitHead] = void 0;
      state.waitHead++;
      this.compactWaiting(state);
      if (req.resolved) continue;
      this.startService(state, req);
    }
  }
  /** Workers pull messages out of the queue nodes that feed them. */
  pumpWorker(state) {
    const capacity = this.effectiveCapacity(state);
    while (state.busy < capacity) {
      const msg = this.takeFromSources(state);
      if (!msg) break;
      msg.nodeId = state.id;
      msg.enterMs = this.now;
      state.arrivals.add(this.now, 1);
      this.startService(state, msg);
    }
  }
  /** Round-robin-free deterministic pull: oldest message across feeding queues. */
  takeFromSources(state) {
    let best = null;
    let bestTime = Infinity;
    for (let i = 0; i < state.sources.length; i++) {
      const q = this.nodes.get(state.sources[i]);
      if (!q || q.waitHead >= q.waiting.length) continue;
      const head = q.waiting[q.waitHead];
      if (head.enterMs < bestTime) {
        bestTime = head.enterMs;
        best = q;
      }
    }
    if (!best) return null;
    const msg = best.waiting[best.waitHead];
    best.waiting[best.waitHead] = void 0;
    best.waitHead++;
    this.compactWaiting(best);
    return msg;
  }
  compactWaiting(state) {
    if (state.waitHead > 64 && state.waitHead * 2 >= state.waiting.length) {
      state.waiting.splice(0, state.waitHead);
      state.waitHead = 0;
    }
  }
  /* ---------------- completion of a node's own work ---------------- */
  /** This node finished its own service; decide whether to call downstream. */
  onServiceComplete(state, req) {
    if (req.resolved) return;
    const fault = this.faults.get(state.id);
    if (fault && fault.kind === "crash") {
      state.errors.add(this.now, 1);
      state.totalFailed++;
      this.resolve(req, false, "crashed", req.ownMs);
      return;
    }
    if (state.config.errorRate > 0 && this.rng.next() < state.config.errorRate) {
      state.errors.add(this.now, 1);
      state.totalFailed++;
      this.resolve(req, false, "error", req.ownMs);
      return;
    }
    if (fault && fault.kind === "errors" && this.rng.next() < fault.rate) {
      state.errors.add(this.now, 1);
      state.totalFailed++;
      this.resolve(req, false, "error", req.ownMs);
      return;
    }
    const hook = state.behaviour.onServiceComplete;
    if (hook && hook(this, state, req) === "complete") {
      this.completeNode(state, req, req.ownMs);
      return;
    }
    if (state.out.length === 0) {
      this.completeNode(state, req, req.ownMs);
      return;
    }
    this.dispatchDownstream(state, req);
  }
  /** Record this node's own latency and resolve the call upward. */
  completeNode(state, req, latencyMs) {
    state.completions.add(this.now, 1);
    state.totalCompleted++;
    state.latency.add(this.now, latencyMs);
    this.resolve(req, true, "error", latencyMs);
  }
  /* ---------------- timeouts ---------------- */
  /**
   * Arm the caller's deadline on one outgoing call. `caller` supplies the
   * timeoutMs; `call` is the child request being bounded.
   */
  armTimeout(caller, call) {
    const t = caller.config.timeoutMs;
    if (!(t > 0)) return;
    this.push(this.now + t, EV_TIMEOUT, caller.id, call, call.token);
  }
  /**
   * The caller gave up on this call. The call itself is NOT cancelled: it
   * keeps its server slot and keeps running until its service time elapses.
   * Abandoned-but-still-running work is what makes a retry storm compound,
   * so it must stay on the books.
   */
  onTimeout(call) {
    if (call.resolved) return;
    const parent = call.parent;
    const callerState = parent ? this.nodes.get(parent.nodeId) : null;
    if (callerState) {
      callerState.timeouts.add(this.now, 1);
      callerState.totalFailed++;
    }
    call.parent = null;
    call.abandoned = true;
    if (parent && !parent.resolved) {
      this.childResolvedFrom(call, parent, false, "timeout", this.now - call.enterMs);
    }
  }
  /* ---------------- resolution & join ---------------- */
  /**
   * Resolve one request. `latencyMs` is the latency this call contributes to
   * its parent (own service time plus the joined subtree).
   */
  resolve(req, ok, reason, latencyMs) {
    if (req.resolved) return;
    req.resolved = true;
    this.recordHop(req);
    const parent = req.parent;
    if (parent === null) {
      if (req.detached || req.abandoned) {
        if (this.tracing === req) this.tracing = null;
        this.recycle(req);
        return;
      }
      const total = this.now - req.rootStartMs;
      if (req.trace) {
        const hops = req.trace.slice().sort((a, b) => a.depth - b.depth);
        this.lastTrace = {
          startMs: req.rootStartMs,
          totalMs: total,
          ok,
          reason: ok ? null : reason,
          hops
        };
        this.tracing = null;
      }
      const client2 = this.nodes.get(req.nodeId);
      if (ok) {
        this.sysGood.add(this.now, 1);
        this.sysLatency.add(this.now, total);
        if (client2) {
          client2.completions.add(this.now, 1);
          client2.totalCompleted++;
          client2.latency.add(this.now, total);
        }
      } else {
        this.sysFailed.add(this.now, 1);
        this.totalFailed++;
        this.failures[reason]++;
        if (client2) {
          client2.totalFailed++;
          if (reason === "timeout") client2.timeouts.add(this.now, 1);
          else if (reason === "shed") client2.sheds.add(this.now, 1);
          else client2.errors.add(this.now, 1);
        }
      }
      this.recycle(req);
      return;
    }
    this.childResolvedFrom(req, parent, ok, reason, latencyMs);
    this.recycle(req);
  }
  /** A backoff delay elapsed: re-issue the failed downstream call. */
  onRetry(state, parent) {
    if (parent.resolved) return;
    const edge2 = state.out.find((e) => e.id === parent.retryTarget);
    if (!edge2) {
      this.childResolved(parent, false, "no-route", 0);
      return;
    }
    this.sendChild(state, parent, edge2, parent.retryAttempt);
  }
  childResolvedFrom(child, parent, ok, reason, latencyMs) {
    if (parent.resolved) return;
    const caller = this.nodes.get(parent.nodeId);
    if (caller && caller.behaviour.observesOutcome) {
      caller.behaviour.onDownstreamResult(this, caller, child, ok, reason);
    }
    if (!ok) {
      const parentState = this.nodes.get(parent.nodeId);
      const retries = parentState ? Math.max(0, Math.floor(parentState.config.retries)) : 0;
      if (parentState && child.attempt < retries && reason !== "depth" && reason !== "no-route") {
        const edge2 = parentState.out.find((e) => e.id === child.retryTarget);
        if (edge2) {
          const attempt = child.attempt + 1;
          const backoff = RETRY_BASE_BACKOFF_MS * Math.pow(2, attempt - 1);
          const delay = backoff * (0.5 + this.rng.next());
          parent.retryTarget = edge2.id;
          parent.retryAttempt = attempt;
          this.push(this.now + delay, EV_RETRY, parentState.id, parent, parent.token);
          return;
        }
      }
    }
    this.childResolved(parent, ok, reason, latencyMs);
  }
  childResolved(parent, ok, reason, latencyMs) {
    if (parent.resolved) return;
    if (!ok && !parent.childFailed) {
      parent.childFailed = true;
      parent.childReason = reason;
    }
    if (latencyMs > parent.maxChildMs) parent.maxChildMs = latencyMs;
    parent.pending--;
    if (parent.pending > 0) return;
    const parentState = this.nodes.get(parent.nodeId);
    const total = parent.ownMs + parent.maxChildMs;
    if (parent.childFailed) {
      if (parentState && parentState.behaviour.creditsJoinCompletion)
        parentState.totalFailed++;
      this.resolve(parent, false, parent.childReason, total);
      return;
    }
    if (parentState && parentState.behaviour.creditsJoinCompletion) {
      parentState.completions.add(this.now, 1);
      parentState.totalCompleted++;
      parentState.latency.add(this.now, total);
    }
    this.resolve(parent, true, "error", total);
  }
  /* ---------------- BehaviourCtx surface ---------------- *
   *
   * The small, deliberate set of engine operations a behaviour may drive.
   * Everything a behaviour needs goes through here; nothing exposes the heap,
   * the request pool, or a writable clock, so a behaviour cannot desynchronise
   * the simulation.
   * ------------------------------------------------------ */
  /** One draw from the deterministic RNG. */
  roll() {
    return this.rng.next();
  }
  /** Queued (not yet in service) request count. */
  queueDepth(state) {
    const s = state;
    return s.waiting.length - s.waitHead;
  }
  effectiveQueueLimit(state) {
    return Math.max(0, Math.floor(state.config.queueLimit));
  }
  /**
   * How many requests this node can serve at once: `instances * capacity`.
   *
   * `capacity` is the slot count of ONE instance and `instances` is how many
   * of them are running, so the product is the node's real parallelism and is
   * what every slot decision in the engine compares against. A topology that
   * never mentions `instances` runs one instance, and the product collapses to
   * `capacity` -- which is exactly the quantity this function returned before
   * the instance model existed, so nothing written against the old meaning
   * changes behaviour.
   */
  /**
   * The rate this client is offering RIGHT NOW, after its traffic pattern.
   *
   * `config.rps` is the baseline the reader set and keeps its meaning; the
   * pattern scales it. Absent or `steady` returns the baseline unchanged, so
   * every design written before patterns existed behaves exactly as it did.
   *
   * A pure function of simulation time and config: nothing is carried between
   * ticks and the RNG is untouched, so the same seed and topology still
   * replay byte-identically.
   */
  effectiveRps(state) {
    const base = state.config.rps;
    const pattern = state.config.traffic;
    if (!pattern || pattern === "steady" || !(base > 0)) return base;
    const periodS = state.config.trafficPeriodS ?? DEFAULT_TRAFFIC_PERIOD_S;
    if (!(periodS > 0)) return base;
    const t = this.now / 1e3 % periodS;
    const phase = t / periodS;
    switch (pattern) {
      case "ramp":
        return this.now / 1e3 >= periodS ? base : base * phase;
      case "spike":
        return phase >= SPIKE_QUIET_FRACTION ? base * SPIKE_PEAK : base * SPIKE_TROUGH;
      case "diurnal": {
        const swing = (1 - Math.cos(2 * Math.PI * phase)) / 2;
        return base * (DIURNAL_TROUGH + (DIURNAL_PEAK - DIURNAL_TROUGH) * swing);
      }
      default:
        return base;
    }
  }
  effectiveCapacity(state) {
    return Math.max(1, Math.floor(state.config.capacity)) * this.effectiveInstances(state);
  }
  /**
   * How many instances this node is running, >= 1.
   *
   * Absent means one, which is what makes the field additive: every topology,
   * preset and saved graph written before instances existed keeps its exact
   * behaviour. Floored rather than rounded so a slider mid-drag can never
   * conjure a fractional machine.
   */
  effectiveInstances(state) {
    const raw = state.config.instances;
    return raw === void 0 ? 1 : Math.max(1, Math.floor(raw));
  }
  countHit(state) {
    state.hits.add(this.now, 1);
  }
  countMiss(state) {
    state.misses.add(this.now, 1);
  }
  fail(req, reason, latencyMs) {
    this.resolve(req, false, reason, latencyMs);
  }
  /**
   * Refuse a request at a node with an arbitrary reason. This is the general
   * form of the engine's own shed path; the two differ only in which counter
   * is credited. Booked against `errors` rather than `sheds` because a
   * throttle or an open circuit is a deliberate refusal, not queue overflow.
   */
  reject(stateLike, req, reason) {
    const state = stateLike;
    state.errors.add(this.now, 1);
    state.totalFailed++;
    this.resolve(req, false, reason, 0);
  }
  countCustom(stateLike, name, n) {
    const state = stateLike;
    let map = state.custom;
    if (!map) {
      map = /* @__PURE__ */ new Map();
      state.custom = map;
    }
    let counter = map.get(name);
    if (!counter) {
      counter = new RateCounter();
      map.set(name, counter);
    }
    counter.add(this.now, n);
  }
  counterRate(stateLike, name) {
    const counter = stateLike.custom?.get(name);
    return counter ? counter.rate(this.now) : 0;
  }
  /* ---- controller surface ---- */
  utilizationOf(nodeId) {
    const state = this.nodes.get(nodeId);
    return state ? state.utilization : null;
  }
  /**
   * How big a node's fleet is, in whatever unit that kind scales along --
   * instances for an ordinary server, slots-per-shard for a sharded store.
   *
   * Null means the kind has no fleet a controller can move. Reading back the
   * same quantity setScale() writes is what keeps a controller's arithmetic
   * consistent with what it observes.
   */
  scaleOf(nodeId) {
    const state = this.nodes.get(nodeId);
    if (!state) return null;
    const field = state.behaviour.scaleField;
    if (field === void 0) return null;
    if (field === "shardCapacity") {
      return Math.max(1, Math.floor(state.config.shardCapacity));
    }
    return this.effectiveInstances(state);
  }
  /**
   * Resize a node's fleet. Both the runtime state and the stored topology are
   * updated, so the Inspector shows what the controller actually did rather
   * than the value the student last typed. Newly freed slots are pumped
   * immediately, exactly as a manual change is.
   *
   * Writing `instances` rather than `capacity` is the substance of the
   * intuitiveness fix: the controller adds machines, the canvas draws
   * machines, and the number it moved is the number the student sees grow.
   */
  setScale(nodeId, units) {
    const next = Math.max(1, Math.floor(units));
    const state = this.nodes.get(nodeId);
    if (!state) return;
    const field = state.behaviour.scaleField;
    if (field === void 0) return;
    if (state.config[field] === next) return;
    state.config[field] = next;
    const node2 = this.topology.nodes.find((n) => n.id === nodeId);
    if (node2) node2.config[field] = next;
    this.pumpQueue(state);
  }
  /**
   * The node a controller drives, or '' when it is not wired to one.
   *
   * Read from the node's CONTROL edges, which are held separately from `out`
   * so that a request can never be dispatched down one. Taking the first is
   * deliberate: one controller drives one target, and a second control edge
   * would be an ambiguity the student should see rather than a silent merge.
   */
  controlTargetOf(stateLike) {
    const ctrl = stateLike.ctrl;
    if (ctrl.length === 0) return "";
    if (this.cutEdges.size > 0 && this.cutEdges.has(ctrl[0].id)) return "";
    return ctrl[0].to;
  }
  isCrashed(nodeId) {
    return this.faults.get(nodeId)?.kind === "crash";
  }
  /**
   * Service a request on behalf of a behaviour that runs its own slot
   * discipline. Deliberately does NOT touch state.busy: for a sharded store
   * the meaningful occupancy is per-shard, and double-counting it in the
   * node-level counter would make `utilization` wrong. Everything after the
   * service time -- error roll, onServiceComplete, routing, completion -- is
   * the same path every other kind takes.
   */
  serveWithin(stateLike, reqLike, onDrained) {
    const state = stateLike;
    const req = reqLike;
    req.enterMs = this.now;
    req.onDrained = onDrained;
    const ms = this.rng.serviceTime(state.config.serviceMs, state.config.serviceCv) + req.extraServiceMs;
    req.extraServiceMs = 0;
    req.ownMs = ms;
    if (ms > 0) {
      this.push(this.now + ms, EV_SERVICE_DONE, state.id, req, req.token);
    } else {
      this.onServiceDone(state, req);
    }
  }
  addServiceDelay(reqLike, extraMs) {
    if (!(extraMs > 0)) return;
    reqLike.extraServiceMs += extraMs;
  }
  markWrite(reqLike, isWrite) {
    reqLike.isWrite = isWrite;
  }
  /**
   * Create a DETACHED request at `state` and dispatch it down one specific
   * outgoing edge, for kinds that originate traffic of their own on an
   * event-driven schedule: a stream broker delivering the next message to a
   * consumer group, a pub/sub topic fanning one publish out to each
   * subscriber, a cron job dumping its batch.
   *
   * The message is a detached root -- nobody upstream is waiting on it, so
   * its eventual success or failure is booked at the nodes it visits and
   * never at the system level, exactly like a queue message drained by a
   * worker. The join still runs through this node, which means a behaviour
   * declaring `observesOutcome` hears about each delivery's result in
   * onDownstreamResult; that is how a broker paces a consumer group.
   *
   * Returns false without emitting when the edge is cut, the target does
   * not exist, or the live-request ceiling is reached -- the caller decides
   * what an undeliverable message means (a broker holds it; a cron drops
   * it). Consumes no randomness itself; the service-time draws happen at
   * the nodes the message visits, in event order, like any other request.
   *
   * This is a ctx addition, not an event-loop change: dispatch, admission
   * and resolution all run the same code every other request runs.
   */
  emitDetached(stateLike, edge2, key) {
    const state = stateLike;
    if (this.liveRequests >= MAX_LIVE_REQUESTS) return false;
    if (this.cutEdges.size > 0 && this.cutEdges.has(edge2.id)) return false;
    if (!this.nodes.has(edge2.to)) return false;
    const msg = this.acquireReq();
    msg.nodeId = state.id;
    msg.parent = null;
    msg.rootStartMs = this.now;
    msg.enterMs = this.now;
    msg.hop = 0;
    msg.detached = true;
    msg.key = (Math.floor(key) % KEYSPACE3 + KEYSPACE3) % KEYSPACE3;
    msg.pending = 1;
    this.sendChild(state, msg, edge2, 0);
    return true;
  }
  /**
   * Store a per-shard utilisation vector for the snapshot. Copied rather than
   * retained, so a behaviour reusing its own scratch array cannot mutate what
   * the UI is about to read.
   */
  reportShardUtilization(stateLike, perShard) {
    const dst = stateLike.shardUtil;
    dst.length = perShard.length;
    for (let i = 0; i < perShard.length; i++) dst[i] = perShard[i];
  }
  /**
   * Publish a kind's own instance vector. Writes into the node's working
   * buffer; the copy-on-change publish happens once per snapshot in
   * finishInstances(), so a behaviour cannot force an allocation by calling
   * this more often than another kind does.
   */
  reportInstances(stateLike, perUnit, pending) {
    const state = stateLike;
    const units = state.instanceUnits;
    units.length = perUnit.length;
    for (let i = 0; i < perUnit.length; i++) units[i] = perUnit[i];
    state.instancePending = pending > 0 ? Math.floor(pending) : 0;
  }
  /**
   * Fill the working instance buffer for an `instanceModel: 'slots'` kind.
   *
   * One unit per INSTANCE -- one machine, holding `capacity` slots -- and the
   * per-unit numbers are a WATERLINE: `utilization * instances`
   * busy-instance-equivalents poured in from index 0. The engine integrates
   * one smoothed utilisation per node, not one per instance, and requests go
   * to whichever slot is free, so there is no per-machine truth to report and
   * inventing one would be a prettier lie than the waterline.
   *
   * The count is read through effectiveInstances(), so a stack drawn from this
   * is exactly the fleet the autoscaler most recently wrote.
   */
  fillSlotInstances(state) {
    const instances = this.effectiveInstances(state);
    const units = state.instanceUnits;
    units.length = instances;
    const util = state.utilization > 1 ? 1 : state.utilization > 0 ? state.utilization : 0;
    let remaining = util * instances;
    for (let i = 0; i < instances; i++) {
      if (remaining >= 1) {
        units[i] = 1;
        remaining -= 1;
      } else {
        units[i] = remaining > 0 ? remaining : 0;
        remaining = 0;
      }
    }
  }
  /**
   * Copy the working instance buffer into the snapshot, allocating a new array
   * only when something actually changed.
   *
   * The reference identity of the published array is the signal a memoised
   * consumer keys off: unchanged contents keep the previous array, so a
   * shallow compare correctly says "nothing to redraw", and any change at all
   * -- a single unit's utilisation, the unit count, the pending count --
   * yields a brand new array that no such compare can mistake for the old one.
   */
  finishInstances(state, entry) {
    const units = state.instanceUnits;
    const prev = state.instancePublished;
    let changed = prev === null || prev.length !== units.length;
    if (!changed && prev !== null) {
      for (let i = 0; i < units.length; i++) {
        if (prev[i] !== units[i]) {
          changed = true;
          break;
        }
      }
    }
    if (state.instancePending !== state.instancePendingPublished) changed = true;
    if (changed) {
      state.instancePublished = units.slice();
      state.instancePendingPublished = state.instancePending;
    }
    entry.instances = units.length;
    entry.perInstance = state.instancePublished ?? units;
    if (state.instancePending > 0) entry.instancesPending = state.instancePending;
  }
  /**
   * Record a self-managing kind's true occupancy, for the utilisation
   * integration to use in place of state.busy. Stored rather than applied
   * immediately so smoothing stays on the engine's clock and a behaviour
   * cannot make its meter jump by reporting more often than another kind.
   */
  reportOccupancy(stateLike, busySlots, capacity) {
    const state = stateLike;
    const busy = busySlots > 0 ? busySlots : 0;
    const cap = capacity > 0 ? capacity : 1;
    const slot = state.ownOccupancy;
    if (slot === null) {
      state.ownOccupancy = { busy, capacity: cap };
      return;
    }
    slot.busy = busy;
    slot.capacity = cap;
  }
  isEdgeCut(edgeId) {
    return this.cutEdges.has(edgeId);
  }
  /* ---------------- utilization integration ---------------- */
  integrateAll() {
    for (const state of this.nodes.values()) {
      const dt = this.now - state.lastIntegrateMs;
      state.lastIntegrateMs = this.now;
      if (dt <= 0) continue;
      const own = state.ownOccupancy;
      const capacity = own !== null ? own.capacity : this.effectiveCapacity(state);
      const busy = own !== null ? own.busy : state.busy;
      const instant = state.behaviour.servesRequests ? Math.min(capacity > 0 ? busy / capacity : 0, 1) : 0;
      const alpha = 1 - Math.exp(-dt / 500);
      state.utilization += (instant - state.utilization) * alpha;
      state.busyMsAccum += busy * dt;
    }
  }
  /**
   * Autonomous per-advance work for kinds that act without a request arriving.
   * No current kind declares onTick, so tickNodes is empty and this is never
   * reached; it exists so an autoscaler or circuit breaker can be added as a
   * pure registry entry.
   */
  runTicks(dtMs) {
    for (let i = 0; i < this.tickNodes.length; i++) {
      const state = this.tickNodes[i];
      state.behaviour.onTick(this, state, dtMs);
    }
  }
  /* ---------------- history ---------------- */
  maybeRecordHistory() {
    while (this.now - this.lastHistoryMs >= HISTORY_INTERVAL_MS) {
      this.lastHistoryMs += HISTORY_INTERVAL_MS;
      const t = this.lastHistoryMs;
      this.sysLatency.percentiles(this.now, this.pctScratch);
      const good = this.sysGood.rate(this.now);
      const failed = this.sysFailed.rate(this.now);
      const done = good + failed;
      this.history.push({
        t,
        p50: this.pctScratch[0],
        p95: this.pctScratch[1],
        p99: this.pctScratch[2],
        goodput: good,
        offered: this.sysOffered.rate(this.now),
        errorRate: done > 0 ? failed / done : 0
      });
      if (this.history.length > HISTORY_MAX) {
        this.history.splice(0, this.history.length - HISTORY_MAX);
      }
    }
  }
  /* ---------------- pools ---------------- */
  push(time, kind, nodeId, req, token) {
    const ev = this.freeEv.pop();
    if (ev) {
      ev.time = time;
      ev.seq = this.seq++;
      ev.kind = kind;
      ev.nodeId = nodeId;
      ev.req = req;
      ev.token = token;
      this.heap.push(ev);
      return;
    }
    this.heap.push({ time, seq: this.seq++, kind, nodeId, req, token });
  }
  releaseEv(ev) {
    ev.req = null;
    if (this.freeEv.length < 4096) this.freeEv.push(ev);
  }
  acquireReq() {
    this.liveRequests++;
    const pooled = this.freeReq;
    if (pooled) {
      this.freeReq = pooled.next;
      pooled.next = null;
      pooled.token = pooled.token + 1 | 0;
      pooled.nodeId = "";
      pooled.parent = null;
      pooled.pending = 0;
      pooled.maxChildMs = 0;
      pooled.childFailed = false;
      pooled.childReason = "error";
      pooled.enterMs = 0;
      pooled.arriveMs = 0;
      pooled.trace = null;
      pooled.rootStartMs = 0;
      pooled.hop = 0;
      pooled.attempt = 0;
      pooled.abandoned = false;
      pooled.holdingSlot = false;
      pooled.viaEdge = "";
      pooled.detached = false;
      pooled.retryTarget = "";
      pooled.retryAttempt = 0;
      pooled.ownMs = 0;
      pooled.resolved = false;
      pooled.key = 0;
      pooled.isWrite = false;
      pooled.extraServiceMs = 0;
      pooled.onDrained = null;
      return pooled;
    }
    return {
      token: 1,
      nodeId: "",
      parent: null,
      pending: 0,
      maxChildMs: 0,
      childFailed: false,
      childReason: "error",
      enterMs: 0,
      arriveMs: 0,
      trace: null,
      rootStartMs: 0,
      hop: 0,
      attempt: 0,
      abandoned: false,
      holdingSlot: false,
      viaEdge: "",
      detached: false,
      retryTarget: "",
      retryAttempt: 0,
      ownMs: 0,
      next: null,
      resolved: false,
      key: 0,
      isWrite: false,
      extraServiceMs: 0,
      onDrained: null
    };
  }
  recycle(req) {
    this.liveRequests--;
    req.token = req.token + 1 | 0;
    req.parent = null;
    req.next = this.freeReq;
    this.freeReq = req;
  }
};
function createNodeState(node2) {
  return {
    id: node2.id,
    kind: node2.kind,
    behaviour: behaviourFor(node2.kind),
    config: { ...node2.config },
    busy: 0,
    waiting: [],
    waitHead: 0,
    out: [],
    ctrl: [],
    sources: [],
    busyMsAccum: 0,
    lastIntegrateMs: 0,
    utilization: 0,
    arrivals: new RateCounter(),
    completions: new RateCounter(),
    errors: new RateCounter(),
    sheds: new RateCounter(),
    timeouts: new RateCounter(),
    hits: new RateCounter(),
    misses: new RateCounter(),
    latency: new LatencyRing(),
    totalCompleted: 0,
    totalFailed: 0,
    pollScheduled: false,
    shardUtil: [],
    instanceUnits: [],
    instancePublished: null,
    instancePending: 0,
    instancePendingPublished: -1,
    ext: null,
    custom: null,
    ownOccupancy: null
  };
}
function describeFault(nodeId, f) {
  const out = { nodeId, kind: f.kind, sinceMs: f.sinceMs };
  if (f.kind === "slow") out.factor = f.factor;
  else if (f.kind === "errors") out.rate = f.rate;
  else if (f.kind === "partition") out.edgeIds = f.edgeIds.slice();
  return out;
}
function clamp014(v) {
  return v < 0 ? 0 : v > 1 ? 1 : v;
}
function cloneTopology(t) {
  return {
    nodes: t.nodes.map((n) => ({ ...n, config: { ...n.config } })),
    edges: t.edges.map((e) => ({ ...e }))
  };
}

// engine/sim/presets.ts
var EXTRA_DEFAULTS = {
  // Every scalable kind starts as a single machine. Present explicitly rather
  // than left undefined so the Inspector has something to show and a student
  // can see that "1" is a choice, not an absence -- the engine treats the two
  // identically.
  instances: 1,
  // replica: a 3-node read replica set, 50ms behind, mostly-read traffic.
  replicaCount: 3,
  replicationLagMs: 50,
  readFraction: 0.9,
  // shard: 4 partitions, keys spread evenly (no hot key) until you make one.
  shardCount: 4,
  shardCapacity: 4,
  hotKeyFraction: 0
};
function defaultConfig(kind) {
  return { ...EXTRA_DEFAULTS, ...baseConfig(kind) };
}
function baseConfig(kind) {
  switch (kind) {
    case "client":
      return {
        capacity: 1,
        serviceMs: 0,
        serviceCv: 0,
        queueLimit: 0,
        hitRate: 0,
        errorRate: 0,
        timeoutMs: 1e3,
        retries: 0,
        rps: 50
      };
    case "lb":
      return {
        capacity: 256,
        serviceMs: 0.5,
        serviceCv: 0.2,
        queueLimit: 1024,
        hitRate: 0,
        errorRate: 0,
        timeoutMs: 0,
        retries: 0,
        rps: 0
      };
    case "service":
      return {
        capacity: 8,
        serviceMs: 25,
        serviceCv: 0.6,
        queueLimit: 64,
        hitRate: 0,
        errorRate: 0,
        timeoutMs: 0,
        retries: 0,
        rps: 0
      };
    case "cache":
      return {
        capacity: 32,
        serviceMs: 3,
        serviceCv: 0.4,
        queueLimit: 256,
        hitRate: 0.8,
        errorRate: 0,
        timeoutMs: 0,
        retries: 0,
        rps: 0
      };
    case "db":
      return {
        capacity: 6,
        serviceMs: 30,
        serviceCv: 0.7,
        queueLimit: 32,
        hitRate: 0,
        errorRate: 0,
        timeoutMs: 0,
        retries: 0,
        rps: 0,
        lockMs: 15
      };
    case "queue":
      return {
        capacity: 1,
        serviceMs: 1,
        serviceCv: 0.2,
        queueLimit: 5e3,
        hitRate: 0,
        errorRate: 0,
        timeoutMs: 0,
        retries: 0,
        rps: 0
      };
    case "worker":
      return {
        capacity: 4,
        serviceMs: 25,
        serviceCv: 0.6,
        queueLimit: 0,
        hitRate: 0,
        errorRate: 0,
        timeoutMs: 0,
        retries: 0,
        rps: 0
      };
    case "replica":
      return {
        capacity: 4,
        serviceMs: 20,
        serviceCv: 0.6,
        queueLimit: 64,
        hitRate: 0,
        errorRate: 0,
        timeoutMs: 0,
        retries: 0,
        rps: 0
      };
    case "shard":
      return {
        capacity: 4,
        serviceMs: 25,
        serviceCv: 0.6,
        queueLimit: 32,
        hitRate: 0,
        errorRate: 0,
        timeoutMs: 0,
        retries: 0,
        rps: 0
      };
    case "cdn":
      return {
        capacity: 256,
        serviceMs: 2,
        serviceCv: 0.3,
        queueLimit: 2048,
        hitRate: 0.92,
        errorRate: 0,
        timeoutMs: 0,
        retries: 0,
        rps: 0
      };
    case "ratelimiter":
      return {
        capacity: 1,
        serviceMs: 0,
        serviceCv: 0,
        queueLimit: 0,
        hitRate: 0,
        errorRate: 0,
        timeoutMs: 0,
        retries: 0,
        rps: 0,
        rateLimitRps: 100,
        burst: 100
      };
    case "breaker":
      return {
        capacity: 1,
        serviceMs: 0,
        serviceCv: 0,
        queueLimit: 0,
        hitRate: 0,
        errorRate: 0,
        timeoutMs: 0,
        retries: 0,
        rps: 0,
        errorThreshold: 0.5,
        windowMs: 5e3,
        openMs: 3e3,
        halfOpenProbes: 3
      };
    case "autoscaler":
      return {
        capacity: 1,
        serviceMs: 0,
        serviceCv: 0,
        queueLimit: 0,
        hitRate: 0,
        errorRate: 0,
        timeoutMs: 0,
        retries: 0,
        rps: 0,
        targetUtil: 0.7,
        // In INSTANCES, not slots: between 1 and 12 machines.
        minCapacity: 1,
        maxCapacity: 12,
        cooldownMs: 3e3,
        scaleStepPct: 0.5,
        warmupMs: 4e3
      };
    case "region":
      return {
        capacity: 1,
        serviceMs: 0,
        serviceCv: 0,
        queueLimit: 0,
        hitRate: 0,
        errorRate: 0,
        timeoutMs: 0,
        retries: 0,
        rps: 0,
        regions: 2,
        activeRegion: 0,
        failoverMs: 5e3
      };
    case "objectstore":
      return {
        capacity: 64,
        serviceMs: 90,
        serviceCv: 0.4,
        queueLimit: 1024,
        hitRate: 0,
        errorRate: 0,
        timeoutMs: 0,
        retries: 0,
        rps: 0,
        prefixRps: 150
      };
    case "searchindex":
      return {
        capacity: 12,
        serviceMs: 8,
        serviceCv: 0.5,
        queueLimit: 128,
        hitRate: 0,
        errorRate: 0,
        timeoutMs: 0,
        retries: 0,
        rps: 0,
        indexMs: 60,
        indexLagMs: 1500
      };
    case "timeseriesdb":
      return {
        capacity: 16,
        serviceMs: 1.5,
        serviceCv: 0.4,
        queueLimit: 1024,
        hitRate: 0,
        errorRate: 0,
        timeoutMs: 0,
        retries: 0,
        rps: 0,
        rangeQueryFraction: 0.05,
        rangeQueryMs: 120
      };
    case "graphdb":
      return {
        capacity: 8,
        serviceMs: 6,
        serviceCv: 0.5,
        queueLimit: 64,
        hitRate: 0,
        errorRate: 0,
        timeoutMs: 0,
        retries: 0,
        rps: 0,
        traversalDepth: 2
      };
    case "coldstorage":
      return {
        capacity: 24,
        serviceMs: 2800,
        serviceCv: 0.3,
        queueLimit: 64,
        hitRate: 0,
        errorRate: 0,
        timeoutMs: 0,
        retries: 0,
        rps: 0
      };
    case "vectordb":
      return {
        capacity: 16,
        serviceMs: 0.5,
        serviceCv: 0.4,
        queueLimit: 128,
        hitRate: 0,
        errorRate: 0,
        timeoutMs: 0,
        retries: 0,
        rps: 0,
        indexSizeK: 1e3,
        recallTarget: 0.9
      };
    case "streambroker":
      return {
        capacity: 1,
        serviceMs: 1,
        serviceCv: 0.2,
        queueLimit: 2e3,
        hitRate: 0,
        errorRate: 0,
        timeoutMs: 0,
        retries: 0,
        rps: 0,
        partitions: 4
      };
    case "pubsub":
      return {
        capacity: 1,
        serviceMs: 0.5,
        serviceCv: 0.2,
        queueLimit: 0,
        hitRate: 0,
        errorRate: 0,
        timeoutMs: 0,
        retries: 0,
        rps: 0
      };
    case "websocket":
      return {
        capacity: 400,
        serviceMs: 5,
        serviceCv: 0.4,
        queueLimit: 0,
        hitRate: 0,
        errorRate: 0,
        timeoutMs: 0,
        retries: 0,
        rps: 0,
        connectionMs: 3e4
      };
    case "apigateway":
      return {
        capacity: 64,
        serviceMs: 2,
        serviceCv: 0.3,
        queueLimit: 256,
        hitRate: 0,
        errorRate: 0,
        timeoutMs: 0,
        retries: 0,
        rps: 0,
        rateLimitRps: 300,
        burst: 300,
        authFailRate: 0.01
      };
    case "sidecar":
      return {
        capacity: 32,
        serviceMs: 2,
        serviceCv: 0.2,
        queueLimit: 64,
        hitRate: 0,
        errorRate: 0,
        timeoutMs: 500,
        retries: 2,
        rps: 0,
        outlierAfter: 5,
        openMs: 3e3
      };
    case "lambda":
      return {
        capacity: 1,
        serviceMs: 25,
        serviceCv: 0.5,
        queueLimit: 0,
        hitRate: 0,
        errorRate: 0,
        timeoutMs: 0,
        retries: 0,
        rps: 0,
        coldStartMs: 350,
        keepWarmMs: 12e3,
        maxConcurrency: 40
      };
    case "cron":
      return {
        capacity: 1,
        serviceMs: 0,
        serviceCv: 0,
        queueLimit: 0,
        hitRate: 0,
        errorRate: 0,
        timeoutMs: 0,
        retries: 0,
        rps: 0,
        intervalMs: 2e4,
        batchSize: 50
      };
    case "bulkhead":
      return {
        capacity: 1,
        serviceMs: 0,
        serviceCv: 0,
        queueLimit: 0,
        hitRate: 0,
        errorRate: 0,
        timeoutMs: 0,
        retries: 0,
        rps: 0,
        bulkheadMax: 8
      };
    case "retryqueue":
      return {
        capacity: 8,
        serviceMs: 3,
        serviceCv: 0.3,
        queueLimit: 2e3,
        hitRate: 0,
        errorRate: 0,
        timeoutMs: 1e3,
        retries: 2,
        rps: 0
      };
    case "transcoder":
      return {
        capacity: 2,
        serviceMs: 1200,
        serviceCv: 0.4,
        queueLimit: 8,
        hitRate: 0,
        errorRate: 0,
        timeoutMs: 0,
        retries: 0,
        rps: 0,
        renditions: 3
      };
    case "edgecompute":
      return {
        capacity: 64,
        serviceMs: 1,
        serviceCv: 0.3,
        queueLimit: 512,
        hitRate: 0,
        errorRate: 0,
        timeoutMs: 0,
        retries: 0,
        rps: 0,
        edgeShare: 0.3,
        cpuMsCap: 2
      };
    case "writebehind":
      return {
        capacity: 256,
        serviceMs: 1,
        serviceCv: 0.3,
        queueLimit: 512,
        hitRate: 0,
        errorRate: 0,
        timeoutMs: 0,
        retries: 0,
        rps: 0,
        flushDelayMs: 200
      };
    case "loadshedder":
      return {
        capacity: 1,
        serviceMs: 0,
        serviceCv: 0,
        queueLimit: 0,
        hitRate: 0,
        errorRate: 0,
        timeoutMs: 0,
        retries: 0,
        rps: 0,
        rateLimitRps: 300,
        burst: 300,
        lowPriorityShare: 0.3,
        priorityReserve: 0.3
      };
    default:
      return {
        capacity: 1,
        serviceMs: 10,
        serviceCv: 0.5,
        queueLimit: 32,
        hitRate: 0,
        errorRate: 0,
        timeoutMs: 0,
        retries: 0,
        rps: 0
      };
  }
}
var DEFAULT_LABEL = {
  client: "Client",
  lb: "Load Balancer",
  service: "Service",
  cache: "Cache",
  db: "Database",
  queue: "Queue",
  worker: "Worker",
  replica: "Read Replicas",
  shard: "Sharded Store",
  cdn: "CDN",
  ratelimiter: "Rate Limiter",
  breaker: "Circuit Breaker",
  autoscaler: "Autoscaler",
  region: "Region",
  objectstore: "Object Storage",
  searchindex: "Search Index",
  timeseriesdb: "Time-Series DB",
  graphdb: "Graph Database",
  coldstorage: "Cold Storage",
  vectordb: "Vector Database",
  streambroker: "Stream Broker",
  pubsub: "Pub/Sub Topic",
  websocket: "WebSocket Gateway",
  apigateway: "API Gateway",
  sidecar: "Sidecar Proxy",
  lambda: "Lambda",
  cron: "Cron Job",
  bulkhead: "Bulkhead",
  retryqueue: "Retry Queue",
  transcoder: "Transcoder",
  edgecompute: "Edge Compute",
  writebehind: "Write-Behind Cache",
  loadshedder: "Load Shedder"
};
var nodeCounter = 0;
function makeNode(kind, x, y, label) {
  nodeCounter += 1;
  return {
    id: `${kind}-${nodeCounter}`,
    kind,
    label: label ?? DEFAULT_LABEL[kind],
    x,
    y,
    config: defaultConfig(kind)
  };
}
function node(id, kind, label, x, y, overrides = {}) {
  return {
    id,
    kind,
    label,
    x,
    y,
    config: { ...defaultConfig(kind), ...overrides }
  };
}
function edge(from, to, weight = 1) {
  return { id: `${from}->${to}`, from, to, weight };
}
function control(from, to) {
  return { id: `${from}->${to}`, from, to, weight: 1, control: true };
}
var singleServer = {
  nodes: [
    node("client", "client", "Client", 40, 200, { rps: 50, timeoutMs: 2e3 }),
    node("api", "service", "API Server", 340, 200, {
      capacity: 8,
      serviceMs: 25,
      serviceCv: 0.6,
      queueLimit: 64
    }),
    node("db", "db", "Database", 660, 200, {
      capacity: 6,
      serviceMs: 30,
      serviceCv: 0.7,
      queueLimit: 48
    })
  ],
  edges: [edge("client", "api"), edge("api", "db")],
  annotations: [
    note(
      "ss-note-db",
      40,
      320,
      "One server, one database. The database is the smaller of the two: 6 requests at a time at 30ms each, so it runs out near 200 a second. Drag the load past 4x and the wait builds there first, not at the API.",
      340
    )
  ]
};
var loadBalanced = {
  nodes: [
    node("client", "client", "Client", 40, 220, { rps: 140, timeoutMs: 2e3 }),
    node("lb", "lb", "Load Balancer", 260, 220, { capacity: 512, serviceMs: 0.5 }),
    node("api1", "service", "API 1", 500, 80, {
      capacity: 6,
      serviceMs: 25,
      serviceCv: 0.6,
      queueLimit: 48
    }),
    node("api2", "service", "API 2", 500, 220, {
      capacity: 6,
      serviceMs: 25,
      serviceCv: 0.6,
      queueLimit: 48
    }),
    node("api3", "service", "API 3", 500, 360, {
      capacity: 6,
      serviceMs: 25,
      serviceCv: 0.6,
      queueLimit: 48
    }),
    node("db", "db", "Database", 800, 220, {
      capacity: 12,
      serviceMs: 25,
      serviceCv: 0.7,
      queueLimit: 96
    })
  ],
  edges: [
    edge("client", "lb"),
    edge("lb", "api1"),
    edge("lb", "api2"),
    edge("lb", "api3"),
    edge("api1", "db"),
    edge("api2", "db"),
    edge("api3", "db")
  ],
  annotations: [
    note(
      "lb-note-db",
      40,
      480,
      "Three servers share one database. That triples the API capacity and does nothing for the database, which still tops out near 480 a second. Adding servers only helps when the servers were the problem.",
      340
    )
  ]
};
var cacheAside = {
  nodes: [
    node("client", "client", "Client", 40, 200, { rps: 200, timeoutMs: 2e3 }),
    node("api", "service", "API Server", 280, 200, {
      capacity: 24,
      serviceMs: 8,
      serviceCv: 0.5,
      queueLimit: 128
    }),
    node("cache", "cache", "Cache", 550, 200, {
      capacity: 64,
      serviceMs: 2,
      serviceCv: 0.4,
      hitRate: 0.85,
      queueLimit: 512
    }),
    node("db", "db", "Database", 820, 200, {
      capacity: 4,
      serviceMs: 30,
      serviceCv: 0.7,
      queueLimit: 32
    })
  ],
  edges: [edge("client", "api"), edge("api", "cache"), edge("cache", "db")],
  annotations: [
    note(
      "ca-note-miss",
      40,
      320,
      "The database only ever sees the misses. At an 85 percent hit rate it takes about 30 requests a second out of 200. Drag the hit rate down to 0.3 and it is instantly over its 133 a second ceiling, with no change in load at all.",
      340
    )
  ]
};
var asyncWorkers = {
  nodes: [
    node("client", "client", "Client", 40, 200, { rps: 120, timeoutMs: 2e3 }),
    node("api", "service", "API Server", 260, 200, {
      capacity: 24,
      serviceMs: 8,
      serviceCv: 0.5,
      queueLimit: 128
    }),
    node("queue", "queue", "Message Queue", 500, 200, {
      serviceMs: 1,
      serviceCv: 0.2,
      queueLimit: 5e3
    }),
    node("worker", "worker", "Workers", 730, 200, {
      capacity: 6,
      serviceMs: 30,
      serviceCv: 0.6
    }),
    node("db", "db", "Database", 860, 380, {
      capacity: 8,
      serviceMs: 20,
      serviceCv: 0.7,
      queueLimit: 64
    })
  ],
  edges: [
    edge("client", "api"),
    edge("api", "queue"),
    edge("queue", "worker"),
    edge("worker", "db")
  ],
  annotations: [
    note(
      "aw-note-queue",
      40,
      520,
      "The workers drain 200 a second. Push the load past that and the client still gets an instant yes, because the queue is absorbing the difference. Nothing looks wrong until the queue fills, so the graph to watch is the queue depth, not the error rate.",
      340
    )
  ]
};
var retryStorm = {
  nodes: [
    node("client", "client", "Client", 40, 200, { rps: 45, timeoutMs: 3e3 }),
    node("api", "service", "API Server", 340, 200, {
      capacity: 32,
      serviceMs: 5,
      serviceCv: 0.4,
      queueLimit: 256,
      timeoutMs: 250,
      retries: 2
    }),
    node("db", "db", "Database", 680, 200, {
      capacity: 4,
      serviceMs: 40,
      serviceCv: 0.5,
      queueLimit: 64
    })
  ],
  edges: [edge("client", "api"), edge("api", "db")],
  annotations: [
    note(
      "rt-note-storm",
      40,
      320,
      "The database handles 100 a second. Once waiting passes the 250ms timeout on the API, every request starts retrying three times, so the load triples onto a database that was already full. It is steady at 80 a second. Push to 85 and it starts losing most of its traffic; by 90 almost none gets through.",
      340
    )
  ]
};
var COL0 = 40;
var COL_PITCH = 260;
var ROW0 = 60;
var ROW_PITCH = 130;
var COL = (i) => COL0 + i * COL_PITCH;
var ROW = (j) => ROW0 + j * ROW_PITCH;
var SEC_PAD_X = 28;
var SEC_PAD_T = 16;
var SEC_PAD_B = 16;
var LANE_GAP = 64;
var LROW = (j, lane = 0) => ROW(j) + lane * LANE_GAP;
function sectionOver(id, label, tone, c0, c1, r0, r1, lane = 0) {
  const x = COL(c0) - SEC_PAD_X;
  const y = LROW(r0, lane) - SEC_PAD_T;
  return {
    id,
    kind: "section",
    label,
    x,
    y,
    width: COL(c1) + NODE_W + SEC_PAD_X - x,
    height: LROW(r1, lane) + NODE_H + SEC_PAD_B - y,
    tone
  };
}
function note(id, x, y, text, width = 220, size = "md", font = "hand") {
  return { id, kind: "note", text, x, y, width, size, font };
}
var NODE_W = 184;
var NODE_H = 88;
var cdnOrigin = {
  nodes: [
    node("client", "client", "Client", COL(0), ROW(1), { rps: 400, timeoutMs: 2e3 }),
    node("cdn", "cdn", "CDN Edge", COL(1), ROW(1), {
      capacity: 256,
      serviceMs: 2,
      serviceCv: 0.3,
      hitRate: 0.9,
      queueLimit: 2048
    }),
    node("origin", "service", "Origin Server", COL(2), ROW(1), {
      capacity: 3,
      serviceMs: 25,
      serviceCv: 0.6,
      queueLimit: 64
    }),
    node("db", "db", "Database", COL(3), ROW(1), {
      capacity: 8,
      serviceMs: 10,
      serviceCv: 0.6,
      queueLimit: 64
    })
  ],
  edges: [edge("client", "cdn"), edge("cdn", "origin"), edge("origin", "db")],
  annotations: [
    note(
      "co-note-miss",
      40,
      320,
      "The origin only ever sees what the cache misses. It handles 120 a second, and the cache is currently hiding 90 percent of the traffic from it. Drag the hit rate down and the origin gets a load nobody ever sized it for.",
      340
    )
  ]
};
var rateLimitedApi = {
  nodes: [
    node("client", "client", "Client", COL(0), ROW(1), { rps: 150, timeoutMs: 2e3 }),
    node("limiter", "ratelimiter", "Rate Limiter", COL(1), ROW(1), {
      rateLimitRps: 200,
      burst: 200
    }),
    node("api", "service", "API Server", COL(2), ROW(1), {
      capacity: 6,
      serviceMs: 25,
      serviceCv: 0.5,
      queueLimit: 48
    }),
    node("db", "db", "Database", COL(3), ROW(1), {
      capacity: 12,
      serviceMs: 12,
      serviceCv: 0.6,
      queueLimit: 96
    })
  ],
  edges: [edge("client", "limiter"), edge("limiter", "api"), edge("api", "db")],
  annotations: [
    note(
      "rl-note-trade",
      40,
      320,
      "The limiter does not add capacity. It refuses some requests quickly so the rest are answered quickly: at 600 offered it serves 200 at 36ms, where removing it serves 234 at 235ms. Saying no fast is what buys the predictable wait.",
      340
    )
  ]
};
var circuitBreaker = {
  nodes: [
    node("client", "client", "Client", COL(0), ROW(1), { rps: 100, timeoutMs: 2e3 }),
    node("api", "service", "API Server", COL(1), ROW(1), {
      capacity: 24,
      serviceMs: 6,
      serviceCv: 0.4,
      queueLimit: 128,
      timeoutMs: 600
    }),
    node("breaker", "breaker", "Circuit Breaker", COL(2), ROW(1), {
      errorThreshold: 0.5,
      windowMs: 4e3,
      openMs: 3e3,
      halfOpenProbes: 3
    }),
    node("payments", "service", "Payments API", COL(3), ROW(1), {
      capacity: 4,
      serviceMs: 30,
      serviceCv: 0.6,
      queueLimit: 32
    })
  ],
  edges: [edge("client", "api"), edge("api", "breaker"), edge("breaker", "payments")],
  annotations: [
    note(
      "cb-note-trip",
      40,
      320,
      "Right click the payments API and inject a crash. Once half the calls in a four second window fail, the breaker opens and the rest fail in microseconds instead of waiting out a timeout. After three seconds it closes and probes again.",
      340
    )
  ]
};
var readReplicas = {
  nodes: [
    node("client", "client", "Client", COL(0), ROW(1), { rps: 300, timeoutMs: 2e3 }),
    node("api", "service", "API Server", COL(1), ROW(1), {
      capacity: 32,
      serviceMs: 5,
      serviceCv: 0.4,
      queueLimit: 256
    }),
    node("replicas", "replica", "Replica Set", COL(2), ROW(1), {
      capacity: 4,
      serviceMs: 20,
      serviceCv: 0.5,
      queueLimit: 128,
      replicaCount: 3,
      replicationLagMs: 60,
      readFraction: 0.85
    })
  ],
  edges: [edge("client", "api"), edge("api", "replicas")],
  annotations: [
    note(
      "rr-note-writes",
      40,
      320,
      "Copies give you read capacity and nothing else: 600 reads a second across three of them, but still only 200 writes against the one primary. Adding copies fixes one of those and does nothing at all for the other.",
      340
    )
  ]
};
var shardedDatabase = {
  nodes: [
    node("client", "client", "Client", COL(0), ROW(1), { rps: 400, timeoutMs: 2e3 }),
    node("api", "service", "API Server", COL(1), ROW(1), {
      capacity: 32,
      serviceMs: 5,
      serviceCv: 0.4,
      queueLimit: 256
    }),
    node("shards", "shard", "Sharded Store", COL(2), ROW(1), {
      serviceMs: 25,
      serviceCv: 0.6,
      queueLimit: 32,
      shardCount: 4,
      shardCapacity: 4,
      hotKeyFraction: 0
    })
  ],
  edges: [edge("client", "api"), edge("api", "shards")],
  annotations: [
    note(
      "sd-note-hot",
      40,
      320,
      "Four shards carry 160 a second each. Push the hot key share to 0.8 and one shard alone is offered 320 against its own 160, so it pins and sheds while the utilisation meter, an average across all four, still looks comfortable.",
      340
    )
  ]
};
var autoscalingService = {
  nodes: [
    node("client", "client", "Client", COL(0), ROW(1), { rps: 250, timeoutMs: 3e3 }),
    node("api", "service", "API Server", COL(1), ROW(1), {
      // Three slots on one machine; three machines running right now.
      capacity: 3,
      instances: 3,
      serviceMs: 25,
      serviceCv: 0.5,
      queueLimit: 256
    }),
    node("db", "db", "Database", COL(2), ROW(1), {
      capacity: 24,
      serviceMs: 8,
      serviceCv: 0.6,
      queueLimit: 128
    }),
    // The controller sits below the node it scales, joined to it by a CONTROL
    // edge. That edge names its target and carries no requests -- the engine
    // keeps control edges out of routing entirely, so this is a supervisory
    // relationship the topology states outright rather than something a
    // student has to infer from a wire that looks like every other wire.
    node("scaler", "autoscaler", "Autoscaler", COL(1), ROW(2), {
      targetUtil: 0.7,
      // In INSTANCES: never fewer than 2 machines, never more than 5.
      minCapacity: 2,
      maxCapacity: 5,
      cooldownMs: 3e3,
      scaleStepPct: 0.5,
      warmupMs: 4e3
    })
  ],
  edges: [edge("client", "api"), edge("api", "db"), control("scaler", "api")],
  annotations: [
    note(
      "as-note-lag",
      40,
      456,
      "Raise the load and capacity does not follow. The controller waits out three seconds of cooldown, decides, and the new machines take four more to boot. Requests fail in that gap, and the gap is the point: an autoscaler lags, it does not shield.",
      340
    )
  ]
};
var multiRegion = {
  nodes: [
    node("client", "client", "Client", COL(0), ROW(1), { rps: 250, timeoutMs: 2e3 }),
    node("router", "region", "Region Router", COL(1), ROW(1), {
      regions: 2,
      activeRegion: 0,
      failoverMs: 5e3
    }),
    node("us-api", "service", "US API", COL(2), ROW(0), {
      capacity: 10,
      serviceMs: 25,
      serviceCv: 0.5,
      queueLimit: 64
    }),
    node("eu-api", "service", "EU API", COL(2), ROW(2), {
      capacity: 10,
      serviceMs: 25,
      serviceCv: 0.5,
      queueLimit: 64
    }),
    node("us-db", "db", "US Database", COL(3), ROW(0), {
      capacity: 16,
      serviceMs: 12,
      serviceCv: 0.6,
      queueLimit: 96
    }),
    node("eu-db", "db", "EU Database", COL(3), ROW(2), {
      capacity: 16,
      serviceMs: 12,
      serviceCv: 0.6,
      queueLimit: 96
    })
  ],
  // Edge ORDER is region index: the first edge out of the router is region 0.
  edges: [
    edge("client", "router"),
    edge("router", "us-api"),
    edge("router", "eu-api"),
    edge("us-api", "us-db"),
    edge("eu-api", "eu-db")
  ],
  annotations: [
    sectionOver("mr-sec-us", "United States, taking traffic", 0, 2, 3, 0, 0),
    sectionOver("mr-sec-eu", "Europe, idle until it is needed", 3, 2, 3, 2, 2),
    note(
      "mr-note-fail",
      1064,
      40,
      "Crash the US API. For five seconds every request fails while the router notices, then Europe picks it all up. Only one region takes traffic at a time, so this pair buys you survival, not extra capacity: at 4x the active region melts while the standby sits at zero.",
      300
    )
  ]
};
var fullStack = {
  nodes: [
    node("client", "client", "Client", COL(0), ROW(1), { rps: 600, timeoutMs: 3e3 }),
    node("cdn", "cdn", "CDN Edge", COL(1), ROW(1), {
      capacity: 256,
      serviceMs: 2,
      serviceCv: 0.3,
      hitRate: 0.7,
      queueLimit: 2048
    }),
    node("lb", "lb", "Load Balancer", COL(2), ROW(1), {
      capacity: 512,
      serviceMs: 0.5
    }),
    node("api1", "service", "API 1", COL(3), ROW(0), {
      capacity: 12,
      serviceMs: 8,
      serviceCv: 0.5,
      queueLimit: 128
    }),
    node("api2", "service", "API 2", COL(3), ROW(2), {
      capacity: 12,
      serviceMs: 8,
      serviceCv: 0.5,
      queueLimit: 128
    }),
    node("cache", "cache", "Cache", COL(4), ROW(0), {
      capacity: 64,
      serviceMs: 2,
      serviceCv: 0.4,
      hitRate: 0.6,
      queueLimit: 512
    }),
    node("shards", "shard", "Sharded Store", COL(5), ROW(0), {
      serviceMs: 25,
      serviceCv: 0.6,
      queueLimit: 32,
      shardCount: 4,
      shardCapacity: 2,
      hotKeyFraction: 0
    }),
    node("queue", "queue", "Job Queue", COL(4), ROW(2), {
      serviceMs: 1,
      serviceCv: 0.2,
      queueLimit: 5e3
    }),
    node("workers", "worker", "Workers", COL(5), ROW(2), {
      capacity: 6,
      serviceMs: 25,
      serviceCv: 0.6
    })
  ],
  edges: [
    edge("client", "cdn"),
    edge("cdn", "lb"),
    edge("lb", "api1"),
    edge("lb", "api2"),
    // Each api reads through the cache AND books async work. Both edges are
    // taken for every request: a 'service' fans out to all its downstreams.
    edge("api1", "cache"),
    edge("api1", "queue"),
    edge("api2", "cache"),
    edge("api2", "queue"),
    edge("cache", "shards"),
    edge("queue", "workers")
  ],
  annotations: [
    // Only the two ends are framed. The point of this example is the
    // contrast between them, and boxing the middle would bury it.
    sectionOver("fs-sec-sync", "Answered while you wait", 0, 4, 5, 0, 0),
    sectionOver("fs-sec-async", "Answered later", 3, 4, 5, 2, 2),
    note(
      "fs-note-cdn",
      1584,
      40,
      "Two thirds of the traffic never gets past the edge cache. Only the misses reach anything below, which is why everything behind it looks so lightly loaded at rest.",
      236
    ),
    // The lesson. Both halves run out of room at 4x; only one of them
    // tells you about it.
    note(
      "fs-note-lesson",
      16,
      336,
      "Take the load to 4x and watch both halves. The sharded store saturates and the client sees errors straight away. The workers saturate too, but the queue swallows the excess, so the client is still told everything is fine while a backlog builds that takes hours to drain.",
      300
    )
  ]
};
var specialisedStores = {
  nodes: [
    node("client", "client", "Client", COL(0), ROW(1), { rps: 240, timeoutMs: 3e3 }),
    node("batch", "client", "Batch Jobs", COL(0), LROW(3, 1), {
      rps: 6,
      timeoutMs: 2e3
    }),
    node("lb", "lb", "Load Balancer", COL(1), ROW(1), {
      capacity: 512,
      serviceMs: 0.5
    }),
    node("archive-q", "queue", "Archive Queue", COL(1), LROW(3, 1), {
      serviceMs: 1,
      serviceCv: 0.2,
      queueLimit: 5e3
    }),
    node("search-api", "service", "Search API", COL(2), ROW(0), {
      capacity: 8,
      serviceMs: 6,
      serviceCv: 0.4,
      queueLimit: 128
    }),
    node("recs-api", "service", "Recs API", COL(2), ROW(1), {
      capacity: 8,
      serviceMs: 6,
      serviceCv: 0.4,
      queueLimit: 128
    }),
    node("media-api", "service", "Media API", COL(2), ROW(2), {
      capacity: 8,
      serviceMs: 6,
      serviceCv: 0.4,
      queueLimit: 128
    }),
    node("archiver", "worker", "Archiver", COL(2), LROW(3, 1), {
      capacity: 2,
      serviceMs: 40,
      serviceCv: 0.4
    }),
    node("search", "searchindex", "Search Index", COL(3), ROW(0)),
    // 12 slots at ~50ms per query is a 240 rps ceiling: a third used at 1x,
    // and the first thing to saturate at 4x, which makes the vector index
    // the knee of the whole preset.
    node("vectors", "vectordb", "Vector Index", COL(3), ROW(1), { capacity: 12 }),
    node("blobs", "objectstore", "Object Storage", COL(3), ROW(2)),
    node("glacier", "coldstorage", "Cold Storage", COL(3), LROW(3, 1)),
    node("social", "graphdb", "Social Graph", COL(4), ROW(0)),
    node("metrics", "timeseriesdb", "Metrics Store", COL(4), ROW(2))
  ],
  edges: [
    edge("client", "lb"),
    edge("lb", "search-api"),
    edge("lb", "recs-api"),
    edge("lb", "media-api"),
    // Each API talks to its own store AND emits a metric append; a service
    // fans out to all its downstreams, so the metrics edge is taken for
    // every request, which is exactly what instrumentation does.
    edge("search-api", "search"),
    edge("search-api", "metrics"),
    edge("recs-api", "vectors"),
    edge("recs-api", "social"),
    edge("recs-api", "metrics"),
    edge("media-api", "blobs"),
    edge("media-api", "metrics"),
    // The archive path: acknowledged at the queue, drained at the
    // archiver's pace, paid for in seconds at the cold tier.
    edge("batch", "archive-q"),
    edge("archive-q", "archiver"),
    edge("archiver", "glacier")
  ],
  annotations: [
    sectionOver("ss-sec-stores", "A different store for each question", 0, 2, 4, 0, 2),
    sectionOver("ss-sec-arch", "Moving old data somewhere cheaper", 2, 0, 3, 3, 3, 1),
    note(
      "ss-note-stores",
      1320,
      40,
      "One general purpose database would do all of this badly. A text index, a vector index and a graph each answer a question the others are slow at, and the price is four stores to run instead of one.",
      300
    ),
    // The lesson: the same overload, told two different ways depending on
    // whether the caller is still waiting for an answer.
    note(
      "ss-note-lesson",
      1320,
      296,
      "Take the load to 4x. The vector index saturates and the client sees the errors immediately. The archive path saturates too and says nothing: the batch client is still acked while cold storage sheds behind the queue.",
      300
    ),
    note(
      "ss-note-cold",
      16,
      296,
      "Cold storage is slow on purpose, seconds per restore, because almost nothing is ever read back. Paying for fast storage you never read is the mistake this avoids.",
      236
    )
  ]
};
var eventDriven = {
  nodes: [
    node("client", "client", "API Clients", 40, 140, { rps: 60, timeoutMs: 2500 }),
    node("gw", "apigateway", "API Gateway", 250, 140, {
      capacity: 64,
      serviceMs: 2,
      // 60 rps of interactive traffic fits comfortably; at 4x the door is
      // exactly what refuses the excess, which is its job and its lesson.
      rateLimitRps: 150,
      burst: 150,
      authFailRate: 0.01
    }),
    node("api", "service", "API Service", 470, 140, {
      capacity: 16,
      serviceMs: 10,
      serviceCv: 0.5,
      queueLimit: 128
    }),
    node("broker", "streambroker", "Event Stream", 700, 60, {
      serviceMs: 1,
      partitions: 4,
      queueLimit: 2e3
    }),
    node("indexer", "service", "Search Indexer", 930, 20, {
      capacity: 4,
      serviceMs: 55,
      serviceCv: 0.6,
      queueLimit: 32
    }),
    node("billing", "service", "Billing", 930, 130, {
      capacity: 4,
      serviceMs: 12,
      serviceCv: 0.5,
      queueLimit: 32
    }),
    node("topic", "pubsub", "Fan-out Topic", 700, 230, { serviceMs: 0.5 }),
    node("push", "service", "Push Notifs", 930, 240, {
      capacity: 4,
      serviceMs: 8,
      serviceCv: 0.5,
      queueLimit: 32
    }),
    node("audit", "service", "Audit Log", 930, 350, {
      capacity: 2,
      serviceMs: 40,
      serviceCv: 0.6,
      queueLimit: 24
    }),
    node("metrics", "service", "Metrics", 930, 460, {
      capacity: 4,
      serviceMs: 5,
      serviceCv: 0.4,
      queueLimit: 32
    }),
    node("chat", "client", "Chat Clients", 40, 420, { rps: 30, timeoutMs: 2e3 }),
    node("ws", "websocket", "WS Gateway", 250, 420, {
      capacity: 400,
      serviceMs: 5,
      connectionMs: 8e3
    }),
    node("mesh", "sidecar", "Chat Sidecar", 470, 420, {
      capacity: 32,
      serviceMs: 2,
      timeoutMs: 500,
      retries: 2,
      outlierAfter: 5,
      openMs: 3e3
    }),
    node("chatsvc", "service", "Chat Service", 690, 420, {
      capacity: 8,
      serviceMs: 12,
      serviceCv: 0.5,
      queueLimit: 64
    }),
    node("cron", "cron", "Nightly Report", 40, 560, {
      intervalMs: 15e3,
      batchSize: 40
    }),
    node("fn", "lambda", "Report Fn", 300, 560, {
      serviceMs: 25,
      serviceCv: 0.5,
      coldStartMs: 350,
      keepWarmMs: 1e4,
      maxConcurrency: 30
    }),
    node("db", "db", "Database", 560, 560, {
      capacity: 8,
      serviceMs: 20,
      serviceCv: 0.6,
      queueLimit: 64
    })
  ],
  edges: [
    edge("client", "gw"),
    // The gateway's route table: 3 parts interactive API, 1 part function.
    edge("gw", "api", 3),
    edge("gw", "fn", 1),
    edge("api", "broker"),
    edge("api", "topic"),
    // Each broker edge is an independent consumer group.
    edge("broker", "indexer"),
    edge("broker", "billing"),
    // Each topic edge is one more delivery per publish.
    edge("topic", "push"),
    edge("topic", "audit"),
    edge("topic", "metrics"),
    edge("chat", "ws"),
    edge("ws", "mesh"),
    edge("mesh", "chatsvc"),
    edge("cron", "fn"),
    edge("fn", "db")
  ],
  // Notes only, no sections. These nodes are hand-placed rather than on the
  // COL/ROW grid, and the twelve pixels between Billing and the fan-out
  // topic cannot hold two frames plus a label plate. Nudging the layout to
  // make room would cost more than the frames are worth here.
  annotations: [
    note(
      "ed-note-sync",
      16,
      248,
      "Everything on the left is a caller waiting for an answer. Everything on the right runs after that answer was already sent.",
      236
    ),
    // The lesson. Five consumers, one of which quietly cannot keep up.
    note(
      "ed-note-fanout",
      1160,
      16,
      "One event becomes five pieces of work here, each read by its own consumer. Turn the load up and watch the audit log: it takes 40ms a message and falls behind while the four beside it keep up, and nobody calling the API sees a thing.",
      300
    ),
    note(
      "ed-note-cold",
      16,
      680,
      "The report function starts cold. The first call after a quiet spell pays 350ms of startup, then stays warm for ten seconds. Watch the first burst after the timer fires.",
      236
    )
  ]
};
var resilientDelivery = {
  nodes: [
    node("client", "client", "Client", COL(0), ROW(1), { rps: 240, timeoutMs: 2500 }),
    node("shedder", "loadshedder", "Load Shedder", COL(1), ROW(1), {
      rateLimitRps: 700,
      burst: 700,
      lowPriorityShare: 0.3,
      priorityReserve: 0.3
    }),
    node("api", "service", "API Server", COL(2), ROW(1), {
      capacity: 16,
      serviceMs: 6,
      serviceCv: 0.4,
      queueLimit: 128
    }),
    node("bulkhead", "bulkhead", "Recs Bulkhead", COL(3), ROW(0), {
      bulkheadMax: 12
    }),
    node("recs", "service", "Recommendations", COL(4), ROW(0), {
      capacity: 6,
      serviceMs: 15,
      serviceCv: 0.5,
      queueLimit: 32
    }),
    node("writebuf", "writebehind", "Write-Behind Cache", COL(3), ROW(1), {
      capacity: 256,
      serviceMs: 1,
      serviceCv: 0.3,
      queueLimit: 512,
      flushDelayMs: 200
    }),
    node("db", "db", "Database", COL(4), ROW(1), {
      capacity: 12,
      serviceMs: 15,
      serviceCv: 0.6,
      queueLimit: 96
    }),
    node("retryq", "retryqueue", "Notify Queue", COL(3), ROW(2), {
      capacity: 8,
      serviceMs: 3,
      serviceCv: 0.3,
      queueLimit: 2e3,
      timeoutMs: 1e3,
      retries: 2
    }),
    node("notify", "service", "Notification Service", COL(4), ROW(2), {
      capacity: 6,
      serviceMs: 12,
      serviceCv: 0.5,
      errorRate: 0.15,
      queueLimit: 48
    }),
    node("uploader", "client", "Upload Client", COL(0), LROW(3, 1), {
      rps: 3,
      timeoutMs: 4e3
    }),
    node("encodeq", "queue", "Encode Queue", COL(1), LROW(3, 1), {
      serviceMs: 1,
      serviceCv: 0.2,
      queueLimit: 5e3
    }),
    node("transcoder", "transcoder", "Transcoder Farm", COL(2), LROW(3, 1), {
      instances: 2,
      capacity: 2,
      serviceMs: 1200,
      serviceCv: 0.3
    })
  ],
  edges: [
    edge("client", "shedder"),
    edge("shedder", "api"),
    // The api fans out to all three: recommendations behind their own
    // bulkhead, writes into the write-behind buffer, and a notification
    // job into the retry queue. Both delivery nodes ack instantly, so
    // the client's fate rides on the recommendations path alone.
    edge("api", "bulkhead"),
    edge("api", "writebuf"),
    edge("api", "retryq"),
    edge("bulkhead", "recs"),
    edge("writebuf", "db"),
    edge("retryq", "notify"),
    edge("uploader", "encodeq"),
    edge("encodeq", "transcoder")
  ],
  annotations: [
    sectionOver("rd-sec-live", "Failing on purpose, not by surprise", 0, 1, 4, 0, 2),
    sectionOver("rd-sec-batch", "Work that can wait", 2, 0, 2, 3, 3, 1),
    note(
      "rd-note-shed",
      16,
      16,
      "At 4x the shedder drops the traffic marked best effort and keeps serving the rest. Choosing what to drop beats letting a queue choose for you.",
      236
    ),
    // The lesson. A bulkhead is the one protection whose effect you can
    // trigger by hand and see in a single round trip.
    note(
      "rd-note-bulkhead",
      1320,
      40,
      "Right click recommendations and inject slow. The bulkhead in front fills within one round trip, and after that the extra calls fail in microseconds instead of queueing. One slow feature stops being everyone else's problem.",
      300
    ),
    note(
      "rd-note-writebuf",
      1320,
      264,
      "The write cache acks in about a millisecond and flushes 200ms later, so roughly 50 rows are always acknowledged but not yet stored. Crash it and exactly that many writes are gone.",
      300
    ),
    note(
      "rd-note-batch",
      800,
      496,
      "The transcoders drain about 3.3 jobs a second against 3 arriving, so they are already 90 percent busy at rest. Turn the load up and the backlog grows about 9 jobs every second and never drains on its own. Raise the number of instances to fix it.",
      300
    )
  ]
};
var discord = {
  nodes: [
    // Connection lane: capacity here is held connections, not rps.
    node("conn", "client", "New Connections", COL(0), ROW(0), {
      rps: 30,
      timeoutMs: 3e3
    }),
    node("gateway", "websocket", "Gateway (WS)", COL(1), ROW(0), {
      capacity: 400,
      instances: 4,
      serviceMs: 5,
      serviceCv: 0.4,
      connectionMs: 4e4
    }),
    node("sessions", "service", "Session Servers", COL(2), ROW(0), {
      capacity: 8,
      serviceMs: 10,
      serviceCv: 0.5,
      queueLimit: 64
    }),
    // Message lane.
    node("senders", "client", "Message Senders", COL(0), ROW(2), {
      rps: 80,
      timeoutMs: 2500
    }),
    node("limiter", "ratelimiter", "API Rate Limit", COL(1), ROW(2), {
      rateLimitRps: 200,
      burst: 200
    }),
    node("msg-api", "service", "Message API", COL(2), ROW(2), {
      capacity: 16,
      serviceMs: 6,
      serviceCv: 0.5,
      queueLimit: 128
    }),
    node("fanout", "pubsub", "Guild Fan-out", COL(3), ROW(1), { serviceMs: 0.5 }),
    node("push-a", "service", "Gateway Push A", COL(4), ROW(0), {
      capacity: 2,
      serviceMs: 15,
      serviceCv: 0.5,
      queueLimit: 32
    }),
    node("push-b", "service", "Gateway Push B", COL(4), ROW(1), {
      capacity: 2,
      serviceMs: 15,
      serviceCv: 0.5,
      queueLimit: 32
    }),
    node("push-c", "service", "Gateway Push C", COL(4), ROW(2), {
      capacity: 2,
      serviceMs: 15,
      serviceCv: 0.5,
      queueLimit: 32
    }),
    // Messages are partitioned by channel; a shard here IS a channel range.
    node("scylla", "shard", "Message Store", COL(3), ROW(2), {
      serviceMs: 40,
      serviceCv: 0.6,
      queueLimit: 32,
      shardCount: 6,
      shardCapacity: 2,
      hotKeyFraction: 0
    }),
    node("search-q", "queue", "Index Queue", COL(3), LROW(3, 1), {
      serviceMs: 1,
      serviceCv: 0.2,
      queueLimit: 5e3
    }),
    node("indexer", "worker", "Search Indexer", COL(4), LROW(3, 1), {
      capacity: 4,
      serviceMs: 12,
      serviceCv: 0.5
    }),
    node("search", "searchindex", "Message Search", COL(5), LROW(3, 1), {
      readFraction: 0.15
    }),
    // Media lane: attachments behind a CDN.
    node("media", "client", "Media Fetch", COL(0), LROW(4, 1), {
      rps: 120,
      timeoutMs: 2e3
    }),
    node("cdn", "cdn", "Media CDN", COL(1), LROW(4, 1), {
      capacity: 256,
      serviceMs: 2,
      serviceCv: 0.3,
      hitRate: 0.9,
      queueLimit: 2048
    }),
    node("blobs", "objectstore", "Attachments", COL(2), LROW(4, 1)),
    // Voice lane: a separate fleet entirely.
    node("voice", "client", "Voice Joins", COL(0), LROW(5, 2), {
      rps: 20,
      timeoutMs: 3e3
    }),
    node("rtc", "lb", "RTC Discovery", COL(1), LROW(5, 2), {
      capacity: 256,
      serviceMs: 0.5
    }),
    node("sfu-a", "service", "Voice Server A", COL(2), LROW(5, 2), {
      capacity: 8,
      serviceMs: 20,
      serviceCv: 0.5,
      queueLimit: 64
    }),
    node("sfu-b", "service", "Voice Server B", COL(2), LROW(6, 2), {
      capacity: 8,
      serviceMs: 20,
      serviceCv: 0.5,
      queueLimit: 64
    })
  ],
  edges: [
    edge("conn", "gateway"),
    edge("gateway", "sessions"),
    edge("senders", "limiter"),
    edge("limiter", "msg-api"),
    // A message write fans to all three: the store decides the sender's
    // fate; the fan-out and the index pipeline ack instantly and fail,
    // when they fail, where the sender cannot see it.
    edge("msg-api", "scylla"),
    edge("msg-api", "fanout"),
    edge("msg-api", "search-q"),
    edge("fanout", "push-a"),
    edge("fanout", "push-b"),
    edge("fanout", "push-c"),
    edge("search-q", "indexer"),
    edge("indexer", "search"),
    edge("media", "cdn"),
    edge("cdn", "blobs"),
    edge("voice", "rtc"),
    edge("rtc", "sfu-a"),
    edge("rtc", "sfu-b")
  ],
  annotations: [
    // Four independent paths that share a company. Only the message path
    // and the push tier are coupled, and the coupling is the lesson.
    sectionOver("dc-sec-conn", "Holding the sockets open", 0, 0, 2, 0, 0),
    sectionOver("dc-sec-msg", "Writing a message", 1, 0, 2, 2, 2),
    sectionOver("dc-sec-push", "Pushing it out", 2, 4, 4, 0, 2),
    sectionOver("dc-sec-search", "Indexed later, off to the side", 3, 3, 5, 3, 3, 1),
    sectionOver("dc-sec-media", "Attachments", 4, 0, 2, 4, 4, 1),
    sectionOver("dc-sec-voice", "Voice, on its own hardware", 5, 0, 2, 5, 6, 2),
    note(
      "dc-note-conn",
      1320,
      48,
      "The gateway counts open connections, not requests. 30 new a second, each held for 40 seconds, is 1200 sockets against a ceiling of 1600. Double the load and it starts refusing connections outright.",
      300
    ),
    // The lesson. Placed clear of every frame so it reads as commentary on
    // the whole message path rather than as a label on one box.
    note(
      "dc-note-fanout",
      1608,
      272,
      "Drag the message rate up and watch the push servers. One message becomes one delivery per pod, and each pod can only do about 130 a second. Past that, members stop seeing messages while the senders see no errors at all. That silence is what makes fan-out hard.",
      320
    ),
    note(
      "dc-note-hot",
      792,
      760,
      "Messages are split across six channel shards. Give the message store a hot key and one busy channel takes far more than its share, so that shard melts while the average still looks healthy.",
      300
    ),
    note(
      "dc-note-voice",
      792,
      976,
      "Voice runs on its own servers and never touches the text path, so chat can be on fire while calls stay up.",
      300
    )
  ]
};
var uber = {
  nodes: [
    // Rider read side.
    node("riders", "client", "Rider Apps", COL(0), ROW(1), {
      rps: 30,
      timeoutMs: 3e3
    }),
    node("gw", "apigateway", "Edge Gateway", COL(1), ROW(1), {
      capacity: 32,
      serviceMs: 2,
      rateLimitRps: 200,
      burst: 200,
      authFailRate: 5e-3
    }),
    node("match", "service", "Dispatch", COL(2), ROW(0), {
      capacity: 8,
      serviceMs: 10,
      serviceCv: 0.5,
      queueLimit: 64
    }),
    node("eta", "service", "Maps ETA", COL(3), ROW(0), {
      capacity: 8,
      serviceMs: 15,
      serviceCv: 0.5,
      queueLimit: 64
    }),
    node("push", "service", "Offer Push", COL(4), ROW(0), {
      capacity: 4,
      serviceMs: 8,
      serviceCv: 0.5,
      queueLimit: 32
    }),
    node("pricing", "service", "Dynamic Pricing", COL(3), ROW(1), {
      capacity: 8,
      serviceMs: 6,
      serviceCv: 0.4,
      queueLimit: 64
    }),
    // The sink store the surge pipeline writes and pricing reads.
    node("surge-kv", "db", "Surge KV Store", COL(4), ROW(1), {
      capacity: 16,
      serviceMs: 4,
      serviceCv: 0.5,
      queueLimit: 128
    }),
    // Trip state and payments.
    node("trips", "service", "Trip Service", COL(2), LROW(2, 1), {
      capacity: 8,
      serviceMs: 12,
      serviceCv: 0.5,
      queueLimit: 64
    }),
    node("trip-db", "replica", "Trip Store", COL(3), LROW(2, 1), {
      capacity: 4,
      serviceMs: 10,
      serviceCv: 0.5,
      queueLimit: 64,
      replicaCount: 2,
      replicationLagMs: 50,
      readFraction: 0.7
    }),
    node("pay-brk", "breaker", "Payment Breaker", COL(3), LROW(3, 1), {
      errorThreshold: 0.5,
      windowMs: 4e3,
      openMs: 3e3,
      halfOpenProbes: 3
    }),
    node("payments", "service", "Payments", COL(4), LROW(3, 1), {
      capacity: 4,
      serviceMs: 200,
      serviceCv: 0.5,
      queueLimit: 16,
      errorRate: 0.01
    }),
    // Driver write side: the firehose.
    node("drivers", "client", "Driver Pings", COL(0), LROW(4, 2), {
      rps: 400,
      timeoutMs: 2e3
    }),
    node("ingest", "service", "Location Ingest", COL(1), LROW(4, 2), {
      capacity: 24,
      serviceMs: 3,
      serviceCv: 0.4,
      queueLimit: 256
    }),
    node("kafka", "streambroker", "Kafka Event Bus", COL(2), LROW(4, 2), {
      serviceMs: 0.5,
      serviceCv: 0.2,
      partitions: 12,
      queueLimit: 4e3
    }),
    node("geo-upd", "service", "Geo Updater", COL(3), LROW(4, 2), {
      capacity: 8,
      serviceMs: 6,
      serviceCv: 0.5,
      queueLimit: 64
    }),
    // Sharded by region cell: one shard is one slice of the city.
    node("geo", "shard", "Geo Index (H3)", COL(4), LROW(4, 2), {
      serviceMs: 10,
      serviceCv: 0.5,
      queueLimit: 32,
      shardCount: 6,
      shardCapacity: 2,
      hotKeyFraction: 0
    }),
    node("surge-w", "service", "Surge Pipeline", COL(3), LROW(5, 2), {
      capacity: 8,
      serviceMs: 6,
      serviceCv: 0.5,
      queueLimit: 64
    }),
    node("m3", "timeseriesdb", "M3 Metrics", COL(3), LROW(6, 2), {
      rangeQueryFraction: 0.02,
      rangeQueryMs: 120
    })
  ],
  edges: [
    edge("riders", "gw"),
    // The gateway's route table: 3 parts dispatch, 1 part trip state.
    edge("gw", "match", 3),
    edge("gw", "trips", 1),
    // Matching fans to everything it needs to answer one request.
    edge("match", "geo"),
    edge("match", "eta"),
    edge("match", "pricing"),
    edge("match", "push"),
    edge("pricing", "surge-kv"),
    edge("trips", "trip-db"),
    edge("trips", "pay-brk"),
    // Trip events join the same stream the location pings ride.
    edge("trips", "kafka"),
    edge("pay-brk", "payments"),
    edge("drivers", "ingest"),
    edge("ingest", "kafka"),
    // Each broker edge is an independent consumer group with its own lag.
    edge("kafka", "geo-upd"),
    edge("kafka", "surge-w"),
    edge("kafka", "m3"),
    edge("geo-upd", "geo"),
    edge("surge-w", "surge-kv")
  ],
  annotations: [
    sectionOver("ub-sec-match", "Finding a driver", 0, 2, 4, 0, 1),
    sectionOver("ub-sec-trip", "The trip, and getting paid", 1, 2, 4, 2, 3, 1),
    sectionOver(
      "ub-sec-fire",
      "Where the drivers are, updated constantly",
      2,
      0,
      4,
      4,
      6,
      2
    ),
    note(
      "ub-note-fan",
      1320,
      40,
      "One rider request touches four services before an offer comes back. The slowest of them sets the wait, so a match happens at the speed of the weakest link here.",
      300
    ),
    // The lesson. Payments is the only node in the diagram whose ceiling is
    // low enough to hit at 4x, and the breaker's reaction is the point.
    note(
      "ub-note-pay",
      1320,
      368,
      "Drag the load to 4x and watch payments. Each charge takes 200ms and only four run at once, so it tops out near 20 a second. The breaker in front notices and starts failing fast, which beats every trip request piling up behind a slow card network.",
      300
    ),
    note(
      "ub-note-geo",
      1320,
      696,
      "The location index is split by map cell. Give it a hot key and the whole city crowds into one cell, so that one shard saturates while the rest sit idle.",
      300
    ),
    note(
      "ub-note-lag",
      16,
      336,
      "400 driver pings a second go onto one stream, and three readers consume it on their own clocks. If the map updater falls behind, dispatch quietly matches on stale positions and nothing on the rider path shows an error.",
      236
    )
  ]
};
var netflix = {
  nodes: [
    // Streaming lane: the bytes. Most of the system's traffic, none of
    // its cloud. An OCA is an ISP-embedded cache; its misses fill from S3.
    node("viewers", "client", "Stream Viewers", COL(0), ROW(0), {
      rps: 1500,
      timeoutMs: 2500
    }),
    node("oca", "cdn", "Open Connect OCA", COL(1), ROW(0), {
      capacity: 512,
      serviceMs: 2,
      serviceCv: 0.3,
      hitRate: 0.96,
      queueLimit: 4096
    }),
    node("s3", "objectstore", "S3 Origin (fill)", COL(3), ROW(0)),
    // Encoding lane: one master in, many encodes out, fed by a queue and
    // priced in seconds. Its output lands in the same S3 the OCAs fill from.
    node("studio", "client", "Studio Ingest", COL(0), ROW(1), {
      rps: 2,
      timeoutMs: 3e3
    }),
    node("cosmosq", "queue", "Cosmos Job Queue", COL(1), ROW(1), {
      serviceMs: 1,
      serviceCv: 0.2,
      queueLimit: 5e3
    }),
    node("ves", "transcoder", "VES Encode Farm", COL(2), ROW(1), {
      instances: 2,
      capacity: 2,
      serviceMs: 1500,
      serviceCv: 0.3
    }),
    node("atlas", "timeseriesdb", "Atlas Telemetry", COL(3), ROW(1), {
      capacity: 16,
      serviceMs: 1.5,
      serviceCv: 0.4,
      queueLimit: 1024,
      rangeQueryFraction: 0.03,
      rangeQueryMs: 120
    }),
    // Control plane: the API calls. Two orders of magnitude less traffic
    // than streaming, and where all the complexity lives.
    node("capi", "client", "Device API Calls", COL(0), LROW(3, 1), {
      rps: 240,
      timeoutMs: 2500
    }),
    node("zuul", "apigateway", "Zuul 2 Gateway", COL(1), LROW(3, 1), {
      capacity: 96,
      serviceMs: 2,
      serviceCv: 0.3,
      queueLimit: 512,
      rateLimitRps: 1200,
      burst: 600,
      authFailRate: 5e-3
    }),
    node("playapi", "service", "PlayAPI", COL(2), LROW(2, 1), {
      capacity: 16,
      serviceMs: 8,
      serviceCv: 0.5,
      queueLimit: 128,
      timeoutMs: 600
    }),
    node("hystrix", "breaker", "Hystrix Breaker", COL(3), LROW(2, 1), {
      errorThreshold: 0.4,
      windowMs: 4e3,
      openMs: 4e3,
      halfOpenProbes: 3
    }),
    // 5 slots / 18ms = 278 rps: the deliberate knee of the play path.
    node("license", "service", "DRM License Svc", COL(4), LROW(2, 1), {
      capacity: 5,
      serviceMs: 18,
      serviceCv: 0.5,
      queueLimit: 24
    }),
    node("evcache", "cache", "EVCache (viewing)", COL(3), LROW(3, 1), {
      capacity: 48,
      serviceMs: 1,
      serviceCv: 0.3,
      hitRate: 0.9,
      queueLimit: 512
    }),
    node("cassandra", "shard", "Cassandra Ring", COL(4), LROW(3, 1), {
      serviceMs: 35,
      serviceCv: 0.6,
      queueLimit: 32,
      shardCount: 4,
      shardCapacity: 2,
      hotKeyFraction: 0
    }),
    node("keystone", "streambroker", "Keystone Pipeline", COL(3), LROW(4, 1), {
      serviceMs: 1,
      partitions: 6,
      queueLimit: 4e3
    }),
    node("history", "service", "Viewing History", COL(4), LROW(4, 1), {
      capacity: 6,
      serviceMs: 20,
      serviceCv: 0.5,
      queueLimit: 48
    }),
    node("browse", "service", "Browse API", COL(2), LROW(5, 2), {
      capacity: 12,
      serviceMs: 10,
      serviceCv: 0.5,
      queueLimit: 96
    }),
    node("recsbh", "bulkhead", "Recs Bulkhead", COL(3), LROW(5, 2), {
      bulkheadMax: 10
    }),
    node("recs", "service", "Personalisation", COL(4), LROW(5, 2), {
      capacity: 8,
      serviceMs: 20,
      serviceCv: 0.5,
      queueLimit: 48
    }),
    // Precomputed offline; the read path almost never misses. The
    // recompute pipeline itself is deliberately out of frame.
    node("evrecs", "cache", "EVCache (recs)", COL(5), LROW(5, 2), {
      capacity: 48,
      serviceMs: 1,
      serviceCv: 0.3,
      hitRate: 0.97,
      queueLimit: 256
    })
  ],
  edges: [
    edge("viewers", "oca"),
    edge("oca", "s3"),
    edge("studio", "cosmosq"),
    edge("cosmosq", "ves"),
    edge("ves", "s3"),
    edge("capi", "zuul"),
    // Zuul's route table: 3 parts playback control, 2 parts browsing.
    edge("zuul", "playapi", 3),
    edge("zuul", "browse", 2),
    // PlayAPI fans out to everything a real playback start touches:
    // entitlement/licensing behind its breaker, viewing state through
    // EVCache, an event onto Keystone, a telemetry append into Atlas.
    edge("playapi", "hystrix"),
    edge("hystrix", "license"),
    edge("playapi", "evcache"),
    edge("evcache", "cassandra"),
    edge("playapi", "keystone"),
    edge("playapi", "atlas"),
    // Keystone's one modelled consumer group: the history writer.
    edge("keystone", "history"),
    edge("browse", "recsbh"),
    edge("recsbh", "recs"),
    edge("recs", "evrecs")
  ],
  annotations: [
    // Three regions, because the diagram is really three systems that happen
    // to share a company. The top one moves almost all the bytes and has
    // almost none of the logic, which is the first surprising thing about it.
    sectionOver("nf-sec-bytes", "Moving the video", 0, 0, 3, 0, 1),
    sectionOver("nf-sec-edge", "Where API calls arrive", 1, 0, 1, 3, 3, 1),
    sectionOver("nf-sec-play", "Starting a stream", 2, 2, 4, 2, 4, 1),
    sectionOver("nf-sec-recs", "Browsing, kept separate", 3, 2, 5, 5, 5, 2),
    note(
      "nf-note-bytes",
      1080,
      64,
      "1500 requests a second of video, and 240 of everything else. The video almost never reaches Netflix: the edge cache answers 96 percent of it from inside your ISP.",
      380
    ),
    note(
      "nf-note-encode",
      1080,
      216,
      "Encoding is slow and that is fine. Jobs wait in a queue and nobody is watching a spinner, so seconds here cost nothing.",
      380
    ),
    // The lesson, placed in the empty column beside the play path rather
    // than above the API tier, where a reader would attach it to the wrong
    // group of boxes.
    note(
      "nf-note-license",
      40,
      360,
      "The bottleneck is the licence service, not the video. It has 5 slots at 18ms, so it runs out at about 280 requests a second. Raise the load and watch it fill before anything else does.",
      440,
      "md"
    ),
    note(
      "nf-note-breaker",
      40,
      648,
      "The breaker in front of it is what stops one slow service from holding every request open. Trip it and playback fails fast instead of hanging.",
      440
    ),
    note(
      "nf-note-recs",
      40,
      840,
      "Recommendations sit behind a bulkhead, so if they get slow the play path is untouched. Browsing breaking is survivable; playback breaking is not.",
      440
    )
  ]
};
var spotify = {
  nodes: [
    // Audio lane: bytes from blob storage through an edge cache.
    node("listeners", "client", "Listeners (audio)", COL(0), ROW(0), {
      rps: 1100,
      timeoutMs: 2500
    }),
    node("audiocdn", "cdn", "Audio CDN", COL(1), ROW(0), {
      capacity: 384,
      serviceMs: 3,
      serviceCv: 0.3,
      hitRate: 0.82,
      queueLimit: 4096
    }),
    // A shortish queue on purpose: when misses outrun the 710 rps
    // ceiling the store should refuse loudly, not buffer for seconds.
    node("gcs", "objectstore", "GCS Audio Storage", COL(2), ROW(0), {
      queueLimit: 256
    }),
    // Metadata/control lane: the app's API calls.
    node("app", "client", "App Clients", COL(0), LROW(2, 1), {
      rps: 220,
      timeoutMs: 2500
    }),
    node("gw", "apigateway", "API Gateway", COL(1), LROW(2, 1), {
      capacity: 96,
      serviceMs: 2,
      serviceCv: 0.3,
      queueLimit: 512,
      rateLimitRps: 700,
      burst: 350,
      authFailRate: 0.01
    }),
    node("meta", "service", "Metadata Service", COL(2), LROW(1, 1), {
      capacity: 12,
      serviceMs: 8,
      serviceCv: 0.5,
      queueLimit: 96
    }),
    node("metacache", "cache", "Metadata Cache", COL(3), LROW(1, 1), {
      capacity: 48,
      serviceMs: 1,
      serviceCv: 0.3,
      hitRate: 0.9,
      queueLimit: 512
    }),
    node("cassmeta", "db", "Track Metadata DB", COL(4), LROW(1, 1), {
      capacity: 6,
      serviceMs: 25,
      serviceCv: 0.6,
      queueLimit: 48
    }),
    node("search", "service", "Search API", COL(2), LROW(2, 1), {
      capacity: 8,
      serviceMs: 6,
      serviceCv: 0.4,
      queueLimit: 64
    }),
    node("es", "searchindex", "Search Index (ES)", COL(3), LROW(2, 1), {
      capacity: 12,
      serviceMs: 8,
      serviceCv: 0.5,
      queueLimit: 128,
      indexMs: 60,
      indexLagMs: 2e3,
      readFraction: 0.95
    }),
    node("playlist", "service", "Playlist Service", COL(2), LROW(3, 1), {
      capacity: 10,
      serviceMs: 7,
      serviceCv: 0.5,
      queueLimit: 96
    }),
    // Write-heavy: 45% writes serialise through a 3-slot / 30ms primary
    // (100 writes/s) while reads spread across 3 replicas (300 reads/s).
    node("pldb", "replica", "Playlist Store", COL(3), LROW(3, 1), {
      capacity: 3,
      serviceMs: 30,
      serviceCv: 0.6,
      queueLimit: 64,
      replicaCount: 3,
      replicationLagMs: 150,
      readFraction: 0.55
    }),
    node("recs", "service", "Home & Discover Feed", COL(2), LROW(4, 1), {
      capacity: 10,
      serviceMs: 8,
      serviceCv: 0.5,
      queueLimit: 96
    }),
    node("vecs", "vectordb", "Taste Vectors", COL(3), LROW(4, 1), {
      capacity: 16,
      serviceMs: 0.5,
      serviceCv: 0.4,
      queueLimit: 128,
      indexSizeK: 1e3,
      recallTarget: 0.9
    }),
    // Discover Weekly: a batch pipeline that exists entirely outside the
    // request path, except that its output lands in the store the online
    // path reads. The cron burst every 20s is the weekly job on a clock a
    // student can actually watch.
    node("wkcron", "cron", "Discover Weekly Batch", COL(0), LROW(5, 2), {
      intervalMs: 2e4,
      batchSize: 300
    }),
    node("featq", "queue", "Feature Job Queue", COL(1), LROW(5, 2), {
      serviceMs: 1,
      serviceCv: 0.2,
      queueLimit: 5e3
    }),
    // 8 slots / 22ms = ~360 writes/s of drain: deliberately faster than
    // the vector store can absorb on top of its online reads, so each
    // burst briefly queues the store and the online path feels it.
    node("featwork", "worker", "Feature Pipeline", COL(2), LROW(5, 2), {
      instances: 2,
      capacity: 4,
      serviceMs: 22,
      serviceCv: 0.4
    }),
    // Event lane: the firehose. Producers are acked in ~1ms; each
    // outgoing edge of the broker is an independent consumer group.
    node("events", "client", "Event Firehose", COL(0), LROW(6, 3), {
      rps: 600,
      timeoutMs: 1500
    }),
    node("kafka", "streambroker", "Event Delivery", COL(1), LROW(6, 3), {
      serviceMs: 1,
      partitions: 8,
      queueLimit: 6e3
    }),
    node("royalty", "service", "Royalty & Reporting", COL(2), LROW(6, 3), {
      capacity: 12,
      serviceMs: 6,
      serviceCv: 0.4,
      queueLimit: 128
    }),
    node("analytics", "timeseriesdb", "Analytics Store", COL(2), LROW(7, 3), {
      capacity: 16,
      serviceMs: 1.5,
      serviceCv: 0.4,
      queueLimit: 1024,
      // Pure ingest: appends only. A range query costs ~80x an append
      // here, and with the broker delivering at most `partitions`
      // messages at once, per-delivery cost is exactly what sets a
      // consumer group's ceiling. Keeping this group cheap is what lets
      // it keep up while Royalty, at 6ms per message, falls behind.
      rangeQueryFraction: 0,
      rangeQueryMs: 120
    })
  ],
  edges: [
    edge("listeners", "audiocdn"),
    edge("audiocdn", "gcs"),
    edge("app", "gw"),
    // The gateway's route table: metadata 35%, search 20%, playlists
    // 25%, home/recs 20%.
    edge("gw", "meta", 7),
    edge("gw", "search", 4),
    edge("gw", "playlist", 5),
    edge("gw", "recs", 4),
    edge("meta", "metacache"),
    edge("metacache", "cassmeta"),
    edge("search", "es"),
    edge("playlist", "pldb"),
    edge("recs", "vecs"),
    edge("wkcron", "featq"),
    edge("featq", "featwork"),
    edge("featwork", "vecs"),
    edge("events", "kafka"),
    // Two independent consumer groups: royalties, and raw analytics.
    edge("kafka", "royalty"),
    edge("kafka", "analytics")
  ],
  annotations: [
    sectionOver("sp-sec-audio", "Playing the music", 0, 0, 2, 0, 0),
    sectionOver("sp-sec-app", "Everything else the app asks for", 1, 2, 4, 1, 4, 1),
    sectionOver(
      "sp-sec-batch",
      "Recomputing recommendations offline",
      2,
      0,
      2,
      5,
      5,
      2
    ),
    sectionOver("sp-sec-ev", "What was played, counted twice", 3, 0, 2, 6, 7, 3),
    note(
      "sp-note-audio",
      1320,
      40,
      "Audio and everything else are separate systems. 1100 song fetches a second go to a cache near the listener, but a music catalogue has a long tail, so nearly one in five still reaches storage.",
      300
    ),
    note(
      "sp-note-playlist",
      1320,
      240,
      "Adding a song is a write, and writes only go to the one main copy of the playlist store. It manages about 100 a second while its two read copies sit idle. Reads also run up to 150ms behind, so a song you just added can be missing when the playlist loads back.",
      300
    ),
    // The lesson. Batch and serving sharing one store is the thing this
    // example exists to show, and it is visible at 1x on a 20 second clock.
    note(
      "sp-note-batch",
      1320,
      528,
      "Watch the taste vectors. Every 20 seconds the recommendation batch writes into the same index the home feed reads from, and the slow tail on the feed rises and falls on that clock. Turn the load up and the two fight over one store.",
      300
    ),
    note(
      "sp-note-events",
      800,
      1024,
      "Two readers share one event stream. Turn the load up and the royalty job cannot keep up while the analytics store beside it does fine. Royalty falls behind silently, and eventually the oldest events expire before it reaches them.",
      300
    )
  ]
};
var twitter = {
  nodes: [
    // Read row: the cheap half. The whole point of fanout-on-write is
    // that this row is one cache hit deep for 92% of requests.
    node("readers", "client", "Timeline Readers", COL(0), ROW(1), {
      rps: 300,
      timeoutMs: 2e3
    }),
    node("gw", "apigateway", "API Gateway", COL(1), ROW(1), {
      capacity: 64,
      serviceMs: 1.5,
      serviceCv: 0.3,
      rateLimitRps: 2500,
      burst: 2500,
      authFailRate: 0
    }),
    node("tlsvc", "service", "Timeline Service", COL(2), ROW(1), {
      capacity: 16,
      serviceMs: 5,
      serviceCv: 0.5,
      queueLimit: 128
    }),
    node("tlcache", "cache", "Timeline Cache (Redis)", COL(3), ROW(1), {
      capacity: 32,
      serviceMs: 2,
      serviceCv: 0.4,
      hitRate: 0.92,
      queueLimit: 256
    }),
    // A miss rebuilds the timeline the slow way: fetch the follow graph
    // and the tweets, join both. This is also the shape of the hybrid
    // celebrity merge, so it stands in for that too.
    node("tweetstore", "shard", "Tweet Store (sharded)", COL(4), ROW(1), {
      shardCount: 4,
      shardCapacity: 4,
      serviceMs: 12,
      serviceCv: 0.6,
      queueLimit: 64
    }),
    node("socialgraph", "graphdb", "Social Graph", COL(5), ROW(2), {
      capacity: 8,
      serviceMs: 6,
      serviceCv: 0.5,
      traversalDepth: 2,
      queueLimit: 64
    }),
    // Search: the blender fans queries to the index the firehose feeds.
    // readFraction 0.6 approximates the query:ingest mix it sees.
    node("searchsvc", "service", "Search Blender", COL(2), ROW(0), {
      capacity: 8,
      serviceMs: 10,
      serviceCv: 0.5,
      queueLimit: 64
    }),
    node("searchindex", "searchindex", "Earlybird Index", COL(4), ROW(0), {
      capacity: 8,
      serviceMs: 8,
      indexMs: 60,
      indexLagMs: 800,
      readFraction: 0.6,
      queueLimit: 64
    }),
    // Write row: the expensive half. A per-account limiter (write rate
    // limits are real and visible on the platform), the write API
    // persisting to the shard ring, and the firehose broker.
    node("tweeters", "client", "Tweet Writers", COL(0), LROW(3, 1), {
      rps: 30,
      timeoutMs: 2500
    }),
    node("wlimit", "ratelimiter", "Write Rate Limit", COL(1), LROW(3, 1), {
      rateLimitRps: 90,
      burst: 120
    }),
    node("writeapi", "service", "Tweet Write API", COL(2), LROW(3, 1), {
      capacity: 8,
      serviceMs: 8,
      serviceCv: 0.5,
      queueLimit: 64
    }),
    node("firehose", "streambroker", "Tweet Firehose", COL(3), LROW(3, 1), {
      serviceMs: 1,
      partitions: 8,
      queueLimit: 4e3
    }),
    // The fanout group. 120ms per delivery is the follower-list lookup
    // plus one timeline insert per follower, priced as one job. The
    // broker's 8 partitions cap the group at 8 deliveries in flight,
    // which is the ceiling that matters, not this node's slot count.
    node("fanout", "service", "Fanout Workers", COL(4), LROW(3, 1), {
      instances: 4,
      capacity: 4,
      serviceMs: 120,
      serviceCv: 0.4,
      queueLimit: 64
    }),
    node("tlstore", "service", "Timeline Store (Redis)", COL(5), LROW(3, 1), {
      capacity: 64,
      serviceMs: 2,
      serviceCv: 0.3,
      queueLimit: 512
    }),
    // One celebrity tweet is not one message: it is a burst of fanout
    // jobs. 200 every 20s here; the real number would be millions,
    // which is exactly why the real system stopped fanning them out.
    node("celebrity", "cron", "Celebrity Tweet", COL(2), LROW(4, 1), {
      intervalMs: 2e4,
      batchSize: 200
    }),
    node("pushsvc", "service", "Push Notifications", COL(4), LROW(4, 1), {
      capacity: 6,
      serviceMs: 10,
      serviceCv: 0.5,
      queueLimit: 64
    })
  ],
  edges: [
    edge("readers", "gw"),
    // The gateway's route table: 5 parts timeline, 1 part search.
    edge("gw", "tlsvc", 5),
    edge("gw", "searchsvc", 1),
    edge("tlsvc", "tlcache"),
    edge("tlcache", "tweetstore"),
    edge("tlcache", "socialgraph"),
    edge("searchsvc", "searchindex"),
    edge("tweeters", "wlimit"),
    edge("wlimit", "writeapi"),
    edge("writeapi", "tweetstore"),
    edge("writeapi", "firehose"),
    edge("celebrity", "firehose"),
    // Each broker edge is an independent consumer group: fanout is the
    // expensive one, search ingest and push notifications keep up.
    edge("firehose", "fanout"),
    edge("firehose", "searchindex"),
    edge("firehose", "pushsvc"),
    edge("fanout", "socialgraph"),
    edge("fanout", "tlstore")
  ],
  annotations: [
    // The whole design is one trade: make reading cheap by making writing
    // expensive. Two frames, one for each half of that bargain.
    sectionOver("tw-sec-read", "Reading a timeline", 0, 2, 5, 0, 2),
    sectionOver("tw-sec-write", "Writing one, which costs far more", 1, 2, 5, 3, 4, 1),
    note(
      "tw-note-read",
      1584,
      40,
      "Reading a timeline is cheap because the answer was written in advance. 300 reads a second are almost all served straight from cache; only a miss goes back to look up who you follow and rebuild it.",
      300
    ),
    // The lesson. The point is that the read side stays green throughout,
    // so the only place the failure is visible is the lag on this group.
    note(
      "tw-note-fanout",
      1584,
      496,
      "Turn the load up to 2x and watch the fan-out workers. One tweet means one insert into every follower timeline, about 120ms each, and only eight run at once. They fall behind and never catch up, so timelines go stale while every read still comes back fast and green.",
      300
    ),
    note(
      "tw-note-celeb",
      16,
      632,
      "Every 20 seconds a famous account tweets, and that one tweet becomes 200 fan-out jobs at once. This is why very large accounts get handled differently from everyone else.",
      236
    )
  ]
};
var stripe = {
  nodes: [
    node("merchants", "client", "Merchant API Calls", COL(0), ROW(1), {
      rps: 100,
      timeoutMs: 4e3
    }),
    // The front door from the rate-limiter post: a token bucket that
    // sheds excess API traffic before it can queue behind the money.
    node("gw", "apigateway", "API Gateway", COL(1), ROW(1), {
      capacity: 64,
      serviceMs: 2,
      serviceCv: 0.3,
      rateLimitRps: 250,
      burst: 250,
      authFailRate: 0.01
    }),
    // Idempotency-Key dedupe: a hit is a retried duplicate answered
    // from the stored response. The 8% hit rate is the duplicate share.
    node("idem", "cache", "Idempotency Keys", COL(2), ROW(1), {
      capacity: 32,
      serviceMs: 2,
      serviceCv: 0.3,
      hitRate: 0.08,
      queueLimit: 128
    }),
    node("paysvc", "service", "Payment Service", COL(3), ROW(1), {
      capacity: 16,
      serviceMs: 10,
      serviceCv: 0.5,
      queueLimit: 128
    }),
    // Radar runs IN the charge path: scoring is worth 30ms of latency
    // on every charge because the alternative is charging fraudsters.
    node("fraud", "service", "Radar Fraud Check", COL(4), ROW(0), {
      capacity: 12,
      serviceMs: 30,
      serviceCv: 0.5,
      timeoutMs: 1500,
      queueLimit: 64
    }),
    node("breaker", "breaker", "Network Breaker", COL(5), ROW(0), {
      errorThreshold: 0.5,
      windowMs: 5e3,
      openMs: 4e3,
      halfOpenProbes: 3
    }),
    // The slow external truth: a card authorisation is a quarter of a
    // second somewhere you do not control and cannot blindly retry.
    node("cardnet", "service", "Card Networks (external)", COL(6), ROW(0), {
      capacity: 72,
      serviceMs: 250,
      serviceCv: 0.35,
      errorRate: 0.01,
      queueLimit: 128
    }),
    node("ledger", "db", "Ledger (double-entry)", COL(4), ROW(1), {
      capacity: 8,
      serviceMs: 15,
      serviceCv: 0.5,
      queueLimit: 96
    }),
    node("events", "streambroker", "Payment Events", COL(4), ROW(3), {
      serviceMs: 1,
      partitions: 4,
      queueLimit: 4e3
    }),
    // Webhooks, per the docs: redeliver with backoff, then give up
    // onto a shelf you can inspect, because merchant endpoints fail.
    node("webhookq", "retryqueue", "Webhook Delivery", COL(5), ROW(3), {
      capacity: 8,
      serviceMs: 3,
      serviceCv: 0.3,
      timeoutMs: 1e3,
      retries: 2,
      queueLimit: 2e3
    }),
    node("merchantep", "service", "Merchant Endpoints", COL(6), ROW(3), {
      capacity: 8,
      serviceMs: 40,
      serviceCv: 0.6,
      errorRate: 0.12,
      queueLimit: 64
    }),
    node("tsdb", "timeseriesdb", "Billing Metrics", COL(5), LROW(4, 1), {
      capacity: 16,
      rangeQueryFraction: 0.02,
      rangeQueryMs: 120
    }),
    // Payouts arrive on a clock, not on demand, and share the ledger
    // primary with live charges: watch its queue breathe every 25s.
    node("payoutcron", "cron", "Payout Batch", COL(2), ROW(3), {
      intervalMs: 25e3,
      batchSize: 150
    }),
    node("payoutsvc", "service", "Payout Jobs", COL(3), ROW(3), {
      capacity: 8,
      serviceMs: 20,
      serviceCv: 0.5,
      queueLimit: 256
    }),
    // Reporting reads never touch the primary: replicas plus their own
    // limiter mean dashboard load is structurally unable to slow money.
    node("dashboards", "client", "Dashboard Readers", COL(0), LROW(4, 1), {
      rps: 120,
      timeoutMs: 2e3
    }),
    node("dlimit", "ratelimiter", "Reporting Limiter", COL(1), LROW(4, 1), {
      rateLimitRps: 150,
      burst: 200
    }),
    node("reportsvc", "service", "Reporting API", COL(2), LROW(4, 1), {
      capacity: 12,
      serviceMs: 8,
      serviceCv: 0.5,
      queueLimit: 96
    }),
    node("replica", "replica", "Ledger Replicas", COL(3), LROW(4, 1), {
      capacity: 4,
      serviceMs: 20,
      serviceCv: 0.6,
      replicaCount: 3,
      replicationLagMs: 400,
      readFraction: 1,
      queueLimit: 96
    })
  ],
  edges: [
    edge("merchants", "gw"),
    edge("gw", "idem"),
    edge("idem", "paysvc"),
    // A charge is a JOIN of three branches: the authorisation chain,
    // the ledger write, and the event publish. All must land.
    edge("paysvc", "fraud"),
    edge("paysvc", "ledger"),
    edge("paysvc", "events"),
    edge("fraud", "breaker"),
    edge("breaker", "cardnet"),
    edge("events", "webhookq"),
    edge("events", "tsdb"),
    edge("webhookq", "merchantep"),
    edge("payoutcron", "payoutsvc"),
    edge("payoutsvc", "ledger"),
    edge("dashboards", "dlimit"),
    edge("dlimit", "reportsvc"),
    edge("reportsvc", "replica")
  ],
  annotations: [
    sectionOver("sr-sec-charge", "Taking one payment", 0, 2, 6, 0, 1),
    sectionOver("sr-sec-after", "What happens after the money moves", 1, 2, 6, 3, 3),
    sectionOver("sr-sec-report", "Reading the money back out", 2, 2, 5, 4, 4, 1),
    note(
      "sr-note-idem",
      16,
      320,
      "A payment is the one thing you must never do twice. Each charge carries a key, and a retry of a charge already made gets the stored answer back instead of a second charge.",
      236
    ),
    // The lesson. Every protection in this example is about refusing
    // cleanly, and the breaker is where a student can watch that happen.
    note(
      "sr-note-breaker",
      1840,
      40,
      "Right click the card networks and inject errors. The breaker beside them opens, and charges start failing immediately instead of hanging. A payment that fails cleanly can be retried; one left in the air while a slow network times out is the one nobody can account for.",
      300
    ),
    note(
      "sr-note-report",
      16,
      776,
      "Dashboards read copies of the ledger, behind their own limit. Someone loading a big report can never make a payment wait, because the two never share a queue.",
      236
    )
  ]
};
var whatsapp = {
  nodes: [
    node("phones", "client", "Message Senders", COL(0), LROW(1, 1), {
      rps: 400,
      timeoutMs: 2e3
    }),
    // The chat core: one Erlang hop. With E2E encryption the server
    // just moves ciphertext, so per-message cost is close to nothing,
    // and the node runs practically idle at any load this app offers.
    node("router", "service", "Erlang Router", COL(1), LROW(1, 1), {
      capacity: 48,
      serviceMs: 1.5,
      serviceCv: 0.3,
      queueLimit: 512
    }),
    // Weighted split standing in for a presence lookup: 7 of 10
    // recipients are online right now, 3 are not.
    node("lookup", "lb", "Recipient Lookup (70% online)", COL(2), LROW(1, 1), {
      capacity: 256,
      serviceMs: 0.5
    }),
    node("push", "service", "Push to Connected", COL(3), LROW(1, 1), {
      capacity: 32,
      serviceMs: 2,
      serviceCv: 0.4,
      queueLimit: 256
    }),
    // Store-and-forward: the offline message is ACKED to the sender
    // and parked. Losing this node loses exactly the parked messages.
    node("offlineq", "queue", "Offline Store (Mnesia)", COL(3), LROW(2, 1), {
      serviceMs: 1,
      serviceCv: 0.2,
      queueLimit: 2e4
    }),
    node("drain", "worker", "Deliver on Reconnect", COL(4), LROW(2, 1), {
      capacity: 4,
      serviceMs: 20,
      serviceCv: 0.5
    }),
    node("midnight", "cron", "Midnight Spike", COL(0), LROW(2, 1), {
      intervalMs: 3e4,
      batchSize: 600
    }),
    // The connection tier: what a gateway box actually rations. The
    // real boxes held ~2M tcp connections each; 1800 here, run at 75%
    // on purpose, because connection count WAS the capacity plan.
    node("churn", "client", "Phones Connecting", COL(0), ROW(0), {
      rps: 45,
      timeoutMs: 2e3
    }),
    node("wsgw", "websocket", "Chat Gateway (Erlang)", COL(1), ROW(0), {
      capacity: 1800,
      serviceMs: 4,
      serviceCv: 0.4,
      connectionMs: 3e4
    }),
    node("session", "db", "Session Store (Mnesia)", COL(2), ROW(0), {
      capacity: 16,
      serviceMs: 3,
      serviceCv: 0.4,
      queueLimit: 128
    }),
    // Media: its own HTTP lane, exactly as published. Bytes never
    // touch the chat core.
    node("mediaup", "client", "Media Uploads", COL(0), LROW(3, 2), {
      rps: 25,
      timeoutMs: 4e3
    }),
    node("mediasvc", "service", "Media HTTP Service", COL(1), LROW(3, 2), {
      capacity: 12,
      serviceMs: 25,
      serviceCv: 0.5,
      queueLimit: 96
    }),
    node("blob", "objectstore", "Media Blob Store", COL(2), LROW(3, 2), {
      capacity: 64,
      serviceMs: 90,
      serviceCv: 0.4,
      queueLimit: 512
    }),
    node("mediadl", "client", "Media Downloads", COL(0), LROW(4, 2), {
      rps: 80,
      timeoutMs: 2500
    }),
    node("mediacdn", "cdn", "Media Cache", COL(1), LROW(4, 2), {
      capacity: 128,
      serviceMs: 2,
      serviceCv: 0.3,
      hitRate: 0.6,
      queueLimit: 1024
    })
  ],
  edges: [
    edge("phones", "router"),
    edge("midnight", "router"),
    edge("router", "lookup"),
    // The split that makes store-and-forward visible: online messages
    // push through, offline ones park. Weights are the 70/30 mix.
    edge("lookup", "push", 7),
    edge("lookup", "offlineq", 3),
    edge("offlineq", "drain"),
    edge("churn", "wsgw"),
    edge("wsgw", "session"),
    edge("mediaup", "mediasvc"),
    edge("mediasvc", "blob"),
    edge("mediadl", "mediacdn"),
    edge("mediacdn", "blob")
  ],
  annotations: [
    sectionOver("wa-sec-conn", "Keeping phones connected", 0, 0, 2, 0, 0),
    sectionOver("wa-sec-route", "Sending a message, or parking it", 1, 1, 4, 1, 2, 1),
    sectionOver(
      "wa-sec-media",
      "Photos and video, on their own path",
      2,
      0,
      2,
      3,
      4,
      2
    ),
    note(
      "wa-note-conn",
      800,
      16,
      "What runs out here is open connections, not requests. 45 phones connect a second and each holds on for 30 seconds, filling 1350 of the 1800 slots. This box is run deliberately hot, because connections were the entire cost of the system.",
      300
    ),
    note(
      "wa-note-route",
      1320,
      232,
      "The router does almost nothing per message: it cannot even read them. At 400 a second it is about one percent busy. Simple and small beat big here, and that was the point.",
      300
    ),
    // The lesson. Senders essentially cannot fail, which is exactly what
    // makes the pile-up invisible unless you watch the right graph.
    note(
      "wa-note-store",
      1320,
      472,
      "Turn the load up to 4x and watch the offline store, not the error rate. A message to a phone that is not online parks here until it reconnects, so senders keep succeeding while undelivered messages pile up by the hundreds a second.",
      300
    ),
    note(
      "wa-note-media",
      800,
      568,
      "Photos and video ride a separate path and never touch the chat core, so a media outage leaves messaging alone.",
      236
    )
  ]
};
var PRESETS = [
  {
    id: "single-server",
    name: "Single server",
    tagline: "Watch the database become the bottleneck",
    description: "One service in front of one database. Latency climbs sharply as the database fills up.",
    topology: singleServer
  },
  {
    id: "load-balanced",
    name: "Load balanced",
    tagline: "More servers, same database behind them",
    description: "Three servers share the load, but they all still talk to the same database.",
    topology: loadBalanced
  },
  {
    id: "cache-aside",
    name: "Cache aside",
    tagline: "What happens when the hit rate falls",
    description: "The cache absorbs most reads. Lower the hit rate and the database takes the whole load.",
    topology: cacheAside
  },
  {
    id: "async-workers",
    name: "Async workers",
    tagline: "A backlog that grows faster than it drains",
    description: "Requests are acknowledged instantly and buffered. Watch the backlog grow when workers fall behind.",
    topology: asyncWorkers
  },
  {
    id: "retry-storm",
    name: "Retry storm",
    tagline: "Retries making an overload worse",
    description: "A short timeout with retries in front of a small database. Retries multiply the load that caused them.",
    topology: retryStorm
  },
  {
    id: "cdn-origin",
    name: "CDN and origin",
    tagline: "How much traffic never reaches you",
    description: "The CDN answers most requests at the edge, so only a trickle reaches the origin. Drop the hit rate and watch the origin melt.",
    topology: cdnOrigin
  },
  {
    id: "rate-limited-api",
    name: "Rate limited API",
    tagline: "Turning excess away before it queues",
    description: "A limiter refuses excess traffic at the door. It serves slightly less, but what it does serve stays fast instead of queueing.",
    topology: rateLimitedApi
  },
  {
    id: "circuit-breaker",
    name: "Circuit breaker",
    tagline: "Giving a failing dependency room to recover",
    description: "A breaker watches a failing dependency and stops calling it. Break the payments API and watch the circuit trip, then recover.",
    topology: circuitBreaker
  },
  {
    id: "read-replicas",
    name: "Read replicas",
    tagline: "Reads that scale, and reads that go stale",
    description: "Replicas scale reads but not writes, and a read can arrive before the write it should have seen.",
    topology: readReplicas
  },
  {
    id: "sharded-database",
    name: "Sharded database",
    tagline: "One hot key undoing all the partitions",
    description: "Four partitions share the load evenly until one key gets hot, and then a single shard melts while the average still looks healthy.",
    topology: shardedDatabase
  },
  {
    id: "autoscaling-service",
    name: "Autoscaling service",
    tagline: "Capacity arriving after it was needed",
    description: "Capacity chases the load, but new servers take time to boot, so requests fail in the gap between the two.",
    topology: autoscalingService
  },
  {
    id: "multi-region",
    name: "Multi-region failover",
    tagline: "What a failover actually costs you",
    description: "Two regions, one serving. Crash the active one and every request fails until failover lands.",
    topology: multiRegion
  },
  {
    id: "full-stack",
    name: "Full stack",
    tagline: "Every piece at once, under real load",
    description: "Every tier at once: edge cache, load balancer, services, cache, shards, and a queue of async work behind it all.",
    topology: fullStack
  },
  {
    id: "specialised-stores",
    name: "Specialised stores",
    tagline: "The right store for each job, side by side",
    description: "Search, vectors, graph, blobs, metrics and an archive tier, each store built for one job. Watch which one saturates first, and which fails without a sound.",
    topology: specialisedStores
  },
  {
    id: "event-driven",
    name: "Event-driven backend",
    tagline: "Lag, fan-out, cold starts and a cron burst",
    description: "A stream with two consumer groups, a fan-out topic, a websocket tier, a sidecar, a lambda and a cron burst. Watch consumer lag grow, cold starts spike on the quarter-minute, and connections, not requests, run out.",
    topology: eventDriven
  },
  {
    id: "resilient-delivery",
    name: "Resilient delivery",
    tagline: "Choosing what fails, and where failures go",
    description: "Failing on purpose: a shedder drops the traffic that matters least, a bulkhead contains a slow dependency, retried deliveries land on a dead letter shelf, and a write-behind buffer trades durability for speed.",
    topology: resilientDelivery
  },
  {
    id: "discord",
    name: "Discord: real-time chat",
    tagline: "Millions of sockets, and fan-out per message",
    description: "A simplified reconstruction of Discord from their engineering blog; numbers are illustrative. The gateway runs out of connections, not requests; one message fans out to every gateway pod, and at 2x the push tier sheds deliveries the senders never see; make one channel hot and its store shard melts alone. Voice rides its own servers.",
    topology: discord
  },
  {
    id: "uber",
    name: "Uber: ride dispatch",
    tagline: "Matching riders to drivers, city by city",
    description: "A simplified reconstruction of Uber from their published architecture; numbers are illustrative. Driver pings outnumber rider requests 13 to 1 and ride a Kafka-style stream; at 4x the geo consumer lags and dispatch matches on stale positions without a single rider-facing error. Crash the payment processor and the breaker contains it.",
    topology: uber
  },
  {
    id: "netflix",
    name: "Netflix: streaming at scale",
    tagline: "Almost everything served from the edge",
    description: "A simplified reconstruction of Netflix from their tech blog; numbers are illustrative. Open Connect appliances inside ISPs serve ~96% of the bytes, so streaming barely touches the cloud; the control plane behind Zuul is where 4x hurts: licensing trips its Hystrix breaker, the recs bulkhead fills, Keystone quietly falls behind, and the encode farm backlog grows while viewers stream on.",
    topology: netflix
  },
  {
    id: "spotify",
    name: "Spotify: music and discovery",
    tagline: "A steady catalogue beside a heavy recommender",
    description: "A simplified reconstruction of Spotify from their engineering blog; numbers are illustrative. Audio flows from object storage through a CDN, apart from the metadata path. Playlist writes pin a replica primary while its read replicas idle, the event firehose outruns the royalty consumer at 4x, and every 20s the Discover Weekly batch writes the same vector index the home feed reads, so p99 breathes on the batch clock.",
    topology: spotify
  },
  {
    id: "twitter",
    name: "Twitter/X: timeline fan-out",
    tagline: "One tweet becoming a hundred thousand writes",
    description: `A simplified reconstruction of the Twitter timeline from Raffi Krikorian's "Timelines at Scale" talk; numbers are illustrative. Tweets fan out on write into precomputed timelines, so reads are one cache hit; every 20s a celebrity tweet dumps 200 fanout jobs on the firehose. Past 2x the fanout group never catches up again: reads stay green while timelines quietly go stale, and only the consumer lag tells the truth.`,
    topology: twitter
  },
  {
    id: "stripe",
    name: "Stripe: correctness over availability",
    tagline: "Refusing work rather than charging twice",
    description: "A simplified reconstruction of Stripe from their published posts on rate limiters, idempotency and the ledger; numbers are illustrative. Duplicate retries answer from the idempotency store, a breaker fails charges fast when the card networks brown out, webhooks redeliver onto a dead-letter shelf, and at 4x the gateway sheds excess charges at the door while dashboards throttle against their replicas. Crash the ledger: charges stop dead, dashboards keep reading, and that ordering is the design.",
    topology: stripe
  },
  {
    id: "whatsapp",
    name: "WhatsApp: store and forward",
    tagline: "Messages that wait instead of failing",
    description: "A simplified reconstruction of WhatsApp from Rick Reed's Erlang scaling talks; numbers are illustrative. The chat core is deliberately tiny and nearly idle: one routing hop, then online recipients get pushed and offline ones park in the Mnesia store until they reconnect, so senders essentially cannot fail. At 4x undelivered messages pile up by the hundreds per second with zero errors, and the gateway runs out of held connections, not requests. Watch the queue depth, not the error rate.",
    topology: whatsapp
  }
];
export {
  Engine,
  PRESETS,
  defaultConfig,
  makeNode
};
