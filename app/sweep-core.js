/**
 * The sweep (§4.5) — pure over engine snapshots (I7): no UI, no model, no
 * network. Given the engine class and a topology, it loads the system across a
 * geometric RPS ladder, watches each component knee, and returns a
 * deterministic, ranked findings list. Same (topology, seed, steps) → same
 * findings; the gate asserts it.
 *
 * Runs identically in the sweep Worker and under Node (gate-c3).
 */

const DEFAULT_STEPS = 20;
const RPS_MIN = 1;
const RPS_MAX = 10000;
const SETTLE_MS = 20000;      // sim-time per ladder step; queue window read across it
const SPOF_SETTLE_MS = 10000; // a crash craters goodput fast; no need for a full settle
const FRAME_MS = 1000 / 60;   // engine tick; matches the determinism fixture
const QUEUE_SAMPLES = 6;      // sub-window snapshots to judge monotonic growth

/** Geometric RPS ladder from RPS_MIN to RPS_MAX, `steps` points inclusive. */
export function rpsLadder(steps = DEFAULT_STEPS) {
  const lo = Math.log10(RPS_MIN);
  const hi = Math.log10(RPS_MAX);
  const out = [];
  for (let i = 0; i < steps; i += 1) {
    out.push(Math.round(10 ** (lo + ((hi - lo) * i) / (steps - 1))));
  }
  return out;
}

function clientIds(topology) {
  return topology.nodes.filter((n) => n.kind === 'client').map((n) => n.id);
}

/**
 * Settle one engine to `rps` and return the snapshot plus, per node, the
 * queue-depth series sampled across the settle window (for monotonic-growth
 * detection).
 */
function settle(Engine, topology, seed, rps, { crashNodeId = null, settleMs = SETTLE_MS } = {}) {
  const engine = new Engine(structuredClone(topology), seed);
  for (const id of clientIds(topology)) engine.updateNodeConfig(id, { rps });
  if (crashNodeId) engine.injectFailure(crashNodeId, 'crash', {});

  const frames = Math.round(settleMs / FRAME_MS);
  const sampleEvery = Math.floor(frames / QUEUE_SAMPLES);
  const queueSeries = {};
  for (const n of topology.nodes) queueSeries[n.id] = [];

  for (let f = 0; f < frames; f += 1) {
    engine.advance(FRAME_MS);
    if (sampleEvery > 0 && f % sampleEvery === sampleEvery - 1) {
      const snap = engine.snapshot();
      for (const n of topology.nodes) queueSeries[n.id].push(snap.nodes[n.id]?.queued ?? 0);
    }
  }
  return { snapshot: engine.snapshot(), queueSeries };
}

/** A series counts as unbounded when it rises across the window and ends high. */
function isMonotonicGrowth(series) {
  if (series.length < 3) return false;
  let rises = 0;
  for (let i = 1; i < series.length; i += 1) if (series[i] > series[i - 1] + 1e-6) rises += 1;
  const last = series[series.length - 1];
  const first = series[0];
  return rises >= series.length - 2 && last > first + 1 && last > 5;
}

/**
 * sweep(Engine, topology, { seed, steps }) → { ladder, findings, perNode, system }.
 * findings are ranked worst-first per §4.5.
 */
export function sweep(Engine, topology, { seed = 42, steps = DEFAULT_STEPS } = {}) {
  const ladder = rpsLadder(steps);
  const nodes = topology.nodes;
  const perNode = {};
  for (const n of nodes) {
    perNode[n.id] = { kneeRps: null, unboundedRps: null, shedRps: null, kind: n.kind, label: n.label };
  }

  const systemSeries = [];  // { rps, goodput, p99, offered, anyUnbounded }
  let firstP99 = null;

  const results = [];
  for (const rps of ladder) {
    const { snapshot, queueSeries } = settle(Engine, topology, seed, rps);
    results.push({ rps, snapshot, queueSeries });
    const sys = snapshot.system;
    if (firstP99 === null) firstP99 = sys.p99;

    for (const n of nodes) {
      const st = snapshot.nodes[n.id];
      if (!st) continue;
      const unbounded = isMonotonicGrowth(queueSeries[n.id]);
      if (perNode[n.id].kneeRps === null && st.utilization >= 0.8) perNode[n.id].kneeRps = rps;
      if (perNode[n.id].shedRps === null && st.shedRate > 0) perNode[n.id].shedRps = rps;
      if (perNode[n.id].unboundedRps === null && unbounded) perNode[n.id].unboundedRps = rps;
    }
    // Timeout share of all failures this step. A collapsing system drowns in
    // slow, timing-out work; a circuit breaker fast-REJECTS (timeout ~ 0), so
    // this cleanly separates catastrophic collapse from clean shedding.
    const fbr = snapshot.failuresByReason ?? {};
    const totalFail = Object.values(fbr).reduce((a, b) => a + b, 0);
    const timeoutShare = totalFail > 0 ? (fbr.timeout ?? 0) / totalFail : 0;
    systemSeries.push({ rps, goodput: sys.goodputRps, p99: sys.p99, offered: sys.offeredRps, timeoutShare });
  }

  // System-level.
  let goodputPeak = 0;
  let peakRps = ladder[0];
  for (const s of systemSeries) if (s.goodput > goodputPeak) { goodputPeak = s.goodput; peakRps = s.rps; }
  // Collapse is CATASTROPHIC, not clean shedding: goodput craters past peak
  // AND the failures are timeouts (the system is drowning in slow work). A
  // circuit breaker fast-rejects with no timeouts, so it never trips this —
  // which is the whole point of a breaker (sanity fixture §4.5).
  let collapseRps = null;
  for (const s of systemSeries) {
    if (s.rps > peakRps && s.goodput < goodputPeak * 0.5 && s.timeoutShare > 0.05) { collapseRps = s.rps; break; }
  }
  let p99CliffRps = null;
  if (firstP99 > 0) {
    for (const s of systemSeries) if (s.p99 > firstP99 * 5) { p99CliffRps = s.rps; break; }
  }

  // Retry amplification at collapse (or peak if no collapse): downstream
  // arrivals / upstream offered along each request edge.
  const ampRps = collapseRps ?? peakRps;
  const atAmp = results.find((r) => r.rps === ampRps)?.snapshot;
  const configOf = {};
  for (const n of nodes) configOf[n.id] = n.config;
  const retryFindings = [];
  if (atAmp) {
    for (const e of topology.edges) {
      if (e.control) continue;
      // Only a node that actually retries can amplify. Without this the metric
      // mistakes fan-out (one request touching several downstreams) for a
      // retry storm — e.g. a sharded store reads 5.8x and is not retrying.
      if (!((configOf[e.from]?.retries ?? 0) > 0)) continue;
      const up = atAmp.nodes[e.from];
      const down = atAmp.nodes[e.to];
      if (!up || !down) continue;
      const offered = up.throughput + up.shedRate + (up.timeoutRate ?? 0);
      const arrivals = down.arrivalRate ?? 0;
      if (offered > 1 && arrivals > offered * 1.5) {
        retryFindings.push({
          kind: 'retry_amplification',
          nodes: [e.from, e.to],
          at_rps: ampRps,
          evidence: { metric: 'amplification', value: +(arrivals / offered).toFixed(2) },
          explanation_id: 'retry',
        });
      }
    }
  }
  retryFindings.sort((a, b) => b.evidence.value - a.evidence.value);

  // SPOF: crash each node at peak load; if system goodput collapses, it's a spof.
  const spofFindings = [];
  for (const n of nodes) {
    if (n.kind === 'client') continue;
    const { snapshot } = settle(Engine, topology, seed, peakRps, { crashNodeId: n.id, settleMs: SPOF_SETTLE_MS });
    if (snapshot.system.goodputRps < goodputPeak * 0.1) {
      spofFindings.push({
        kind: 'spof',
        nodes: [n.id],
        at_rps: peakRps,
        evidence: { metric: 'goodput_after_crash', value: Math.round(snapshot.system.goodputRps) },
        explanation_id: 'spof',
      });
    }
  }

  // Assemble ranked findings (§4.5 order): knee (asc knee_rps), retry_amp,
  // spof, p99_cliff, collapse.
  const kneeFindings = nodes
    .filter((n) => perNode[n.id].kneeRps !== null)
    .map((n) => ({
      kind: 'knee',
      nodes: [n.id],
      at_rps: perNode[n.id].kneeRps,
      evidence: { metric: 'utilization', value: 0.8 },
      explanation_id: 'utilization',
    }))
    .sort((a, b) => a.at_rps - b.at_rps);

  // Rank (§4.5): a retry storm is a feedback loop that makes every other
  // symptom worse, so when amplification is present it leads; then knees
  // (asc rps), spof, p99_cliff, collapse.
  const findings = [...retryFindings, ...kneeFindings];
  spofFindings.forEach((f) => findings.push(f));
  if (p99CliffRps !== null) {
    findings.push({ kind: 'p99_cliff', nodes: [], at_rps: p99CliffRps, evidence: { metric: 'p99_multiple', value: 5 }, explanation_id: 'p99' });
  }
  if (collapseRps !== null) {
    findings.push({ kind: 'collapse', nodes: [], at_rps: collapseRps, evidence: { metric: 'goodput_drop', value: 0.5 }, explanation_id: 'goodput' });
  }

  return {
    ladder,
    findings,
    perNode,
    system: { goodputPeak: Math.round(goodputPeak), peakRps, collapseRps, p99CliffRps },
    seed,
  };
}
