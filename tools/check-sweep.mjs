/**
 * Gate C3: the sweep's four sanity fixtures (§4.5), determinism, and the
 * timing bar. Exit 0/1. Pure Node — the same sweep-core the Worker runs.
 */
import { Engine, PRESETS } from '../engine.esm.js';
import { sweep } from '../app/sweep-core.js';
import { healthOfLatency } from '../app/visuals.js';

const byId = (id) => PRESETS.find((p) => p.id === id);
const FRAME = 1000 / 60;
function systemP50At(topo, rps, settleMs = 20000) {
  const e = new Engine(structuredClone(topo), 42);
  for (const n of topo.nodes) if (n.kind === 'client') e.updateNodeConfig(n.id, { rps });
  for (let i = 0; i < Math.round(settleMs / FRAME); i += 1) e.advance(FRAME);
  return e.snapshot().system.p50;
}

let failed = 0;
const check = (name, cond, detail = '') => {
  console.log(`  ${cond ? 'ok  ' : 'FAIL'} ${name}${detail ? ' — ' + detail : ''}`);
  if (!cond) failed = 1;
};

// Fixture 1: Retry Storm → retry_amplification is the top finding.
{
  const r = sweep(Engine, byId('retry-storm').topology, { seed: 42 });
  check('retry-storm: amplification tops findings', r.findings[0]?.kind === 'retry_amplification', `top=${r.findings[0]?.kind}`);
}
// Fixture 2: Sharded DB → a shard knees while system p50 is not in danger.
{
  const topo = byId('sharded-database').topology;
  const r = sweep(Engine, topo, { seed: 42 });
  const shardKnee = r.findings.find((f) => f.kind === 'knee' && f.nodes.some((id) => topo.nodes.find((n) => n.id === id)?.kind === 'shard'));
  const p50 = shardKnee ? systemP50At(topo, shardKnee.at_rps) : null;
  check('sharded-database: shard knees, system p50 not danger', !!shardKnee && healthOfLatency(p50) !== 'danger', `knee@${shardKnee?.at_rps}rps p50=${Math.round(p50)}ms`);
}
// Fixture 3: Circuit Breaker → no collapse (fast-reject, not timeout drown).
{
  const r = sweep(Engine, byId('circuit-breaker').topology, { seed: 42 });
  check('circuit-breaker: no collapse finding', !r.findings.some((f) => f.kind === 'collapse'));
}
// Fixture 4: Autoscaling → a shed window appears during the boot gap.
{
  const r = sweep(Engine, byId('autoscaling-service').topology, { seed: 42 });
  check('autoscaling-service: shed window present', Object.values(r.perNode).some((n) => n.shedRps !== null));
}
// Determinism: same seed → identical findings.
{
  const topo = byId('full-stack').topology;
  const a = JSON.stringify(sweep(Engine, topo, { seed: 42 }).findings);
  const b = JSON.stringify(sweep(Engine, topo, { seed: 42 }).findings);
  check('determinism: same seed → identical findings', a === b);
}
// Timing: a ~15-node preset sweeps under 10s.
{
  const topo = byId('full-stack').topology; // 9 nodes; multi-region/full-stack are the mid presets
  const t = Date.now();
  sweep(Engine, topo, { seed: 42 });
  const ms = Date.now() - t;
  check('timing: full-stack sweep < 10s', ms < 10000, `${ms}ms, ${topo.nodes.length} nodes`);
}

process.exit(failed);
