/**
 * Buckle runtime — owns the engine instance and the staged/active split (I5:
 * everything lands staged; only accept_graph makes it run). Headless: no DOM,
 * runs identically under Node (gates) and on the main thread. The engine
 * module and preset list are injected so dev layout and the inlined ship
 * artifact wire the same code.
 */
import { createBus } from './bus.js';
import { validateTopology } from './validate.js';
import { sweep } from './sweep-core.js';

export function createRuntime({ Engine, PRESETS }, { seed = 42 } = {}) {
  const bus = createBus();
  const state = {
    staged: null,        // { topology, source, provenance }
    engine: null,
    topology: null,
    provenance: {},
    findings: null,
    seed,
    load: 0,
  };

  const stage = (topology, source, provenance = {}) => {
    const result = validateTopology(topology);
    if (!result.ok) throw new Error(result.errors.join('; '));
    state.staged = { topology: result.topology, source, provenance };
    return { staged: true, source, nodes: topology.nodes.length };
  };

  bus.register('load_topology', { mutating: 'stages', handler: ({ topology, source = 'topology', provenance = {} }) => stage(topology, source, provenance) });

  bus.register('load_preset', {
    mutating: 'stages',
    handler: ({ name }) => {
      const p = PRESETS.find((x) => x.id === name || x.name === name || x.title === name);
      if (!p) throw new Error(`unknown preset "${name}" (valid: ${PRESETS.map((x) => x.id).join(', ')})`);
      return stage(structuredClone(p.topology), `preset:${p.id}`);
    },
  });

  bus.register('accept_graph', {
    mutating: true,
    handler: () => {
      if (!state.staged) throw new Error('nothing staged: load a preset, repo, or topology first');
      state.topology = state.staged.topology;
      state.provenance = state.staged.provenance || {};
      state.engine = new Engine(structuredClone(state.topology), state.seed);
      state.findings = null;
      const source = state.staged.source;
      state.staged = null;
      if (state.load > 0) applyLoad(state.load);
      return { active: true, source, nodes: state.topology.nodes.length };
    },
  });

  bus.register('get_graph', { mutating: false, handler: () => ({ topology: state.topology || state.staged?.topology || null, provenance: state.provenance, staged: !!state.staged }) });

  bus.register('set_param', {
    mutating: true,
    handler: ({ node, field, value }) => {
      if (!state.engine) throw new Error('no active graph');
      const n = state.topology.nodes.find((x) => x.id === node);
      if (!n) throw new Error(`no node "${node}"`);
      if (!Number.isFinite(value)) throw new Error('value must be a finite number');
      n.config[field] = value;
      state.engine.updateNodeConfig(node, { [field]: value });
      state.findings = null;
      return { node, field, value };
    },
  });

  const applyLoad = (rps) => {
    for (const n of state.topology.nodes) if (n.kind === 'client') state.engine.updateNodeConfig(n.id, { rps });
  };
  bus.register('set_load', {
    mutating: false,
    handler: ({ rps }) => {
      state.load = rps;
      if (!state.engine) return { rps };
      if (!Number.isFinite(rps) || rps < 0) throw new Error(`rps must be >= 0 (got ${rps})`);
      applyLoad(rps);
      return { rps };
    },
  });

  bus.register('apply_chaos', {
    mutating: true,
    handler: ({ node, kind, value }) => {
      if (!state.engine) throw new Error('no active graph');
      if (kind === 'clear') { state.engine.clearFailure(node); return { node, cleared: true }; }
      const opts = {};
      if (kind === 'slow') opts.factor = value ?? 3;
      if (kind === 'errors') opts.rate = value ?? 0.3;
      state.engine.injectFailure(node, kind, opts);
      return { node, kind, value };
    },
  });

  bus.register('run_sweep', {
    mutating: false,
    handler: ({ seed = state.seed, steps = 20 } = {}) => {
      if (!state.topology) throw new Error('no graph to sweep');
      const result = sweep(Engine, state.topology, { seed, steps });
      state.findings = result;
      return result;
    },
  });
  bus.register('get_findings', { mutating: false, handler: () => state.findings });

  bus.register('get_snapshot', { mutating: false, handler: () => { if (!state.engine) throw new Error('no active graph'); return state.engine.snapshot(); } });

  bus.register('export', {
    mutating: false,
    handler: () => ({
      version: 1,
      tool: 'buckle',
      topology: state.topology,
      provenance: state.provenance,
      findings: state.findings,
      seed: state.seed,
      load: state.load,
      exportedFrom: state.staged ? 'staged' : 'active',
    }),
  });

  return {
    bus,
    state,
    advance(deltaMs) { if (state.engine) state.engine.advance(deltaMs); },
    snapshot() { return state.engine ? state.engine.snapshot() : null; },
  };
}
