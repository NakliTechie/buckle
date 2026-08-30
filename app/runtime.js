/**
 * Buckle runtime — owns the engine instance and the staged/active topology
 * split (I5: everything lands staged; only accept_graph makes it run).
 * Headless: no DOM, runs identically under Node (gates) and in the Worker
 * (D11). The engine module is injected so dev layout and the inlined ship
 * artifact wire the same code.
 */
import { createBus } from './bus.js';
import { validateTopology } from './validate.js';

export function createRuntime({ Engine, PRESETS }, { seed = 42 } = {}) {
  const bus = createBus();
  const state = {
    staged: null, // { topology, source } awaiting accept_graph
    engine: null,
    topology: null,
    seed,
  };

  bus.register('load_preset', {
    mutating: 'stages',
    handler: ({ name }) => {
      const preset = PRESETS.find((p) => p.id === name || p.title === name);
      if (!preset) {
        throw new Error(
          `unknown preset "${name}" (valid: ${PRESETS.map((p) => p.id).join(', ')})`,
        );
      }
      const topology = structuredClone(preset.topology);
      const result = validateTopology(topology);
      if (!result.ok) throw new Error(`preset failed validation: ${result.errors.join('; ')}`);
      state.staged = { topology: result.topology, source: `preset:${preset.id}` };
      return { staged: true, source: state.staged.source, nodes: topology.nodes.length };
    },
  });

  bus.register('accept_graph', {
    mutating: true,
    handler: () => {
      if (!state.staged) throw new Error('nothing staged: load a preset, repo, or topology first');
      state.topology = state.staged.topology;
      state.engine = new Engine(structuredClone(state.topology), state.seed);
      const source = state.staged.source;
      state.staged = null;
      return { active: true, source };
    },
  });

  bus.register('get_snapshot', {
    mutating: false,
    handler: () => {
      if (!state.engine) throw new Error('no active graph: accept_graph first');
      return state.engine.snapshot();
    },
  });

  bus.register('set_load', {
    mutating: false,
    handler: ({ rps }) => {
      if (!state.engine) throw new Error('no active graph: accept_graph first');
      if (!Number.isFinite(rps) || rps < 0) throw new Error(`rps must be a finite number >= 0 (got ${rps})`);
      const clients = state.topology.nodes.filter((n) => n.kind === 'client');
      if (clients.length === 0) throw new Error('active topology has no client node to drive');
      for (const c of clients) state.engine.updateNodeConfig(c.id, { rps });
      return { rps, clients: clients.map((c) => c.id) };
    },
  });

  return {
    bus,
    state,
    /** Advance sim time; the Worker's frame loop and the sweep both call this. */
    advance(deltaMs) {
      if (state.engine) state.engine.advance(deltaMs);
    },
  };
}
