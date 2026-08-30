/**
 * The one manifest (D10, rule ⑥: two doors, one core). Every tool an agent can
 * call, every command the UI issues. WebMCP is the primary door; window.buckle
 * is the polyfill. Parity is linted (tools/parity-lint.mjs): manifest ⊇ bus.
 */
export const MANIFEST = [
  { name: 'load_repo', args: { url: 'string', ref: 'string?', token: 'string?' }, mutating: 'stages', description: 'Pull a GitHub repo through the edge worker, extract a topology, and stage it. Nothing runs until accept_graph.' },
  { name: 'load_topology', args: { topology: 'object' }, mutating: 'stages', description: 'Stage a topology JSON directly (validated through the one ingress).' },
  { name: 'load_preset', args: { name: 'string' }, mutating: 'stages', description: 'Stage a bundled breakscale preset topology.' },
  { name: 'accept_graph', args: {}, mutating: true, description: 'Land the staged topology into the running engine (I5: the one commit point).' },
  { name: 'get_graph', args: {}, mutating: false, description: 'The active (or staged) topology plus provenance.' },
  { name: 'set_param', args: { node: 'string', field: 'string', value: 'number' }, mutating: true, description: 'Edit one config field on a node (reversible via History).' },
  { name: 'set_load', args: { rps: 'number' }, mutating: false, description: 'Set the traffic slider: baseline rps on all client nodes.' },
  { name: 'apply_chaos', args: { node: 'string', kind: 'string', value: 'number?' }, mutating: true, description: 'Inject a failure on a node: crash | slow | errors | partition (reversible).' },
  { name: 'run_sweep', args: { seed: 'number?', steps: 'number?' }, mutating: false, description: 'Run the load sweep and compute ranked findings deterministically.' },
  { name: 'get_findings', args: {}, mutating: false, description: 'The ranked findings from the last sweep.' },
  { name: 'get_snapshot', args: {}, mutating: false, description: "The engine's current SimSnapshot, verbatim." },
  { name: 'export', args: {}, mutating: false, description: 'The full closure: topology + provenance + assumptions + findings + seed.' },
];

export function createAgentFace(bus, globalObject = globalThis) {
  const face = {};
  for (const tool of MANIFEST) face[tool.name] = (args) => bus.dispatch(tool.name, args, 'window');
  face.manifest = MANIFEST;
  globalObject.buckle = face;

  const mcp = globalObject.navigator?.modelContext;
  if (mcp?.registerTool) {
    for (const tool of MANIFEST) {
      mcp.registerTool({ name: tool.name, description: tool.description, execute: (args) => bus.dispatch(tool.name, args, 'modelContext') });
    }
  }
  return face;
}
