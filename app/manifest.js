/**
 * The one manifest (D10, rule ⑥: two doors, one core). Every tool an agent
 * can call, every command the UI can issue. WebMCP is the primary door;
 * window.buckle is the polyfill. Parity is linted, not promised.
 *
 * C1 scaffold: the engine-facing tools. The full v1.0 table (HANDOFF.md §4.6)
 * lands tool-by-tool as the chunks that implement them land; the lint keeps
 * the bus from ever running ahead of this manifest.
 */
export const MANIFEST = [
  {
    name: 'load_preset',
    args: { name: 'string — preset id or title' },
    mutating: 'stages',
    description: 'Stage a bundled breakscale preset topology. Nothing runs until accept_graph.',
  },
  {
    name: 'accept_graph',
    args: {},
    mutating: true,
    description: 'Land the staged topology into the running engine (I5: the one commit point).',
  },
  {
    name: 'get_snapshot',
    args: {},
    mutating: false,
    description: "The engine's current SimSnapshot, verbatim.",
  },
  {
    name: 'set_load',
    args: { rps: 'number — baseline offered load on every client node' },
    mutating: false,
    description: 'Set the traffic slider: baseline rps on all client-kind nodes.',
  },
];

/**
 * Expose the bus through the agent doors. Browser-only effects; safe to call
 * under Node (it just returns the polyfill object).
 */
export function createAgentFace(bus, globalObject = globalThis) {
  const face = {};
  for (const tool of MANIFEST) {
    face[tool.name] = (args) => bus.dispatch(tool.name, args, 'window');
  }
  face.manifest = MANIFEST;
  globalObject.buckle = face;

  // WebMCP door (primary when the host provides it).
  const mcp = globalObject.navigator?.modelContext;
  if (mcp?.registerTool) {
    for (const tool of MANIFEST) {
      mcp.registerTool({
        name: tool.name,
        description: tool.description,
        execute: (args) => bus.dispatch(tool.name, args, 'modelContext'),
      });
    }
  }
  return face;
}
