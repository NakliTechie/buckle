/**
 * Buckle's bundle entry over the vendored breakscale engine (engine/sim, I4:
 * vendored verbatim, never patched). Everything Buckle consumes from the
 * engine crosses this line and nothing else; esbuild bundles it to
 * engine.esm.js, the single plain-ESM file the shipped HTML inlines.
 */
export { Engine } from './sim/engine';
export { PRESETS, defaultConfig, makeNode } from './sim/presets';
export type {
  Topology,
  SimNode,
  SimEdge,
  NodeKind,
  NodeConfig,
  NodeStats,
  SystemStats,
  SimSnapshot,
  TrafficPattern,
  FailureKind,
  FailureOpts,
} from './sim/types';
export type { Preset } from './sim/presets';
