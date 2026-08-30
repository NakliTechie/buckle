# Buckle

**An architecture linter with numbers.** Point it at a GitHub repo. It pulls the
code, reads the shape, builds a queueing model of the runtime topology, and loads
it until it buckles — then tells you which component gave way first and why.

Live: **https://buckle.naklitechie.com**

*Teaching-grade fidelity, stated as such.* The sim is a queueing model (finite
server slots, gamma service times, FIFO queues, real timeouts and retries), not a
capacity planner. It tells you *which* component knees first and *why*,
directionally right at the shape level. It does not tell you your Black Friday RPS.

## How it works

```
GitHub URL ─► edge worker: fetch tarball, untar, deterministic file select (≤40 files, ≤200 KB)
                                        │
                    extract topology (compose read, or BYOK model)  ─ every field READ or ASSUMED
                                        │
                        validateTopology (one ingress) ─► staged graph ─► you accept
                                        │
              engine (Web Worker) ─ breakscale src/sim, vendored, deterministic, seeded
                                        │
                    sweep 1→10⁴ rps × 20 steps ─► ranked findings (knee · retry-amp · spof · p99-cliff · collapse)
                                        │
                    export: one JSON (topology · provenance · findings · seed) ─ replays without the tool
```

- **Findings come from the engine's sweep, never a model.** The model (optional,
  BYOK) only proposes the graph; the engine grades it. Findings are deterministic
  and replayable from the export.
- **No key needed.** A `docker-compose` file is evidence: Buckle reads it into a
  topology with READ provenance, no model involved. Paste JSON or load a preset too.
- **Sovereign.** The repo is public code, fetched by Buckle's own edge worker;
  only the selected topology slice is ever handed to your model, and the tab shows
  it first. BYOK keys and GitHub tokens are memory-only, never stored.

## Two doors, one core

Every UI action is a command on one manifest, exposed to agents as `window.buckle`
(WebMCP primary). `window.buckle.manifest` lists the tools: `load_repo`,
`load_topology`, `load_preset`, `accept_graph`, `set_param`, `set_load`,
`apply_chaos`, `run_sweep`, `get_findings`, `get_snapshot`, `export`.

## Build

```bash
npm install
node tools/build.mjs        # → dist/index.html (single file)
npx wrangler dev            # local: serves the app + /api/analyze
npx wrangler deploy         # → buckle.naklitechie.com
```

Gates (fresh-context, exit 0/1): `tools/gate-c1.sh` (engine), `node
tools/check-sweep.mjs` (sweep sanity + determinism), `node tools/check-validate.mjs`,
`node tools/parity-lint.mjs`.

## Engine

The discrete-event engine is **vendored verbatim** from
[breakscale](https://github.com/xevrion/breakscale) (`src/sim`, MIT) at a pinned
commit — see `engine/VENDOR.md`. Buckle never patches it (I4); it adds ingest,
extraction, the sweep, findings, and a Dense vanilla UI over it. UI mechanics and
glossary copy are cribbed from breakscale (`CRIB.md`); the skin is Buckle's.

MIT for the vendored engine; attribution retained in the app header.
