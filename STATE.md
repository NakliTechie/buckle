# STATE — after C1 (Engine)

Date: 2026-08-30. Chunk C1 of HANDOFF.md §5.

## Passed (gate: `tools/gate-c1.sh`, exit 0)

- Engine vendored verbatim: breakscale `src/sim` (23 files) at pinned commit
  `dc9c1a07e573da5441c12e1a205ffe8678504dd5` → `engine/sim/` (see `engine/VENDOR.md`).
- Transpile: `tsc -p tsconfig.engine.json` → `build/sim/`, 0 errors.
- Engine's own tests on the transpiled files: 229/229 pass (`npx vitest run --dir build/sim`).
- Bundle: `engine.esm.js` (single plain-ESM file, 231,713 bytes, esbuild unminified).
- Determinism fixture: `snapshot()` for PRESETS[0], seed 42, 600 ticks @60fps is
  byte-identical (8,108 bytes) between native breakscale TS (`tools/gen-native-fixture.test.ts`,
  needs vendor-src/) and the bundled engine (`tools/check-snapshot.mjs`).
- `validateTopology` (`app/validate.js`): all 23 presets pass; mangled input rejected
  with per-field errors; NODE_KINDS covers all 32 kinds the presets use (`tools/check-validate.mjs`).
- Manifest scaffold (`app/manifest.js` + `app/bus.js` + `app/runtime.js`):
  `load_preset` (stages) · `accept_graph` · `get_snapshot` · `set_load`; parity lint
  green (`tools/parity-lint.mjs`, manifest ⊇ bus).
- Gate negative-checked: corrupting the fixture makes it exit 1.

## Deviations from the handoff (noted per §8; none block)

- D1 says "transpiled once (tsc, strip types)". tsc alone cannot emit ONE file from 22
  modules, so the pipeline is tsc (type-strip fidelity, gate step 1-2) + esbuild
  `--bundle --format=esm` unminified (the single inlinable file, gate step 3). The
  byte-identical fixture is the proof the bundle didn't change behaviour.
- `engine/sim/presets.annotations.test.ts` is vendored but excluded from transpile/tests:
  it imports `../components/annotationLayout` (UI layout, outside the vendored engine
  boundary). Engine correctness is untouched; the other 6 test files run.
- Manifest carries `accept_graph` already (handoff lists it for the full table): a
  staging `load_preset` with no commit point would make `get_snapshot` unreachable.

## Open

- C2 next: port breakscale `src/components` + `src/content` to vanilla (CRIB.md),
  Dense retheme, canvas/inspector/sliders/metrics, storage façade, export/import.
- O1–O3 (HANDOFF.md §7) still open; close before C5. O4: chatwoot pin at C4.
- `window.buckle` / WebMCP door exists in `createAgentFace` but no page hosts it yet (C2).

## Tried (nothing rolled back)

- Gate negative check against `build/` failed by design — gate re-transpiles from
  source, healing it; moved the check to the fixture layer, which fails correctly.
