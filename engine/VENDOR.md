# Vendored engine — breakscale src/sim

- Upstream: https://github.com/xevrion/breakscale (MIT — see `engine/LICENSE`)
- Pinned commit: `dc9c1a07e573da5441c12e1a205ffe8678504dd5` (2026-08-30)
- Contents: `src/sim/*.ts` copied verbatim, tests included. 23 files.
- Invariant I4: these files are never patched here. Needed changes go in
  `upstream/` as proposed diffs and are re-vendored after they land.
- `engine/entry.ts` is Buckle's (not vendored): the bundle entry that re-exports
  the engine surface for `engine.esm.js`.

Re-vendor procedure: clone upstream at the new commit into `vendor-src/`
(gitignored), `cp vendor-src/src/sim/*.ts engine/sim/`, update the pin above,
re-run `tools/gate-c1.sh`.
