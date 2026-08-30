#!/bin/bash
# Gate C1 — machine-checked, fresh-context (HANDOFF.md §5, §8). Exit 0/1.
#   1. tsc transpiles the vendored engine cleanly
#   2. the engine's own tests pass under Node on the transpiled files
#   3. engine.esm.js rebuilds and its snapshot (PRESETS[0], seed 42, 600
#      ticks) is byte-identical to the native-breakscale fixture
#   4. validateTopology passes all presets, rejects mangled input
#   5. parity lint: manifest ⊇ bus, C1 trio present
set -eo pipefail
cd "$(dirname "$0")/.."

echo "[1/5] transpile"
npx tsc -p tsconfig.engine.json

echo "[2/5] engine tests on transpiled files"
npx vitest run --dir build/sim 2>&1 | tail -6

echo "[3/5] bundle + snapshot vs native fixture"
npx esbuild engine/entry.ts --bundle --format=esm --outfile=engine.esm.js --log-level=warning
node tools/check-snapshot.mjs

echo "[4/5] validateTopology"
node tools/check-validate.mjs

echo "[5/5] parity lint"
node tools/parity-lint.mjs

echo "GATE C1: PASS"
