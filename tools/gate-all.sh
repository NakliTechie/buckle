#!/bin/bash
# Run every machine-checked gate, fresh context. Exit 0 only if all pass.
set -eo pipefail
cd "$(dirname "$0")/.."

echo "== C1 engine =="        && ./tools/gate-c1.sh >/dev/null && echo "  C1 PASS"
echo "== validateTopology ==" && node tools/check-validate.mjs
echo "== parity lint =="       && node tools/parity-lint.mjs
echo "== C3 sweep =="          && node tools/check-sweep.mjs
echo "== C4 select =="         && node tools/check-select.mjs
echo "== C5 sidecar =="        && node tools/check-sidecar.mjs
echo "GATE-ALL: PASS"
