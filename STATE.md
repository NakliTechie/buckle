# STATE — v1.0 shipped

Date: 2026-08-30. C1–C6 complete. Live: **https://buckle.naklitechie.com**.

## Passed (`tools/gate-all.sh`, exit 0)

- **C1 engine** — breakscale `src/sim` vendored @ `dc9c1a0`, transpiled + bundled to
  `engine.esm.js`; 229/229 upstream tests on the transpiled files; snapshot
  byte-identical native-vs-bundle (`gate-c1.sh`).
- **validateTopology** — 23 presets pass, mangled input rejected; the one ingress (I3).
- **parity lint** — manifest (12 tools) ⊇ bus (11 commands); the two doors, one core.
- **C3 sweep** — four sanity fixtures + determinism + timing (`check-sweep.mjs`):
  retry-storm→amplification top, sharded→shard knee w/ healthy p50, breaker→no
  collapse, autoscaling→shed window; same-seed identical findings; 9-node sweep 1.7 s.
- **C4 select** — five named chatwoot config files selected, budgeted, deterministic,
  order-independent (`check-select.mjs`).
- **C5 sidecar** — compose→topology heuristic validates with resolving READ tags,
  every numeric field tagged; FakeTransport model path validates; key never imported
  into storage (`check-sidecar.mjs`).

## Live-verified on the deployed site (real repos, real edge worker)

- **chatwoot/chatwoot** (D12 target) — 9102 files → 5 named configs selected; 8-node
  topology from `docker-compose.yaml` (rails/sidekiq/postgres/redis/vite/mailhog);
  5 knee findings, sidekiq the first to knee.
- **dockersamples/example-voting-app** — 6 nodes; 11 findings, retry_amplification
  (worker→redis/db) ranked top; goodput collapse at 127/s.
- **mastodon/mastodon** — 6 nodes (web/streaming/sidekiq/postgres/redis); 9 findings,
  collapse at 207/s.
- **supabase/supabase** — graceful "repo too large (>500 MB)".
- **plausible/analytics** — graceful "no docker-compose; add a key or paste JSON".

## Architecture note (Chirag's change from the handoff)

The repo pull moved from in-tab (handoff §4.1) to the **edge worker** (`worker/worker.js`):
it fetches the GitHub tarball, gunzips + streams the tar (bounded memory, so chatwoot's
9102 files untar without OOM), runs the deterministic selector, and returns the tree +
selected slice. Sovereignty invariant kept in spirit — only the topology slice ever goes
to the person's model; the repos are public code fetched by Buckle's own edge.

## Deviations / open

- **Theme**: default is now **light** Dense (Chirag asked; too dark before); near-black
  is the toggle / system-dark. Handoff §6 specified dark-only; superseded by request.
- **Extraction default is the deterministic compose heuristic**, not a model (handoff
  §4.3 led with the model). Rationale: a compose file is READ evidence, so the no-key
  path is honest and the live site works with no BYOK. Model path (BYOK) is the richer
  optional tier and is built + gated (FakeTransport).
- Cold-open worked example is the `full-stack` preset with precomputed findings, not
  Chatwoot (D8/D12). Swapping in a bundled Chatwoot topology is a small follow-up.
- Enter-in-repo-field didn't fire under browser automation; the Run button is the
  verified path. Worth a manual check with a human keypress.
- O1–O3 (handoff §7): O1 answered (BYOK is the honest default, key-field copy says so);
  O2 taken verbatim from breakscale-style defaults; O3 (upstream WebMCP PR) deferred.

## Tried (nothing left broken)

- Buffered untar OOM'd on chatwoot (>90 MB) → rewrote as a true streaming parser with
  incremental drain; the 24 MB text budget starved `docker-compose.yaml` of content →
  high-value config files now captured unconditionally.
- collapse-by-unbounded-queue mis-detected (finite queueLimits shed instead of growing)
  → collapse now keys on timeout-share (a storm times out; a breaker fast-rejects).
