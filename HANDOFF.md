# Buckle — Vision, Roadmap, Agent Handoff

**Tier: Tool.** Single HTML file, sovereign, ships and is maintained. One doc (this one).
Sidecar + Edge-First apply. Forward-pass once at ship. Design: timeline check throughout,
one rubric pass at ship. Guide generated at ship.

*Point it at a repo. It pulls the code, reads the shape, builds a queueing model, and loads
it until it buckles — then tells you which component gave way first and why.*

---

## 1. Vision

Most architecture review is diagrams and rules of thumb. Buckle runs the experiment. A GitHub
URL goes in; a discrete-event simulation of that codebase's runtime topology comes out, with
a ranked list of bottlenecks the engine found by sweeping load, and sliders to explore them.

**What it is.** An architecture linter with numbers. Teaching-grade fidelity, stated as such:
the sim is a queueing model (finite server slots, gamma service times, FIFO queues, real
timeouts and retries), not a capacity planner. It tells you *which* component knees first and
*why*, directionally right at the shape level. It does not tell you your Black Friday RPS.

**What it is not.** Not a load tester (nothing is hit). Not a profiler. Not a capacity
planner — until service times are fitted from telemetry (v1.1), every service-time number is
a default and is labelled as one.

**Audience.** Engineers reviewing a system they didn't write, PR reviewers, anyone who has
said "add a cache" without being able to say what breaks when the hit rate drops. Sovereign:
the repo is pulled into the tab, read there, and never leaves except the topology-carrying
slice sent to the person's own model or key.

**Engine.** Vendored from breakscale (`xevrion/breakscale`, MIT): the `src/sim` discrete-event
engine — headless, deterministic, seeded, no DOM. Buckle adds ingest, extraction, sweep,
findings, and a vanilla UI over it. Everything the engine already does, Buckle does not
reimplement (Build Doctrine: drive the engine).

**Unity sentence.** *An instrument panel for watching a system fail: dense, exact, nothing
decorative.* Direction: **Dense** (Berkeley). Budgets: up to 6 type styles, lines are
structure, one accent working on state (red = shed/tripped, nothing else).

---

## 2. Locked decisions

| # | Decision | Reason |
|---|---|---|
| D1 | Single HTML, no build step at ship | House shape. Engine transpiled once (tsc, strip types) to a plain ESM string and inlined; the one-time transpile is a dev step, not a ship step. |
| D2 | Engine vendored verbatim, schema adopted verbatim | Code wins. Read `src/sim/*.ts` and use its topology types and field names exactly. Do not invent a parallel schema. |
| D3 | Bottlenecks come from the engine sweep, never the model | The AI proposes the graph; the engine grades it. Findings are deterministic and replayable. |
| D4 | No-AI state is first-class | Paste topology JSON, draw the graph, load a preset — sweep and findings run with no key configured. |
| D5 | Every node and every number carries provenance: `READ` (file + line) or `ASSUMED` (default id) | Wrong extraction must be visible before it becomes wrong findings. |
| D6 | Sidecar rides the Edge-First ladder; BYOK never persisted (VaultMind pattern) | Standing. GitHub token same rule: memory only. |
| D7 | Read budget for the model: ≤ 40 files, ≤ 200 KB total, topology-carrying files only | A monorepo is not a prompt. |
| D8 | Time-to-first-value ≤ 5 s: page opens on a worked example (bundled Chatwoot topology, precomputed findings); the live pull streams in behind it | Rule 2. |
| D9 | Export = one JSON: topology + provenance + assumptions + findings + seed. Replays without the tool and without a model | Closure lives in the artifact. |
| D10 | Agent face on the manifest from chunk 1; WebMCP primary, `window.buckle` polyfill; parity linted | Rule 6. Verifier drives the manifest. |
| D11 | Runtime target: Chrome floor, single seed per run, engine + sweep in a Web Worker | UI stays responsive during a 20-step sweep. |
| D12 | First target repo: `chatwoot/chatwoot` | docker-compose present; Rails + Sidekiq + Redis + Postgres + ActionCable; a known failure shape to check against. |
| D13 | Crib breakscale's UI elements, not its skin | MIT. Port `src/components` (SVG node/edge rendering, hand-drawn charts, inspector field layout, palette) and `src/content` (glossary) to vanilla; retheme through tokens. The Caveat hand-drawn look and banner are theirs — Buckle is Dense. Attribution kept in the file header. |

Scope fences: v1.0 = one repo at a time, GitHub only, HTTP request/response + queue/worker
topologies. **v1.1** (same spec, additive): service times fitted from an OTel/Prometheus
export; PR-diff mode (two commits → before/after findings); GitLab. **Out** for now: multi-
region, cost model, anything that hits the real system.

---

## 3. Invariants (spine before features)

- **I1 Determinism.** Same `(topology, seed, load)` → byte-identical `snapshot()`; the test
  suite asserts it, in Node and in the browser.
- **I2 Provenance.** Every field in the topology carries `{src: "READ", file, line}` or
  `{src: "ASSUMED", default: <id>}`. Schema validation rejects an untagged field.
- **I3 One ingress.** Model output, pasted JSON, and imported exports all pass one
  validator (`validateTopology`) before touching state. One door, checks coats.
- **I4 Engine untouched.** `src/sim` is vendored; Buckle never patches it. Needed changes go
  upstream as PRs and are re-vendored.
- **I5 Sidecar commit rule.** Extraction lands as a *staged* graph. Nothing runs until the
  person accepts (or the agent face calls `accept_graph` explicitly).
- **I6 Nothing leaves but the slice.** Only files selected by the read budget go to the
  model; the selection is shown before it's sent. Repo tarball is held in memory and dropped
  on tab close.
- **I7 Sweep is pure.** `sweep(topology, seed, loadSteps) → findings` is a pure function
  over engine snapshots; no UI state, no model.
- **I8 Version string** visible in UI and meta tag.

---

## 4. Architecture

```
GitHub URL ──► ingest (tarball fetch, in-tab) ──► file select (budget, deterministic rules)
                                                        │
                    ┌───────────────────────────────────┴───────────────┐
                    │ sidecar: extract → topology JSON (READ/ASSUMED)   │  ◄── Edge-First ladder
                    └───────────────────────────────────┬───────────────┘
                                                        ▼
                                   validateTopology  (one ingress)
                                                        │
                                   staged graph ──► person accepts / edits
                                                        │
                        ┌───────────────────────────────┼──────────────────────┐
                        ▼                               ▼                      ▼
                  canvas (SVG)                 engine (Worker)            sweep → findings
                  inspector · sliders          breakscale src/sim         ranked, deterministic
                        └───────────────────────────────┴──────────────────────┘
                                                        │
                                   export JSON (topology · provenance · findings · seed)
                     agent face: one manifest, WebMCP + window.buckle, parity-linted
```

Storage façade (standing): File System Access → OPFS mirror → IndexedDB fallback. Buckle
files are `.buckle.json`; filename pinned on first save.

### 4.1 Ingest

- Input: `https://github.com/<owner>/<repo>[/tree/<ref>]`. Default ref = default branch.
- Fetch `https://api.github.com/repos/<o>/<r>/tarball/<ref>` from the tab (CORS OK).
  Optional token header from memory-only field; public repos need none.
- Untar in a Worker (inline a small tar+gzip reader; no npm at ship). Hold file map in
  memory. Hard cap 50 MB uncompressed; over cap → ask before continuing.
- Show the file tree with the selected slice highlighted before anything is sent anywhere.

### 4.2 File selection (deterministic, no model)

Score files by path/name rules, take top-N within budget. Tiers, highest first:

1. `docker-compose*.yml`, `compose*.yml`, `Procfile*`, `k8s/**`, `helm/**`, `*.tf`,
   `fly.toml`, `render.yaml`, `app.json`, `Dockerfile*`
2. `config/database.yml`, `config/sidekiq.yml`, `config/cable.yml`, `config/redis*`,
   `config/queue.yml`, `config/puma.rb`, `config/initializers/{sidekiq,redis,http,faraday}*`
   (and the equivalents for Node/Python/Go/Java: `*.env.example`, `ormconfig*`,
   `settings.py`, `celery*.py`, `application.yml`)
3. Route tables: `config/routes.rb`, `routes/*.ts`, `urls.py`, `*.proto`
4. Client init sites: grep hits for `Redis.new|Sidekiq|ActiveJob|Faraday|HTTParty|Net::HTTP|
   axios|fetch\(|pg\.|Pool\(|celery|boto3|grpc` — one file per distinct client, shortest first
5. Retry/timeout config: grep `retry|timeout|circuit|backoff|rate_limit`

Selection is shown as a list with the tier that picked each file. Rules live in one table so
they are extendable without touching the extractor.

### 4.3 Sidecar extraction

- Ladder per Edge-First: L1 local runtime via `nakli-local-bridge` if detected → L2 WebGPU
  (Transformers.js, small model — quality floor likely not cleared; honest default is C1) →
  C1 BYOK. Detect, don't ask; only the key field is visible.
- Prompt (one file, `EXTRACT-PROMPT.md`, inlined): the engine's component catalogue and JSON
  schema; the selected files with paths; instruction to emit **only** topology JSON where
  every field carries provenance; unknown numbers → `ASSUMED` with the named default; never
  invent a component the files don't evidence. Ask for one JSON object, no prose.
- Defaults table (`DEFAULTS.md`, inlined, editable in UI): per component type, service-time
  mean and CV, capacity, instances, timeout, retries, queue limit. Each has an id so
  `ASSUMED` tags point at it.
- Output → `validateTopology` → staged. Validation failure is shown verbatim (loud), with a
  one-click "repair" that re-prompts with the validator's message. Max 2 repair rounds, then
  hand the person the raw output to edit.
- **FakeTransport** for offline dev/test: a recorded extraction for Chatwoot at a pinned
  commit, checked into fixtures. All extraction tests run against it; live calls are an
  opt-in test.

### 4.4 Surface — the UX brief (Tool tier: the first HTML is the mockup; build toward this)

**Crib first (D13).** Before drawing anything, read `src/components/` and `src/content/` in
breakscale. Port, don't reinvent: the SVG node glyphs and edge routing (`canvas`), the
direct-drawn charts (`metrics`), the inspector's field layout, the component palette, and the
glossary text keyed by metric/unit. Strip React to vanilla; strip the hand-drawn skin (Caveat,
banner, sketch strokes) and rebind every colour, size and weight to Buckle's tokens. What
survives the port is mechanics and copy; the look is Dense.

**Layout, 1440 floor.** Four regions; lines are structure, no boxes.

- **Top strip.** URL field (`/` focuses), ref, key icon opening the memory-only token/BYOK
  field, Run. Version string far right. Nothing else.
- **Left rail, 280.** *Findings*, ranked worst-first: kind · node · at N rps · one evidence
  number. Click → traffic slider jumps to `at_rps`, node selects, canvas pans. Before findings
  exist the rail shows the current run stage (below).
- **Centre.** Canvas. Directly beneath it one wide traffic slider with live readouts inline on
  the same line: rps · goodput · p99 · shed. The only slider always visible.
- **Right rail, 320.** Inspector for the selected node. Every field with its provenance chip:
  READ links to `file:line` in an in-tab viewer; ASSUMED opens the default it came from.
  Numbers edit inline; `s` re-sweeps. Chaos controls (crash · slow ×k · error % · cut link)
  live here per node — no global chaos panel.
- **Bottom strip, collapsible.** p50/p95/p99, goodput, per-node utilisation and queue depth as
  sparklines, drawn the breakscale way. Tufte: no gridlines, labels not legends.

**The run, as the left rail shows it.**

1. Cold open — a preset is already running, findings already ranked. The 5 s frame is this.
2. Ingest — file tree fills the rail, selected slice highlighted with the tier that picked it.
   Canvas keeps the preset running; nothing blanks.
3. Staged — extracted graph on the canvas *dimmed*; rail reads "14 nodes · 38 READ ·
   22 ASSUMED · Accept" — one primary action. Dim, never hide.
4. Accepted — graph goes full ink, sweep runs, findings replace the rail, elapsed shown.
5. Failure — validator message verbatim, raw model output in an editor, Repair or Edit.
   No key — sidecar chip dims; rail offers Paste JSON / Draw / Preset. Same rail, no separate
   empty state.

**Rules that carry the direction.** One accent, warm red, only on shed/tripped/crashed — the
canvas is grey until something breaks, so red means something. One mono-adjacent family, six
styles max, two weights minimum. Keyboard: `space` pause, `[ ]` slider, `s` sweep, `e` export,
`?` guide. No onboarding, no mascots. Engine runs continuously in the Worker; every number on
screen is the engine's own snapshot, nothing derived by the UI. Presets: breakscale's 23
examples ship inline so the page is useful with no repo and no key.

### 4.5 Sweep and findings (deterministic)

```
sweep(topology, seed, steps = geometric RPS from 1 to 10⁴, 20 steps, settle 60 s sim-time each)
  for each step: run engine to settle, snapshot
  per node:
    knee_rps      = first step where utilisation ≥ 0.80
    unbounded_rps = first step where queue depth grows monotonically across settle window
    shed_rps      = first step where shed > 0
  system:
    goodput_peak  = argmax goodput; collapse_rps = first step goodput < 50% of peak
    p99_cliff_rps = first step p99 > 5× p99 at step 1
  retry paths:
    amplification = arrivals at downstream / offered at upstream, at collapse_rps
  spof: for each node, crash it at goodput_peak load, measure goodput; spof if goodput < 10% of peak
findings = ranked list: [knee (asc knee_rps), retry_amplification (> 1.5), spof, p99_cliff, collapse]
```

Each finding: `{kind, node(s), at_rps, evidence: {metric, value}, explanation_id}`. Explanations
are static text keyed by `kind` (reuse breakscale's glossary), not generated. A finding is
clickable: it sets the traffic slider to `at_rps` and selects the node — the sliders are how
you *explore* a finding, the sweep is how you *get* one.

Sanity fixtures (must hold on breakscale's presets): Retry Storm → `retry_amplification` top;
Sharded Database → `knee` on one shard while system p50 is fine; Circuit Breaker → no
`collapse`; Autoscaling → `shed` window during boot gap.

### 4.6 Agent face (one manifest, two doors)

| tool | args | mutating |
|---|---|---|
| `load_repo` | `{url, ref?, token?}` | stages (nothing lands until `accept_graph`) |
| `load_topology` | `{topology}` | stages |
| `load_preset` | `{name}` | stages |
| `accept_graph` | `{}` | **yes** — person-confirmable; staged → active |
| `get_graph` | `{}` | no |
| `set_param` | `{node, field, value}` | yes, reversible (History) |
| `set_load` | `{rps}` | no |
| `apply_chaos` | `{node, kind, value}` | yes, reversible |
| `run_sweep` | `{seed?, steps?}` | no |
| `get_findings` | `{}` | no |
| `get_snapshot` | `{}` | no |
| `export` | `{}` | no |

Parity lint: manifest ⊇ command bus, failing test if a UI command lacks a manifest entry.
Attribution: every call logged with door (`modelContext` / `window` / channel) into History.
Cross-tab channel opt-in behind a developer setting. No person-only acts in v1.0.

---

## 5. Roadmap — continuous run, gate-terminated

Chunks run back to back; a green checkpoint means proceed immediately. Stop only for a
locked-decision conflict, a new dependency need, or scope ambiguity that changes the product.
No-progress exit: same failure 3× → write `TRIED.md`, escalate. Per-chunk budget: 3 h wall.

| Chunk | Deliverable | Checkpoint (machine-checked, fresh context) |
|---|---|---|
| **C1 Engine** | `src/sim` vendored at pinned commit; transpiled to `engine.esm.js`; inlined; presets inlined; `validateTopology` written against the engine's types | Engine's own tests pass under Node on the transpiled file; `snapshot()` for `PRESETS[0]`, seed 42, 600 ticks is byte-identical between native breakscale and the inlined engine (fixture checked in). Manifest scaffold with `load_preset`, `get_snapshot`, `set_load`; parity lint green. |
| **C2 No-AI surface** | `src/components` + `src/content` ported to vanilla per §4.4 (crib log in `CRIB.md`: what was ported, what was dropped); canvas, inspector, sliders, metrics panel, storage façade, export/import | Timeline capture: 0/5/30 s + failure, 5 s frame shows a running preset. Paste-JSON round-trips through export byte-identically. Lines/tints/density checks at Dense budgets. |
| **C3 Sweep + findings** | Worker sweep, findings schema, ranked panel, click-to-explore | Four sanity fixtures (§4.5) hold. `run_sweep` twice with same seed → identical findings. Sweep of a 15-node preset < 10 s on M4. |
| **C4 Ingest + select** | Tarball fetch, in-tab untar, budget selector, file tree with slice highlighted | `chatwoot/chatwoot` at pinned commit pulls, untars, selects ≤ 40 files / ≤ 200 KB, and the selection contains `docker-compose.yaml`, `config/sidekiq.yml`, `config/database.yml`, `config/cable.yml`, `config/routes.rb`. Selection is deterministic (fixture). |
| **C5 Sidecar** | Ladder detection, BYOK field (memory only), prompt, defaults table, staged graph + provenance chips, repair loop, FakeTransport fixture | Against FakeTransport: validator passes; graph contains nodes typed ≥ {web, worker (sidekiq), redis, postgres} each with ≥ 1 READ tag whose `file:line` resolves in the tarball; every numeric field tagged. Key never appears in any storage tier (grep OPFS/IDB after a run). Live BYOK run against Chatwoot yields a graph the same validator passes (recorded as the new fixture). |
| **C6 Ship** | Full manifest, guide (`?`), version string, forward-pass, rubric pass, README, portfolio entry | Parity lint green; `/forward-pass` list closed; timeline + four checks green; recognition test passes in critique; Cloudflare Pages deploy; `Chatwoot` bundled as the worked example with precomputed findings. |

Park walls: none. Only external input is the model, and FakeTransport covers it.

**v1.1 (same doc, later):** telemetry fit (paste OTel span export → per-node service-time
mean/CV, provenance `FITTED`), PR-diff mode (`load_repo` twice, findings diff), GitLab tarball.
Additive: no schema change except one new provenance kind.

---

## 6. Design verification

Floor viewport 1440×900; secondary 1024×768. Timeline captures via the `/guide` capture path
at every chunk that touches a surface. Direction Dense: alpha-ladder cool ink on near-black
canvas, one mono-adjacent family, two weights minimum, one accent (a warm red) used only for
shed/tripped/crashed state. Version string bottom-right. No onboarding, no empty-state
mascot; the empty state is a running preset.

---

## 7. Open decisions (close before C5)

- **O1** Default extraction model when local doesn't clear the floor — accept C1 BYOK as the
  honest default and say so in the key field copy? (Recommended: yes.)
- **O2** Defaults table values — take breakscale's per-component defaults verbatim, or
  re-derive? (Recommended: verbatim; they're already tuned to the engine.)
- **O3** Upstream: offer breakscale a PR adding a WebMCP manifest to their app, separate from
  Buckle. (Recommended: yes, after C6; small, and the author ships AGENTS.md.)

---

## 8. Agent instructions

- Read `src/sim/*.ts` in breakscale first. Its types are the schema. Code wins over this doc
  wherever they disagree; note the disagreement in `TRIED.md` and continue.
- Crib before you draw (D13). Every UI element that exists in breakscale is ported from
  there and logged in `CRIB.md`; a from-scratch element is a finding unless breakscale has
  no equivalent. MIT notice for breakscale sits in the HTML header.
- Do not modify vendored engine code (I4). If it needs a change, write the patch as a
  proposed upstream diff in `upstream/` and work around it locally without forking behaviour.
- One HTML file at ship. During dev you may keep `engine.esm.js` and the page separate; the
  ship step inlines and the checkpoint tests the inlined artifact, not the dev layout.
- Every model call goes through one function with a transport interface; FakeTransport is a
  first-class implementation, not a test hack.
- Nothing advances on self-report. Each checkpoint above is a script under `tools/` that
  exits 0 or 1; the verifier runs it in a fresh context.
- Write `STATE.md` at the end of every chunk: what passed, what's open, what was tried.
