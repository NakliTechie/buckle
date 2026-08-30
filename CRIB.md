# CRIB — what was ported from breakscale, what was dropped

D13: crib breakscale's UI mechanics and glossary copy, not its skin. Every UI
element that exists in breakscale is ported from there; a from-scratch element
is a finding unless breakscale has no equivalent. Upstream: `xevrion/breakscale`
@ `dc9c1a0` (MIT).

## Ported (mechanics / copy kept, skin rebound to Dense tokens)

| Buckle | breakscale source | What was taken |
|---|---|---|
| `app/visuals.js` KIND_NAME | `src/components/nodeVisuals.ts` | the kind→label table, verbatim |
| `app/visuals.js` KIND_GROUPS | `src/components/nodeVisuals.ts` | the six-group taxonomy, verbatim |
| `app/kind-icons.json` | `nodeVisuals.ts` KIND_ICON + lucide-react@1.34.0 | the 33 Lucide `__iconNode` primitive lists, extracted and inlined (the authentic node glyphs) |
| `app/visuals.js` formatters | `src/components/format.ts` | formatMs/Rate/Count/Pct + health thresholds, behaviour verbatim |
| `app/glossary.json` | `src/content/glossary.ts` | all 100 glossary entries, verbatim (the teaching copy) |
| canvas node/edge render | `src/components/Canvas.tsx` | node-as-positioned-`<g>`, glyph painted into the parent SVG, edges as lines with state colour |
| inspector field layout | `src/components/Inspector.tsx` | per-field rows, provenance-chip affordance, inline edit |
| findings ↔ explanation | `src/content/glossary.ts` register | static explanation text keyed by kind (`app/explanations.js`), Buckle's copy in breakscale's voice |

## Dropped (the skin — D13 says strip it)

- The Caveat hand-drawn font, sketch strokes, and the "teaching" banner. Buckle
  is Dense: one mono-adjacent family, cool ink on near-black, one warm-red
  accent used only on shed/tripped/crashed state.
- React / Fast-Refresh scaffolding — everything is vanilla DOM/SVG.
- breakscale's palette drag-and-drop authoring, minimap, cost panel, vendor
  panel, saved-designs gallery, challenges — Buckle authors topologies by
  extraction from a repo, not by hand-drawing, so these have no equivalent here.

## From-scratch (breakscale has no equivalent — not a crib violation)

- `app/sweep-core.js` — the load sweep and findings (§4.5). breakscale runs one
  load point interactively; the sweep across a geometric RPS ladder and the
  ranked, deterministic findings are Buckle's reason to exist.
- `app/select.js`, `worker/worker.js` — repo ingest, untar, deterministic file
  selection. breakscale has no repo input.
- `app/extract-heuristic.js`, `app/extract-model.js` — compose→topology and the
  BYOK model path. New.
- provenance (READ/ASSUMED) throughout — Buckle's invariant I2, not in breakscale.
