/**
 * Sweep Worker entry (D11: engine + sweep run off the main thread so the UI
 * stays responsive during a 20-step sweep). Bundled to an IIFE and inlined
 * into the single HTML; instantiated from a Blob URL at runtime.
 */
import { Engine } from '../engine/entry';
import { sweep, rpsLadder } from './sweep-core.js';

self.onmessage = (e) => {
  const { topology, seed = 42, steps = 20 } = e.data || {};
  try {
    const result = sweep(Engine, topology, { seed, steps });
    self.postMessage({ type: 'done', result });
  } catch (err) {
    self.postMessage({ type: 'error', message: err && err.message ? err.message : String(err) });
  }
};

// Keep rpsLadder reachable for callers that only want the ladder.
self.rpsLadder = rpsLadder;
