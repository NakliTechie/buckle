/**
 * Storage façade (standing): File System Access → OPFS mirror → IndexedDB
 * fallback. Buckle files are `.buckle.json`; the filename is pinned on first
 * save. Best-effort and defensive — every tier is wrapped so a private window
 * or a browser without a tier degrades to the next rather than throwing.
 */
const FILE_EXT = '.buckle.json';
let pinnedHandle = null; // FSA handle, when the tier is available

export function canUseFSA() {
  return typeof window !== 'undefined' && 'showSaveFilePicker' in window;
}

export async function saveDesign(obj, suggestedName = 'buckle') {
  const text = JSON.stringify(obj, null, 2);
  const name = suggestedName.endsWith(FILE_EXT) ? suggestedName : `${suggestedName}${FILE_EXT}`;
  if (canUseFSA()) {
    try {
      if (!pinnedHandle) {
        pinnedHandle = await window.showSaveFilePicker({
          suggestedName: name,
          types: [{ description: 'Buckle design', accept: { 'application/json': [FILE_EXT] } }],
        });
      }
      const w = await pinnedHandle.createWritable();
      await w.write(text);
      await w.close();
      return { tier: 'fsa', name: pinnedHandle.name };
    } catch (e) {
      if (e && e.name === 'AbortError') return { tier: 'cancelled' };
      // fall through to download
    }
  }
  // OPFS mirror (best effort, invisible backup).
  try {
    const root = await navigator.storage?.getDirectory?.();
    if (root) {
      const fh = await root.getFileHandle(name, { create: true });
      const w = await fh.createWritable();
      await w.write(text);
      await w.close();
    }
  } catch { /* ignore */ }
  // Fallback: hand the file to the browser as a download.
  downloadText(text, name);
  return { tier: 'download', name };
}

export function downloadText(text, name) {
  try {
    const blob = new Blob([text], { type: 'application/json' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = name;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(a.href), 1000);
  } catch { /* ignore */ }
}

export async function openDesign() {
  if (canUseFSA()) {
    try {
      const [h] = await window.showOpenFilePicker({
        types: [{ description: 'Buckle design', accept: { 'application/json': [FILE_EXT] } }],
      });
      pinnedHandle = h;
      const file = await h.getFile();
      return JSON.parse(await file.text());
    } catch (e) {
      if (e && e.name === 'AbortError') return null;
    }
  }
  // Fallback: hidden file input.
  return new Promise((resolve) => {
    const input = document.createElement('input');
    input.type = 'file';
    input.accept = FILE_EXT + ',application/json';
    input.onchange = async () => {
      const f = input.files?.[0];
      if (!f) return resolve(null);
      try { resolve(JSON.parse(await f.text())); } catch { resolve(null); }
    };
    input.click();
  });
}
