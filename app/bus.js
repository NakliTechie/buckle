/**
 * Command bus — every mutation or query of Buckle state is a named command
 * dispatched here, whichever door it came through (UI, WebMCP, window.buckle,
 * cross-tab channel). The parity lint (tools/parity-lint.mjs) fails if a
 * registered command lacks a manifest entry (D10: manifest ⊇ bus).
 *
 * Every dispatch is logged with its door into History (attribution, §4.6).
 */
export function createBus() {
  const commands = new Map();
  const history = [];

  return {
    register(name, { mutating, handler }) {
      if (commands.has(name)) throw new Error(`command "${name}" registered twice`);
      commands.set(name, { mutating, handler });
    },
    dispatch(name, args = {}, door = 'window') {
      const cmd = commands.get(name);
      if (!cmd) throw new Error(`unknown command "${name}"`);
      history.push({ name, args, door, mutating: cmd.mutating });
      return cmd.handler(args);
    },
    names() {
      return [...commands.keys()];
    },
    isMutating(name) {
      return commands.get(name)?.mutating ?? false;
    },
    history,
  };
}
