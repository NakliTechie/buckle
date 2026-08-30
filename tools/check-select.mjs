/**
 * Gate C4 (selection half): the deterministic file selector picks the right
 * tiers within budget and is order-independent. The live chatwoot pull is the
 * acceptance for the ingest half (README / STATE); this locks selection logic
 * without a network call. Exit 0/1.
 */
import { selectFiles, BUDGET } from '../app/select.js';

// A chatwoot-shaped synthetic tree: the five named config files plus noise.
const files = [
  { path: 'docker-compose.yaml', size: 1400, content: 'services:\n  rails:\n    image: chatwoot\n' },
  { path: 'config/sidekiq.yml', size: 300, content: ':queues:\n' },
  { path: 'config/database.yml', size: 500, content: 'default: &default\n' },
  { path: 'config/cable.yml', size: 200, content: 'development:\n  adapter: redis\n' },
  { path: 'config/routes.rb', size: 2000, content: 'Rails.application.routes.draw do\nend\n' },
  { path: 'app/models/user.rb', size: 4000, content: 'class User < ApplicationRecord\n  Redis.new\nend\n' },
  { path: 'app/assets/huge.js', size: 900000, content: null },
  { path: 'lib/tasks/retry.rake', size: 800, content: 'task :x do\n  timeout 5\n  retry\nend\n' },
  { path: 'README.md', size: 5000, content: '# chatwoot\n' },
  { path: 'node_modules/x/index.js', size: 100, content: 'module.exports={}' },
];

let failed = 0;
const check = (name, cond, detail = '') => { console.log(`  ${cond ? 'ok  ' : 'FAIL'} ${name}${detail ? ' — ' + detail : ''}`); if (!cond) failed = 1; };

const r = selectFiles(files);
const paths = r.selected.map((s) => s.path);
for (const need of ['docker-compose.yaml', 'config/sidekiq.yml', 'config/database.yml', 'config/cable.yml', 'config/routes.rb']) {
  check(`selects ${need}`, paths.includes(need));
}
check('within file budget', r.selected.length <= BUDGET.maxFiles, `${r.selected.length} files`);
check('within byte budget', r.selectedBytes <= BUDGET.maxBytes, `${r.selectedBytes} bytes`);
check('skips node_modules', !paths.some((p) => p.startsWith('node_modules/')));
check('orchestration tiered above source', r.selected[0].tier === 1);

// Determinism: shuffled input → identical selection.
const shuffled = [...files].reverse();
const r2 = selectFiles(shuffled);
check('order-independent (deterministic)', JSON.stringify(r.selected) === JSON.stringify(r2.selected));

process.exit(failed);
