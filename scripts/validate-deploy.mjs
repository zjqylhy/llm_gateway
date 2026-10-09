import fs from 'node:fs';
import path from 'node:path';
import { parseAllDocuments } from '../web/node_modules/yaml/dist/index.js';

const root = path.resolve(import.meta.dirname, '..');
let count = 0;
function inspect(dir) {
  for (const file of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, file.name);
    if (file.isDirectory()) { inspect(full); continue; }
    if (!/\.ya?ml$/.test(file.name)) continue;
    const docs = parseAllDocuments(fs.readFileSync(full, 'utf8'));
    for (const doc of docs) {
      if (doc.errors.length) throw Error(`${full}: ${doc.errors.join('\n')}`);
      const x = doc.toJS(); count++;
      if (x?.kind === 'Deployment') {
        const pod = x.spec.template.spec;
        if (x.spec.replicas < 2 || pod.terminationGracePeriodSeconds < 130 || pod.automountServiceAccountToken !== false) throw Error(`${full}: missing availability or security policy`);
        for (const c of pod.containers) {
          if (!c.readinessProbe || !c.livenessProbe || !c.resources?.limits || !c.securityContext?.readOnlyRootFilesystem) throw Error(`${full}: missing workload checks`);
        }
      }
    }
  }
}
inspect(path.join(root, 'deploy'));
inspect(path.join(root, '.github'));
for (const doc of parseAllDocuments(fs.readFileSync(path.join(root, 'compose.yaml'), 'utf8'))) { if(doc.errors.length) throw Error(doc.errors.join('\n')); count++; }
const dashboard = JSON.parse(fs.readFileSync(path.join(root,'deploy/observability/dashboard.json'),'utf8'));
if (dashboard.panels.length < 20) throw Error('monitoring coverage too small');
console.log(`PASS: ${count} YAML documents; ${dashboard.panels.length} monitoring panels. Container and cluster integration must run separately.`);
