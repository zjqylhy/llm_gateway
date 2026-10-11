import fs from 'node:fs';
import path from 'node:path';
import { parseAllDocuments, parse } from '../web/node_modules/yaml/dist/index.js';

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
const lite = parse(fs.readFileSync(path.join(root, 'deploy/lite/compose.yaml'), 'utf8'), { merge: true });
for (const name of ['milvus', 'minio', 'redis-cache', 'prometheus', 'grafana']) {
  if (lite.services[name]) throw Error(`lite must not start ${name}`);
}
for (const name of ['gateway', 'admin', 'worker', 'migrate', 'bootstrap']) {
  const service = lite.services[name];
  if (service.environment.GW_CACHE_ENABLED !== 'false' || service.mem_limit !== '256m' ||
      service.environment.GW_MYSQL_MAX_OPEN_CONNS !== '12' || service.environment.GW_REDIS_POOL_SIZE !== '8') {
    throw Error(`lite ${name}: cache or pool budget mismatch`);
  }
  if (service.environment.GW_MAX_ADMIN_REQUESTS !== '8' ||
      service.environment.GW_MAX_CONCURRENT_REQUESTS !== '2' ||
      service.environment.GW_MAX_METERING_INFLIGHT !== '2') {
    throw Error(`lite ${name}: console/business concurrency mismatch`);
  }
}
for (const name of ['mysql', 'redis', 'kafka', 'etcd', 'gateway', 'admin', 'worker']) {
  if (lite.services[name].ports) throw Error(`lite ${name}: unexpected host port`);
}
if (!lite.services.edge.profiles.includes('public') || !lite.services['edge-local'].profiles.includes('local')) {
  throw Error('lite public/local entrypoints must require explicit selection');
}
if (!lite.services.redis.command.join(' ').includes('--maxmemory-policy noeviction') ||
    !lite.services.redis.command.join(' ').includes('--appendfsync always')) throw Error('lite Redis durability changed');
console.log(`PASS: ${count} YAML documents; ${dashboard.panels.length} monitoring panels. Container and cluster integration must run separately.`);
