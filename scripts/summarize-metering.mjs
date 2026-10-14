import fs from 'node:fs';
import path from 'node:path';

// 原始 Prometheus 快照和 Docker 采样生成报告数据，保留分母和观测范围。
function metrics(file) {
  return fs.readFileSync(file, 'utf8').split(/\r?\n/).flatMap(line => {
    const match = line.match(/^(\w+)(?:\{(.*)\})? ([-+\d.eE]+)$/);
    if (!match) return [];
    const labels = Object.fromEntries([...((match[2] ?? '').matchAll(/(\w+)="((?:\\.|[^"\\])*)"/g))].map(m => [m[1], JSON.parse(`"${m[2]}"`)]));
    return [{name:match[1],labels,value:Number(match[3])}];
  });
}
function sum(rows,name,filter={}) {
  return rows.filter(x=>x.name===name && Object.entries(filter).every(([k,v])=>x.labels[k]===v)).reduce((a,x)=>a+x.value,0);
}
function mean(rows,name,filter={},before=[]) {
  const count=sum(rows,`${name}_count`,filter)-sum(before,`${name}_count`,filter);
  return {count,mean_ms:count ? 1000*(sum(rows,`${name}_sum`,filter)-sum(before,`${name}_sum`,filter))/count : null};
}
function summarize(dir) {
  const gateways=[1,2].flatMap(i=>metrics(path.join(dir,`gateway-${i}-metrics.txt`)));
  const worker=metrics(path.join(dir,'worker-metrics.txt'));
  const stages=[...new Set(gateways.filter(x=>x.name==='gateway_metering_stage_seconds_count').map(x=>x.labels.stage))];
  const result={run:path.basename(dir),pressure:JSON.parse(fs.readFileSync(path.join(dir,'pressure.json'),'utf8')),fixed_arrival:JSON.parse(fs.readFileSync(path.join(dir,'sustained.json'),'utf8')),
    burst_overhead:mean(gateways,'gateway_overhead_seconds',{purpose:'inference'}),
    burst_gateway_stages:stages.flatMap(stage=>['started','completed','request','other'].map(kind=>({stage,kind,...mean(gateways,'gateway_metering_stage_seconds',{stage,kind,result:'ok'})}))).filter(x=>x.count),
    burst_worker_ledger:Object.fromEntries(['started','completed','request'].map(kind=>[kind,sum(worker,'gateway_metering_stage_seconds_count',{stage:'ledger_apply',kind,result:'ok'})])),
    scope:'Burst snapshots include earlier quota contests; histogram means are per instrumented operation, not HTTP percentiles.'};
  const afterFile=path.join(dir,'profile-5-after-role-0.prom');
  if (fs.existsSync(afterFile)) {
    const before=[0,1].flatMap(i=>metrics(path.join(dir,`profile-4-after-role-${i}.prom`)));
    const after=[0,1].flatMap(i=>metrics(path.join(dir,`profile-5-after-role-${i}.prom`)));
    const states=[0,1,2].map(i=>metrics(path.join(dir,`profile-5-after-role-${i}.prom`)));
    const resources={};
    for(const line of fs.readFileSync(path.join(dir,'profile-5-resources.jsonl'),'utf8').trim().split('\n')) {
      const snapshot=JSON.parse(line);
      for(const raw of snapshot.docker_stats.trim().split('\n')) {
        const item=JSON.parse(raw),name=item.Name.replace(path.basename(dir),'run');
        const [_,number,unit]=item.MemUsage.match(/^([\d.]+)(B|KiB|MiB|GiB)/);
        const mib=Number(number)*({B:1/1048576,KiB:1/1024,MiB:1,GiB:1024}[unit]);
        const v=resources[name]??={samples:0,cpu_percent_max:0,memory_mib_min:Infinity,memory_mib_max:0};
        v.samples++;v.cpu_percent_max=Math.max(v.cpu_percent_max,parseFloat(item.CPUPerc));v.memory_mib_min=Math.min(v.memory_mib_min,mib);v.memory_mib_max=Math.max(v.memory_mib_max,mib);
      }
    }
    result.five_minute={overhead:mean(after,'gateway_overhead_seconds',{purpose:'inference'},before),resources,
      sql_pool_waits:states.map(x=>sum(x,'gateway_sql_pool_wait_total')),
      scope:'2 RPS; Docker samples every 10 seconds. CPU percent is relative to one core; resource limits cover roles, not the shared host or middleware.'};
  }
  return result;
}
const [current,baseline]=process.argv.slice(2);
if(!current) throw Error('Usage: node scripts/summarize-metering.mjs CURRENT_RUN_DIR [BASELINE_RUN_DIR]');
console.log(JSON.stringify({current:summarize(current),...(baseline?{baseline:summarize(baseline)}:{})},null,2));
