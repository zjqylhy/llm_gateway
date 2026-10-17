import fs from 'node:fs';
import path from 'node:path';

const [input, output] = process.argv.slice(2);
if (!input || !output) {
  throw new Error('Usage: node scripts/summarize-capacity.mjs <run-directory> <output.json>');
}
const directory = path.resolve(input);
const manifest = JSON.parse(fs.readFileSync(path.join(directory, 'manifest.json'), 'utf8').replace(/^\uFEFF/, ''));

function readJson(file) {
  try { return JSON.parse(fs.readFileSync(file, 'utf8').replace(/^\uFEFF/, '')); }
  catch { return null; }
}

function metrics(file) {
  if (!fs.existsSync(file)) return null;
  const result = new Map();
  for (const line of fs.readFileSync(file, 'utf8').split('\n')) {
    if (line.startsWith('#')) continue;
    const match = line.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{.*\})?\s+(\S+)(?:\s+\S+)?$/);
    if (!match) continue;
    const value = Number(match[3]);
    if (!Number.isFinite(value)) continue;
    const labels = {};
    for (const label of (match[2] ?? '').matchAll(/([a-zA-Z_][a-zA-Z0-9_]*)="((?:\\.|[^"\\])*)"/g)) {
      labels[label[1]] = JSON.parse(`"${label[2]}"`);
    }
    const key = `${match[1]}|${JSON.stringify(Object.entries(labels).sort(([a], [b]) => a.localeCompare(b)))}`;
    result.set(key, { name: match[1], labels, value });
  }
  return result;
}

function telemetry(folder, index, row) {
  const prefix = `case-${index}-${row.phase}-${row.offered_rps}`;
  const stages = new Map();
  let overheadSum = 0;
  let overheadCount = 0;
  let poolWaits = 0;
  let poolWaitSeconds = 0;
  let available = 0;
  let invalidCounterDelta = false;
  const buckets = new Map();
  for (let role = 0; role < 2; role++) {
    const before = metrics(path.join(folder, `${prefix}-before-role-${role}.prom`));
    const after = metrics(path.join(folder, `${prefix}-after-role-${role}.prom`));
    if (!before || !after) continue;
    available++;
    for (const [key, point] of after) {
      if (!point.name.startsWith('gateway_metering_stage_seconds_') &&
          !point.name.startsWith('gateway_overhead_seconds_') &&
          !point.name.startsWith('gateway_sql_pool_wait')) continue;
      const delta = point.value - (before.get(key)?.value ?? 0);
      if (delta < -1e-9) { invalidCounterDelta = true; continue; }
      const increment = Math.max(0, delta);
      if (point.name === 'gateway_metering_stage_seconds_sum' || point.name === 'gateway_metering_stage_seconds_count') {
        const stageKey = JSON.stringify(point.labels);
        const stage = stages.get(stageKey) ?? { ...point.labels, sum: 0, count: 0 };
        if (point.name.endsWith('_sum')) stage.sum += increment;
        else stage.count += increment;
        stages.set(stageKey, stage);
      }
      if (point.labels.purpose === 'inference') {
        if (point.name === 'gateway_overhead_seconds_sum') overheadSum += increment;
        if (point.name === 'gateway_overhead_seconds_count') overheadCount += increment;
        if (point.name === 'gateway_overhead_seconds_bucket') {
          const bound = point.labels.le === '+Inf' ? Infinity : Number(point.labels.le);
          buckets.set(bound, (buckets.get(bound) ?? 0) + increment);
        }
      }
      if (point.name === 'gateway_sql_pool_wait_total') poolWaits += increment;
      if (point.name === 'gateway_sql_pool_wait_seconds_total') poolWaitSeconds += increment;
    }
  }
  function quantileBounds(fraction) {
    if (overheadCount === 0) return null;
    let lower = 0;
    for (const [upper, count] of [...buckets].sort(([a], [b]) => a - b)) {
      if (count >= overheadCount * fraction) {
        return { lower_exclusive_ms: lower * 1000, upper_inclusive_ms: Number.isFinite(upper) ? upper * 1000 : null };
      }
      lower = upper;
    }
    return null;
  }
  const workerBefore = metrics(path.join(folder, `${prefix}-before-role-2.prom`));
  const workerAfter = metrics(path.join(folder, `${prefix}-after-role-2.prom`));
  const workerAudit = { count: 0, seconds: 0, failures: 0 };
  if (workerBefore && workerAfter) {
    for (const [key, point] of workerAfter) {
      const delta = Math.max(0, point.value - (workerBefore.get(key)?.value ?? 0));
      if (point.name === 'gateway_metering_audit_seconds_count') workerAudit.count += delta;
      if (point.name === 'gateway_metering_audit_seconds_sum') workerAudit.seconds += delta;
      if (point.name === 'gateway_metering_audit_failures_total') workerAudit.failures += delta;
    }
  }
  return {
    gateway_roles_available: available,
    counter_reset_or_invalid_delta: invalidCounterDelta,
    overhead: {
      count: overheadCount,
      mean_ms: overheadCount ? overheadSum * 1000 / overheadCount : null,
      p50_bounds: quantileBounds(.5),
      p99_bounds: quantileBounds(.99),
    },
    sql_pool_waits: poolWaits,
    sql_pool_wait_ms: poolWaitSeconds * 1000,
    worker_audits: workerAudit,
    stages: [...stages.values()].filter(item => item.count > 0).map(({ sum, count, ...labels }) => ({
      ...labels, count, mean_ms: sum * 1000 / count,
    })).sort((a, b) => `${a.stage}/${a.kind}/${a.result}`.localeCompare(`${b.stage}/${b.kind}/${b.result}`)),
  };
}

const cases = [];
const capacityIndices = manifest.quick ? [0] : [0, 1, 2, 3, 4, 5];
for (const index of capacityIndices) {
  for (const mode of manifest.modes) {
    const folder = path.join(directory, `case-${index}`, mode);
    const rows = readJson(path.join(folder, 'capacity-results.json'));
    if (!rows?.length) continue;
    const log = fs.existsSync(path.join(folder, 'test.log')) ? fs.readFileSync(path.join(folder, 'test.log'), 'utf8') : '';
    const rates = new Map();
    for (const row of rows) {
      if (!row.phase.startsWith('confirm-')) continue;
      const samples = rates.get(row.offered_rps) ?? [];
      samples.push(row);
      rates.set(row.offered_rps, samples);
    }
    const confirmed = [...rates].filter(([, samples]) => samples.length === 3 && samples.every(row => row.meets_comparison_threshold));
    const highest = confirmed.length ? Math.max(...confirmed.map(([rate]) => rate)) : null;
    const chosen = highest ? rates.get(highest) : [];
    const first = rows[0];
    cases.push({
      index, mode, scope: first.scope,
      applications: first.scope === 'application' ? 1 : 8,
      application_mib: first.application_mib, global_mib: first.global_mib,
      test_passed: /^PASS\s*$/m.test(log),
      test_failed: /^--- FAIL:/m.test(log),
      highest_three_run_confirmed_qps: highest,
      confirmed_requests: chosen.reduce((sum, row) => sum + row.requests, 0),
      confirmed_accepted: chosen.reduce((sum, row) => sum + row.accepted, 0),
      confirmed_worst_p95_ms: chosen.length ? Math.max(...chosen.map(row => row.accepted_p95_ms)) : null,
      confirmed_worst_p99_ms: chosen.length ? Math.max(...chosen.map(row => row.accepted_p99_ms)) : null,
      peak_global_bytes: Math.max(...rows.map(row => row.peak_global_bytes)),
      peak_application_bytes: Math.max(...rows.map(row => row.peak_application_bytes)),
      peak_over_global_bytes: Math.max(...rows.map(row => row.peak_over_global_bytes)),
      peak_over_application_bytes: Math.max(...rows.map(row => row.peak_over_application_bytes)),
      sql_explain: readJson(path.join(folder, 'capacity-explain.json')),
      backpressure: readJson(path.join(folder, 'capacity-backpressure.json')),
      samples: rows.map(row => {
        const reasons = [];
        if (row.accepted < .99 * row.requests) reasons.push('success_rate_below_99_percent');
        if (row.failed) reasons.push('protocol_failures');
        if (row.scheduler_dropped) reasons.push('client_scheduler_drops');
        if (row.accepted_p95_ms > 500) reasons.push('p95_above_500_ms');
        if (row.accepted_p99_ms > 1000) reasons.push('p99_above_1000_ms');
        if (row.drain_seconds > 5) reasons.push('drain_above_5_seconds');
        if (row.second_half_mean_bytes > row.first_half_mean_bytes + (1 << 20)) reasons.push('backlog_growing');
        return { ...row, comparison_failures: reasons, telemetry: telemetry(folder, index, row) };
      }),
    });
  }
}

const expected = capacityIndices.length * manifest.modes.length;
const summary = {
  run: manifest.run,
  status: cases.length === expected && cases.every(item => item.test_passed) ? 'complete' : cases.some(item => item.test_failed) ? 'failed' : 'incomplete',
  manifest,
  expected_cases: expected,
  collected_cases: cases.length,
  cases,
  scope: {
    upstream: 'controlled mock, no external model API calls',
    capacity_semantics: 'locked mode uses transactional counters; background mode permits transient multi-instance overshoot',
    latency: 'client success percentiles are empirical order statistics; rejected, dropped and failed calls are counted separately',
    overhead: 'attempt time minus upstream read/forward time; excludes request-summary publication and is not direct-vs-gateway A/B',
    telemetry: 'before/after per-process counter deltas; includes resource-sampling/drain boundary work; stage means have different denominators and must not be summed',
    occupancy: 'observed positive occupancy sampled about every 200 ms; peaks and over-limit durations are sampled observations',
    sql_statements: 'capacity-lock operation counters are available; complete SQL statement and commit totals are not collected',
    limits: 'shared host and middleware; finite controlled loads do not establish production capacity or SLO',
  },
};
fs.writeFileSync(path.resolve(output), `${JSON.stringify(summary, null, 2)}\n`, 'utf8');
console.log(JSON.stringify({ run: summary.run, status: summary.status, collected: cases.length, expected }));
