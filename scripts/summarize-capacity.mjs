import fs from 'node:fs';
import path from 'node:path';

const [input, output] = process.argv.slice(2);
if (!input || !output) {
  throw new Error('Usage: node scripts/summarize-capacity.mjs <run-directory[,resume-directory]> <output.json>');
}
const directories = input.split(',').map(item => path.resolve(item));
const manifests = directories.map(directory => JSON.parse(fs.readFileSync(path.join(directory, 'manifest.json'), 'utf8').replace(/^\uFEFF/, '')));
const manifest = manifests[0];
const signature = item => JSON.stringify([
  item.binary_sha256, item.host_binary_sha256, item.seconds, item.quick,
  Boolean(item.auxiliary), Number(item.sustained_qps ?? 0), Number(item.confirm_qps ?? 0),
  Number(item.benchmark_driver_version ?? 1), Number(item.drain_observation_limit_seconds ?? 40),
]);
for (const item of manifests.slice(1)) {
  if (signature(item) !== signature(manifest)) {
    throw new Error('Cannot merge runs with different binaries or load parameters');
  }
}

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

function resources(folder, profile) {
  const roles = new Map();
  for (const file of fs.readdirSync(folder).filter(name => /^profile-\d+-resources\.jsonl$/.test(name))) {
    if (profile !== undefined && file !== `profile-${profile}-resources.jsonl`) continue;
    for (const line of fs.readFileSync(path.join(folder, file), 'utf8').split('\n').filter(Boolean)) {
      const sample = JSON.parse(line);
      for (const raw of sample.docker_stats.split('\n').filter(Boolean)) {
        const point = JSON.parse(raw);
        const role = point.Name.match(/-(gateway-[12]|worker|upstream)$/)?.[1];
        if (!role) continue;
        const current = roles.get(role) ?? { role, samples: 0, peak_cpu_percent: 0, peak_memory_bytes: 0 };
        const memory = point.MemUsage.match(/^([\d.]+)(B|kB|KB|KiB|MB|MiB|GB|GiB)\s/);
        const factors = { B: 1, kB: 1000, KB: 1000, KiB: 1024, MB: 1e6, MiB: 2**20, GB: 1e9, GiB: 2**30 };
        if (!memory || !Number.isFinite(Number.parseFloat(point.CPUPerc))) throw new Error(`Invalid resource sample: ${file}`);
        current.samples++;
        current.peak_cpu_percent = Math.max(current.peak_cpu_percent, Number.parseFloat(point.CPUPerc));
        current.peak_memory_bytes = Math.max(current.peak_memory_bytes, Number(memory[1]) * factors[memory[2]]);
        roles.set(role, current);
      }
    }
  }
  return [...roles.values()].sort((a, b) => a.role.localeCompare(b.role));
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
  const workerAudit = { available: Boolean(workerBefore && workerAfter), count: 0, seconds: 0, failures: 0 };
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

const attempts = [];
const expectedKeys = new Set();
for (const [sourceIndex, directory] of directories.entries()) {
const sourceManifest = manifests[sourceIndex];
const capacityIndices = sourceManifest.quick ? [0] : sourceManifest.case_indices ?? [0, 1, 2, 3, 4, 5];
for (const index of capacityIndices) {
  for (const mode of sourceManifest.modes) {
    expectedKeys.add(`${index}/${mode}`);
    const folder = path.join(directory, `case-${index}`, mode);
    const rows = readJson(path.join(folder, 'capacity-results.json'));
    const log = fs.existsSync(path.join(folder, 'test.log')) ? fs.readFileSync(path.join(folder, 'test.log'), 'utf8') : '';
    if (!rows?.length && !log) continue;
    const testPassed = /^PASS\s*$/m.test(log) && !/panic:|^--- FAIL:/m.test(log);
    const testFailed = /panic:|^--- FAIL:|^FAIL\s*$/m.test(log);
    if (!rows?.length) {
      attempts.push({ index, mode, source_run: sourceManifest.run, test_passed: false, test_failed: testFailed, failure: testFailed ? 'test_failed_before_samples' : 'no_load_samples', samples: [] });
      continue;
    }
    const completedTest = testPassed && rows.every(row => !row.sample_state || row.sample_state === 'complete');
    const rates = new Map();
    for (const row of rows) {
      if (!row.phase.startsWith('confirm-')) continue;
      const samples = rates.get(row.offered_rps) ?? [];
      samples.push(row);
      rates.set(row.offered_rps, samples);
    }
    const confirmed = [...rates].filter(([, samples]) => samples.length === 3 && samples.every(row => row.meets_comparison_threshold && (!row.sample_state || row.sample_state === 'complete')));
    const highest = completedTest && confirmed.length ? Math.max(...confirmed.map(([rate]) => rate)) : null;
    const chosen = highest ? rates.get(highest) : [];
    const first = rows[0];
    attempts.push({
      index, mode, source_run: sourceManifest.run, scope: first.scope,
      applications: first.scope === 'application' ? 1 : 8,
      application_mib: first.application_mib, global_mib: first.global_mib,
      test_passed: completedTest,
      test_failed: testFailed,
      failure: /panic: test timed out/.test(log) ? 'test_timeout' : /timeout: capacity drained/.test(log) ? 'drain_observation_timeout' : testFailed ? 'test_failed' : !completedTest ? 'incomplete_test' : null,
      reported_peak_covers_completed_test: completedTest,
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
      worker_restart: readJson(path.join(folder, 'capacity-worker-restart.json')),
      resource_observations: resources(folder),
      samples: rows.map((row, sampleIndex) => {
        const reasons = [];
        if (row.sample_state && row.sample_state !== 'complete') reasons.push('incomplete_load_sample');
        if (row.accepted < .99 * row.requests) reasons.push('success_rate_below_99_percent');
        if (row.failed) reasons.push('protocol_failures');
        if (row.scheduler_dropped) reasons.push('client_scheduler_drops');
        if (row.accepted_p95_ms > 500) reasons.push('p95_above_500_ms');
        if (row.accepted_p99_ms > 1000) reasons.push('p99_above_1000_ms');
        if (row.drain_seconds > 5) reasons.push('drain_above_5_seconds');
        if (row.second_half_mean_bytes > row.first_half_mean_bytes + (1 << 20)) reasons.push('backlog_growing');
        const observation = telemetry(folder, index, row);
        const stageCount = name => observation.gateway_roles_available === 2 && !observation.counter_reset_or_invalid_delta
          ? observation.stages.filter(item => item.stage === name && item.kind === 'started' && item.result === 'ok').reduce((total, item) => total + item.count, 0)
          : null;
        const httpAndDrain = Number.isFinite(row.wall_seconds) && Number.isFinite(row.drain_seconds) ? row.wall_seconds + row.drain_seconds : null;
        return { ...row, comparison_failures: reasons, http_plus_drain_seconds: httpAndDrain, accepted_per_http_plus_drain_second: httpAndDrain > 0 ? row.accepted / httpAndDrain : null, prepared_attempts: stageCount('prepare'), armed_attempts: stageCount('arm'), telemetry: observation, resource_observations: resources(folder, index * 100 + sampleIndex) };
      }),
    });
  }
}
}

const selected = new Map();
for (const attempt of attempts) {
  const key = `${attempt.index}/${attempt.mode}`;
  if (!selected.get(key)?.test_passed || attempt.test_passed) selected.set(key, attempt);
}
const cases = [...selected.values()].sort((a, b) => a.index-b.index || a.mode.localeCompare(b.mode));
const pending = [...expectedKeys].filter(key => !selected.get(key)?.test_passed);
const expected = expectedKeys.size;
const summary = {
  run: manifests.map(item => item.run).join(','),
  status: cases.length === expected && cases.every(item => item.test_passed) ? 'complete' : cases.some(item => item.test_failed) ? 'failed' : 'incomplete',
  manifest,
  manifests,
  expected_cases: expected,
  collected_cases: cases.length,
  pending_cases: pending,
  previous_failed_attempts: attempts.filter(item => item.test_failed && selected.get(`${item.index}/${item.mode}`) !== item),
  cases,
  scope: {
    upstream: 'controlled mock, no external model API calls',
    capacity_semantics: 'background permits transient multi-instance overshoot; historical comparison inputs may include retired transactional counters',
    latency: 'client success percentiles are empirical order statistics; rejected, dropped and failed calls are counted separately',
    overhead: 'attempt time minus upstream read/forward time; excludes request-summary publication and is not direct-vs-gateway A/B',
    telemetry: 'before/after per-process counter deltas; includes resource-sampling/drain boundary work; stage means have different denominators and must not be summed',
    occupancy: 'saved HTTP windows, including HTTP tails, sampled about every 200 ms; excludes subsequent drain; failed tests may not have saved all windows; over-limit durations are sampled estimates',
    sql_statements: 'historical inputs may include retired capacity-lock metrics; complete SQL statement and commit totals are not collected',
    limits: 'shared host and middleware; finite controlled loads do not establish production capacity or SLO',
  },
};
fs.writeFileSync(path.resolve(output), `${JSON.stringify(summary, null, 2)}\n`, 'utf8');
console.log(JSON.stringify({ run: summary.run, status: summary.status, collected: cases.length, expected }));
