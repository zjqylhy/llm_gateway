import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import test from 'node:test';

test('merge preserves timeout evidence and accepts only matching, completed reruns', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'capacity-summary-'));
  try {
    const original = path.join(root, 'original');
    const resume = path.join(root, 'resume');
    const output = path.join(root, 'result.json');
    for (const [directory, run] of [[original, 'original'], [resume, 'resume']]) {
      const folder = path.join(directory, 'case-0', 'background-wait');
      fs.mkdirSync(folder, { recursive: true });
      fs.writeFileSync(path.join(directory, 'manifest.json'), JSON.stringify({ run, quick: true, seconds: 20, modes: ['background-wait'], binary_sha256: 'linux', host_binary_sha256: 'windows' }));
      const sample = { scope: 'application', application_mib: 8, global_mib: 1024, phase: 'confirm-1', offered_rps: 2, requests: 120, accepted: 120, accepted_p95_ms: 100, accepted_p99_ms: 120, peak_global_bytes: 1048576, peak_application_bytes: 1048576, peak_over_global_bytes: 0, peak_over_application_bytes: 0, meets_comparison_threshold: true };
      fs.writeFileSync(path.join(folder, 'capacity-results.json'), JSON.stringify([1, 2, 3].map(repeat => ({ ...sample, phase: `confirm-${repeat}` }))));
      fs.writeFileSync(path.join(folder, 'profile-0-resources.jsonl'), `${JSON.stringify({ docker_stats: JSON.stringify({ Name: 'test-gateway-1', CPUPerc: '150.5%', MemUsage: '1.5MiB / 256MiB' }) })}\n`);
      for (const role of [0, 1]) {
        fs.writeFileSync(path.join(folder, `case-0-confirm-1-2-before-role-${role}.prom`), '');
        fs.writeFileSync(path.join(folder, `case-0-confirm-1-2-after-role-${role}.prom`), 'gateway_metering_stage_seconds_count{stage="prepare",kind="started",result="ok"} 3\ngateway_metering_stage_seconds_count{stage="arm",kind="started",result="ok"} 2\n');
      }
    }
    const run = input => spawnSync(process.execPath, ['scripts/summarize-capacity.mjs', input, output], { encoding: 'utf8' });
    fs.writeFileSync(path.join(original, 'case-0/background-wait/test.log'), 'panic: test timed out after 1h35m0s\n');
    assert.equal(run(original).status, 0);
    let summary = JSON.parse(fs.readFileSync(output));
    assert.equal(summary.status, 'failed');
    assert.equal(summary.cases[0].failure, 'test_timeout');
    assert.equal(summary.cases[0].reported_peak_covers_completed_test, false);
    assert.equal(summary.cases[0].highest_three_run_confirmed_qps, null);

    fs.writeFileSync(path.join(resume, 'case-0/background-wait/test.log'), 'PASS\n');
    assert.equal(run(`${original},${resume}`).status, 0);
    summary = JSON.parse(fs.readFileSync(output));
    assert.equal(summary.status, 'complete');
    assert.equal(summary.cases[0].source_run, 'resume');
    assert.equal(summary.cases[0].reported_peak_covers_completed_test, true);
    assert.equal(summary.previous_failed_attempts[0].failure, 'test_timeout');
    assert.equal(summary.cases[0].highest_three_run_confirmed_qps, 2);
    assert.equal(summary.cases[0].resource_observations[0].peak_memory_bytes, 1.5 * 2**20);
    assert.equal(summary.cases[0].resource_observations[0].peak_cpu_percent, 150.5);
    assert.equal(summary.cases[0].samples[0].prepared_attempts, 6);
    assert.equal(summary.cases[0].samples[0].armed_attempts, 4);
    assert.equal(summary.cases[0].samples[1].prepared_attempts, null);

    const partialPath = path.join(resume, 'case-0/background-wait/capacity-results.json');
    const partial = JSON.parse(fs.readFileSync(partialPath));
    partial[2].sample_state = 'awaiting_drain';
    partial[2].drain_seconds = null;
    fs.writeFileSync(partialPath, JSON.stringify(partial));
    assert.equal(run(resume).status, 0);
    summary = JSON.parse(fs.readFileSync(output));
    assert.equal(summary.status, 'incomplete');
    assert.equal(summary.cases[0].highest_three_run_confirmed_qps, null);
    assert.equal(summary.cases[0].samples[2].accepted_per_http_plus_drain_second, null);
    assert.ok(summary.cases[0].samples[2].comparison_failures.includes('incomplete_load_sample'));

    fs.unlinkSync(path.join(resume, 'case-0/background-wait/capacity-results.json'));
    assert.equal(run(resume).status, 0);
    summary = JSON.parse(fs.readFileSync(output));
    assert.equal(summary.status, 'incomplete');
    assert.equal(summary.cases[0].failure, 'no_load_samples');

    const bad = JSON.parse(fs.readFileSync(path.join(resume, 'manifest.json')));
    bad.binary_sha256 = 'changed';
    fs.writeFileSync(path.join(resume, 'manifest.json'), JSON.stringify(bad));
    assert.notEqual(run(`${original},${resume}`).status, 0);
  } finally {
    assert.equal(path.dirname(fs.realpathSync(root)).toLowerCase(), fs.realpathSync(os.tmpdir()).toLowerCase());
    assert.match(path.basename(root), /^capacity-summary-/);
    fs.rmSync(root, { recursive: true, force: true });
  }
});
