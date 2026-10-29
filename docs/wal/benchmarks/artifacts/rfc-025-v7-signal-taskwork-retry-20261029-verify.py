#!/usr/bin/env python3
"""Verify the independent retry, binary provenance, and exact baseline restoration."""
import csv
import hashlib
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-signal-taskwork-retry-20261029'
WORK = ROOT / 'target/wal-opt/20261029-v7-signal-taskwork-retry'
CHANGED = 'kv-engine/src/wal/parallel/worker.rs'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(suffix):
    return json.loads((ART / (STEM + suffix)).read_text())


def main():
    plan, env, identity = read('-plan.json'), read('-environment.json'), read('-binaries.json')
    baseline = list(csv.DictReader((ART / (STEM + '-baseline-sha256.tsv')).open(), delimiter='\t'))
    candidate = list(csv.DictReader((ART / (STEM + '-source-sha256.tsv')).open(), delimiter='\t'))
    assert len(baseline) == len(candidate) == len({r['path'] for r in candidate}) == 128
    before = {r['path']: r['sha256'] for r in baseline}
    assert [r['path'] for r in candidate if r['sha256'] != before[r['path']]] == [CHANGED]
    scratch = WORK / 'verify-source'
    assert not scratch.exists()
    try:
        scratch.mkdir()
        for row in baseline:
            data = subprocess.check_output(['git', 'show', plan['source_base'] + ':' + row['path']], cwd=ROOT)
            assert hashlib.sha256(data).hexdigest() == row['sha256']
            path = scratch / row['path']
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        subprocess.run(['patch', '--fuzz=0', '-p1', '-i', str(ART / (STEM + '-candidate.patch'))],
                       cwd=scratch, check=True, capture_output=True)
        assert all(sha(scratch / row['path']) == row['sha256'] for row in candidate)
    finally:
        if scratch.exists():
            shutil.rmtree(scratch)

    origin = plan['source_origin']
    for suffix in ['-candidate.patch', '-source-sha256.tsv', '-baseline-sha256.tsv', '-binaries.json']:
        assert (ART / (STEM + suffix)).read_bytes() == (ART / (origin + suffix)).read_bytes()
    assert sha(ART / (STEM + '-candidate.patch')) == plan['candidate_patch_sha256']
    assert identity['baseline'] == plan['baseline'] and identity['candidate'] == plan['candidate']
    assert all(sha(ROOT / item['path']) == item['sha256'] for item in identity.values())
    assert env['source_base'] == plan['source_base'] and ' ext4 ' in env['filesystem']
    assert env['numa_nodes'] == ['node0'] and not env['allocator_environment_overrides']
    for key, suffix in [('source_manifest_sha256', '-source-sha256.tsv'), ('patch_sha256', '-candidate.patch'),
                        ('plan_sha256', '-plan.json'), ('matrix_sha256', '-matrix.json'),
                        ('driver_sha256', '-run.py'), ('orchestrator_sha256', '-run-all.py'),
                        ('child_sha256', '-child.py'), ('analysis_sha256', '-analyze.py')]:
        assert env[key] == sha(ART / (STEM + suffix))
    prior_path = ART / plan['prior_verification']
    assert sha(prior_path) == env['prior_verification_sha256'] == plan['prior_verification_sha256']
    assert (WORK / 'parent-verification.json').read_bytes() == prior_path.read_bytes()
    prior = json.loads(prior_path.read_text())
    assert prior['global_unique_invocations'] == plan['prior_global_invocations'] == 857
    assert prior['full_archive_unique_invocations'] == plan['prior_full_archive_invocations'] == 4953
    assert prior['retained_source_restored'] and prior['canonical_sha256'] == identity['baseline']['sha256']
    for field, suffix in [('checks_sha256', '-checks.json'), ('decision_sha256', '-decision.json'),
                          ('verifier_sha256', '-verify.py')]:
        assert prior[field] == sha(ART / (origin + suffix))

    data = subprocess.check_output([sys.executable, str(ART / (STEM + '-analyze.py'))], cwd=ROOT)
    assert data == (ART / (STEM + '-summary.json')).read_bytes()
    summary = json.loads(data)
    status, matrix = summary['status'], read('-matrix.json')
    assert status['captured_cells'] == matrix['screen'] == ['ext4-batch16']
    assert status['cancelled_cells'] == matrix['followup']
    assert not status['all_planned_cells_complete'] and not status['captured_cells_pass']
    assert status['records'] == 12 and status['scored_records'] == 10 and status['warmup_records'] == 2
    filtered = summary['ext4-batch16']['stall_filter']
    assert filtered['matched_pairs'] == [1, 5] and not filtered['guards']['enough_matched_pairs']
    assert not filtered['guards']['paired_throughput'] and not filtered['guards']['arm_median_throughput']
    assert filtered['guards']['paired_p99'] and filtered['guards']['identical_data_bytes']
    ids = {run for cell in status['captured_cells'] for run in summary[cell]['run_ids']}
    assert len(ids) == 12
    other_ids = set()
    for path in ART.glob('rfc-025-v7-*.jsonl'):
        if path.name.startswith(STEM):
            continue
        for line in path.read_text().splitlines():
            record = json.loads(line)
            if isinstance(record, dict) and 'run_id' in record:
                other_ids.add(record['run_id'])
    assert ids.isdisjoint(other_ids)
    assert len(other_ids) >= plan['prior_full_archive_invocations']

    checks = read('-checks.json')
    assert checks['outside_sandbox'] and checks['tests'] == 1975 and checks['focused_tests'] == 49
    assert sha(ROOT / checks['validation_driver']['path']) == checks['validation_driver']['sha256']
    reused = checks['reused_binary_validation']
    assert reused['checks'] == origin + '-checks.json'
    assert sha(ART / reused['checks']) == reused['checks_sha256']
    assert sha(ART / (origin + '-source-sha256.tsv')) == reused['candidate_source_sha256']
    assert sha(ART / (origin + '-binaries.json')) == reused['binaries_sha256']
    old_checks = json.loads((ART / reused['checks']).read_text())
    release = next(c for c in old_checks['checks'] if c['phase'] == 'release')
    assert release['exit_code'] == 0 and sha(ROOT / release['log']) == release['log_sha256']
    phases = {c['phase']: c for c in checks['checks']}
    for check in checks['checks']:
        assert check['exit_code'] == 0 and sha(ROOT / check['log']) == check['log_sha256']
    for phase, count in [('check', 1975), ('restored check', 1973)]:
        text = (ROOT / phases[phase]['log']).read_text()
        assert re.search(rf'{count} tests run: {count} passed(?: \([^)]*\))?, 0 skipped', text)
        assert 'FLAKY' not in text and 'error:' not in text
    focused = (ROOT / phases['focused']['log']).read_text()
    assert '49 tests run: 49 passed' in focused and 'FLAKY' not in focused
    assert 'skipping test' not in focused and 'ext filesystem unavailable' not in focused
    assert 'io_worker_retires_mixed_async_waves_and_drains_close' in focused
    assert 'io_worker_retires_small_only_waves_and_drains_close' in focused
    assert 'final spelling' in phases and checks['restored_tests'] == 1973

    decision, restoration = read('-decision.json'), read('-restoration.json')
    assert decision['assessment'] == 'inconclusive' and not decision['assessment_passed']
    assert not decision['source_retained'] and decision['source_restored']
    assert all(sha(ROOT / row['path']) == row['sha256'] for row in baseline)
    canonical = sha(ROOT / 'target/release/write-perf')
    assert canonical == identity['baseline']['sha256'] == decision['canonical_sha256'] == restoration['canonical_sha256']
    assert restoration['restored_inputs'] == 128 and restoration['rust_mtime_refreshed']
    assert restoration['atomic_binary_replacement']
    assert restoration['baseline_manifest_sha256'] == sha(ART / (STEM + '-baseline-sha256.tsv'))
    assert restoration['candidate_worker_sha256'] == next(row['sha256'] for row in candidate if row['path'] == CHANGED)
    assert read('-peaks.json')['cells'] == {cell + '/' + part: c['per_arm']
                                          for cell in status['captured_cells']
                                          for part, c in summary[cell]['comparisons'].items()}
    assert all(not (WORK / (cell + '-scratch')).exists() for cell in matrix['cells'])
    print(json.dumps(dict(source_base=plan['source_base'], reconstructed_inputs=128, final_inputs_verified=128,
                          exact_prior_candidate_reused=True, summary_regenerates=True, first_stage_complete=True,
                          followups_cancelled=True, new_completed_invocations=len(ids), matrix_records=12,
                          scored_records=10, global_unique_invocations=plan['prior_global_invocations'] + len(ids),
                          full_archive_unique_invocations=plan['prior_full_archive_invocations'] + len(ids),
                          inherited_interrupted_without_json=prior['inherited_interrupted_without_json'],
                          source_retained=False, retained_source_restored=True, canonical_sha256=canonical,
                          candidate_tests=1975, focused_tests=49, restored_tests=1973,
                          decision_sha256=sha(ART / (STEM + '-decision.json')),
                          restoration_sha256=sha(ART / (STEM + '-restoration.json')),
                          checks_sha256=sha(ART / (STEM + '-checks.json')),
                          verifier_sha256=sha(Path(__file__))), indent=2))


if __name__ == '__main__':
    main()
