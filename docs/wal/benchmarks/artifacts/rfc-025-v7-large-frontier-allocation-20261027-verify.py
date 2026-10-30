#!/usr/bin/env python3
"""Reconstruct the rejected candidate and verify retained source and SSD evidence."""
import csv
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-large-frontier-allocation-20261027'
WORK = ROOT / 'target/wal-opt/20261027-v7-large-frontier-allocation'
CHANGED = 'kv-engine/src/wal/parallel/runtime.rs'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(suffix):
    return json.loads((ART / (STEM + suffix)).read_text())


def main():
    plan, env, identity = read('-plan.json'), read('-environment.json'), read('-binaries.json')
    manifest = list(csv.DictReader((ART / (STEM + '-source-sha256.tsv')).open(), delimiter='\t'))
    assert len(manifest) == len({r['path'] for r in manifest}) == 128
    scratch = WORK / 'verification-source'
    assert not scratch.exists()
    baseline = {}
    try:
        scratch.mkdir()
        for row in manifest:
            data = subprocess.check_output(['git', 'show', plan['source_base'] + ':' + row['path']], cwd=ROOT)
            baseline[row['path']] = hashlib.sha256(data).hexdigest()
            p = scratch / row['path']
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(data)
        subprocess.run(['patch', '--fuzz=0', '-p1', '-i', str(ART / (STEM + '-candidate.patch'))],
                       cwd=scratch, check=True, capture_output=True)
        assert all(sha(scratch / r['path']) == r['sha256'] for r in manifest)
    finally:
        if scratch.exists():
            shutil.rmtree(scratch)
    assert [r['path'] for r in manifest if r['sha256'] != baseline[r['path']]] == [CHANGED]
    assert all(sha(ROOT / path) == digest for path, digest in baseline.items())
    assert identity['baseline'] == plan['baseline']
    assert all(sha(ROOT / item['path']) == item['sha256'] for item in identity.values())
    assert sha(ROOT / 'target/release/write-perf') == identity['baseline']['sha256']
    assert env['source_base'] == plan['source_base'] and not env['allocator_environment_overrides']
    assert ' ext4 ' in env['filesystem']
    for key, suffix in [('source_manifest_sha256', '-source-sha256.tsv'), ('patch_sha256', '-candidate.patch'),
                        ('plan_sha256', '-plan.json'), ('matrix_sha256', '-matrix.json'), ('driver_sha256', '-run.py')]:
        assert env[key] == sha(ART / (STEM + suffix))
    prior_path = ART / plan['prior_verification']
    assert sha(prior_path) == env['prior_verification_sha256']
    prior = json.loads(prior_path.read_text())
    assert prior['global_unique_invocations'] == plan['prior_global_invocations'] == 525
    assert not prior['source_changed'] and prior['canonical_sha256'] == identity['baseline']['sha256']
    data = subprocess.check_output([sys.executable, str(ART / (STEM + '-analyze.py'))], cwd=ROOT)
    assert data == (ART / (STEM + '-summary.json')).read_bytes()
    summary = json.loads(data)
    status, matrix = summary['status'], read('-matrix.json')
    assert status['captured_cells'] == ['ext4-batch16']
    assert status['cancelled_cells'] == matrix['followup']
    assert not status['all_planned_cells_complete'] and not status['captured_cells_pass']
    screen = summary['ext4-batch16']['stall_filter']
    assert screen['matched_pairs'] == [1, 4, 5]
    assert screen['excluded_by_arm'] == dict(old_v7=2, new_v7=0)
    assert screen['guards']['enough_matched_pairs'] and screen['guards']['identical_data_bytes']
    assert not any(screen['guards'][key] for key in ['paired_throughput', 'arm_median_throughput', 'paired_p99'])
    ids = set(summary['ext4-batch16']['run_ids'])
    assert len(ids) == status['records'] == 12 and status['scored_records'] == 10 and status['warmup_records'] == 2
    archive_ids = set()
    for path in ART.glob('rfc-025-v7-*.jsonl'):
        if path.name.startswith(STEM):
            continue
        for line in path.read_text().splitlines():
            record = json.loads(line)
            if isinstance(record, dict) and 'run_id' in record:
                archive_ids.add(record['run_id'])
    assert ids.isdisjoint(archive_ids)
    assert all(not (WORK / (cell + '-scratch')).exists() for cell in matrix['cells'])
    decision = read('-decision.json')
    assert not decision['assessment_passed'] and not decision['source_retained']
    assert decision['source_restored'] and decision['cpu_is_diagnostic'] and decision['source_candidates'] == 1
    assert decision['captured_cells'] == status['captured_cells'] and decision['cancelled_cells'] == status['cancelled_cells']
    assert decision['canonical_sha256'] == sha(ROOT / 'target/release/write-perf')
    assert decision['restored_source_sha256'] == sha(ROOT / CHANGED)
    assert read('-peaks.json')['cells'] == {cell + '/' + part: c['per_arm'] for cell in status['captured_cells']
                                          for part, c in summary[cell]['comparisons'].items()}
    checks = read('-checks.json')
    phases = {c['phase']: c for c in checks['checks']}
    assert checks['outside_sandbox'] and checks['tests'] == 1974 and checks['focused_tests'] == 20
    assert checks['restored_tests'] == 1972 and checks['candidate_retries'] == 0
    assert checks['restored_flaky_tests'] == ['tests::simple_leveled_compaction::test_integration']
    assert {'focused', 'format', 'check', 'release', 'direct spelling', 'restored check', 'final spelling'} <= set(phases)
    for check in checks['checks']:
        assert check['exit_code'] == 0 and sha(ROOT / check['log']) == check['log_sha256']
    focused = (ROOT / phases['focused']['log']).read_text()
    assert '20 tests run: 20 passed' in focused and 'skipping FRONTIER' not in focused and 'skipping test' not in focused
    candidate_log = (ROOT / phases['check']['log']).read_text()
    assert '1974 tests run: 1974 passed, 0 skipped' in candidate_log and 'FLAKY' not in candidate_log
    restored_log = (ROOT / phases['restored check']['log']).read_text()
    assert '1972 tests run: 1972 passed (1 flaky), 0 skipped' in restored_log
    assert 'FLAKY 2/4' in restored_log and 'tests::simple_leveled_compaction::test_integration' in restored_log
    assert all('error:' not in log for log in [candidate_log, restored_log])
    print(json.dumps(dict(source_base=plan['source_base'], reconstructed_candidate_inputs=128,
                          verified_restored_inputs=128, retained_source_restored=True,
                          verified_binaries=2, summary_regenerates=True,
                          records=12, scored_records=10, warmup_records=2,
                          global_unique_invocations=525 + len(ids),
                          full_archive_unique_invocations=len(archive_ids | ids),
                          filtered_pairs=3, excluded_scored_pairs=2, source_retained=False,
                          candidate_tests=1974, focused_tests=20, restored_tests=1972,
                          restored_flaky_tests=checks['restored_flaky_tests'], cpu_is_diagnostic=True,
                          canonical_sha256=identity['baseline']['sha256'],
                          prior_verification_sha256=sha(prior_path),
                          checks_sha256=sha(ART / (STEM + '-checks.json')),
                          decision_sha256=sha(ART / (STEM + '-decision.json')),
                          verifier_sha256=sha(Path(__file__))), indent=2))


if __name__ == '__main__':
    main()
