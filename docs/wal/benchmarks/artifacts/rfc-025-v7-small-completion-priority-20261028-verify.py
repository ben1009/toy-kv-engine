#!/usr/bin/env python3
"""Reconstruct the candidate, validate fixed captures, and check its final disposition."""
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
STEM = 'rfc-025-v7-small-completion-priority-20261028'
WORK = ROOT / 'target/wal-opt/20261028-v7-small-completion-priority'
CHANGED = 'kv-engine/src/wal/parallel/worker.rs'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(suffix):
    return json.loads((ART / (STEM + suffix)).read_text())


def main():
    plan, env, identity = read('-plan.json'), read('-environment.json'), read('-binaries.json')
    candidate = list(csv.DictReader((ART / (STEM + '-source-sha256.tsv')).open(), delimiter='\t'))
    baseline = list(csv.DictReader((ART / (STEM + '-baseline-sha256.tsv')).open(), delimiter='\t'))
    assert len(candidate) == len(baseline) == len({r['path'] for r in candidate}) == 128
    before = {r['path']: r['sha256'] for r in baseline}
    assert [r['path'] for r in candidate if r['sha256'] != before[r['path']]] == [CHANGED]
    scratch = WORK / 'verify-source'
    assert not scratch.exists()
    try:
        scratch.mkdir()
        for row in candidate:
            data = subprocess.check_output(['git', 'show', plan['source_base'] + ':' + row['path']], cwd=ROOT)
            assert hashlib.sha256(data).hexdigest() == before[row['path']]
            path = scratch / row['path']
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        subprocess.run(['patch', '--fuzz=0', '-p1', '-i', str(ART / (STEM + '-candidate.patch'))],
                       cwd=scratch, check=True, capture_output=True)
        assert all(sha(scratch / row['path']) == row['sha256'] for row in candidate)
    finally:
        if scratch.exists():
            shutil.rmtree(scratch)
    assert identity['baseline'] == plan['baseline']
    assert all(sha(ROOT / item['path']) == item['sha256'] for item in identity.values())
    assert env['source_base'] == plan['source_base'] and ' ext4 ' in env['filesystem']
    assert env['numa_nodes'] == ['node0'] and not env['allocator_environment_overrides']
    for key, suffix in [('source_manifest_sha256', '-source-sha256.tsv'), ('patch_sha256', '-candidate.patch'),
                        ('plan_sha256', '-plan.json'), ('matrix_sha256', '-matrix.json'), ('driver_sha256', '-run.py')]:
        assert env[key] == sha(ART / (STEM + suffix))
    prior_path = ART / plan['prior_verification']
    assert sha(prior_path) == env['prior_verification_sha256']
    prior = json.loads(prior_path.read_text())
    assert prior['retained_source_restored'] and prior['global_unique_invocations'] == plan['prior_global_invocations'] == 739
    assert prior['canonical_sha256'] == identity['baseline']['sha256']
    old_stem = plan['prior_verification'].removesuffix('-verification.json')
    for field, suffix in [('checks_sha256', '-checks.json'), ('decision_sha256', '-decision.json'), ('verifier_sha256', '-verify.py')]:
        assert prior[field] == sha(ART / (old_stem + suffix))

    data = subprocess.check_output([sys.executable, str(ART / (STEM + '-analyze.py'))], cwd=ROOT)
    assert data == (ART / (STEM + '-summary.json')).read_bytes()
    summary = json.loads(data)
    status, matrix = summary['status'], read('-matrix.json')
    assert status['all_planned_cells_complete'] and status['captured_cells'] == list(matrix['cells'])
    assert all(summary[c]['v7_passed'] for c in matrix['screen'] + matrix['followup'])
    assert not summary['ext4-point8-confirm']['v7_passed']
    assert not status['cancelled_cells'] and status['records'] == 64 and status['scored_records'] == 50 and status['warmup_records'] == 14
    matrix_ids = {run for cell in status['captured_cells'] for run in summary[cell]['run_ids']}
    combined = matrix_ids
    assert len(matrix_ids) == 64 and len(combined) == 64
    prior_ids = set()
    for p in ART.glob('rfc-025-v7-*.jsonl'):
        if p.name.startswith(STEM):
            continue
        for line in p.read_text().splitlines():
            r = json.loads(line)
            if isinstance(r, dict) and 'run_id' in r:
                prior_ids.add(r['run_id'])
    assert combined.isdisjoint(prior_ids)
    assert len(prior_ids) == prior['full_archive_unique_invocations']
    checks = read('-checks.json')
    assert checks['outside_sandbox'] and checks['tests'] == 1976 and checks['focused_tests'] == 50
    assert sha(ROOT / checks['validation_driver']['path']) == checks['validation_driver']['sha256']
    phases = {c['phase']: c for c in checks['checks']}
    for check in checks['checks']:
        assert check['exit_code'] == 0 and sha(ROOT / check['log']) == check['log_sha256']
    full = (ROOT / phases['check']['log']).read_text()
    assert re.search(r'1976 tests run: 1976 passed(?: \([^)]*\))?, 0 skipped', full) and 'FLAKY' not in full and 'error:' not in full
    focused = (ROOT / phases['focused']['log']).read_text()
    assert '50 tests run: 50 passed' in focused and 'skipping test' not in focused and 'ext filesystem unavailable' not in focused and 'FLAKY' not in focused
    assert 'io_worker_retires_mixed_async_waves_and_drains_close' in focused
    assert 'io_worker_retires_small_only_waves_and_drains_close' in focused
    assert 'small_completion_priority_tracks_mixed_group_retirement' in focused
    assert 'final spelling' in phases
    decision = read('-decision.json')
    assert decision['assessment_passed'] == status['captured_cells_pass']
    assert decision['source_retained'] == decision['assessment_passed']
    active_manifest = candidate if decision['source_retained'] else baseline
    assert all(sha(ROOT / row['path']) == row['sha256'] for row in active_manifest)
    canonical = identity['candidate' if decision['source_retained'] else 'baseline']['sha256']
    assert sha(ROOT / 'target/release/write-perf') == canonical == decision['canonical_sha256']
    if not decision['source_retained']:
        assert decision['source_restored'] and checks['restored_tests'] == 1973
        restored = (ROOT / phases['restored check']['log']).read_text()
        assert re.search(r'1973 tests run: 1973 passed(?: \([^)]*\))?, 0 skipped', restored) and 'error:' not in restored
        assert 'FLAKY' not in restored
    assert read('-peaks.json')['cells'] == {cell + '/' + part: c['per_arm'] for cell in status['captured_cells']
                                          for part, c in summary[cell]['comparisons'].items()}
    assert all(not (WORK / (cell + '-scratch')).exists() for cell in matrix['cells'])
    print(json.dumps(dict(source_base=plan['source_base'], reconstructed_inputs=128,
                          final_inputs_verified=128, summary_regenerates=True, all_seven_cells_complete=True,
                          new_completed_invocations=len(combined), matrix_records=64, scored_records=50,
                          global_unique_invocations=739 + len(combined),
                          full_archive_unique_invocations=len(prior_ids | combined),
                          inherited_interrupted_without_json=1, source_retained=decision['source_retained'],
                          retained_source_restored=decision.get('source_restored', False), canonical_sha256=canonical,
                          candidate_tests=1976, focused_tests=50, restored_tests=checks.get('restored_tests'),
                          decision_sha256=sha(ART / (STEM + '-decision.json')),
                          checks_sha256=sha(ART / (STEM + '-checks.json')),
                          verifier_sha256=sha(Path(__file__))), indent=2))


if __name__ == '__main__':
    main()
