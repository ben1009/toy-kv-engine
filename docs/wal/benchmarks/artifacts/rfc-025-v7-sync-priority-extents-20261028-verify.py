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
STEM = 'rfc-025-v7-sync-priority-extents-20261028'
WORK = ROOT / 'target/wal-opt/20261028-v7-sync-priority-extents'
CHANGED = 'kv-engine/src/wal/parallel/runtime.rs'


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
    assert prior['retained_source_restored'] and prior['global_unique_invocations'] == plan['prior_global_invocations'] == 628
    assert prior['canonical_sha256'] == identity['baseline']['sha256']
    old_stem = plan['prior_verification'].removesuffix('-verification.json')
    for field, suffix in [('checks_sha256', '-checks.json'), ('decision_sha256', '-decision.json'), ('verifier_sha256', '-verify.py')]:
        assert prior[field] == sha(ART / (old_stem + suffix))

    probe, captures = read('-probe-plan.json'), read('-probe-captures.json')
    raw = {r['run_id']: r for line in (ART / (STEM + '-probe-runs.jsonl')).read_text().splitlines()
           for r in [json.loads(line)]}
    assert len(captures) == len(raw) == 1 and probe['binary'] == identity['candidate']
    assert probe['candidate_source_manifest_sha256'] == env['source_manifest_sha256']
    for entry in [probe['binary'], probe['trace_library']]:
        assert sha(ROOT / entry['path']) == entry['sha256']
    assert probe['trace_source_sha256'] == sha(ART / 'rfc-025-v7-writer-scaling-20261027-io-trace.c')
    kernel = json.loads((ART / env['kernel_source_provenance']).read_text())
    for path, digest in kernel['kernel_sources'].items():
        assert sha(ROOT / path) == digest
    capture = captures[0]
    assert capture['excluded_from_scoring'] and capture['clean_sampled'] and capture['returncode'] == 0
    assert not capture['sampling_errors'] and capture['binary'] == probe['binary']
    for path, digest in capture['files'].items():
        assert sha(ROOT / path) == digest
    record = raw[capture['run_id']]
    assert record['params']['threads'] == capture['writers'] == 8 and record['params']['wal_format_version'] == 7
    assert record['result']['ops'] == 750000 and record['params']['batch_size'] == 64
    paths = list(capture['files'])
    samples = [json.loads(line) for line in (ROOT / next(p for p in paths if p.endswith('-samples.jsonl'))).read_text().splitlines()]
    events = list(csv.DictReader((ROOT / next(p for p in paths if p.endswith('-io.csv'))).open()))
    frontiers = [e for e in events if e['kind'] == '2' and int(e['ticket_end']) > 0]
    inode = max(frontiers, key=lambda e: int(e['ticket_end']))['inode']
    frontiers = [e for e in frontiers if e['inode'] == inode]
    start, end = min(int(e['start_ns']) for e in frontiers), max(int(e['end_ns']) for e in frontiers)
    inside = [s for s in samples if start <= s['monotonic_ns'] <= end]
    counts = [sum(t['name'].startswith('iou-') for t in s['tasks'].values()) for s in inside]
    active = [sum(t['name'].startswith('iou-') and t['state'] in ['R', 'D'] for t in s['tasks'].values()) for s in inside]
    assert counts == capture['helper_counts'] and active == capture['active_helper_counts']
    assert max(counts) == capture['max_helpers']
    assert len(inside) == capture['active_window_samples']
    assert json.loads((ROOT / next(p for p in paths if p.endswith('.stdout'))).read_text()) == record
    for sample in [capture['before'], *[s['host_audit'] for s in samples if 'host_audit' in s], capture['after']]:
        assert not sample['competing']

    data = subprocess.check_output([sys.executable, str(ART / (STEM + '-analyze.py'))], cwd=ROOT)
    assert data == (ART / (STEM + '-summary.json')).read_bytes()
    summary = json.loads(data)
    status, matrix = summary['status'], read('-matrix.json')
    assert status['all_planned_cells_complete'] and status['captured_cells'] == list(matrix['cells'])
    assert not status['cancelled_cells'] and status['records'] == 36 and status['scored_records'] == 28 and status['warmup_records'] == 8
    matrix_ids = {run for cell in status['captured_cells'] for run in summary[cell]['run_ids']}
    combined = matrix_ids | set(raw)
    assert len(matrix_ids) == 36 and len(combined) == 37
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
    assert checks['outside_sandbox'] and checks['tests'] == 1975 and checks['focused_tests'] == 49
    phases = {c['phase']: c for c in checks['checks']}
    for check in checks['checks']:
        assert check['exit_code'] == 0 and sha(ROOT / check['log']) == check['log_sha256']
    full = (ROOT / phases['check']['log']).read_text()
    assert '1975 tests run: 1975 passed, 0 skipped' in full and 'FLAKY' not in full and 'error:' not in full
    focused = (ROOT / phases['focused']['log']).read_text()
    assert '49 tests run: 49 passed' in focused and 'skipping test' not in focused and 'FLAKY' not in focused
    assert 'async_v7_extent_sync_priority_defers_chunks_but_allows_demand_and_recovery' in focused
    assert 'async_v7_extent_close_does_not_wait_for_sync_priority' in focused
    assert 'final spelling' in phases
    diagnosis = subprocess.check_output([sys.executable, str(ART / (STEM + '-diagnose.py'))], cwd=ROOT)
    assert diagnosis == (ART / (STEM + '-diagnosis.json')).read_bytes()
    attempts = read('-attempts.json')
    identity_fix = attempts['post_validation_identity_correction']
    assert sha(ROOT / identity_fix['driver_path']) == identity_fix['driver_sha256']
    assert identity_fix['release_build_passed'] and identity_fix['tests'] == 1975
    for prefix, expected in [('initial', '49 tests run: 47 passed, 2 failed'), ('second', '49 tests run: 48 passed, 1 failed')]:
        assert expected in (WORK / (prefix + '-focused.log')).read_text()
    for files in [attempts['files'], attempts['second_attempt']['files']]:
        for path, digest in files.items():
            assert sha(ROOT / path) == digest
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
        assert re.search(r'1973 tests run: 1973 passed(?: \([^)]*\))?, 0 skipped', restored) and 'FLAKY' not in restored and 'error:' not in restored
    assert read('-peaks.json')['cells'] == {cell + '/' + part: c['per_arm'] for cell in status['captured_cells']
                                          for part, c in summary[cell]['comparisons'].items()}
    assert all(not (WORK / (cell + '-scratch')).exists() for cell in matrix['cells'])
    print(json.dumps(dict(source_base=plan['source_base'], reconstructed_inputs=128,
                          final_inputs_verified=128, summary_regenerates=True, all_four_cells_complete=True,
                          new_completed_invocations=len(combined), matrix_records=36, scored_records=28,
                          global_unique_invocations=628 + len(combined),
                          full_archive_unique_invocations=len(prior_ids | combined),
                          inherited_interrupted_without_json=1, source_retained=decision['source_retained'],
                          retained_source_restored=decision.get('source_restored', False), canonical_sha256=canonical,
                          candidate_tests=1975, focused_tests=49, restored_tests=checks.get('restored_tests'),
                          probe_max_helpers=capture['max_helpers'],
                          diagnosis_sha256=sha(ART / (STEM + '-diagnosis.json')),
                          decision_sha256=sha(ART / (STEM + '-decision.json')),
                          checks_sha256=sha(ART / (STEM + '-checks.json')),
                          verifier_sha256=sha(Path(__file__))), indent=2))


if __name__ == '__main__':
    main()
