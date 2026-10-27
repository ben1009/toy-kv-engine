#!/usr/bin/env python3
"""Verify both worker-limit variants, interrupted attempt, complete matrix and restoration."""
import csv
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-async-pool-limits-20261028'
OLD = 'rfc-025-v7-async-worker-limit-20261028'
WORK = ROOT / 'target/wal-opt/20261028-v7-async-pool-limits'
CHANGED = 'kv-engine/src/wal/parallel/worker.rs'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(stem, suffix):
    return json.loads((ART / (stem + suffix)).read_text())


def sources(stem):
    plan, env = read(stem, '-plan.json'), read(stem, '-environment.json')
    manifest = list(csv.DictReader((ART / (stem + '-source-sha256.tsv')).open(), delimiter='\t'))
    baseline = list(csv.DictReader((ART / (stem + '-baseline-sha256.tsv')).open(), delimiter='\t'))
    assert len(manifest) == len(baseline) == len({r['path'] for r in manifest}) == 128
    original = {r['path']: r['sha256'] for r in baseline}
    scratch = WORK / ('verify-' + stem)
    assert not scratch.exists()
    try:
        scratch.mkdir()
        for row in manifest:
            data = subprocess.check_output(['git', 'show', plan['source_base'] + ':' + row['path']], cwd=ROOT)
            assert hashlib.sha256(data).hexdigest() == original[row['path']]
            p = scratch / row['path']
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(data)
        subprocess.run(['patch', '--fuzz=0', '-p1', '-i', str(ART / (stem + '-candidate.patch'))],
                       cwd=scratch, check=True, capture_output=True)
        assert all(sha(scratch / row['path']) == row['sha256'] for row in manifest)
    finally:
        if scratch.exists():
            shutil.rmtree(scratch)
    assert [r['path'] for r in manifest if r['sha256'] != original[r['path']]] == [CHANGED]
    assert all(sha(ROOT / path) == digest for path, digest in original.items())
    identity = read(stem, '-binaries.json')
    assert identity['baseline'] == plan['baseline']
    assert all(sha(ROOT / item['path']) == item['sha256'] for item in identity.values())
    assert sha(ROOT / 'target/release/write-perf') == identity['baseline']['sha256']
    assert env['source_base'] == plan['source_base'] and ' ext4 ' in env['filesystem']
    assert not env['allocator_environment_overrides']
    for key, suffix in [('source_manifest_sha256', '-source-sha256.tsv'), ('patch_sha256', '-candidate.patch'),
                        ('plan_sha256', '-plan.json'), ('matrix_sha256', '-matrix.json'), ('driver_sha256', '-run.py')]:
        assert env[key] == sha(ART / (stem + suffix))
    prior_path = ART / plan['prior_verification']
    assert env['prior_verification_sha256'] == sha(prior_path)
    prior = json.loads(prior_path.read_text())
    assert prior['source_retained'] and prior['global_unique_invocations'] == plan['prior_global_invocations'] == 549
    assert prior['canonical_sha256'] == identity['baseline']['sha256']
    return plan, identity


def diagnostic(stem, kind, expected_count, expected_helpers):
    captures = read(stem, '-' + kind + '-captures.json')
    raw = {r['run_id']: r for line in (ART / (stem + '-' + kind + '-runs.jsonl')).read_text().splitlines()
           for r in [json.loads(line)]}
    assert len(captures) == len(raw) == expected_count
    for capture, expected in zip(captures, expected_helpers):
        assert capture['excluded_from_scoring'] and capture['clean_sampled'] and capture['returncode'] == 0
        assert not capture['sampling_errors']
        for path, digest in capture['files'].items():
            assert sha(ROOT / path) == digest
        record = raw[capture['run_id']]
        assert record['params']['threads'] == capture['writers'] and record['params']['wal_format_version'] == 7
        assert record['result']['ops'] == 750000 and record['params']['batch_size'] == 64
        paths = list(capture['files'])
        samples = [json.loads(line) for line in (ROOT / next(p for p in paths if p.endswith('-samples.jsonl'))).read_text().splitlines()]
        events = list(csv.DictReader((ROOT / next(p for p in paths if p.endswith('-io.csv'))).open()))
        frontiers = [e for e in events if e['kind'] == '2' and int(e['ticket_end']) > 0]
        inode = max(frontiers, key=lambda e: int(e['ticket_end']))['inode']
        frontiers = [e for e in frontiers if e['inode'] == inode]
        start = min(int(e['start_ns']) for e in frontiers)
        end = max(int(e['end_ns']) for e in frontiers)
        inside = [s for s in samples if start <= s['monotonic_ns'] <= end]
        counts = [sum(t['name'].startswith('iou-') for t in s['tasks'].values()) for s in inside]
        active = [sum(t['name'].startswith('iou-') and t['state'] in ['R', 'D'] for t in s['tasks'].values()) for s in inside]
        assert counts == capture['helper_counts'] and active == capture['active_helper_counts']
        assert max(counts) == capture['max_helpers'] == expected
        assert len(inside) == capture['active_window_samples']
        for sample in samples:
            if 'host_audit' in sample:
                assert not sample['host_audit']['competing']
        stdout = ROOT / next(p for p in paths if p.endswith('.stdout'))
        assert json.loads(stdout.read_text()) == record
    return set(raw)


def main():
    old_plan, old_identity = sources(OLD)
    plan, identity = sources(STEM)
    assert old_identity['baseline'] == identity['baseline']
    assert old_plan['source_base'] == plan['source_base']
    old_ids = diagnostic(OLD, 'diagnostic', 2, [70, 69])
    old_probe = diagnostic(OLD, 'probe', 1, [231])
    new_probe = diagnostic(STEM, 'probe', 1, [4])
    assert old_ids.isdisjoint(old_probe) and new_probe.isdisjoint(old_ids | old_probe)
    interrupted = read(OLD, '-interrupted.json')
    assert interrupted['completed_invocations'] == 5 and interrupted['completed_scored'] == 0
    assert interrupted['completed_warmups'] == 2 and interrupted['interrupted_invocations_without_json'] == 1
    assert interrupted['probe_returncode'] == 1 and interrupted['probe_max_helpers'] == 231
    for group in ['files', 'kernel_sources']:
        for path, digest in interrupted[group].items():
            assert sha(ROOT / path) == digest
    assert sha(ART / plan['interrupted_predecessor']) == plan['interrupted_predecessor_sha256']
    correction = read(OLD, '-attempts.json')
    assert sha(ROOT / correction['original_driver_path']) == correction['original_driver_sha256']
    assert sha(ART / (OLD + '-diagnostic-run.py')) == correction['corrected_driver_sha256']
    assert sha(ROOT / correction['check_log']) == correction['check_log_sha256']
    warm_lines = (ART / (OLD + '-ext4-batch16.jsonl')).read_bytes().splitlines()
    warm_rows = list(csv.DictReader((ART / (OLD + '-ext4-batch16-runs.tsv')).open(), delimiter='\t'))
    warm_audits = (ART / (OLD + '-ext4-batch16-host.jsonl')).read_bytes().splitlines()
    assert len(warm_lines) == len(warm_rows) == len(warm_audits) == 2
    warm_ids = set()
    for line, row, audit_line in zip(warm_lines, warm_rows, warm_audits):
        record, audit = json.loads(line), json.loads(audit_line)
        assert row['phase'] == 'warmup' and row['pair'] == '0'
        assert record['run_id'] == row['run_id'] == audit['run_id']
        assert hashlib.sha256(line).hexdigest() == row['raw_sha256']
        assert hashlib.sha256(audit_line).hexdigest() == row['audit_sha256']
        assert row['binary_sha256'] == old_identity['baseline' if row['arm'].startswith('old') else 'candidate']['sha256']
        assert audit['clean_sampled'] and audit['returncode'] == 0
        warm_ids.add(record['run_id'])
    data = subprocess.check_output([sys.executable, str(ART / (STEM + '-analyze.py'))], cwd=ROOT)
    assert data == (ART / (STEM + '-summary.json')).read_bytes()
    summary = json.loads(data)
    status, matrix = summary['status'], read(STEM, '-matrix.json')
    assert status['all_planned_cells_complete'] and status['captured_cells'] == list(matrix['cells'])
    assert not status['captured_cells_pass'] and not status['cancelled_cells']
    assert status['records'] == 36 and status['scored_records'] == 28 and status['warmup_records'] == 8
    assert summary['ext4-batch16']['stall_filter']['matched_pairs'] == [4, 5]
    assert all(summary[cell]['stall_filter']['matched_pairs'] == [1, 2, 3] for cell in matrix['followup'])
    point = summary['ext4-point8']['stall_filter']
    assert point['median']['throughput'] < .46 and point['median']['p99'] > 7.9
    assert not point['guards']['paired_throughput'] and not point['guards']['paired_p99']
    matrix_ids = {run for cell in status['captured_cells'] for run in summary[cell]['run_ids']}
    combined = [*old_ids, *old_probe, *warm_ids, *new_probe, *matrix_ids]
    assert len(combined) == len(set(combined)) == 42
    prior_ids = set()
    for p in ART.glob('rfc-025-v7-*.jsonl'):
        if p.name.startswith((OLD, STEM)):
            continue
        for line in p.read_text().splitlines():
            r = json.loads(line)
            if isinstance(r, dict) and 'run_id' in r:
                prior_ids.add(r['run_id'])
    assert set(combined).isdisjoint(prior_ids)
    for stem, full_error_count in [(OLD, 2), (STEM, 0)]:
        checks = read(stem, '-checks.json')
        assert checks['outside_sandbox'] and checks['tests'] == 1974 and checks['focused_tests'] == 48
        phases = {c['phase']: c for c in checks['checks']}
        for check in checks['checks']:
            assert check['exit_code'] == 0 and sha(ROOT / check['log']) == check['log_sha256']
        full = (ROOT / phases['check']['log']).read_text()
        assert '1974 tests run: 1974 passed, 0 skipped' in full and 'FLAKY' not in full
        assert full.count('error:') == full_error_count
        focused = (ROOT / phases['focused']['log']).read_text()
        assert '48 tests run: 48 passed' in focused and 'skipping test' not in focused
        if stem == OLD:
            assert 'artifact spelling' in phases
        else:
            assert checks['restored_tests'] == 1973 and 'final spelling' in phases
            restored = (ROOT / phases['restored check']['log']).read_text()
            assert '1973 tests run: 1973 passed, 0 skipped' in restored and 'FLAKY' not in restored
    decision = read(STEM, '-decision.json')
    assert not decision['source_retained'] and not decision['assessment_passed'] and decision['source_restored']
    assert decision['canonical_sha256'] == identity['baseline']['sha256']
    assert decision['restored_source_sha256'] == sha(ROOT / CHANGED)
    assert read(STEM, '-peaks.json')['cells'] == {cell + '/' + part: c['per_arm'] for cell in status['captured_cells']
                                               for part, c in summary[cell]['comparisons'].items()}
    assert all(not (WORK / (cell + '-scratch')).exists() for cell in matrix['cells'])
    retained = subprocess.check_output([sys.executable, str(ART / 'rfc-025-v7-large-data-async-retention-20261028-verify.py')], cwd=ROOT)
    assert retained == (ART / plan['prior_verification']).read_bytes()
    print(json.dumps(dict(source_base=plan['source_base'], reconstructed_variants=2,
                          reconstructed_inputs_per_variant=128, restored_inputs=128,
                          summary_regenerates=True, all_four_cells_complete=True,
                          new_completed_invocations=42, interrupted_without_json=1,
                          corrected_matrix_records=36, corrected_scored_records=28,
                          global_unique_invocations=549 + len(combined),
                          full_archive_unique_invocations=len(prior_ids | set(combined)),
                          source_retained=False, retained_source_restored=True,
                          canonical_sha256=identity['baseline']['sha256'], candidate_tests=1974,
                          focused_tests=48, restored_tests=1973, helpers_before=[70, 69],
                          initial_probe_helpers=231, corrected_probe_helpers=4,
                          decision_sha256=sha(ART / (STEM + '-decision.json')),
                          checks_sha256=sha(ART / (STEM + '-checks.json')),
                          interrupted_sha256=sha(ART / (OLD + '-interrupted.json')),
                          verifier_sha256=sha(Path(__file__))), indent=2))


if __name__ == '__main__':
    main()
