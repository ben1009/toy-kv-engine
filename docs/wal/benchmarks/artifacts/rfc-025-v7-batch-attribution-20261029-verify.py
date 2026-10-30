#!/usr/bin/env python3
"""Reproduce scores and CPU attribution, and verify complete source restoration."""
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
STEM = 'rfc-025-v7-batch-attribution-20261029'
WORK = ROOT / 'target/wal-opt/20261029-v7-batch-attribution'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(suffix):
    return json.loads((ART / (STEM + suffix)).read_text())


def regenerate(script, output):
    data = subprocess.check_output([sys.executable, str(ART / (STEM + script))], cwd=ROOT)
    assert data == (ART / (STEM + output)).read_bytes()
    return json.loads(data)


def main():
    plan, env, identity = read('-plan.json'), read('-environment.json'), read('-binaries.json')
    baseline = list(csv.DictReader((ART / (STEM + '-baseline-sha256.tsv')).open(), delimiter='\t'))
    diagnostic = list(csv.DictReader((ART / (STEM + '-source-sha256.tsv')).open(), delimiter='\t'))
    assert len(baseline) == len(diagnostic) == 128
    before = {row['path']: row['sha256'] for row in baseline}
    assert {row['path'] for row in diagnostic if row['sha256'] != before[row['path']]} == {
        'kv-engine/src/wal.rs', 'kv-engine/src/wal/parallel/runtime.rs', 'kv-engine/src/wal/parallel/worker.rs',
        'kv-engine/src/bin/write-perf.rs', 'kv-engine/src/mvcc.rs'}
    assert all(sha(ROOT / row['path']) == row['sha256'] for row in baseline)
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
        subprocess.run(['patch', '--fuzz=0', '-p1', '-i', str(ART / (STEM + '-instrumentation.patch'))],
                       cwd=scratch, check=True, capture_output=True)
        assert all(sha(scratch / row['path']) == row['sha256'] for row in diagnostic)
    finally:
        if scratch.exists():
            shutil.rmtree(scratch)
    assert identity['v4'] == identity['v7'] == plan['baseline']
    assert all(sha(ROOT / item['path']) == item['sha256'] for item in identity.values())
    diagnostic_identity = read('-diagnostic-binaries.json')
    assert diagnostic_identity['baseline'] == plan['baseline']
    assert sha(ROOT / diagnostic_identity['diagnostic']['path']) == diagnostic_identity['diagnostic']['sha256']
    assert env['source_base'] == plan['source_base'] and ' ext4 ' in env['filesystem']
    for field, suffix in [('baseline_manifest_sha256', '-baseline-sha256.tsv'), ('plan_sha256', '-plan.json'),
                          ('matrix_sha256', '-matrix.json'), ('driver_sha256', '-run.py'),
                          ('orchestrator_sha256', '-run-all.py'), ('child_sha256', '-child.py')]:
        assert env[field] == sha(ART / (STEM + suffix))
    prior_path = ART / plan['prior_verification']
    assert sha(prior_path) == plan['prior_verification_sha256'] == env['prior_verification_sha256']
    assert (WORK / 'parent-verification.json').read_bytes() == prior_path.read_bytes()
    prior_stem = plan['prior_verification'].removesuffix('-verification.json')
    assert subprocess.check_output([sys.executable, str(ART / (prior_stem + '-verify.py'))], cwd=ROOT) == prior_path.read_bytes()
    prior = json.loads(prior_path.read_text())
    assert prior['global_unique_invocations'] == plan['prior_global_invocations'] == 925
    assert prior['full_archive_unique_invocations'] == plan['prior_full_archive_invocations'] == 5021

    score = regenerate('-analyze.py', '-score-summary.json')
    assert score['status']['all_planned_cells_complete'] and score['status']['records'] == 16
    assert score['status']['scored_records'] == 12 and score['status']['warmup_records'] == 4
    best = regenerate('-best.py', '-best.json')
    assert best['scored_records'] == 66
    for cell, current in read('-score-peaks.json')['cells'].items():
        actual = best['current'][cell.removeprefix('ext4-')]
        for arm in ['v4', 'v7']:
            assert current['arms'][arm]['best']['run_id'] == actual['arms'][arm]['best']['run_id']
    trace = regenerate('-trace-analyze.py', '-trace-summary.json')
    assert len(trace['captures']) == 6 and sum(c['mapped_calls'] for c in trace['captures']) == 70317
    captures = read('-captures.json')
    expected = [('v4-w1', 1, 4, False), ('v7-w1', 1, 7, False), ('v7-w8', 8, 7, False),
                ('v4-w8', 8, 4, False), ('v7-init-w1', 1, 7, True), ('v7-init-w8', 8, 7, True)]
    assert [(c['name'], c['writers'], c['format'], c['initialize_large']) for c in captures] == expected
    for capture in captures:
        assert capture['excluded_from_scoring'] and capture['clean_sampled'] and not capture['sampling_errors']
        assert capture['binary'] == diagnostic_identity['diagnostic'] and capture['returncode'] == 0
        for path, digest in capture['files'].items():
            assert sha(ROOT / path) == digest
        metadata_path = next(ROOT / p for p in capture['files'] if p.endswith('-events.meta.json'))
        metadata = json.loads(metadata_path.read_text())
        assert metadata['dropped'] == 0 and metadata['events'] == capture['events']
    diagnostic_env = read('-diagnostic-environment.json')
    for field, suffix in [('driver_sha256', '-diagnostic-run.py'), ('analyzer_sha256', '-trace-analyze.py'),
                          ('source_manifest_sha256', '-source-sha256.tsv'), ('instrumentation_sha256', '-instrumentation.patch'),
                          ('ablation_plan_sha256', '-ablation-plan.json')]:
        assert diagnostic_env[field] == sha(ART / (STEM + suffix))
    for key in ['trace_library', 'native_source']:
        assert sha(ROOT / diagnostic_env[key]['path']) == diagnostic_env[key]['sha256']
    ids = {run for cell in score['status']['captured_cells'] for run in score[cell]['run_ids']}
    assert len(ids) == 16 and ids.isdisjoint({c['run_id'] for c in captures})
    ids.update(c['run_id'] for c in captures)
    assert len(ids) == 22
    other_ids = set()
    for path in ART.glob('rfc-025-v7-*.jsonl'):
        if path.name.startswith(STEM):
            continue
        for line in path.read_text().splitlines():
            row = json.loads(line)
            if isinstance(row, dict) and 'run_id' in row:
                other_ids.add(row['run_id'])
    assert ids.isdisjoint(other_ids) and len(other_ids) >= 5021

    checks = read('-checks.json')
    assert checks['outside_sandbox'] and checks['tests'] == checks['restored_tests'] == 1973
    assert checks['focused_tests'] == 79 and checks['full_check_automatic_retries'] == 1
    for check in checks['checks']:
        assert check['exit_code'] == 0 and sha(ROOT / check['log']) == check['log_sha256']
    for driver in checks['validation_drivers']:
        assert sha(ROOT / driver['path']) == driver['sha256']
    phases = {c['phase']: c for c in checks['checks']}
    full = (ROOT / phases['check']['log']).read_text()
    flaky = re.findall(r'FLAKY[^\n]*', full)
    assert len(flaky) == 1 and 'tests::simple_leveled_compaction::test_integration' in flaky[0]
    assert re.search(r'1973 tests run: 1973 passed(?: \([^)]*\))?, 0 skipped', full)
    isolated = (ROOT / phases['isolated-compaction']['log']).read_text()
    assert re.search(r'1 tests? run: 1 passed', isolated) and 'FLAKY' not in isolated
    focused = (ROOT / phases['focused']['log']).read_text()
    assert '79 tests run: 79 passed' in focused and 'FLAKY' not in focused and 'skipping test' not in focused
    restored = (ROOT / phases['restored check']['log']).read_text()
    assert re.search(r'1973 tests run: 1973 passed(?: \([^)]*\))?, 0 skipped', restored)
    assert 'FLAKY' not in restored and 'error:' not in restored
    assert 'final spelling' in phases
    restoration = read('-restoration.json')
    canonical = sha(ROOT / 'target/release/write-perf')
    assert canonical == restoration['canonical_sha256'] == plan['baseline']['sha256']
    assert restoration['restored_inputs'] == 128 and restoration['atomic_binary_replacement'] and restoration['rust_mtimes_refreshed']
    assert all(not (WORK / (cell + '-scratch')).exists() for cell in read('-matrix.json')['cells'])
    assert all(not (WORK / (c['name'] + '-db')).exists() for c in captures)
    print(json.dumps(dict(source_base=plan['source_base'], reconstructed_inputs=128, restored_inputs=128,
                          summaries_regenerate=True, best_scores_verified=True, mapped_calls=70317,
                          score_invocations=16, scored_records=12, diagnostic_invocations=6,
                          new_completed_invocations=22, global_unique_invocations=947, full_archive_unique_invocations=5043,
                          inherited_interrupted_without_json=1, source_retained=False, retained_source_restored=True,
                          canonical_sha256=canonical, diagnostic_tests=1973, focused_tests=79,
                          diagnostic_full_check_retries=1, isolated_compaction_passes=True, restored_tests=1973,
                          checks_sha256=sha(ART / (STEM + '-checks.json')),
                          restoration_sha256=sha(ART / (STEM + '-restoration.json')),
                          findings_sha256=sha(ART / (STEM + '-findings.json')),
                          verifier_sha256=sha(Path(__file__))), indent=2))


if __name__ == '__main__':
    main()
