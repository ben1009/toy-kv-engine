#!/usr/bin/env python3
"""Reproduce the fixed comparison and check candidate ownership and disposition evidence."""
import csv
import gzip
import hashlib
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-post-submit-hash-20261029'
WORK = ROOT / 'target/wal-opt/20261029-v7-post-submit-hash'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(suffix):
    return json.loads((ART / (STEM + suffix)).read_text())


def regenerate(script, output):
    data = subprocess.check_output([sys.executable, str(ART / (STEM + script))], cwd=ROOT)
    assert data == (ART / (STEM + output)).read_bytes()
    return json.loads(data)


def main():
    plan, env, outcome = read('-plan.json'), read('-environment.json'), read('-outcome.json')
    preflight = read('-preflight.json')
    parent_path = ART / preflight['prior_receipt']
    assert preflight['prior_verified'] and sha(parent_path) == preflight['prior_receipt_sha256']
    assert parent_path.read_bytes() == (WORK / 'parent-verification.json').read_bytes()
    assert json.loads(parent_path.read_text()) == preflight['prior']
    assert preflight['source_base'] == plan['source_base'] == '1dce85d892a2054455843bb29b7f80bc741204a9'
    before = list(csv.DictReader((ART / (STEM + '-baseline-sha256.tsv')).open(), delimiter='\t'))
    after = list(csv.DictReader((ART / (STEM + '-source-sha256.tsv')).open(), delimiter='\t'))
    assert len(before) == len(after) == 128
    baseline = {r['path']: r['sha256'] for r in before}
    assert {r['path'] for r in after if r['sha256'] != baseline[r['path']]} == {
        'kv-engine/src/wal/parallel/runtime.rs', 'kv-engine/src/chaos/failpoint.rs'}
    scratch = WORK / 'verify-source'
    assert not scratch.exists()
    try:
        scratch.mkdir()
        for row in before:
            data = subprocess.check_output(['git', 'show', plan['source_base'] + ':' + row['path']], cwd=ROOT)
            assert hashlib.sha256(data).hexdigest() == row['sha256']
            target = scratch / row['path']
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
        subprocess.run(['patch', '--fuzz=0', '-p1', '-i', str(ART / (STEM + '-candidate.patch'))], cwd=scratch, check=True, capture_output=True)
        assert all(sha(scratch / r['path']) == r['sha256'] for r in after)
    finally:
        if scratch.exists():
            shutil.rmtree(scratch)
    live = after if outcome['source_retained'] else before
    assert all(sha(ROOT / r['path']) == r['sha256'] for r in live)
    identity = read('-binaries.json')
    assert identity['v4'] == identity['retained-v7'] == plan['baseline']
    assert all(sha(ROOT / item['path']) == item['sha256'] for item in identity.values())
    expected = identity['candidate-v7' if outcome['source_retained'] else 'retained-v7']['sha256']
    assert sha(ROOT / 'target/release/write-perf') == outcome['canonical_sha256'] == expected
    assert ' ext4 ' in env['filesystem']
    for key, suffix in [('plan_sha256','-plan.json'),('matrix_sha256','-matrix.json'),('driver_sha256','-run.py'),
                        ('child_sha256','-child.py'),('source_manifest_sha256','-source-sha256.tsv'),
                        ('checks_sha256','-checks.json'),
                        ('analyzer_sha256','-analyze.py'),('orchestrator_sha256','-run-all.py')]:
        assert env[key] == sha(ART / (STEM + suffix))
    patch_notes = read('-patch-notes.json')
    original = ART / patch_notes['original']
    assert sha(original) == patch_notes['compressed_sha256']
    assert hashlib.sha256(gzip.decompress(original.read_bytes())).hexdigest() == patch_notes['original_sha256'] == env['patch_sha256']
    assert sha(ART / (STEM + '-candidate.patch')) == patch_notes['applicable_patch_sha256']
    summary = regenerate('-analyze.py', '-summary.json')
    best = regenerate('-best.py', '-best.json')
    assert summary['screens_complete'] and summary['retention_passed'] == outcome['source_retained']
    if not summary['screen_passed']:
        assert list(summary['cells']) == plan['screen_cells']
        assert summary['missing_cells'] == plan['guard_cells']
    else:
        assert summary['guards_complete'] and not summary['missing_cells']
    ids = {run for c in summary['cells'].values() for run in c['run_ids']}
    assert len(ids) == summary['invocations']
    previous_ids = set()
    for path in ART.glob('rfc-025-v7-*.jsonl'):
        if path.name.startswith(STEM):
            continue
        for line in path.read_text().splitlines():
            record = json.loads(line)
            if isinstance(record, dict) and 'run_id' in record:
                previous_ids.add(record['run_id'])
    assert ids.isdisjoint(previous_ids) and len(previous_ids) >= preflight['prior']['full_archive_unique_invocations']
    checks = read('-checks.json')
    assert checks['outside_sandbox'] and checks['candidate_tests'] == 1975 and checks['focused_tests'] == 55
    for check in checks['checks']:
        assert check['returncode'] == 0 and sha(ROOT / check['log_path']) == check['log_sha256']
    for check in checks['initial_attempt']:
        assert sha(ROOT / check['log_path']) == check['log_sha256']
    assert [c['returncode'] for c in checks['initial_attempt']] == [0, 101]
    phases = {c['step']: c for c in checks['checks']}
    full = (ROOT / phases['check']['log_path']).read_text()
    assert re.search(r'1975 tests run: 1975 passed(?: \([^)]*\))?, 0 skipped', full)
    assert 'FLAKY' not in full and 'error:' not in full
    focused = (ROOT / phases['focused']['log_path']).read_text()
    assert '55 tests run: 55 passed' in focused and 'FLAKY' not in focused and 'skipping test' not in focused
    assert all(name in focused for name in ['failpoint_v7_post_submit_hash_keeps_data_unacknowledged_and_close_drains',
                                            'failpoint_v7_post_submit_hash_error_or_panic_wakes_waiters'])
    if not outcome['source_retained']:
        assert outcome['rust_mtimes_refreshed'] and outcome['atomic_binary_replacement']
        for check in outcome['restored_checks']:
            assert check['returncode'] == 0 and sha(ROOT / check['log_path']) == check['log_sha256']
        restored = next((ROOT / c['log_path']).read_text() for c in outcome['restored_checks'] if c['step'] == 'restored-check')
        assert re.search(r'1973 tests run: 1973 passed(?: \([^)]*\))?, 0 skipped', restored)
        spelling_start = restored.index('Running Task: check-typos')
        assert 'FLAKY' not in restored and 'error:' not in restored[:spelling_start]
        assert any(c['step'] == 'restored-typos' for c in outcome['restored_checks'])
        for check in outcome['preliminary_restored_checks']:
            assert sha(ROOT / check['log_path']) == check['log_sha256']
        parent_verifier = ART / preflight['prior_receipt'].replace('-verification.json', '-verify.py')
        assert subprocess.check_output([sys.executable, str(parent_verifier)], cwd=ROOT) == parent_path.read_bytes()
    assert all(not (WORK / (cell + '-scratch')).exists() for cell in read('-matrix.json')['cells'])
    print(json.dumps(dict(source_base=plan['source_base'], reconstructed_inputs=128, live_inputs_verified=128,
                          source_retained=outcome['source_retained'], new_completed_invocations=len(ids), scored_records=summary['scored_records'],
                          global_unique_invocations=preflight['prior']['global_unique_invocations'] + len(ids),
                          full_archive_unique_invocations=preflight['prior']['full_archive_unique_invocations'] + len(ids),
                          inherited_interrupted_without_json=1, screens_passed=summary['screen_passed'], guards_complete=summary['guards_complete'],
                          best_scores_verified=True, summaries_regenerate=True, candidate_tests=1975, focused_tests=55,
                          restored_tests=1973 if not outcome['source_retained'] else None, canonical_sha256=expected,
                          source_patch_sha256=sha(ART / (STEM + '-candidate.patch')), source_manifest_sha256=env['source_manifest_sha256'],
                          checks_sha256=env['checks_sha256'], summary_sha256=sha(ART / (STEM + '-summary.json')),
                          best_sha256=sha(ART / (STEM + '-best.json')), outcome_sha256=sha(ART / (STEM + '-outcome.json')),
                          verifier_sha256=sha(Path(__file__))), indent=2))


if __name__ == '__main__':
    main()
