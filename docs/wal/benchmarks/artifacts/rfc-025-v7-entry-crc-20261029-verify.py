#!/usr/bin/env python3
"""Verify source reconstruction, fixed captures, best scores, checks, and disposition."""
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
STEM = 'rfc-025-v7-entry-crc-20261029'
WORK = ROOT / 'target/wal-opt/20261029-v7-entry-crc'


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
    preflight, notes = read('-preflight.json'), read('-artifact-notes.json')
    parent_path = ART / 'rfc-025-v7-batch-attribution-20261029-verification.json'
    assert preflight['parent_verifier_reproduced'] and sha(parent_path) == preflight['parent_receipt_sha256']
    assert json.loads(parent_path.read_text()) == preflight['parent_receipt']
    assert preflight['source_base'] == plan['source_base'] == '00d65d82573e760a8e211abab713a3e57f943c54'
    before = list(csv.DictReader((ART / (STEM + '-baseline-sha256.tsv')).open(), delimiter='\t'))
    after = list(csv.DictReader((ART / (STEM + '-source-sha256.tsv')).open(), delimiter='\t'))
    assert len(before) == len(after) == 128
    initial = {r['path']: r['sha256'] for r in before}
    assert {r['path'] for r in after if r['sha256'] != initial[r['path']]} == {'kv-engine/src/pitr/mod.rs'}
    assert sha(ART / notes['applicable_patch']) == notes['applicable_patch_sha256']
    visual = ART / notes['original_visual_diff']
    assert sha(visual) == notes['compressed_sha256']
    assert hashlib.sha256(gzip.decompress(visual.read_bytes())).hexdigest() == notes['original_visual_diff_sha256'] == env['patch_sha256']
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
        subprocess.run(['patch', '--fuzz=0', '-p1', '-i', str(ART / notes['applicable_patch'])], cwd=scratch, check=True, capture_output=True)
        assert all(sha(scratch / row['path']) == row['sha256'] for row in after)
    finally:
        if scratch.exists():
            shutil.rmtree(scratch)
    live = after if outcome['source_retained'] else before
    assert all(sha(ROOT / r['path']) == r['sha256'] for r in live)
    identity = read('-binaries.json')
    assert identity['v4'] == identity['retained-v7'] == plan['baseline']
    assert all(sha(ROOT / item['path']) == item['sha256'] for item in identity.values())
    assert sha(ROOT / 'target/release/write-perf') == outcome['canonical_sha256']
    expected = identity['candidate-v7' if outcome['source_retained'] else 'retained-v7']['sha256']
    assert outcome['canonical_sha256'] == expected
    assert ' ext4 ' in env['filesystem']
    for key, suffix in [('plan_sha256','-plan.json'),('matrix_sha256','-matrix.json'),('driver_sha256','-run.py'),
                        ('child_sha256','-child.py'),('source_manifest_sha256','-source-sha256.tsv'),
                        ('checks_sha256','-checks.json'),('orchestrator_sha256','-run-all.py')]:
        assert env[key] == sha(ART / (STEM + suffix))
    parser = read('-parser-notes.json')
    assert sha(ART / parser['initial_analyzer']) == parser['initial_analyzer_sha256'] == env['analyzer_sha256']
    assert sha(ART / (STEM + '-analyze.py')) == parser['corrected_analyzer_sha256']
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
    others = set()
    for path in ART.glob('rfc-025-v7-*.jsonl'):
        if path.name.startswith(STEM):
            continue
        for line in path.read_text().splitlines():
            r = json.loads(line)
            if isinstance(r, dict) and 'run_id' in r:
                others.add(r['run_id'])
    assert ids.isdisjoint(others) and len(others) >= preflight['parent_receipt']['full_archive_unique_invocations']
    checks = read('-checks.json')
    for step in checks:
        assert step['returncode'] == 0 and sha(ROOT / step['log_path']) == step['log_sha256']
    steps = {c['step']: c for c in checks}
    full = (ROOT / steps['check']['log_path']).read_text()
    assert re.search(r'1974 tests run: 1974 passed(?: \([^)]*\))?, 0 skipped', full)
    assert 'FLAKY' not in full and 'error:' not in full
    focused = (ROOT / steps['focused']['log_path']).read_text()
    assert '19 tests run: 19 passed' in focused and 'FLAKY' not in focused and 'skipping test' not in focused
    if not outcome['source_retained']:
        assert outcome['rust_mtimes_refreshed'] and outcome['atomic_binary_replacement']
        for step in outcome['preliminary_restored_checks']:
            assert sha(ROOT / step['log_path']) == step['log_sha256']
        assert [c['returncode'] for c in outcome['preliminary_restored_checks']] == [0, 2]
        for step in outcome['restored_checks']:
            assert step['returncode'] == 0 and sha(ROOT / step['log_path']) == step['log_sha256']
        full = next((ROOT / c['log_path']).read_text() for c in outcome['restored_checks'] if c['step'] == 'restored-check')
        assert re.search(r'1973 tests run: 1973 passed(?: \([^)]*\))?, 0 skipped', full)
        assert 'FLAKY' not in full and 'error:' not in full
        assert subprocess.check_output([sys.executable, str(ART / 'rfc-025-v7-batch-attribution-20261029-verify.py')], cwd=ROOT) == parent_path.read_bytes()
    assert all(not (WORK / (cell + '-scratch')).exists() for cell in read('-matrix.json')['cells'])
    print(json.dumps(dict(source_base=plan['source_base'], reconstructed_inputs=128, source_retained=outcome['source_retained'],
                          live_inputs_verified=128, new_completed_invocations=len(ids), scored_records=summary['scored_records'],
                          global_unique_invocations=preflight['parent_receipt']['global_unique_invocations'] + len(ids),
                          full_archive_unique_invocations=preflight['parent_receipt']['full_archive_unique_invocations'] + len(ids),
                          inherited_interrupted_without_json=1, screens_passed=summary['screen_passed'],
                          guards_complete=summary['guards_complete'], best_scores_verified=True, summaries_regenerate=True,
                          candidate_tests=1974, focused_tests=19, restored_tests=1973 if not outcome['source_retained'] else None,
                          canonical_sha256=expected, summary_sha256=sha(ART / (STEM + '-summary.json')),
                          best_sha256=sha(ART / (STEM + '-best.json')), outcome_sha256=sha(ART / (STEM + '-outcome.json')),
                          verifier_sha256=sha(Path(__file__))), indent=2))


if __name__ == '__main__':
    main()
