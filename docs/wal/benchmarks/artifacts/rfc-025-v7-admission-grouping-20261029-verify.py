#!/usr/bin/env python3
"""Verify the frozen admission-grouping comparison and its source disposition."""
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
STEM = 'rfc-025-v7-admission-grouping-20261029'
WORK = ROOT / 'target/wal-opt/20261029-v7-admission-grouping'
P1 = 'rfc-025-v7-producer-framing-20261029'
PRODUCTION = {'kv-engine/src/chaos/failpoint.rs', 'kv-engine/src/mem_table.rs',
              'kv-engine/src/mvcc.rs', 'kv-engine/src/wal.rs',
              'kv-engine/src/wal/parallel/runtime.rs'}
FIXTURE = 'kv-engine/integration_tests/chaos_integration.rs'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(suffix):
    return json.loads((ART / (STEM + suffix)).read_text())


def rows(suffix):
    return list(csv.DictReader((ART / (STEM + suffix)).open(), delimiter='\t'))


def reconstruct(source, before, after, patch, scratch):
    assert not scratch.exists()
    try:
        scratch.mkdir(parents=True)
        for row in before:
            data = subprocess.check_output(['git', 'show', source + ':' + row['path']], cwd=ROOT)
            assert hashlib.sha256(data).hexdigest() == row['sha256']
            target = scratch / row['path']
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
        subprocess.run(['patch', '--fuzz=0', '-p1', '-i', str(patch)], cwd=scratch,
                       check=True, capture_output=True)
        assert all(sha(scratch / row['path']) == row['sha256'] for row in after)
    finally:
        if scratch.exists():
            shutil.rmtree(scratch)


def regenerate(script, output):
    data = subprocess.check_output([sys.executable, str(ART / (STEM + script))], cwd=ROOT)
    assert data == (ART / (STEM + output)).read_bytes()
    return json.loads(data)


def main():
    plan, env, outcome = read('-plan.json'), read('-environment.json'), read('-outcome.json')
    pre = read('-preflight.json')
    parent = ART / pre['prior_receipt']
    assert pre['prior_verified_before_fixture_fix'] and sha(parent) == pre['prior_receipt_sha256']
    assert parent.read_bytes() == (WORK / 'parent-verification.json').read_bytes()
    assert json.loads(parent.read_text()) == pre['prior']
    assert plan['source_base'] == pre['source_base'] == pre['fixture_fix_commit']
    changed = subprocess.check_output(['git', 'diff', '--name-only', pre['guard_source_base'],
                                       pre['source_base'], '--'], cwd=ROOT, text=True).splitlines()
    assert changed == [FIXTURE]
    before, after, guard = rows('-baseline-sha256.tsv'), rows('-source-sha256.tsv'), rows('-guard-source-sha256.tsv')
    assert len(before) == len(after) == len(guard) == 128
    baseline = {row['path']: row['sha256'] for row in before}
    guard_map = {row['path']: row['sha256'] for row in guard}
    assert baseline[FIXTURE] == pre['fixture_source_after_sha256']
    assert guard_map[FIXTURE] == pre['fixture_source_before_sha256']
    assert {row['path'] for row in after if row['sha256'] != baseline[row['path']]} == PRODUCTION
    assert {row['path'] for row in after if row['sha256'] != guard_map[row['path']]} == {
        'kv-engine/src/wal/parallel/runtime.rs', FIXTURE}
    prior_rows = list(csv.DictReader((ART / (P1 + '-source-sha256.tsv')).open(), delimiter='\t'))
    prior_map = {row['path']: row['sha256'] for row in prior_rows}
    assert {row['path'] for row in guard if row['sha256'] != prior_map[row['path']]} == {
        'kv-engine/src/chaos/failpoint.rs', 'kv-engine/src/mem_table.rs', 'kv-engine/src/mvcc.rs'}
    reconstruct(plan['source_base'], before, after, ART / (STEM + '-candidate.patch'), WORK / 'verify-final-source')
    guard_before = [dict(path=r['path'], sha256=(pre['fixture_source_before_sha256'] if r['path'] == FIXTURE else r['sha256'])) for r in before]
    reconstruct(pre['guard_source_base'], guard_before, guard, ART / (STEM + '-guard.patch'), WORK / 'verify-guard-source')
    live = after if outcome['source_retained'] else before
    assert all(sha(ROOT / row['path']) == row['sha256'] for row in live)
    identity = read('-binaries.json')
    assert identity['retained-v7'] == plan['baseline']
    assert all(sha(ROOT / item['path']) == item['sha256'] for item in identity.values())
    expected = identity['candidate-v7' if outcome['source_retained'] else 'retained-v7']['sha256']
    assert sha(ROOT / 'target/release/write-perf') == outcome['canonical_sha256'] == expected
    assert ' ext4 ' in env['filesystem']
    for key, suffix in [('plan_sha256','-plan.json'),('matrix_sha256','-matrix.json'),
                        ('driver_sha256','-run.py'),('child_sha256','-child.py'),
                        ('analyzer_sha256','-analyze.py'),('orchestrator_sha256','-run-all.py'),
                        ('source_manifest_sha256','-source-sha256.tsv'),('patch_sha256','-candidate.patch'),
                        ('checks_sha256','-checks.json'),('guard_source_manifest_sha256','-guard-source-sha256.tsv'),
                        ('guard_patch_sha256','-guard.patch'),('guard_checks_sha256','-guard-checks.json'),
                        ('recovery_diagnosis_sha256','-recovery-diagnosis.json')]:
        assert env[key] == sha(ART / (STEM + suffix))
    summary = regenerate('-analyze.py', '-summary.json')
    best = regenerate('-best.py', '-best.json')
    assert summary['screens_complete'] and summary['guards_complete'] and not summary['missing_cells']
    assert summary['retention_passed'] == outcome['source_retained']
    ids = {run for cell in summary['cells'].values() for run in cell['run_ids']}
    assert len(ids) == summary['invocations'] == 84 and summary['scored_records'] == 63
    previous_ids = set()
    for path in ART.glob('rfc-025-v7-*.jsonl'):
        if path.name.startswith(STEM):
            continue
        for line in path.read_text().splitlines():
            record = json.loads(line)
            if isinstance(record, dict) and 'run_id' in record:
                previous_ids.add(record['run_id'])
    assert ids.isdisjoint(previous_ids)
    assert len(previous_ids) >= pre['prior']['full_archive_unique_invocations']
    checks, guard_checks = read('-checks.json'), read('-guard-checks.json')
    assert checks['outside_sandbox'] and checks['candidate_tests'] == 1985 and checks['focused_tests'] == 65
    assert guard_checks['outside_sandbox'] and guard_checks['full_tests'] == 1980 and guard_checks['focused_tests'] == 60
    for check in checks['checks'] + guard_checks['checks']:
        assert check['returncode'] == 0 and sha(ROOT / check['log_path']) == check['log_sha256']
    assert sha(ROOT / checks['race_log_path']) == checks['race_log_sha256']
    assert (ROOT / checks['race_log_path']).read_text().count('5 tests run: 5 passed') == 10
    phases = {c['step']: c for c in checks['checks']}
    full = (ROOT / phases['check']['log_path']).read_text()
    focused = (ROOT / phases['focused']['log_path']).read_text()
    assert re.search(r'1985 tests run: 1985 passed(?: \([^)]*\))?, 0 skipped', full)
    assert 'FLAKY' not in full and 'error:' not in full
    assert '65 tests run: 65 passed' in focused and 'FLAKY' not in focused
    if not outcome['source_retained']:
        assert outcome['rust_mtimes_refreshed'] and outcome['atomic_binary_replacement']
        for attempt in outcome['prior_restored_attempts']:
            assert sha(ROOT / attempt['path']) == attempt['sha256']
        for check in outcome['restored_checks']:
            assert check['returncode'] == 0 and sha(ROOT / check['log_path']) == check['log_sha256']
        restored = next((ROOT / c['log_path']).read_text() for c in outcome['restored_checks'] if c['step'] == 'restored-check')
        assert re.search(r'1973 tests run: 1973 passed(?: \([^)]*\))?, 0 skipped', restored)
        assert 'FLAKY' not in restored and 'error:' not in restored
    assert all(not (WORK / (cell + '-scratch')).exists() for cell in read('-matrix.json')['cells'])
    print(json.dumps(dict(source_base=plan['source_base'], reconstructed_inputs=128, guard_inputs=128,
                          live_inputs_verified=128, source_retained=outcome['source_retained'],
                          new_completed_invocations=len(ids), scored_records=summary['scored_records'],
                          global_unique_invocations=pre['prior']['global_unique_invocations'] + len(ids),
                          full_archive_unique_invocations=pre['prior']['full_archive_unique_invocations'] + len(ids),
                          inherited_interrupted_without_json=1, screens_passed=summary['screen_passed'],
                          guards_complete=summary['guards_complete'], best_scores_verified=True,
                          summaries_regenerate=True, candidate_tests=1985, focused_tests=65,
                          guard_tests=1980, guard_focused_tests=60, restored_tests=1973 if not outcome['source_retained'] else None,
                          canonical_sha256=expected, source_patch_sha256=env['patch_sha256'],
                          source_manifest_sha256=env['source_manifest_sha256'], checks_sha256=env['checks_sha256'],
                          summary_sha256=sha(ART / (STEM + '-summary.json')),
                          best_sha256=sha(ART / (STEM + '-best.json')),
                          outcome_sha256=sha(ART / (STEM + '-outcome.json')),
                          verifier_sha256=sha(Path(__file__))), indent=2))


if __name__ == '__main__':
    main()
