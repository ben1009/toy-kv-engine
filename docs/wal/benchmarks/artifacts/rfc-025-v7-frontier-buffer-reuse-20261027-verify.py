#!/usr/bin/env python3
"""Reconstruct the one-candidate buffer-reuse comparison and verify its outcome."""
import csv
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-frontier-buffer-reuse-20261027'
WORK = ROOT / 'target/wal-opt/20261027-v7-frontier-buffer-reuse'
CHANGED = 'kv-engine/src/wal/parallel/runtime.rs'
PROFILE = 'rfc-025-v7-batch-membership-20261027'
PRIOR = 'rfc-025-v7-batch-retirement-hint-20261027'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(suffix):
    return json.loads((ART / (STEM + suffix)).read_text())


def main():
    plan, matrix, identity = read('-plan.json'), read('-matrix.json'), read('-binaries.json')
    manifest = list(csv.DictReader((ART / (STEM + '-source-sha256.tsv')).open(), delimiter='\t'))
    assert len(manifest) == len({r['path'] for r in manifest}) == 128
    scratch = WORK / 'verification-source'
    assert not scratch.exists()
    scratch.mkdir()
    original = {}
    try:
        for row in manifest:
            p = scratch / row['path']
            p.parent.mkdir(parents=True, exist_ok=True)
            data = subprocess.check_output(['git', 'show', plan['source_base'] + ':' + row['path']], cwd=ROOT)
            original[row['path']] = hashlib.sha256(data).hexdigest()
            p.write_bytes(data)
        subprocess.run(['patch', '-p1', '-i', str(ART / (STEM + '-candidate.patch'))], cwd=scratch,
                       check=True, capture_output=True)
        assert all(sha(scratch / r['path']) == r['sha256'] for r in manifest)
    finally:
        shutil.rmtree(scratch)
    assert [r['path'] for r in manifest if r['sha256'] != original[r['path']]] == [CHANGED]
    assert identity['baseline'] == plan['baseline']
    assert all(sha(ROOT / item['path']) == item['sha256'] for item in identity.values())
    environment = read('-environment.json')
    assert environment['source_base'] == plan['source_base'] and not environment['allocator_environment_overrides']
    for key, suffix in [('source_manifest_sha256', '-source-sha256.tsv'), ('patch_sha256', '-candidate.patch'),
                        ('plan_sha256', '-plan.json'), ('matrix_sha256', '-matrix.json'), ('driver_sha256', '-run.py')]:
        assert environment[key] == sha(ART / (STEM + suffix))
    # The preceding live-state proof was valid when that investigation ended.
    # Bind the saved proof rather than rerunning its old live-source assertions
    # against a subsequent, possibly retained optimization.
    assert environment['prior_verification_sha256'] == sha(ART / (PRIOR + '-verification.json'))
    prior = json.loads((ART / (PRIOR + '-verification.json')).read_text())
    assert prior['global_unique_invocations'] == 384 and not prior['source_retained']
    restored = json.loads((ART / (PRIOR + '-restoration.json')).read_text())
    assert restored['source_hashes'][CHANGED] == original[CHANGED]
    assert restored['canonical_sha256'] == identity['baseline']['sha256']
    summary_bytes = subprocess.check_output([sys.executable, str(ART / (STEM + '-analyze.py'))])
    assert summary_bytes == (ART / (STEM + '-summary.json')).read_bytes()
    summary = json.loads(summary_bytes)
    cells = [c for c in matrix['cells'] if c in summary]
    cancelled = [c for c in matrix['cells'] if c not in summary]
    assert cells and cells == list(matrix['cells'])[:len(cells)]
    if cancelled:
        assert not summary[cells[-1]]['v7_passed']
    assert len(matrix['cells']) == 5 and all(c['pairs'] == 3 and len(c['arms']) == 4 for c in matrix['cells'].values())
    ids = {run for cell in cells for run in summary[cell]['run_ids']}
    assert len(ids) == summary['status']['records'] == len(cells) * 16
    assert summary['status']['scored_records'] == len(cells) * 12
    assert read('-peaks.json')['cells'] == {cell + '/' + part: c['per_arm'] for cell in cells for part, c in summary[cell]['comparisons'].items()}
    old_ids = set()
    for stem in ['rfc-025-v7-ssd-sync-preparation-20261027', 'rfc-025-v7-ssd-two-chunk-lookahead-20261027',
                 'rfc-025-v7-ssd-batch-window-20261027', 'rfc-025-v7-ssd-batch-window-followup-20261027',
                 PROFILE, 'rfc-025-v7-batch-wait-budget-20261027', PRIOR]:
        old = json.loads((ART / (stem + '-summary.json')).read_text())
        for value in old.values():
            if isinstance(value, dict) and 'run_ids' in value:
                assert old_ids.isdisjoint(value['run_ids'])
                old_ids.update(value['run_ids'])
    solo = json.loads((ART / 'rfc-025-v7-ssd-batch-solo-control-20261027-summary.json').read_text())['cell']['run_ids']
    assert old_ids.isdisjoint(solo)
    old_ids.update(solo)
    for stem in ['rfc-025-v7-ssd-sync-preparation-20261027', PROFILE]:
        for line in (ART / (stem + '-profiles.jsonl')).read_text().splitlines():
            run_id = json.loads(line)['run_id']
            assert run_id not in old_ids
            old_ids.add(run_id)
    assert len(old_ids) == 384 and ids.isdisjoint(old_ids)
    profiles = json.loads((ART / (PROFILE + '-profiles.json')).read_text())
    assert len(profiles) == 3
    for meta in profiles:
        assert meta['binary'] == identity['baseline'] and meta['excluded_from_scoring']
        assert all(sha(ROOT / path) == digest for path, digest in meta['files'].items())
    checks = read('-checks.json')
    assert checks['outside_sandbox'] and checks['tests'] == 1971 and checks['focused_tests'] == 5
    phases = {c['phase']: c for c in checks['checks']}
    assert {'format', 'focused', 'check', 'release', 'direct spelling', 'final spelling'} <= set(phases)
    for check in checks['checks']:
        assert check['exit_code'] == 0 and sha(ROOT / check['log']) == check['log_sha256']
    text = (ROOT / phases['check']['log']).read_text()
    assert '1971 tests run: 1971 passed' in text and 'error:' not in text
    assert '5 tests run: 5 passed' in (ROOT / phases['focused']['log']).read_text()
    decision = read('-decision.json')
    expected = summary['status']['all_planned_cells_complete'] and summary['status']['captured_v7_cells_pass'] and summary['status']['confirmed_batch_benefit']
    assert decision['assessment_passed'] == expected and decision['cpu_is_diagnostic']
    assert decision['captured_cells'] == cells and decision['cancelled_cells'] == cancelled
    for cell in matrix['cells']:
        assert not (WORK / (cell + '-scratch')).exists()
        if cell in cancelled:
            assert not (ART / (STEM + '-' + cell + '.jsonl')).exists()
        else:
            for c in summary[cell]['comparisons'].values():
                assert 'cpu_ceiling' not in c['guards'] and all(pair['data_bytes'] == 1 for pair in c['pairs'])
    if decision['source_retained']:
        assert expected and all(sha(ROOT / r['path']) == r['sha256'] for r in manifest)
        committed = subprocess.check_output(['git', 'show', decision['source_commit'] + ':' + CHANGED], cwd=ROOT)
        assert hashlib.sha256(committed).hexdigest() == sha(ROOT / CHANGED)
        assert sha(ROOT / 'target/release/write-perf') == identity['candidate']['sha256']
    else:
        assert all(sha(ROOT / path) == digest for path, digest in original.items())
        assert sha(ROOT / 'target/release/write-perf') == identity['baseline']['sha256']
        restoration = read('-restoration.json')
        assert restoration['source_hashes'][CHANGED] == original[CHANGED]
        assert restoration['canonical_sha256'] == identity['baseline']['sha256']
        text = (ROOT / phases['restored check']['log']).read_text()
        assert 'Compiling kv-engine' in text and '1971 tests run: 1971 passed' in text and 'error:' not in text
        assert restoration['source_mtimes_refreshed'] and restoration['tests'] == 1971
    print(json.dumps(dict(source_base=plan['source_base'], reconstructed_inputs=128, source_reconstructs=True,
                          verified_binaries=2, summary_regenerates=True, records=len(ids), scored_records=len(cells)*12,
                          warmup_records=len(cells)*4, captured_cells=cells, cancelled_cells=cancelled,
                          reused_excluded_profiles=3, new_diagnostic_invocations=0, unique_new_invocations=len(ids),
                          global_unique_invocations=len(ids|old_ids), source_retained=decision['source_retained'],
                          assessment_passed=expected, cpu_is_diagnostic=True, tests=1971, focused_tests=5,
                          recognized_process_samples=sum(summary[c]['host_samples'] for c in cells),
                          checks_sha256=sha(ART/(STEM+'-checks.json')), decision_sha256=sha(ART/(STEM+'-decision.json')),
                          verifier_sha256=sha(Path(__file__))), indent=2))


if __name__ == '__main__':
    main()
