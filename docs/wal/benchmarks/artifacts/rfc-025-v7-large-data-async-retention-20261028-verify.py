#!/usr/bin/env python3
"""Bind provisional retention to the exact previously tested source and evidence."""
import csv
import hashlib
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-large-data-async-retention-20261028'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    meta = json.loads((ART / (STEM + '.json')).read_text())
    old = meta['original_stem']
    prior_path = ART / (old + '-verification.json')
    assert sha(prior_path) == meta['prior_verification_sha256']
    prior = json.loads(prior_path.read_text())
    assert prior['retained_source_restored'] and not prior['source_retained']
    assert prior['global_unique_invocations'] == meta['global_unique_invocations'] == 549
    for key, suffix in [('source_manifest_sha256', '-source-sha256.tsv'),
                        ('original_checks_sha256', '-checks.json'), ('original_decision_sha256', '-decision.json')]:
        assert sha(ART / (old + suffix)) == meta[key]
    manifest = list(csv.DictReader((ART / (old + '-source-sha256.tsv')).open(), delimiter='\t'))
    assert len(manifest) == len({r['path'] for r in manifest}) == meta['verified_candidate_inputs'] == 128
    for row in manifest:
        assert sha(ROOT / row['path']) == row['sha256']
        committed = subprocess.check_output(['git', 'show', meta['source_commit'] + ':' + row['path']], cwd=ROOT)
        assert hashlib.sha256(committed).hexdigest() == row['sha256']
    changed = subprocess.check_output(['git', 'diff-tree', '--no-commit-id', '--name-only', '-r', meta['source_commit']],
                                      cwd=ROOT, text=True).splitlines()
    assert changed == ['kv-engine/src/wal/parallel/runtime.rs', 'kv-engine/src/wal/parallel/worker.rs']
    assert subprocess.check_output(['git', 'rev-parse', meta['source_commit'] + '^'], cwd=ROOT,
                                   text=True).strip() == meta['source_parent']
    identity = json.loads((ART / (old + '-binaries.json')).read_text())
    assert all(sha(ROOT / item['path']) == item['sha256'] for item in identity.values())
    assert sha(ROOT / 'target/release/write-perf') == identity['candidate']['sha256'] == meta['canonical_sha256']
    assert meta['source_retained'] and meta['retention_provisional'] and meta['confirmation_required']
    assert not meta['assessment_passed'] and not meta['performance_confirmed'] and meta['new_benchmarks'] == 0
    summary = subprocess.check_output([sys.executable, str(ART / (old + '-analyze.py'))], cwd=ROOT)
    assert summary == (ART / (old + '-summary.json')).read_bytes()
    data = json.loads(summary)
    assert data['status']['records'] == 12 and not data['status']['captured_cells_pass']
    assert data['status']['cancelled_cells'] == meta['pending_guards']
    assert data['ext4-batch16']['stall_filter']['matched_pairs'] == [4, 5]
    decision = json.loads((ART / (old + '-decision.json')).read_text())
    assert not decision['assessment_passed'] and not decision['source_retained'] and decision['source_restored']
    checks = json.loads((ART / (old + '-checks.json')).read_text())
    assert checks['tests'] == meta['reused_full_tests'] == 1973
    assert checks['focused_tests'] == meta['reused_focused_tests'] == 47
    for check in checks['checks']:
        assert check['exit_code'] == 0 and sha(ROOT / check['log']) == check['log_sha256']
    fresh_checks = json.loads((ART / (STEM + '-checks.json')).read_text())
    for check in fresh_checks['checks']:
        assert check['exit_code'] == 0 and sha(ROOT / check['log']) == check['log_sha256']
    print(json.dumps(dict(source_commit=meta['source_commit'], verified_candidate_inputs=128,
                          source_retained=True, retention_provisional=True, assessment_passed=False,
                          canonical_sha256=meta['canonical_sha256'], reused_tests=1973,
                          reused_focused_tests=47, new_benchmarks=0, global_unique_invocations=549,
                          original_summary_regenerates=True, original_assessment_preserved=True,
                          prior_verification_sha256=sha(prior_path),
                          retention_sha256=sha(ART / (STEM + '.json')),
                          checks_sha256=sha(ART / (STEM + '-checks.json')),
                          verifier_sha256=sha(Path(__file__))), indent=2))


if __name__ == '__main__':
    main()
