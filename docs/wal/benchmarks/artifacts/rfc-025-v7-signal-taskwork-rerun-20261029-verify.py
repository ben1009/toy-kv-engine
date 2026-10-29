#!/usr/bin/env python3
"""Verify the full rerun, requested best-score view, and unchanged live baseline."""
import csv
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-signal-taskwork-rerun-20261029'
WORK = ROOT / 'target/wal-opt/20261029-v7-signal-taskwork-rerun'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(suffix):
    return json.loads((ART / (STEM + suffix)).read_text())


def main():
    plan, env, identity = read('-plan.json'), read('-environment.json'), read('-binaries.json')
    baseline = list(csv.DictReader((ART / (STEM + '-baseline-sha256.tsv')).open(), delimiter='\t'))
    candidate = list(csv.DictReader((ART / (STEM + '-source-sha256.tsv')).open(), delimiter='\t'))
    assert len(baseline) == len(candidate) == len({row['path'] for row in candidate}) == 128
    assert all(sha(ROOT / row['path']) == row['sha256'] for row in baseline)
    before = {row['path']: row['sha256'] for row in baseline}
    assert [row['path'] for row in candidate if row['sha256'] != before[row['path']]] == ['kv-engine/src/wal/parallel/worker.rs']
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
        subprocess.run(['patch', '--fuzz=0', '-p1', '-i', str(ART / (STEM + '-candidate.patch'))],
                       cwd=scratch, check=True, capture_output=True)
        assert all(sha(scratch / row['path']) == row['sha256'] for row in candidate)
    finally:
        if scratch.exists():
            shutil.rmtree(scratch)
    origin = plan['source_origin']
    for suffix in ['-baseline-sha256.tsv', '-source-sha256.tsv', '-candidate.patch', '-binaries.json']:
        assert (ART / (STEM + suffix)).read_bytes() == (ART / (origin + suffix)).read_bytes()
    assert sha(ART / (STEM + '-candidate.patch')) == plan['candidate_patch_sha256']
    assert identity['baseline'] == plan['baseline'] and identity['candidate'] == plan['candidate']
    assert all(sha(ROOT / item['path']) == item['sha256'] for item in identity.values())
    canonical = sha(ROOT / 'target/release/write-perf')
    assert canonical == identity['baseline']['sha256'] == plan['canonical_during_capture_sha256']
    assert env['source_base'] == plan['source_base'] and ' ext4 ' in env['filesystem']
    assert env['numa_nodes'] == ['node0'] and not env['allocator_environment_overrides']
    for field, suffix in [('source_manifest_sha256', '-source-sha256.tsv'), ('patch_sha256', '-candidate.patch'),
                          ('plan_sha256', '-plan.json'), ('matrix_sha256', '-matrix.json'),
                          ('driver_sha256', '-run.py'), ('orchestrator_sha256', '-run-all.py'),
                          ('child_sha256', '-child.py'), ('analysis_sha256', '-analyze.py')]:
        assert env[field] == sha(ART / (STEM + suffix))
    prior_path = ART / plan['prior_verification']
    assert sha(prior_path) == plan['prior_verification_sha256'] == env['prior_verification_sha256']
    assert (WORK / 'parent-verification.json').read_bytes() == prior_path.read_bytes()
    predecessor = plan['prior_verification'].removesuffix('-verification.json')
    assert subprocess.check_output([sys.executable, str(ART / (predecessor + '-verify.py'))], cwd=ROOT) == prior_path.read_bytes()
    prior = json.loads(prior_path.read_text())
    assert prior['global_unique_invocations'] == plan['prior_global_invocations'] == 869
    assert prior['full_archive_unique_invocations'] == plan['prior_full_archive_invocations'] == 4965

    raw_summary = subprocess.check_output([sys.executable, str(ART / (STEM + '-analyze.py'))], cwd=ROOT)
    assert raw_summary == (ART / (STEM + '-summary.json')).read_bytes()
    summary = json.loads(raw_summary)
    status, matrix = summary['status'], read('-matrix.json')
    assert matrix['run_all_cells_regardless_of_performance']
    assert status['captured_cells'] == list(matrix['cells']) and status['all_planned_cells_complete']
    assert not status['cancelled_cells'] and status['records'] == 56
    assert status['scored_records'] == 44 and status['warmup_records'] == 12
    ids = {run for cell in status['captured_cells'] for run in summary[cell]['run_ids']}
    assert len(ids) == 56
    other_ids = set()
    for path in ART.glob('rfc-025-v7-*.jsonl'):
        if path.name.startswith(STEM):
            continue
        for line in path.read_text().splitlines():
            item = json.loads(line)
            if isinstance(item, dict) and 'run_id' in item:
                other_ids.add(item['run_id'])
    assert ids.isdisjoint(other_ids) and len(other_ids) >= plan['prior_full_archive_invocations']

    best_bytes = subprocess.check_output([sys.executable, str(ART / (STEM + '-best-analyze.py'))], cwd=ROOT)
    assert best_bytes == (ART / (STEM + '-best-summary.json')).read_bytes()
    best = json.loads(best_bytes)
    assert len(read('-best-inputs.json')['sources']) == 41 and best['unique_scored_records'] == 194
    assert sum(arm['scored_runs'] for cell in best['current']['cells'].values() for arm in cell['arms'].values()) == 44
    assert all(arm['raw_best'] == arm['best'] for scope in ['current', 'historical']
               for cell in best[scope]['cells'].values() for arm in cell['arms'].values())
    assert read('-maintainer-assessment.json')['instruction'] == 'i told you to use best score'
    decision, unchanged, checks = read('-decision.json'), read('-unchanged.json'), read('-checks.json')
    assert not best['historical']['passed'] and not decision['assessment_passed']
    assert decision['failed_best_score_cells'] == [cell for cell, result in best['historical']['cells'].items() if not result['passed']]
    assert decision['best_summary_sha256'] == sha(ART / (STEM + '-best-summary.json'))
    assert decision['maintainer_assessment_sha256'] == sha(ART / (STEM + '-maintainer-assessment.json'))
    assert not decision['source_retained'] and not decision['source_changed'] and decision['baseline_unchanged']
    assert decision['canonical_sha256'] == unchanged['canonical_sha256'] == canonical
    assert unchanged['baseline_inputs_verified'] == 128 and not unchanged['source_changed'] and not unchanged['canonical_changed']
    assert unchanged['baseline_manifest_sha256'] == sha(ART / (STEM + '-baseline-sha256.tsv'))
    assert checks['correctness_reused'] and checks['candidate_tests'] == 1975 and checks['focused_tests'] == 49
    assert checks['baseline_tests'] == 1973 and not checks['source_changed']
    for field, expected in [('reused_validation', plan['validation_origin']), ('reused_release_build', origin)]:
        ref = checks[field]
        assert ref['path'] == expected + '-checks.json' and sha(ART / ref['path']) == ref['sha256']
    build_checks = json.loads((ART / checks['reused_release_build']['path']).read_text())
    release = next(c for c in build_checks['checks'] if c['phase'] == 'release')
    assert release['exit_code'] == 0 and sha(ROOT / release['log']) == release['log_sha256']
    for check in checks['checks']:
        assert check['exit_code'] == 0 and sha(ROOT / check['log']) == check['log_sha256']
    assert 'final spelling' in {c['phase'] for c in checks['checks']}
    assert all(not (WORK / (cell + '-scratch')).exists() for cell in matrix['cells'])
    print(json.dumps(dict(source_base=plan['source_base'], reconstructed_inputs=128, final_inputs_verified=128,
                          summaries_regenerate=True, best_scores_verified=True, historical_scored_records=194,
                          all_six_cells_complete=True, new_completed_invocations=56, matrix_records=56,
                          scored_records=44, global_unique_invocations=925, full_archive_unique_invocations=5021,
                          inherited_interrupted_without_json=1, source_retained=False, baseline_unchanged=True,
                          retained_source_restored=True, canonical_sha256=canonical, correctness_reused=True,
                          candidate_tests=1975, focused_tests=49, baseline_tests=1973,
                          decision_sha256=sha(ART / (STEM + '-decision.json')),
                          checks_sha256=sha(ART / (STEM + '-checks.json')),
                          unchanged_sha256=sha(ART / (STEM + '-unchanged.json')),
                          verifier_sha256=sha(Path(__file__))), indent=2))


if __name__ == '__main__':
    main()
