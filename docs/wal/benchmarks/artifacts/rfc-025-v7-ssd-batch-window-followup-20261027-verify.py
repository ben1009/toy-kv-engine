#!/usr/bin/env python3
"""Verify the complete user-authorized follow-up without rewriting the initial trial."""
import csv
import hashlib
import gzip
import json
import shutil
import statistics
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-ssd-batch-window-followup-20261027'
PRIOR = 'rfc-025-v7-ssd-batch-window-20261027'
WORK = ROOT / 'target/wal-opt/20261027-v7-ssd-batch-window-followup'
RUNTIME = 'kv-engine/src/wal/parallel/runtime.rs'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(suffix):
    return json.loads((ART / (STEM + suffix)).read_text())


def main():
    plan = read('-plan.json')
    manifest_path = ART / (STEM + '-source-sha256.tsv')
    manifest = list(csv.DictReader(manifest_path.open(), delimiter='\t'))
    assert len(manifest) == len({r['path'] for r in manifest}) == 128
    assert manifest_path.read_bytes() == (ART / (PRIOR + '-source-sha256.tsv')).read_bytes()
    assert (ART / (STEM + '-candidate.patch')).read_bytes() == (ART / (PRIOR + '-candidate.patch')).read_bytes()
    scratch = WORK / 'verification-source'
    assert not scratch.exists()
    scratch.mkdir()
    try:
        for row in manifest:
            p = scratch / row['path']
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(subprocess.check_output(['git', 'show', plan['source_base'] + ':' + row['path']], cwd=ROOT))
        subprocess.run(['patch', '-p1', '-i', str(ART / (STEM + '-candidate.patch'))], cwd=scratch,
                       check=True, capture_output=True)
        for row in manifest:
            assert sha(scratch / row['path']) == row['sha256'], row['path']
    finally:
        shutil.rmtree(scratch)
    source_proof = read('-source-validation.json')
    assert source_proof['inputs'] == 128 and source_proof['differing_paths'] == [RUNTIME]
    assert sha(ROOT / source_proof['baseline_manifest']) == source_proof['baseline_manifest_sha256']
    assert sha(ROOT / source_proof['candidate_manifest']) == source_proof['candidate_manifest_sha256']
    baseline_rows = list(csv.DictReader((ROOT / source_proof['baseline_manifest']).open(), delimiter='\t'))
    baseline_hashes = {r['path']: r['sha256'] for r in baseline_rows}
    candidate_hashes = {r['path']: r['sha256'] for r in manifest}
    assert baseline_hashes.keys() == candidate_hashes.keys()
    assert [p for p in candidate_hashes if candidate_hashes[p] != baseline_hashes[p]] == [RUNTIME]
    for path, digest in baseline_hashes.items():
        assert hashlib.sha256(subprocess.check_output(['git', 'show', plan['source_base'] + ':' + path], cwd=ROOT)).hexdigest() == digest
    identity = read('-binaries.json')
    assert identity == json.loads((ART / (PRIOR + '-binaries.json')).read_text())
    for item in identity.values():
        assert sha(ROOT / item['path']) == item['sha256']
    for name, suffix in [('source_manifest_sha256', '-source-sha256.tsv'), ('patch_sha256', '-candidate.patch'),
                         ('plan_sha256', '-plan.json'), ('matrix_sha256', '-matrix.json'), ('driver_sha256', '-run.py')]:
        assert read('-environment.json')[name] == sha(ART / (STEM + suffix))
    erratum = read('-plan-erratum.json')
    archived_plan = ROOT / erratum['original_gzip_path']
    assert sha(archived_plan) == erratum['original_gzip_sha256']
    original_plan = gzip.decompress(archived_plan.read_bytes())
    assert hashlib.sha256(original_plan).hexdigest() == erratum['original_plan_sha256'] == read('-environment.json')['original_plan_sha256']
    assert sha(ART / (STEM + '-plan.json')) == erratum['corrected_plan_sha256']
    original = json.loads(original_plan)
    assert [k for k in plan if plan[k] != original[k]] == ['user_direction']
    assert plan['user_direction'][0] == original['user_direction'][0]
    assert len(original['user_direction'][1]) == len(plan['user_direction'][1]) + 1
    # The prior verifier checked the live restored state at its own evidence
    # commit. Verify that frozen bundle as historical bytes, not by requiring
    # its baseline-only live-source assertion after a legitimate new trial.
    prior_verification = json.loads((ART / (PRIOR + '-verification.json')).read_text())
    assert sha(ART / (PRIOR + '-verification.json')) == plan['prior_verification_sha256']
    assert sha(ART / (PRIOR + '-verify.py')) == prior_verification['verifier_sha256']
    assert prior_verification['source_retained'] is False and prior_verification['global_unique_invocations'] == 123
    for suffix in ['-plan.json', '-matrix.json', '-candidate.patch', '-source-sha256.tsv', '-binaries.json',
                   '-checks.json', '-decision.json', '-restoration.json', '-summary.json', '-verification.json',
                   '-verify.py', '-analyze.py', '-run.py', '-ext4-batch8.jsonl', '-ext4-batch8-runs.tsv', '-ext4-batch8-host.jsonl']:
        p = ART / (PRIOR + suffix)
        archived = subprocess.check_output(['git', 'show', plan['prior_evidence_commit'] + ':' + str(p.relative_to(ROOT))], cwd=ROOT)
        assert p.read_bytes() == archived, p
    prior_summary = subprocess.check_output([sys.executable, str(ART / (PRIOR + '-analyze.py'))])
    assert prior_summary == (ART / (PRIOR + '-summary.json')).read_bytes()
    assert sha(ART / (PRIOR + '-summary.json')) == plan['prior_screen_summary_sha256']
    regenerated = subprocess.check_output([sys.executable, str(ART / (STEM + '-analyze.py'))])
    assert regenerated == (ART / (STEM + '-summary.json')).read_bytes()
    summary = json.loads(regenerated)
    matrix = read('-matrix.json')
    assert summary['status']['all_planned_cells_complete']
    assert len(matrix['cells']) == 6 and all(c['pairs'] == 3 for c in matrix['cells'].values())
    assert summary['status']['records'] == 96 and summary['status']['scored_records'] == 72
    peaks = read('-peaks.json')
    assert peaks['cells'] == {cell + '/' + part: c['per_arm'] for cell in matrix['cells'] for part, c in summary[cell]['comparisons'].items()}
    ids = {run for cell in matrix['cells'] for run in summary[cell]['run_ids']}
    assert len(ids) == 96
    for old_stem in ['rfc-025-v7-ssd-sync-preparation-20261027', 'rfc-025-v7-ssd-two-chunk-lookahead-20261027', PRIOR]:
        old_summary = json.loads((ART / (old_stem + '-summary.json')).read_text())
        old_ids = {run for c in old_summary.values() if isinstance(c, dict) and 'run_ids' in c for run in c['run_ids']}
        assert ids.isdisjoint(old_ids)
        ids.update(old_ids)
    profiles = [json.loads(line) for line in (ART / 'rfc-025-v7-ssd-sync-preparation-20261027-profiles.jsonl').read_bytes().splitlines()]
    for profile in profiles:
        assert profile['run_id'] not in ids
        ids.add(profile['run_id'])
    assert len(ids) == 219
    mechanism = read('-mechanism.json')
    for cell in matrix['cells']:
        rows = list(csv.DictReader((ART / (STEM + '-' + cell + '-runs.tsv')).open(), delimiter='\t'))
        raw = [json.loads(line) for line in (ART / (STEM + '-' + cell + '.jsonl')).read_bytes().splitlines()]
        for arm in matrix['cells'][cell]['arms']:
            records = [r for row, r in zip(rows, raw) if row['phase'] == 'scored' and row['arm'] == arm]
            pipelines = [r['result']['wal_pipeline'] for r in records]
            m = mechanism[cell][arm]
            assert m['run_ids'] == [r['run_id'] for r in records]
            for key, value in m['pipeline_medians'].items():
                assert value == statistics.median(p[key] for p in pipelines)
            assert m['all_coalescing_waits_zero'] == all(p['sync_coalesce_wait_calls'] == p['sync_coalesce_wait_ns'] == 0 for p in pipelines)
            assert m['batches_per_sync_median'] == statistics.median(p['commit_buffers']/p['sync_count'] for p in pipelines)
    solo_stem = 'rfc-025-v7-ssd-batch-solo-control-20261027'
    solo_bytes = subprocess.check_output([sys.executable, str(ART / (solo_stem + '-analyze.py'))])
    assert solo_bytes == (ART / (solo_stem + '-summary.json')).read_bytes()
    solo = json.loads(solo_bytes)['cell']
    assert solo['records'] == solo['invocations'] == 18 and solo['warmup_records'] == 3
    assert ids.isdisjoint(solo['run_ids'])
    ids.update(solo['run_ids'])
    assert len(ids) == 237
    solo_plan = json.loads((ART / (solo_stem + '-plan.json')).read_text())
    assert solo_plan['prior_summary_sha256'] == sha(ART / (STEM + '-summary.json'))
    assert solo_plan['binary_identities'] == identity
    assert not (ROOT / 'target/wal-opt/20261027-v7-ssd-batch-solo-control/ext4-batch1-scratch').exists()
    solo_interpretation = json.loads((ART / (solo_stem + '-interpretation.json')).read_text())
    null = solo['comparisons']['concurrent_batch_64/repeat_reference']
    assert solo_interpretation['repeat_reference_paired_throughput'] == null['median']['throughput']
    assert solo_interpretation['repeat_reference_paired_p99'] == null['median']['p99']
    assert solo_interpretation['same_binary_control_stable'] is False
    assert null['median']['throughput'] > 1.05 or null['median']['p99'] < 0.90
    assert solo_interpretation['candidate_reference_passed'] == solo['comparisons']['concurrent_batch_64/candidate_reference']['passed']
    assert solo_interpretation['candidate_repeat_passed'] == solo['comparisons']['concurrent_batch_64/candidate_repeat']['passed']
    solo_rows = list(csv.DictReader((ART / (solo_stem + '-ext4-batch1-runs.tsv')).open(), delimiter='\t'))
    solo_raw = [json.loads(line) for line in (ART / (solo_stem + '-ext4-batch1.jsonl')).read_bytes().splitlines()]
    assert solo_interpretation['scored_run_ids'] == [row['run_id'] for row in solo_rows if row['phase'] == 'scored']
    assert solo_interpretation['all_scored_coalescing_waits_zero']
    assert all(r['result']['wal_pipeline']['sync_coalesce_wait_calls'] == r['result']['wal_pipeline']['sync_coalesce_wait_ns'] == 0 for row, r in zip(solo_rows, solo_raw) if row['phase'] == 'scored')
    checks = read('-checks.json')
    assert checks['outside_sandbox'] and checks['tests'] == 1971 and checks['focused_tests'] == 12
    assert checks['validation_reused_from'] == PRIOR
    assert {c['phase'] for c in checks['reused_checks']} == {'format', 'focused', 'check', 'release'}
    for check in checks['reused_checks'] + checks['checks']:
        assert check['exit_code'] == 0 and sha(ROOT / check['log']) == check['log_sha256']
    assert any(c['phase'] == 'final spelling' for c in checks['checks'])
    for attempt in checks.get('prior_attempts', []):
        assert attempt['exit_code'] != 0 and sha(ROOT / attempt['log']) == attempt['log_sha256']
    for cell in matrix['cells']:
        assert not (WORK / (cell + '-scratch')).exists()
        for part, c in summary[cell]['comparisons'].items():
            assert 'cpu_ceiling' not in c['guards'] and 'sync_count_ceiling' not in c['guards']
            assert all(p['data_bytes'] == 1 for p in c['pairs'])
    decision = read('-decision.json')
    expected_pass = summary['status']['captured_v7_cells_pass'] and summary['status']['confirmed_batch_benefit']
    assert decision['assessment_passed'] == expected_pass
    assert decision['cpu_is_diagnostic'] and decision['all_requested_cells_run']
    assert decision['failed_v7_cells'] == [cell for cell in matrix['cells'] if not summary[cell]['v7_passed']]
    assert decision['cancelled_cells'] == []
    live = read('-live-state.json')
    assert live['source_commit'] == decision['source_commit']
    assert live['source_retained'] == decision['source_retained']
    assert live['runtime_sha256'] == sha(ROOT / RUNTIME)
    assert live['canonical_sha256'] == sha(ROOT / 'target/release/write-perf')
    if decision['source_retained']:
        assert expected_pass or (decision.get('provisional_retention') and decision.get('retention_exception') and summary['status']['confirmed_batch_benefit'])
        for row in manifest:
            assert sha(ROOT / row['path']) == row['sha256']
        assert sha(ROOT / 'target/release/write-perf') == identity['candidate']['sha256']
        source = subprocess.check_output(['git', 'show', decision['source_commit'] + ':' + RUNTIME], cwd=ROOT)
        assert hashlib.sha256(source).hexdigest() == sha(ROOT / RUNTIME)
    else:
        original = subprocess.check_output(['git', 'show', plan['source_base'] + ':' + RUNTIME], cwd=ROOT)
        assert hashlib.sha256(original).hexdigest() == sha(ROOT / RUNTIME)
        assert sha(ROOT / 'target/release/write-perf') == identity['baseline']['sha256']
        assert checks['restored_tests'] == 1969
    print(json.dumps(dict(source_base=plan['source_base'],source_reconstructs=True,reconstructed_inputs=128,
                          verified_binaries=2,initial_trial_preserved=True,initial_trial_is_historical=True,
                          reused_full_tests=1971,reused_focused_tests=12,records=96,scored_records=72,warmup_records=24,
                          global_unique_invocations=len(ids),supplemental_solo_host_samples=solo['host_samples'],combined_host_samples=sum(summary[c]['host_samples'] for c in matrix['cells'])+solo['host_samples'],supplemental_solo_records=18,supplemental_solo_scored_records=15,recognized_process_samples=sum(summary[c]['host_samples'] for c in matrix['cells']),
                          summary_regenerates=True,all_requested_cells_run=True,cpu_is_diagnostic=True,
                          assessment_passed=expected_pass,source_retained=decision['source_retained'],
                          checks_sha256=sha(ART / (STEM + '-checks.json')),decision_sha256=sha(ART / (STEM + '-decision.json')),
                          verifier_sha256=sha(Path(__file__))),indent=2))


if __name__ == '__main__':
    main()
