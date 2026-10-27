#!/usr/bin/env python3
"""Reconstruct the bounded-lookahead trial and verify SSD records and decision."""
import csv
import hashlib
import json
import shutil
import statistics
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-ssd-sync-preparation-20261027'
WORK = ROOT / 'target/wal-opt/20261027-v7-ssd-sync-preparation'
RUNTIME = 'kv-engine/src/wal/parallel/runtime.rs'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    plan = json.loads((ART / (STEM + '-plan.json')).read_text())
    matrix = json.loads((ART / (STEM + '-matrix.json')).read_text())
    manifest = list(csv.DictReader((ART / (STEM + '-source-sha256.tsv')).open(), delimiter='\t'))
    assert len(manifest) == len({r['path'] for r in manifest}) == 128
    scratch = WORK / 'verification-source'
    assert not scratch.exists()
    scratch.mkdir()
    try:
        for row in manifest:
            path = scratch / row['path']
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(subprocess.check_output(['git', 'show', plan['source_base'] + ':' + row['path']], cwd=ROOT))
        subprocess.run(['patch', '-p1', '-i', str(ART / (STEM + '-candidate.patch'))],
                       cwd=scratch, check=True, capture_output=True)
        for row in manifest:
            assert sha(scratch / row['path']) == row['sha256'], row['path']
    finally:
        shutil.rmtree(scratch)
    identity = json.loads((ART / (STEM + '-binaries.json')).read_text())
    assert set(identity) == {'baseline', 'candidate'}
    assert identity['baseline']['sha256'] == plan['baseline_sha256']
    for item in identity.values():
        assert sha(ROOT / item['path']) == item['sha256']
    regenerated = subprocess.check_output([sys.executable, str(ART / (STEM + '-analyze.py'))])
    assert regenerated == (ART / (STEM + '-summary.json')).read_bytes()
    summary = json.loads(regenerated)
    cells = [cell for cell in matrix['cells'] if cell in summary]
    decision = json.loads((ART / (STEM + '-decision.json')).read_text())
    assert set(decision['captured_cells']) == set(cells)
    assert set(decision['cancelled_cells']) == set(matrix['cells']) - set(cells)
    for cell in decision['cancelled_cells']:
        assert not (ART / (STEM + '-' + cell + '.jsonl')).exists()
    ids = {run for cell in cells for run in summary[cell]['run_ids']}
    assert len(ids) == summary['status']['invocations']
    peaks = json.loads((ART / (STEM + '-peaks.json')).read_text())
    expected_peaks = {cell + '/' + part: value['per_arm'] for cell in cells for part, value in summary[cell]['comparisons'].items()}
    assert peaks['cells'] == expected_peaks
    profiles = json.loads((ART / (STEM + '-profiles.json')).read_text())
    raw_profiles = [json.loads(line) for line in (ART / (STEM + '-profiles.jsonl')).read_bytes().splitlines()]
    diagnostic_plan = json.loads((ART / (STEM + '-diagnostic-plan.json')).read_text())
    assert len(profiles) == len(raw_profiles) == len(diagnostic_plan['captures']) == 3
    for profile, record, cfg in zip(profiles, raw_profiles, diagnostic_plan['captures']):
        assert profile['run_id'] == record['run_id'] and profile['run_id'] not in ids
        ids.add(profile['run_id'])
        assert profile['excluded_from_scoring'] and profile['binary']['sha256'] == plan['baseline_sha256']
        assert profile['samples'] > 0 and profile['lost_samples'] == 0
        assert record['params']['wal_format_version'] == cfg['format'] and record['result']['ops'] == cfg['num']
        assert profile['audit']['returncode'] == 0 and not profile['audit']['sampling_errors']
        assert not any(s['competing'] for s in [profile['audit']['before'], *profile['audit']['during'], profile['audit']['after']])
        for name, digest in profile['files'].items():
            assert sha(ROOT / name) == digest
    diagnosis = json.loads((ART / (STEM + '-batch-diagnosis.json')).read_text())
    batch = raw_profiles[2]
    pipeline = batch['result']['wal_pipeline']
    observations = pipeline['sync_observations']
    later = [o for o in observations if o['written_frontier_end'] > o['captured_target']]
    targets = [o['captured_target'] for o in observations]
    deltas = [end - start for end, start in zip(targets, [0] + targets[:-1])]
    assert diagnosis['run_id'] == batch['run_id'] and diagnosis['excluded_diagnostic']
    assert diagnosis['source_raw_sha256'] == hashlib.sha256((ART / (STEM + '-profiles.jsonl')).read_bytes().splitlines()[2]).hexdigest()
    assert diagnosis['logical_batches'] == pipeline['commit_buffers']
    assert diagnosis['sync_count'] == len(observations) == pipeline['sync_count']
    assert diagnosis['logical_batches_per_sync'] == pipeline['commit_buffers'] / pipeline['sync_count']
    assert diagnosis['median_captured_ticket_delta'] == statistics.median(deltas)
    assert diagnosis['largest_captured_ticket_delta'] == max(deltas)
    assert diagnosis['syncs_with_later_written_prefix'] == len(later)
    assert diagnosis['fraction_with_later_written_prefix'] == len(later) / len(observations)
    assert diagnosis['median_extra_written_tickets'] == statistics.median(o['written_frontier_end']-o['captured_target'] for o in observations)
    assert diagnosis['median_extra_when_later_prefix_exists'] == statistics.median(o['written_frontier_end']-o['captured_target'] for o in later)
    for key, source in [('coalesce_wait_calls','sync_coalesce_wait_calls'),('coalesce_wait_ns','sync_coalesce_wait_ns'),('frontier_write_ns','frontier_write_ns'),('fdatasync_ns','fdatasync_ns')]:
        assert diagnosis[key] == pipeline[source]
    env = json.loads((ART / (STEM + '-environment.json')).read_text())
    for key, suffix in [('source_manifest_sha256', '-source-sha256.tsv'), ('patch_sha256', '-candidate.patch'),
                        ('plan_sha256', '-plan.json'), ('matrix_sha256', '-matrix.json'), ('driver_sha256', '-run.py')]:
        assert env[key] == sha(ART / (STEM + suffix))
    checks = json.loads((ART / (STEM + '-checks.json')).read_text())
    assert checks['outside_sandbox'] and checks['tests'] == 1970 and checks['focused_tests'] == 9
    assert any(c['phase'] == 'final spelling' for c in checks['checks'])
    for check in checks['checks']:
        assert check['exit_code'] == 0 and sha(ROOT / check['log']) == check['log_sha256']
    assert decision['allocator_unchanged'] and decision['memtable_unchanged']
    all_pass = summary['status']['all_planned_cells_complete'] and summary['status']['captured_v7_cells_pass'] and summary['status']['material_gain']
    assert decision['assessment_passed'] == all_pass
    if decision['source_retained']:
        assert all_pass
        runtime_hash = next(row['sha256'] for row in manifest if row['path'] == RUNTIME)
        assert sha(ROOT / RUNTIME) == runtime_hash
        assert sha(ROOT / 'target/release/write-perf') == identity['candidate']['sha256']
        source = subprocess.check_output(['git', 'show', decision['source_commit'] + ':' + RUNTIME], cwd=ROOT)
        assert hashlib.sha256(source).hexdigest() == runtime_hash
    else:
        restoration = json.loads((ART / (STEM + '-restoration.json')).read_text())
        original = subprocess.check_output(['git', 'show', plan['source_base'] + ':' + RUNTIME], cwd=ROOT)
        assert hashlib.sha256(original).hexdigest() == restoration['runtime_sha256']
        assert restoration['canonical_sha256'] == plan['baseline_sha256']
        if decision.get('superseded_by'):
            later = json.loads((ART / (decision['superseded_by'] + '-decision.json')).read_text())
            assert later['source_base'] == plan['source_base']
            # The later experiment owns the final live-source checks. This
            # record binds the actual restoration observed between trials.
            assert restoration['scope'].startswith('Historical restoration')
        else:
            assert restoration['runtime_sha256'] == sha(ROOT / RUNTIME)
            assert restoration['canonical_sha256'] == sha(ROOT / 'target/release/write-perf')
            assert checks['restored_tests'] == 1969
    for name in ['kv-engine/Cargo.toml', 'Cargo.lock', 'kv-engine/src/mem_table.rs', 'kv-engine/src/wal.rs']:
        assert (ROOT / name).read_bytes() == subprocess.check_output(['git', 'show', plan['source_base'] + ':' + name], cwd=ROOT)
    for cell in matrix['cells']:
        assert not (WORK / (cell + '-scratch')).exists()
    print(json.dumps(dict(source_base=plan['source_base'],reconstructed_inputs=128,verified_binaries=2,
                          captured_cells=cells,cancelled_cells=decision['cancelled_cells'],
                          records=summary['status']['records'],scored_records=summary['status']['scored_records'],
                          warmup_records=summary['status']['records']-summary['status']['scored_records'],
                          excluded_profiles=3,retained_unique_invocation_ids=len(ids),
                          recognized_process_samples=sum(summary[c]['host_samples'] for c in cells),
                          source_reconstructs=True,summary_regenerates=True,source_retained=decision['source_retained'],
                          assessment_passed=all_pass,allocator_unchanged=True,memtable_unchanged=True,
                          live_state_owned_by=decision.get('superseded_by', STEM),scratch_removed=True,checks_sha256=sha(ART/(STEM+'-checks.json')),
                          decision_sha256=sha(ART/(STEM+'-decision.json')),
                          verifier_sha256=sha(Path(__file__))),indent=2))


if __name__ == '__main__':
    main()
