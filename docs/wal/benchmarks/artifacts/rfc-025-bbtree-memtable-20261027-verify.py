#!/usr/bin/env python3
"""Reconstruct BBTree trial sources and verify captures, cancellations and rollback."""
import csv
import hashlib
import json
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
ART = Path(__file__).resolve().parent
STEM = 'rfc-025-bbtree-memtable-20261027'
WORK = ROOT / 'target/wal-opt/20261027-bbtree-investigation'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    plan = json.loads((ART / (STEM + '-plan.json')).read_text())
    matrix = json.loads((ART / (STEM + '-matrix.json')).read_text())
    manifest = list(csv.DictReader((ART / (STEM + '-source-sha256.tsv')).open(), delimiter='\t'))
    assert len(manifest) == len({r['path'] for r in manifest}) == 129
    new_source = 'kv-engine/src/mem_table_bbtree.rs'
    scratch = WORK / 'verification-source'
    assert not scratch.exists()
    scratch.mkdir()
    try:
        for row in manifest:
            old = subprocess.run(['git', 'show', plan['source_base'] + ':' + row['path']],
                                 cwd=ROOT, capture_output=True)
            if row['path'] == new_source:
                assert old.returncode != 0
                continue
            assert old.returncode == 0
            dest = scratch / row['path']
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(old.stdout)
        subprocess.run(['patch', '-p1', '-i', str(ART / (STEM + '-candidate.patch'))],
                       cwd=scratch, check=True, capture_output=True)
        for row in manifest:
            assert sha(scratch / row['path']) == row['sha256'], row['path']
        upstream = json.loads((ART / (STEM + '-upstream.json')).read_text())
        lock = tomllib.loads((scratch / 'Cargo.lock').read_text())
        assert all(p in lock['package'] for p in upstream['crate_packages'])
        assert next(p for p in upstream['crate_packages'] if p['name'] == 'bbtree')['version'] == '0.1.5'
        assert upstream['crate_vcs']['git']['sha1'] == plan['upstream']['crate_source_commit']
    finally:
        shutil.rmtree(scratch)
    identity = json.loads((ART / (STEM + '-binaries.json')).read_text())
    assert set(identity) == {'skipmap', 'bbtree'}
    for item in identity.values():
        assert sha(ROOT / item['path']) == item['sha256']
    regenerated = subprocess.check_output([sys.executable, str(ART / (STEM + '-analyze.py'))])
    assert regenerated == (ART / (STEM + '-summary.json')).read_bytes()
    summary = json.loads(regenerated)
    assert summary['status']['cpu_complete'] and not summary['status']['cpu_passed']
    assert summary['status']['invocations'] == 40 and summary['status']['records'] == 48
    assert summary['status']['scored_records'] == 36
    peaks = json.loads((ART / (STEM + '-peaks.json')).read_text())
    expected_peaks = {cell + '/' + part: c['per_arm'] for cell in matrix['cpu_order'] for part, c in summary[cell]['comparisons'].items()}
    assert peaks['cells'] == expected_peaks
    decision = json.loads((ART / (STEM + '-decision.json')).read_text())
    assert not decision['source_retained'] and not decision['source_default_changed']
    assert not decision['assessment_passed'] and decision['allocator_unchanged']
    assert set(decision['captured_cells']) == set(matrix['cpu_order'])
    assert set(decision['cancelled_cells']) == set(matrix['ssd_screen'] + matrix['ssd_followup'])
    for cell in decision['cancelled_cells']:
        assert not (ART / (STEM + '-' + cell + '.jsonl')).exists()
    ids = {run for cell in matrix['cpu_order'] for run in summary[cell]['run_ids']}
    assert len(ids) == 40
    profiles = json.loads((ART / (STEM + '-profiles.json')).read_text())
    raw_profiles = [json.loads(line) for line in (ART / (STEM + '-profiles.jsonl')).read_bytes().splitlines()]
    assert len(profiles) == len(raw_profiles) == 2
    samples = json.loads((ART / (STEM + '-profile-samples.json')).read_text())
    assert [p['samples'] for p in samples] == [117, 189]
    assert all(p['lost_samples'] == 0 and p['event'] == 'cpu-clock:u' for p in samples)
    for profile, raw in zip(profiles, raw_profiles):
        assert raw['run_id'] == profile['run_id'] and profile['run_id'] not in ids
        ids.add(profile['run_id'])
        assert profile['excluded_from_scoring'] and profile['binary'] == identity[profile['backend']]
        assert raw['result']['ops'] == 1000000 and raw['params']['threads'] == 16
        assert profile['audit']['returncode'] == 0 and not profile['audit']['sampling_errors']
        assert not any(s['competing'] for s in [profile['audit']['before'], *profile['audit']['during'], profile['audit']['after']])
        for name, digest in profile['files'].items():
            assert sha(ROOT / name) == digest
    env = json.loads((ART / (STEM + '-environment.json')).read_text())
    for key, suffix in [('source_manifest_sha256', '-source-sha256.tsv'), ('patch_sha256', '-candidate.patch'),
                        ('plan_sha256', '-plan.json'), ('matrix_sha256', '-matrix.json'), ('driver_sha256', '-run.py')]:
        assert env[key] == sha(ART / (STEM + suffix))
    correction = json.loads((ART / (STEM + '-patch-correction.json')).read_text())
    assert correction['corrected_patch_sha256'] == env['patch_sha256']
    assert sha(ROOT / correction['original_rendered_diff']) == correction['original_sha256']
    harness = json.loads((ART / (STEM + '-harness-correction.json')).read_text())
    assert not harness['raw_warmup_stdout_retained'] and harness['completed_scored_runs_before_correction'] == 0
    assert harness['corrected_driver_sha256'] == env['driver_sha256']
    for name, digest in harness['retained_files'].items():
        assert sha(ROOT / name) == digest
    checks = json.loads((ART / (STEM + '-build-checks.json')).read_text())
    assert checks['selected_tests'] == 1973 and checks['default_tests'] == checks['restored_tests'] == 1969
    assert checks['outside_sandbox'] and checks['skips'] == checks['retries'] == 0
    assert any(c['phase'] == 'final direct spelling' for c in checks['checks'])
    for check in checks['checks']:
        assert check['exit_code'] == 0 and sha(ROOT / check['log']) == check['log_sha256']
    restoration = json.loads((ART / (STEM + '-restoration.json')).read_text())
    for name, digest in restoration['source_sha256'].items():
        old = subprocess.check_output(['git', 'show', plan['source_base'] + ':' + name], cwd=ROOT)
        assert hashlib.sha256(old).hexdigest() == digest == sha(ROOT / name)
    assert restoration['removed_trial_sources'] == [new_source] and not (ROOT / new_source).exists()
    assert restoration['canonical_sha256'] == plan['profile']['baseline_sha256'] == sha(ROOT / 'target/release/write-perf')
    for name in ['kv-engine/src/wal.rs', 'kv-engine/src/wal/parallel/runtime.rs']:
        assert (ROOT / name).read_bytes() == subprocess.check_output(['git', 'show', plan['source_base'] + ':' + name], cwd=ROOT)
    for cell in matrix['cells']:
        assert not (WORK / (cell + '-scratch')).exists()
    print(json.dumps(dict(source_base=plan['source_base'],reconstructed_inputs=129,verified_binaries=2,
                          invocations=40,records=48,scored_records=36,warmup_records=12,
                          excluded_profiles=2,incomplete_unscored_warmup_attempts=1,
                          retained_unique_invocation_ids=len(ids),cancelled_ssd_cells=6,
                          recognized_process_samples=sum(summary[c]['host_samples'] for c in matrix['cpu_order']),
                          source_reconstructs=True,summary_regenerates=True,default_restored=True,
                          allocator_unchanged=True,ssd_optimizations_preserved=True,scratch_removed=True,
                          checks_sha256=sha(ART / (STEM + '-build-checks.json')),
                          decision_sha256=sha(ART / (STEM + '-decision.json')),
                          verifier_sha256=sha(Path(__file__))),indent=2))


if __name__ == '__main__':
    main()
