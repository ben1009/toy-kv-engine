#!/usr/bin/env python3
"""Reconstruct the allocator experiment and verify the SSD/RSS evidence."""
import csv
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-mimalloc-ssd-followup-20261027'
BASE = 'e16e433a90a826d4f51fd8e29e5ec14f8f1ae0fc'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    plan = json.loads((ART / (STEM + '-plan.json')).read_text())
    manifest = list(csv.DictReader((ART / (STEM + '-source-sha256.tsv')).open(), delimiter='\t'))
    assert plan['source_base'] == BASE
    assert len(manifest) == len({row['path'] for row in manifest}) == 128
    scratch = ROOT / 'target/wal-opt/20261027-v7-mimalloc-ssd-followup/verification-source'
    assert not scratch.exists()
    scratch.mkdir()
    try:
        for row in manifest:
            path = scratch / row['path']
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(subprocess.check_output(['git', 'show', BASE + ':' + row['path']], cwd=ROOT))
        subprocess.run(['patch', '-p1', '-i', str(ART / (STEM + '-candidate.patch'))],
                       cwd=scratch, check=True, capture_output=True)
        for row in manifest:
            assert sha(scratch / row['path']) == row['sha256'], row['path']
    finally:
        shutil.rmtree(scratch)
    identity = json.loads((ART / (STEM + '-binaries.json')).read_text())
    assert len(identity) == 3
    for item in identity.values():
        assert sha(ROOT / item['path']) == item['sha256']
    regenerated = subprocess.check_output([sys.executable, str(ART / (STEM + '-analyze.py'))])
    assert regenerated == (ART / (STEM + '-summary.json')).read_bytes()
    summary = json.loads(regenerated)
    cells = list(plan['matrix'])
    ids = [run for cell in cells for run in summary[cell]['run_ids']]
    assert len(ids) == len(set(ids)) == 120
    peaks = json.loads((ART / (STEM + '-peaks.json')).read_text())
    assert peaks['source_base'] == BASE and set(peaks['cells']) == set(cells)
    for cell in cells:
        assert set(peaks['cells'][cell]) == set(plan['matrix'][cell]['arms'])
        for arm, value in peaks['cells'][cell].items():
            measured = summary[cell]['per_arm'][arm]
            assert value['run_id'] == measured['peak_run_id']
            assert value['raw_sha256'] == measured['peak_raw_sha256']
            assert value['puts_per_sec'] == measured['peak_puts_per_sec']
            assert value['binary'] == identity['baseline' if arm == 'base_v7' else ('retained' if arm.startswith('old') else 'candidate')]
    profile_lines = (ART / (STEM + '-profile.jsonl')).read_bytes().splitlines()
    assert len(profile_lines) == 1
    profile = json.loads(profile_lines[0])
    assert profile['run_id'] not in ids
    profile_meta = json.loads((ART / (STEM + '-profile.json')).read_text())
    assert profile_meta['excluded_from_scoring'] and profile_meta['run_id'] == profile['run_id']
    assert profile_meta['binary_sha256'] == identity['baseline']['sha256']
    for name, digest in profile_meta['files'].items():
        assert sha(ROOT / name) == digest
    proof = json.loads((ART / (STEM + '-allocator-proof.json')).read_text())
    assert not proof['c_malloc_overrides'] and proof['selected_symbols']
    assert not proof['system']['native_version_lines']
    assert any('v3.3.2' in line for line in proof['mimalloc']['native_version_lines'])
    for name in ['system', 'mimalloc']:
        assert proof[name]['exit_code'] == 0
        assert sha(ROOT / proof[name]['stderr']) == proof[name]['stderr_sha256']
    env = json.loads((ART / (STEM + '-environment.json')).read_text())
    assert env['source_base'] == BASE and not env['allocator_environment_overrides']
    for key, suffix in [('source_manifest_sha256', '-source-sha256.tsv'),
                        ('patch_sha256', '-candidate.patch'), ('plan_sha256', '-plan.json')]:
        assert env[key] == sha(ART / (STEM + suffix))
    checks = json.loads((ART / (STEM + '-checks.json')).read_text())
    assert checks['outside_sandbox'] and checks['mimalloc_test_count'] == checks['system_test_count'] == 1970
    assert checks['focused_test_count'] == 1 and checks['skips'] == checks['retries'] == 0
    assert checks['restored_test_count'] == 1969
    assert any(item['phase'] == 'restored System full check' for item in checks['checks'])
    for item in checks['checks']:
        assert item['exit_code'] == 0 and sha(ROOT / item['log']) == item['log_sha256']
    correction = checks['validation_correction']
    assert sha(ROOT / correction['log']) == correction['log_sha256']
    decision = json.loads((ART / (STEM + '-decision.json')).read_text())
    assert not decision['default_allocator_changed'] and decision['ssd_optimizations_preserved']
    assert decision['assessment_passed'] == summary['experiment_status']['assessment_passed']
    runtime = 'kv-engine/src/wal/parallel/runtime.rs'
    assert (ROOT / runtime).read_bytes() == subprocess.check_output(['git', 'show', BASE + ':' + runtime], cwd=ROOT)
    if not decision['allocator_source_retained']:
        restoration = json.loads((ART / (STEM + '-restoration.json')).read_text())
        for name, digest in restoration['source_sha256'].items():
            data = subprocess.check_output(['git', 'show', BASE + ':' + name], cwd=ROOT)
            assert hashlib.sha256(data).hexdigest() == digest == sha(ROOT / name)
        assert sha(ROOT / 'target/release/write-perf') == identity['baseline']['sha256']
        assert restoration['canonical_sha256'] == identity['baseline']['sha256']
    for cell in cells:
        capture = 'rfc-025-v7-mimalloc-ssd-followup-initial-' + cell + '-20261027'
        path = ROOT / 'target/wal-opt/20261027-v7-mimalloc-ssd-followup-initial' / (capture + '-scratch')
        assert not path.exists()
    print(json.dumps(dict(source_base=BASE, reconstructed_inputs=128, verified_binaries=3,
                          records=120, scored_records=90, warmups=30, excluded_profiles=1,
                          unique_engine_ids_including_profile=121,
                          host_samples=sum(summary[cell]['host_samples'] for cell in cells),
                          source_reconstructs=True, summary_regenerates=True, scratch_removed=True,
                          allocator_source_retained=decision['allocator_source_retained'],
                          default_allocator_changed=False, ssd_optimizations_preserved=True,
                          checks_record_sha256=sha(ART / (STEM + '-checks.json')),
                          decision_sha256=sha(ART / (STEM + '-decision.json')),
                          verification_script_sha256=sha(Path(__file__))), indent=2))


if __name__ == '__main__':
    main()
