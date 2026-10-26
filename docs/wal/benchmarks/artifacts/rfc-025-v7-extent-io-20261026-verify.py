#!/usr/bin/env python3
"""Reconstruct the exact rejected source and recheck the saved SSD evidence."""
import csv
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-extent-io-20261026'
BASE = '911ab1170027bc4c7faab84376a059417bb3e475'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    manifest_path = ART / (STEM + '-source-sha256.tsv')
    manifest = list(csv.DictReader(manifest_path.open(), delimiter='\t'))
    assert len(manifest) == len({r['path'] for r in manifest}) == 128
    scratch = ROOT / 'target/wal-opt/20261026-v7-extent-io/verification-source'
    assert not scratch.exists()
    scratch.mkdir()
    try:
        for row in manifest:
            data = subprocess.check_output(['git', 'show', BASE + ':' + row['path']], cwd=ROOT)
            path = scratch / row['path']
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        subprocess.run(['patch', '-p1', '-i', str(ART / (STEM + '-candidate.patch'))],
                       cwd=scratch, check=True, capture_output=True)
        for row in manifest:
            assert sha(scratch / row['path']) == row['sha256'], row['path']
    finally:
        shutil.rmtree(scratch)
    identity = json.loads((ART / (STEM + '-binaries.json')).read_text())
    for item in identity.values():
        assert sha(ROOT / item['path']) == item['sha256']
    assert sha(ROOT / 'target/release/write-perf') == identity['retained']['sha256']
    source_path = 'kv-engine/src/wal/parallel/runtime.rs'
    assert (ROOT / source_path).read_bytes() == subprocess.check_output(
        ['git', 'show', BASE + ':' + source_path], cwd=ROOT)
    regenerated = subprocess.check_output([
        sys.executable, str(ART / (STEM + '-analyze.py')), 'ext4-point8', 'ext4-batch64'])
    assert regenerated == (ART / (STEM + '-summary.json')).read_bytes()
    summary = json.loads(regenerated)
    assert not summary['ssd_guards']['ssd_phase_pass']
    assert sum(summary[cell]['host_samples'] for cell in ('ext4-point8', 'ext4-batch64')) == 471
    scored_ids = {run for cell in ('ext4-point8', 'ext4-batch64') for run in summary[cell]['run_ids']}
    diagnostic_ids = set()
    lines = (ART / (STEM + '-profiles.jsonl')).read_bytes().splitlines()
    profiles = {r['run_id']: (r, hashlib.sha256(line).hexdigest())
                for line in lines for r in [json.loads(line)]}
    rows = list(csv.DictReader((ART / (STEM + '-profiles.tsv')).open(), delimiter='\t'))
    assert len(lines) == len(profiles) == len(rows) == 3
    for row in rows:
        assert row['binary_sha256'] == identity['retained']['sha256']
        assert row['run_id'] in profiles
        assert profiles[row['run_id']][1] == row['raw_sha256']
        diagnostic_ids.add(row['run_id'])
    assert not scored_ids & diagnostic_ids
    checks = json.loads((ART / (STEM + '-checks.json')).read_text())
    for item in checks['checks']:
        assert item['exit_code'] == 0
        assert sha(ROOT / item['log']) == item['log_sha256']
    for cell in ('ext4-point8', 'ext4-batch64'):
        work = ROOT / 'target/wal-opt/20261026-v7-extent-io-initial'
        assert not (work / ('rfc-025-v7-extent-io-initial-' + cell + '-20261026-scratch')).exists()
    assert not (ROOT / 'target/wal-opt/20261026-v7-extent-io/profile-scratch').exists()
    print(json.dumps(dict(
        source_base=BASE, reconstructed_inputs=128, verified_binaries=2,
        engine_records=48, scored_records=40, warmups=8,
        excluded_profiles=3, host_samples=471, source_and_executable_restored=True,
        summary_regenerates=True, scratch_removed=True,
        checks_record_sha256=sha(ART / (STEM + '-checks.json')),
        verification_script_sha256=sha(Path(__file__))), indent=2))


if __name__ == '__main__':
    main()
