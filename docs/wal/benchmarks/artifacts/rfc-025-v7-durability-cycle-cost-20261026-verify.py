#!/usr/bin/env python3
"""Reconstruct the measured candidate and verify captures and restored binaries."""
import csv
import hashlib
import json
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-durability-cycle-cost-20261026'
WORK = ROOT / 'target/wal-opt/20261026-v7-durability-cycle-cost'
CELLS = ('tmpfs-batch64', 'tmpfs-point8', 'tmpfs-point1', 'ext4-batch64', 'ext4-point8')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    environment = json.loads((ART / (STEM + '-environment.json')).read_text())
    verification = json.loads((ART / (STEM + '-verification.json')).read_text())
    identity = json.loads((ART / (STEM + '-binaries.json')).read_text())
    manifest = ART / (STEM + '-source-sha256.tsv')
    rows = list(csv.DictReader(manifest.open(), delimiter='\t'))
    assert len(rows) == 128
    assert sha(manifest) == verification['source_manifest_sha256']
    assert sha(ART / (STEM + '-candidate.patch')) == verification['patch_sha256']
    WORK.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='verify-candidate-', dir=WORK) as directory:
        reconstructed = Path(directory)
        for row in rows:
            relative = Path(row['path'])
            assert not relative.is_absolute() and '..' not in relative.parts
            source = subprocess.check_output(
                ['git', 'show', environment['source_base'] + ':' + row['path']], cwd=ROOT
            )
            assert (ROOT / relative).read_bytes() == source, row['path']
            path = reconstructed / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(source)
        subprocess.run(
            ['patch', '--batch', '--fuzz=0', '-p1', '-i', str(ART / (STEM + '-candidate.patch'))],
            cwd=reconstructed, check=True, capture_output=True,
        )
        for row in rows:
            assert sha(reconstructed / row['path']) == row['sha256'], row['path']
    for data in identity.values():
        assert sha(ROOT / data['path']) == data['sha256']
    assert sha(ROOT / 'target/release/write-perf') == identity['retained']['sha256']

    ids = set()
    for cell in CELLS:
        stem = 'rfc-025-v7-durability-cycle-cost-' + cell + '-20261026'
        runs = list(csv.DictReader((ART / (stem + '-runs.tsv')).open(), delimiter='\t'))
        expected = [('warmup', 0, arm) for arm in ('old_v4', 'old_v7', 'new_v4', 'new_v7')]
        for pair in range(1, (5 if cell.startswith('ext4') else 3) + 1):
            arms = ('old_v4', 'new_v4', 'old_v7', 'new_v7') if pair % 2 else ('new_v7', 'old_v7', 'new_v4', 'old_v4')
            expected += [('scored', pair, arm) for arm in arms]
        assert [(r['phase'], int(r['pair']), r['arm']) for r in runs] == expected
        for row in runs:
            assert row['run_id'] not in ids
            ids.add(row['run_id'])
        assert not (WORK / (stem + '-scratch')).exists()
        assert not (Path('/dev/shm') / (stem + '-scratch')).exists()
    assert len(ids) == 96
    regenerated = subprocess.check_output(
        ['python3', str(ART / (STEM + '-analyze.py')), *CELLS], cwd=ROOT
    )
    assert regenerated == (ART / (STEM + '-summary.json')).read_bytes()
    summary = json.loads(regenerated)
    assert summary['experiment_status']['failed_initial_guards'] == ['ssd_material_gain']
    assert not summary['experiment_status']['adopted']
    print('Verified 128 reconstructed inputs, restored source/binaries, 96 unique runs, arm order, scratch cleanup, and exact summary regeneration.')


if __name__ == '__main__':
    main()
