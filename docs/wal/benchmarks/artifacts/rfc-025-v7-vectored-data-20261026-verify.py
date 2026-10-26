#!/usr/bin/env python3
"""Reconstruct the rejected prototype and verify its complete SSD capture."""
import csv
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-vectored-data-20261026'
BASE = 'e2989d6d13b2d4b02d00212788e3988443206d75'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    manifest = list(csv.DictReader((ART / (STEM + '-source-sha256.tsv')).open(), delimiter='\t'))
    assert len(manifest) == len({row['path'] for row in manifest}) == 128
    scratch = ROOT / 'target/wal-opt/20261026-v7-vectored-data/verification-source'
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
    for item in identity.values():
        assert sha(ROOT / item['path']) == item['sha256']
    restoration = json.loads((ART / (STEM + '-restoration.json')).read_text())
    assert restoration['before_next_candidate'] and restoration['source_base'] == BASE
    assert restoration['canonical_sha256'] == identity['retained']['sha256']
    for name, digest in restoration['source_sha256'].items():
        data = subprocess.check_output(['git', 'show', BASE + ':kv-engine/src/wal/parallel/' + name], cwd=ROOT)
        assert hashlib.sha256(data).hexdigest() == digest
    regenerated = subprocess.check_output([sys.executable, str(ART / (STEM + '-analyze.py')),
                                           'ext4-point8', 'ext4-batch64'])
    assert regenerated == (ART / (STEM + '-summary.json')).read_bytes()
    summary = json.loads(regenerated)
    assert not summary['ssd_guards']['ssd_phase_pass']
    ids = [run for cell in ('ext4-point8', 'ext4-batch64') for run in summary[cell]['run_ids']]
    assert len(ids) == len(set(ids)) == 48
    peaks = json.loads((ART / (STEM + '-peaks.json')).read_text())
    for cell in ('ext4-point8', 'ext4-batch64'):
        capture = 'rfc-025-v7-vectored-data-initial-' + cell + '-20261026'
        raw = {r['run_id']: (r, hashlib.sha256(line).hexdigest())
               for line in (ART / (capture + '.jsonl')).read_bytes().splitlines()
               for r in [json.loads(line)]}
        rows = list(csv.DictReader((ART / (capture + '-runs.tsv')).open(), delimiter='\t'))
        for arm, peak in peaks[cell].items():
            scored = [row for row in rows if row['phase'] == 'scored' and row['arm'] == arm]
            best = max(scored, key=lambda row: raw[row['run_id']][0]['result']['ops_per_sec'])
            record, digest = raw[best['run_id']]
            assert peak['run_id'] == best['run_id'] and peak['raw_sha256'] == digest
            assert peak['puts_per_sec'] == record['result']['ops_per_sec']
            assert peak['binary_sha256'] == best['binary_sha256']
        work = ROOT / 'target/wal-opt/20261026-v7-vectored-data-initial'
        assert not (work / (capture + '-scratch')).exists()
    checks = json.loads((ART / (STEM + '-checks.json')).read_text())
    for item in checks['checks']:
        assert item['exit_code'] == 0
        assert sha(ROOT / item['log']) == item['log_sha256']
    assert checks['prototype_test_count'] == 1969
    print(json.dumps(dict(source_base=BASE, reconstructed_inputs=128, verified_binaries=2,
                          engine_records=48, scored_records=40, warmups=8,
                          host_samples=sum(summary[cell]['host_samples'] for cell in peaks),
                          restoration_record_verified=True, summary_regenerates=True,
                          peaks_verified=True, scratch_removed=True,
                          checks_record_sha256=sha(ART / (STEM + '-checks.json')),
                          verification_script_sha256=sha(Path(__file__))), indent=2))


if __name__ == '__main__':
    main()
