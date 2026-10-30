#!/usr/bin/env python3
"""Verify the async extent experiment and explicit maintainer retention."""
import csv
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-async-extent-20261026'
BASE = 'e2989d6d13b2d4b02d00212788e3988443206d75'
CELLS = ('ext4-point8', 'ext4-batch64', 'ext4-point1', 'ext4-point2', 'ext4-point4')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    manifest = list(csv.DictReader((ART / (STEM + '-source-sha256.tsv')).open(), delimiter='\t'))
    assert len(manifest) == len({row['path'] for row in manifest}) == 128
    scratch = ROOT / 'target/wal-opt/20261026-v7-async-extent/verification-source'
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
    assert restoration['source_base'] == BASE
    assert restoration['canonical_sha256'] == identity['retained']['sha256']
    for name, digest in restoration['source_sha256'].items():
        data = subprocess.check_output(['git', 'show', BASE + ':' + name], cwd=ROOT)
        assert hashlib.sha256(data).hexdigest() == digest
    retention = json.loads((ART / (STEM + '-maintainer-retention.json')).read_text())
    assert retention['retained'] and not retention['original_performance_guards_passed']
    assert retention['source_manifest_sha256'] == sha(ART / (STEM + '-source-sha256.tsv'))
    assert retention['candidate_sha256'] == identity['candidate']['sha256']
    for row in manifest:
        data = subprocess.check_output(['git', 'show', retention['source_commit'] + ':' + row['path']], cwd=ROOT)
        assert hashlib.sha256(data).hexdigest() == row['sha256']
    summaries = []
    for suffix, cells in [('-summary.json', CELLS[:2]), ('-low-summary.json', CELLS[2:])]:
        regenerated = subprocess.check_output([sys.executable, str(ART / (STEM + '-analyze.py')), *cells])
        assert regenerated == (ART / (STEM + suffix)).read_bytes()
        summaries.append(json.loads(regenerated))
    initial, low = summaries
    assert initial['ssd_guards']['ssd_phase_pass']
    assert not low['low_concurrency_guards']['low_concurrency_phase_pass']
    decision = json.loads((ART / (STEM + '-decision.json')).read_text())
    assert not decision['retained']
    failures = sorted(key for key, passed in low['low_concurrency_guards'].items()
                      if not passed and key != 'low_concurrency_phase_pass')
    assert decision['failed_guards'] == failures
    combined = {cell: (initial if cell in CELLS[:2] else low)[cell] for cell in CELLS}
    ids = [run for part in combined.values() for run in part['run_ids']]
    assert len(ids) == len(set(ids)) == 96
    vector_ids = {json.loads(line)['run_id']
                  for cell in CELLS[:2]
                  for line in (ART / ('rfc-025-v7-vectored-data-initial-' + cell + '-20261026.jsonl')).read_bytes().splitlines()}
    assert not set(ids) & vector_ids
    peaks = json.loads((ART / (STEM + '-peaks.json')).read_text())
    for cell in CELLS:
        capture = 'rfc-025-v7-async-extent-initial-' + cell + '-20261026'
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
        work = ROOT / 'target/wal-opt/20261026-v7-async-extent-initial'
        assert not (work / (capture + '-scratch')).exists()
    for cell in CELLS[:2]:
        assert not (ART / ('rfc-025-v7-async-extent-confirmation-' + cell + '-20261026.jsonl')).exists()
    env = json.loads((ART / (STEM + '-environment.json')).read_text())
    for key, suffix in [('source_manifest_sha256', '-source-sha256.tsv'),
                        ('patch_sha256', '-candidate.patch'), ('plan_sha256', '-plan.json')]:
        assert env[key] == sha(ART / (STEM + suffix))
    checks = json.loads((ART / (STEM + '-checks.json')).read_text())
    for item in checks['checks']:
        assert item['exit_code'] == 0
        assert sha(ROOT / item['log']) == item['log_sha256']
    for item in checks['earlier_validation_corrections']:
        assert sha(ROOT / item['log']) == item['sha256']
    assert checks['prototype_test_count'] == 1968 and checks['restored_test_count'] == 1963
    print(json.dumps(dict(source_base=BASE, reconstructed_inputs=128, verified_binaries=2,
                          engine_records=96, scored_records=76, warmups=20,
                          host_samples=sum(part['host_samples'] for part in combined.values()),
                          initial_phase_passed=True, low_concurrency_phase_passed=False,
                          confirmation_cancelled=True, failed_guards=failures,
                          historical_restoration_verified=True, retained_by_maintainer=True,
                          retained_source_commit=retention['source_commit'], summaries_regenerate=True,
                          peaks_verified=True, scratch_removed=True,
                          checks_record_sha256=sha(ART / (STEM + '-checks.json')),
                          verification_script_sha256=sha(Path(__file__))), indent=2))


if __name__ == '__main__':
    main()
