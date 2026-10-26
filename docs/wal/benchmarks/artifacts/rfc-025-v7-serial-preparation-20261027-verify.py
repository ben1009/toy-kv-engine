#!/usr/bin/env python3
"""Reconstruct both serial-preparation prototypes and verify their SSD evidence."""
import csv
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
ART = Path(__file__).resolve().parent
BASE = '0ffebd516b865b01371ed65014ac0347ae2fb420'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify(variant, cells, expected_records):
    stem = 'rfc-025-v7-' + variant + '-20261027'
    manifest = list(csv.DictReader((ART / (stem + '-source-sha256.tsv')).open(), delimiter='\t'))
    assert len(manifest) == len({row['path'] for row in manifest}) == 128
    scratch = ROOT / ('target/wal-opt/20261027-v7-' + variant + '/verification-source')
    assert not scratch.exists()
    scratch.mkdir()
    try:
        for row in manifest:
            path = scratch / row['path']
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(subprocess.check_output(['git', 'show', BASE + ':' + row['path']], cwd=ROOT))
        subprocess.run(['patch', '-p1', '-i', str(ART / (stem + '-candidate.patch'))],
                       cwd=scratch, check=True, capture_output=True)
        for row in manifest:
            assert sha(scratch / row['path']) == row['sha256'], row['path']
    finally:
        shutil.rmtree(scratch)
    identity = json.loads((ART / (stem + '-binaries.json')).read_text())
    assert len(identity) == 3
    for item in identity.values():
        assert sha(ROOT / item['path']) == item['sha256']
    regenerated = subprocess.check_output([sys.executable, str(ART / (stem + '-analyze.py')), *cells])
    assert regenerated == (ART / (stem + '-summary.json')).read_bytes()
    summary = json.loads(regenerated)
    ids = [run for cell in cells for run in summary[cell]['run_ids']]
    assert len(ids) == len(set(ids)) == expected_records
    env = json.loads((ART / (stem + '-environment.json')).read_text())
    assert env['source_base'] == BASE
    for key, suffix in [('source_manifest_sha256', '-source-sha256.tsv'),
                        ('patch_sha256', '-candidate.patch'), ('plan_sha256', '-plan.json')]:
        assert env[key] == sha(ART / (stem + suffix))
    checks = json.loads((ART / (stem + '-checks.json')).read_text())
    assert checks['test_count'] == 1969 and checks['focused_test_count'] == 9
    assert checks['outside_sandbox'] and checks['skips'] == checks['retries'] == 0
    for item in checks['checks']:
        assert item['exit_code'] == 0 and sha(ROOT / item['log']) == item['log_sha256']
    for item in checks.get('earlier_validation_corrections', []):
        assert sha(ROOT / item['log']) == item['sha256']
    for cell in cells:
        capture = 'rfc-025-v7-' + variant + '-initial-' + cell + '-20261027'
        scratch = ROOT / ('target/wal-opt/20261027-v7-' + variant + '-initial') / (capture + '-scratch')
        assert not scratch.exists()
    return summary, set(ids), dict(source_inputs=128, binary_identities=3,
                                   records=len(ids), host_samples=sum(summary[cell]['host_samples'] for cell in cells),
                                   source_manifest_sha256=sha(ART / (stem + '-source-sha256.tsv')),
                                   summary_sha256=sha(ART / (stem + '-summary.json')),
                                   checks_sha256=sha(ART / (stem + '-checks.json')),
                                   source_reconstructs=True, summary_regenerates=True, scratch_removed=True)


def main():
    serial_cells = ['ext4-point16', 'ext4-batch16']
    demand_cells = ['ext4-point16', 'ext4-batch16', 'ext4-point1']
    startup_stem = 'rfc-025-v7-serial-startup-20261027'
    startup_cancelled = (ART / (startup_stem + '-cancellation.json')).exists()
    startup_cells = ['ext4-point1'] if startup_cancelled else ['ext4-point1', 'ext4-point8', 'ext4-batch8', 'ext4-point16', 'ext4-batch16']
    startup_count = 30 if startup_cancelled else 94
    serial, serial_ids, serial_result = verify('serial-extent', serial_cells, 32)
    demand, demand_ids, demand_result = verify('demand-extent', demand_cells, 62)
    startup, startup_ids, startup_result = verify('serial-startup', startup_cells, startup_count)
    assert not serial_ids & demand_ids and not serial_ids & startup_ids and not demand_ids & startup_ids
    assert not serial['experiment_status']['candidate_eligible_for_retention']
    assert not serial['ext4-point16']['guards']['p99_ceiling']
    assert not demand['ext4-point1']['guards']['pre_async_p99_ceiling']
    for variant, cells, scored, warmups, cancelled in [
            ('serial-extent', serial_cells, 24, 8, {'ext4-point1', 'ext4-point8', 'ext4-batch8'}),
            ('demand-extent', demand_cells, 49, 13, {'ext4-point8', 'ext4-batch8'})]:
        stem = 'rfc-025-v7-' + variant + '-20261027'
        cancellation = json.loads((ART / (stem + '-cancellation.json')).read_text())
        assert cancellation['completed_cells'] == cells
        assert cancellation['scored_records'] == scored and cancellation['warmups'] == warmups
        assert set(cancellation['cancelled_cells']) == cancelled and not cancellation['prototype_qualified']
        for cell in cancelled:
            path = ART / ('rfc-025-v7-' + variant + '-initial-' + cell + '-20261027.jsonl')
            assert not path.exists() or not path.read_bytes()
    decision = json.loads((ART / (startup_stem + '-decision.json')).read_text())
    assert decision['original_async_retained']
    assert decision['summary_sha256'] == sha(ART / (startup_stem + '-summary.json'))
    assert decision['guards_passed'] == startup['experiment_status']['candidate_eligible_for_retention']
    if decision['retained']:
        if not decision['guards_passed']:
            assert decision['review_only'] and decision['policy_deviation']
            assert startup['ext4-point1']['cell_pass']
            failed = {cell: [key for key, passed in startup[cell]['guards'].items() if not passed]
                      for cell in startup_cells if not startup[cell]['cell_pass']}
            assert decision['failed_guards'] == failed
        manifest = list(csv.DictReader((ART / (startup_stem + '-source-sha256.tsv')).open(), delimiter='\t'))
        for row in manifest:
            data = subprocess.check_output(['git', 'show', decision['source_commit'] + ':' + row['path']], cwd=ROOT)
            assert hashlib.sha256(data).hexdigest() == row['sha256']
    regenerated = subprocess.check_output([sys.executable, str(ART / 'rfc-025-v7-serial-preparation-20261027-peaks.py')])
    assert regenerated == (ART / 'rfc-025-v7-serial-preparation-20261027-peaks.json').read_bytes()
    print(json.dumps(dict(source_base=BASE, serial=serial_result, demand=demand_result,
                          startup=startup_result, rejected_prototypes=2,
                          unrun_cells_explicitly_cancelled=True, original_async_kept=True,
                          startup_fix_retained=decision['retained'], startup_guards_passed=decision['guards_passed'],
                          unique_records=94 + startup_count,
                          scored_records=73 + (25 if startup_cancelled else 73),
                          warmups=21 + (5 if startup_cancelled else 21),
                          verification_script_sha256=sha(Path(__file__))), indent=2))


if __name__ == '__main__':
    main()
