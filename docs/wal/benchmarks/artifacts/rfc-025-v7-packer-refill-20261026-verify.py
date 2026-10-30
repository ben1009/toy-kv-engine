#!/usr/bin/env python3
"""Verify both frozen refill prototypes, sources, raw records and host audits."""
import csv
import hashlib
import json
import subprocess
import tempfile
from pathlib import Path
ROOT = Path(__file__).resolve().parents[4]
ART = Path(__file__).resolve().parent
VARIANTS = ('packer-refill', 'small-packer-refill')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    all_ids = set()
    total_samples = 0
    for variant in VARIANTS:
        stem = 'rfc-025-v7-' + variant + '-20261026'
        environment = json.loads((ART / (stem + '-environment.json')).read_text())
        manifest = ART / (stem + '-source-sha256.tsv')
        assert sha(manifest) == environment['manifest_sha256']
        rows = list(csv.DictReader(manifest.open(), delimiter='\t'))
        assert len(rows) == 128
        with tempfile.TemporaryDirectory(prefix='verify-refill-', dir=ROOT / 'target/wal-opt') as directory:
            restored = Path(directory)
            for row in rows:
                relative = Path(row['path'])
                assert not relative.is_absolute() and '..' not in relative.parts
                data = subprocess.check_output(['git','show',environment['source_base'] + ':' + row['path']], cwd=ROOT)
                assert (ROOT / relative).read_bytes() == data, row['path']
                path = restored / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(data)
            subprocess.run(['patch','--batch','--fuzz=0','-p1','-i',str(ART / (stem + '-candidate.patch'))], cwd=restored, check=True, capture_output=True)
            for row in rows:
                assert sha(restored / row['path']) == row['sha256'], row['path']
        identities = json.loads((ART / (stem + '-binaries.json')).read_text())
        for data in identities.values():
            assert sha(ROOT / data['path']) == data['sha256']
        output = subprocess.check_output(['python3', str(ART / (stem + '-analyze.py')), 'ext4-point8', 'ext4-batch64'], cwd=ROOT)
        assert output == (ART / (stem + '-ssd-summary.json')).read_bytes()
        summary = json.loads(output)
        assert not summary['ssd_guards']['ssd_phase_pass']
        assert not json.loads((ART / (stem + '-assessment.json')).read_text())['adopted']
        assert sha(ROOT / 'target/release/write-perf') == identities['retained']['sha256']
        for cell in ('ext4-point8','ext4-batch64'):
            for run_id in summary[cell]['run_ids']:
                assert run_id not in all_ids
                all_ids.add(run_id)
            total_samples += summary[cell]['host_samples']
            scratch = ROOT / ('target/wal-opt/20261026-v7-' + variant + '-initial') / ('rfc-025-v7-' + variant + '-initial-' + cell + '-20261026-scratch')
            assert not scratch.exists()
        print(variant, 'verified exact source, binary identities, raw records, host audits and SSD summary')
    assert len(all_ids) == 96 and total_samples == 921
    checks = json.loads((ART / 'rfc-025-v7-packer-refill-20261026-checks.json').read_text())
    for item in checks.values():
        log = ROOT / item['log']
        assert sha(log) == item['sha256']
        assert item['nextest_run_id'] in log.read_text() and item['summary'] in log.read_text()
        assert item['retry_lines'] == 0
    peaks = json.loads((ART / 'rfc-025-v7-packer-refill-20261026-peaks.json').read_text())
    for cells in peaks.values():
        for arms in cells.values():
            for winner in arms.values():
                matches = [line for line in (ART / winner['raw']).read_bytes().splitlines() if json.loads(line)['run_id'] == winner['run_id']]
                assert len(matches) == 1 and hashlib.sha256(matches[0]).hexdigest() == winner['raw_record_sha256']
                assert json.loads(matches[0])['result']['ops_per_sec'] == winner['puts_per_sec']
                row = next(r for r in csv.DictReader((ART / winner['manifest']).open(), delimiter='\t') if r['run_id'] == winner['run_id'])
                assert row['phase'] == 'scored' and row['binary_sha256'] == winner['binary_sha256']
    print('Verified',len(all_ids),'unique initial records,',total_samples,'clear host samples, exact peaks, passing checks and restored source/canonical binary.')


if __name__ == '__main__':
    main()
