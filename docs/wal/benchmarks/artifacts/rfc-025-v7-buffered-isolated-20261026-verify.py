#!/usr/bin/env python3
"""Reconstruct both frozen prototypes and verify audited rerun evidence."""
import csv
import hashlib
import json
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
ART = Path(__file__).resolve().parent
VARIANTS = ('buffered-frontier', 'buffered-parallel')
CELLS = ('ext4-point8', 'ext4-batch64')
STEM = 'rfc-025-v7-buffered-isolated-20261026'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    fresh_ids, earlier_ids, samples = set(), set(), 0
    for variant in VARIANTS:
        prefix = 'rfc-025-v7-' + variant + '-20261026'
        environment = json.loads((ART / (prefix + '-environment.json')).read_text())
        rows = list(csv.DictReader((ART / (prefix + '-source-sha256.tsv')).open(), delimiter='\t'))
        assert len(rows) == 128
        assert sha(ART / (prefix + '-source-sha256.tsv')) == environment['manifest_sha256']
        with tempfile.TemporaryDirectory(prefix='verify-buffered-', dir=ROOT / 'target/wal-opt') as directory:
            reconstructed = Path(directory)
            for row in rows:
                relative = Path(row['path'])
                assert not relative.is_absolute() and '..' not in relative.parts
                original = subprocess.check_output(
                    ['git', 'show', environment['source_base'] + ':' + row['path']], cwd=ROOT
                )
                assert (ROOT / relative).read_bytes() == original, row['path']
                path = reconstructed / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(original)
            subprocess.run(['patch', '--batch', '--fuzz=0', '-p1', '-i', str(ART / (prefix + '-candidate.patch'))],
                           cwd=reconstructed, check=True, capture_output=True)
            for row in rows:
                assert sha(reconstructed / row['path']) == row['sha256'], row['path']
        identity = json.loads((ART / (prefix + '-binaries.json')).read_text())
        for data in identity.values():
            assert sha(ROOT / data['path']) == data['sha256']
        assert sha(ROOT / 'target/release/write-perf') == identity['retained']['sha256']
        regenerated = subprocess.check_output(['python3', str(ART / (STEM + '-analyze.py')), variant], cwd=ROOT)
        summary_path = ART / ('rfc-025-v7-' + variant + '-isolated-20261026-summary.json')
        assert regenerated == summary_path.read_bytes()
        summary = json.loads(regenerated)
        assert not summary['ssd_guards']['ssd_phase_pass'] and not summary['adopted']
        for run_id in summary['run_ids']:
            assert run_id not in fresh_ids
            fresh_ids.add(run_id)
        samples += sum(x['host_samples'] for x in summary['isolation'].values())
        assessment = json.loads((ART / (prefix + '-assessment.json')).read_text())
        assert 'Potentially contaminated' in assessment['qualification_status']
        for cell in CELLS:
            stem = 'rfc-025-v7-' + variant + '-' + cell + '-20261026'
            rows = list(csv.DictReader((ART / (stem + '-runs.tsv')).open(), delimiter='\t'))
            lines = (ART / (stem + '.jsonl')).read_bytes().splitlines()
            records = {json.loads(line)['run_id']: hashlib.sha256(line).hexdigest() for line in lines}
            assert len(rows) == len(records) == len(lines) == 24
            for row in rows:
                assert records[row['run_id']] == row['raw_sha256']
                assert row['run_id'] not in earlier_ids
                earlier_ids.add(row['run_id'])
            scratch = ROOT / ('target/wal-opt/20261026-v7-' + variant) / (stem + '-scratch')
            assert not scratch.exists()
            new_stem = 'rfc-025-v7-' + variant + '-isolated-' + cell + '-20261026'
            scratch = ROOT / ('target/wal-opt/20261026-v7-' + variant + '-isolated') / (new_stem + '-scratch')
            assert not scratch.exists()
    assert len(fresh_ids) == len(earlier_ids) == 96 and not fresh_ids & earlier_ids
    assert samples == 967
    peaks = json.loads((ART / (STEM + '-peaks.json')).read_text())
    for entries in peaks.values():
        for winner in entries.values():
            matches = [line for line in (ART / winner['raw']).read_bytes().splitlines()
                       if json.loads(line)['run_id'] == winner['run_id']]
            assert len(matches) == 1 and hashlib.sha256(matches[0]).hexdigest() == winner['raw_record_sha256']
            assert json.loads(matches[0])['result']['ops_per_sec'] == winner['puts_per_sec']
            rows = list(csv.DictReader((ART / winner['manifest']).open(), delimiter='\t'))
            row = next(r for r in rows if r['run_id'] == winner['run_id'])
            assert row['phase'] == 'scored' and row['binary_sha256'] == winner['binary_sha256']
            assert row['isolation_status'] == 'clean_sampled'
    print('Verified two 128-input source reconstructions, saved/canonical binaries, 192 unique records, 967 clear host samples, exact summaries, peaks, and scratch cleanup.')


if __name__ == '__main__':
    main()
