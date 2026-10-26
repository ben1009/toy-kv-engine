#!/usr/bin/env python3
"""Verify the final native comparison and both excluded setup preflights."""
import csv
import hashlib
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-frontier-link-20261026'
WORK = ROOT / 'target/wal-opt/20261026-v7-frontier-link'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_json(path):
    return json.loads(path.read_text())


def main():
    summary_path = ART / (STEM + '-summary.json')
    regenerated = subprocess.check_output([sys.executable, str(ART / (STEM + '-analyze.py'))])
    assert regenerated == summary_path.read_bytes()
    summary = json.loads(regenerated)
    assert not summary['advance_to_engine_candidate'] and not any(summary['guards'].values())
    assert summary['host_samples'] == 151
    build = read_json(ART / (STEM + '-build.json'))
    assert sha(ROOT / build['binary_path']) == build['binary_sha256']
    ids = {json.loads(line)['run_id'] for line in (ART / (STEM + '.jsonl')).read_text().splitlines()}
    assert len(ids) == 48
    for attempt in ('preflight', 'preflight2'):
        prefix = STEM + '-' + attempt
        old_build = read_json(ART / (prefix + '-build.json'))
        failure = read_json(ART / (prefix + '-failure.json'))
        assert sha(ART / (prefix + '-probe.c')) == old_build['source_sha256'] == failure['initial_source_sha256']
        assert sha(WORK / ('frontier-probe-' + attempt)) == old_build['binary_sha256'] == failure['initial_binary_sha256']
        rows = list(csv.DictReader((ART / (prefix + '-runs.tsv')).open(), delimiter='\t'))
        raw = (ART / (prefix + '.jsonl')).read_bytes().splitlines()
        host = (ART / (prefix + '-host.jsonl')).read_bytes().splitlines()
        assert len(rows) == len(raw) == len(host) == 1
        row, record, audit = rows[0], json.loads(raw[0]), json.loads(host[0])
        assert row['raw_sha256'] == hashlib.sha256(raw[0]).hexdigest()
        assert row['audit_sha256'] == hashlib.sha256(host[0]).hexdigest()
        assert row['binary_sha256'] == old_build['binary_sha256']
        assert row['phase'] == record['phase'] == 'warmup'
        assert row['policy'] == record['policy'] == 'syscalls'
        assert audit['clean_sampled'] and record['benchmark']['readback_verified']
        assert record['run_id'] not in ids
        ids.add(record['run_id'])
    assert len(ids) == 50
    extent = 'rfc-025-v7-extent-io-20261026'
    engine_ids = set()
    for cell in ('ext4-point8', 'ext4-batch64'):
        for line in (ART / ('rfc-025-v7-extent-io-initial-' + cell + '-20261026.jsonl')).read_text().splitlines():
            run_id = json.loads(line)['run_id']
            assert run_id not in engine_ids
            engine_ids.add(run_id)
    profiles = {json.loads(line)['run_id'] for line in (ART / (extent + '-profiles.jsonl')).read_text().splitlines()}
    assert len(engine_ids) == 48 and len(profiles) == 3
    assert len(ids | engine_ids | profiles) == 101
    subprocess.run(['git', 'diff', '--exit-code', 'HEAD', '--', 'kv-engine', 'Cargo.toml', 'Cargo.lock'],
                   cwd=ROOT, check=True, capture_output=True)
    assert not (WORK / 'scratch').exists()
    assert not (WORK / 'register-check.data').exists()
    checks = read_json(ART / (extent + '-checks.json'))
    assert checks['restored_test_count'] == 1963 and checks['skips'] == checks['retries'] == 0
    print(json.dumps(dict(
        native_records=48, scored_native_records=40, native_warmups=8,
        excluded_setup_preflights=2, host_samples=151, verified_native_binaries=3,
        total_distinct_records_across_experiments=101, summary_regenerates=True,
        scratch_removed=True, engine_source_unchanged=True,
        summary_sha256=sha(summary_path),
        verification_script_sha256=sha(Path(__file__))), indent=2))


if __name__ == '__main__':
    main()
