#!/usr/bin/env python3
"""Verify the fixed SSD rerun, its preflight, and exact-binary score history."""
import csv
import gzip
import hashlib
import json
import math
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
ART = Path(__file__).resolve().parent
WORK = ROOT / 'target/wal-opt/20261029-v7-ssd-rerun'
STEM = 'rfc-025-v7-ssd-rerun-20261029'
PARENT = 'rfc-025-v7-admitted-group-20261029'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(suffix):
    return json.loads((ART / (STEM + suffix)).read_text())


def regenerate(script, output):
    actual = subprocess.check_output([sys.executable, str(ART / (STEM + script))], cwd=ROOT)
    assert actual == (ART / (STEM + output)).read_bytes()
    return json.loads(actual)


def percentile(values, p):
    return sorted(values)[max(0, math.ceil(len(values) * p) - 1)] / 1e6


def main():
    plan, env, identity = read('-plan.json'), read('-environment.json'), read('-binaries.json')
    assert (ART / (STEM + '-parent-verification.json')).read_bytes() == (ART / (PARENT + '-verification.json')).read_bytes()
    parent = read('-parent-verification.json')
    assert sha(ART / (STEM + '-parent-verification.json')) == env['parent_receipt_sha256']
    baseline = list(csv.DictReader((ART / (PARENT + '-baseline-sha256.tsv')).open(), delimiter='\t'))
    assert len(baseline) == 128 and all(sha(ROOT / r['path']) == r['sha256'] for r in baseline)
    assert identity['retained-v4'] == identity['retained-v7'] == plan['baseline']
    assert identity['candidate-v7'] == plan['candidate']
    for item in identity.values():
        assert sha(ROOT / item['path']) == item['sha256']
    assert sha(ROOT / plan['candidate_patch']) == plan['candidate_patch_sha256'] == env['candidate_patch_sha256']
    for key, suffix in [('plan_sha256', '-plan.json'), ('matrix_sha256', '-matrix.json'),
                        ('driver_sha256', '-run.py'), ('child_sha256', '-child.py'),
                        ('analyzer_sha256', '-analyze.py'), ('orchestrator_sha256', '-run-all.py'),
                        ('identity_sha256', '-binaries.json'), ('historical_sha256', '-historical-inputs.json'),
                        ('historical_analyzer_sha256', '-best.py'),
                        ('captured_probe_script_sha256', '-preflight-probe-captured.txt'),
                        ('reproducer_probe_script_sha256', '-preflight-probe.py'),
                        ('preflight_probe_plan_sha256', '-preflight-probe-plan.json'),
                        ('preflight_probe_result_sha256', '-preflight-probe-result.json'),
                        ('preflight_probe_raw_sha256', '-preflight-probe-latencies.json.gz')]:
        assert env[key] == sha(ART / (STEM + suffix))
    assert ' ext4 ' in env['filesystem']
    probe = read('-preflight-probe-result.json')
    probe_plan = read('-preflight-probe-plan.json')
    assert probe['plan_sha256'] == env['preflight_probe_plan_sha256']
    assert probe['raw_sha256'] == env['preflight_probe_raw_sha256']
    assert probe['clean'] and not probe['stable'] and not probe['sample_errors']
    assert all(not s['competing'] for s in [probe['before'], *probe['host_samples'], probe['after']])
    with gzip.open(ART / (STEM + '-preflight-probe-latencies.json.gz'), 'rt') as stream:
        raw_probe = json.load(stream)
    assert len(raw_probe) == len(probe['passes']) == 4
    for raw, result in zip(raw_probe, probe['passes']):
        writes, syncs = raw['write_ns'], raw['sync_ns']
        assert len(writes) == result['writes'] == 32768 and len(syncs) == result['syncs'] == 8192
        assert percentile(writes, .99) == result['write_p99_ms'] and percentile(syncs, .99) == result['sync_p99_ms']
        assert sum(x >= 1000000 for x in writes) == result['write_at_least_1ms']
        assert sum(x >= 5000000 for x in writes) == result['write_at_least_5ms']
        bounds = probe_plan['eligibility']
        eligible = (result['write_p99_ms'] <= bounds['write_p99_ms_max']
                    and result['write_at_least_1ms'] / result['writes'] <= bounds['write_at_least_1ms_fraction_max']
                    and result['sync_p99_ms'] <= bounds['sync_p99_ms_max'])
        assert eligible == result['eligible'] is False
    summary = regenerate('-analyze.py', '-summary.json')
    regenerate('-best.py', '-best.json')
    assert summary['screens_complete'] and not summary['screen_passed']
    assert not summary['guards_complete'] and not summary['retention_passed']
    assert summary['missing_cells'] == plan['guard_cells']
    assert summary['invocations'] == 36 and summary['scored_records'] == 27
    current_ids = {run for cell in summary['cells'].values() for run in cell['run_ids']}
    assert len(current_ids) == 36
    older_ids = set()
    for path in ART.glob('rfc-025-v7-*.jsonl'):
        if path.name.startswith(STEM):
            continue
        for line in path.read_text().splitlines():
            record = json.loads(line)
            if isinstance(record, dict) and 'run_id' in record:
                older_ids.add(record['run_id'])
    assert len(older_ids) >= parent['full_archive_unique_invocations']
    assert current_ids.isdisjoint(older_ids)
    assert all(not (WORK / (cell + '-scratch')).exists() for cell in read('-matrix.json')['cells'])
    print(json.dumps(dict(source_retained=False, live_source_files_verified=len(baseline),
                          new_invocations=len(current_ids), scored_records=summary['scored_records'],
                          full_archive_unique_invocations=len(older_ids | current_ids),
                          failed_preflight_passes=4, summaries_regenerate=True,
                          screens_passed=summary['screen_passed'], guards_complete=summary['guards_complete'],
                          summary_sha256=sha(ART / (STEM + '-summary.json')),
                          historical_scores_sha256=sha(ART / (STEM + '-best.json')),
                          verifier_sha256=sha(Path(__file__))), indent=2))


if __name__ == '__main__':
    main()
