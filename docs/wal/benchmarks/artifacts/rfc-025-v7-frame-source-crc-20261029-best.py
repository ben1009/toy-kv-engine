#!/usr/bin/env python3
"""Keep exact retained-binary historical highs beside the physical-frame CRC comparison."""
import csv
import hashlib
import json
from pathlib import Path

ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-frame-source-crc-20261029'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    plan = json.loads((ART / (STEM + '-plan.json')).read_text())
    matrix = json.loads((ART / (STEM + '-matrix.json')).read_text())
    summary = json.loads((ART / (STEM + '-summary.json')).read_text())
    historical = json.loads((ART / (STEM + '-historical-inputs.json')).read_text())
    indexed = {}
    for source in historical['sources']:
        for field in ['matrix', 'manifest', 'raw']:
            assert sha(ART / source[field]) == source[field + '_sha256']
        cfg = json.loads((ART / source['matrix']).read_text())['cells'][source['cell']]
        key = (cfg['bench'], cfg['operations'], cfg['writers'], cfg['batch'] or None, cfg['sample_every'])
        raw = {r['run_id']: (r, hashlib.sha256(line).hexdigest()) for line in (ART / source['raw']).read_bytes().splitlines() for r in [json.loads(line)]}
        for row in csv.DictReader((ART / source['manifest']).open(), delimiter='\t'):
            if row['phase'] != 'scored' or row['binary_sha256'] != plan['baseline']['sha256']:
                continue
            record, digest = raw[row['run_id']]
            assert digest == row['raw_sha256']
            p, result = record['params'], record['result']
            assert p['wal_io_mode'] == 'parallel' and p['wal_format_version'] in [4, 7]
            assert p['threads'] == cfg['writers'] and p['value_size'] == 1024 and p['batch_size'] == cfg['batch']
            assert result['ops'] == cfg['operations'] and record['latency']['sample_every'] == cfg['sample_every']
            assert record['engine_options']['target_sst_size'] == 2147483648 and record['engine_options']['compaction'] == 'none'
            q = result['wal_pipeline']
            marker = q['frontier_write_ns'] / max(q['frontier_writes'], 1) / 1e6
            sync = q['fdatasync_ns'] / max(q['sync_count'], 1) / 1e6
            item = dict(run_id=record['run_id'], format=p['wal_format_version'], puts_s=result['ops_per_sec'],
                        p99_ms=record['latency']['p99_ms'], flagged=marker >= 1 or sync >= 1.5,
                        frontier_mean_ms=marker, fdatasync_mean_ms=sync, raw=source['raw'], raw_sha256=digest)
            bucket = indexed.setdefault(key, {})
            assert item['run_id'] not in bucket
            bucket[item['run_id']] = item
    output = {}
    for cell, fresh in summary['cells'].items():
        cfg = matrix['cells'][cell]
        key = (cfg['bench'], cfg['operations'], cfg['writers'], cfg['batch'] or None, cfg['sample_every'])
        available = dict(indexed.get(key, {}))
        raw_path = ART / (STEM + '-' + cell + '.jsonl')
        raw = {r['run_id']: (r, hashlib.sha256(line).hexdigest()) for line in raw_path.read_bytes().splitlines() for r in [json.loads(line)]}
        for row in csv.DictReader((ART / (STEM + '-' + cell + '-runs.tsv')).open(), delimiter='\t'):
            if row['phase'] != 'scored' or row['binary_sha256'] != plan['baseline']['sha256']:
                continue
            r, digest = raw[row['run_id']]
            assert digest == row['raw_sha256']
            q = r['result']['wal_pipeline']
            marker = q['frontier_write_ns'] / max(q['frontier_writes'], 1) / 1e6
            sync = q['fdatasync_ns'] / max(q['sync_count'], 1) / 1e6
            assert row['run_id'] not in available
            available[row['run_id']] = dict(run_id=row['run_id'], format=r['params']['wal_format_version'],
                                            puts_s=r['result']['ops_per_sec'], p99_ms=r['latency']['p99_ms'],
                                            flagged=marker >= 1 or sync >= 1.5, frontier_mean_ms=marker,
                                            fdatasync_mean_ms=sync, raw=raw_path.name, raw_sha256=digest)
        arms = {}
        for fmt in [4, 7]:
            items = [r for r in available.values() if r['format'] == fmt]
            eligible = [r for r in items if not r['flagged']]
            arms['v' + str(fmt)] = dict(scored=len(items), eligible=len(eligible),
                                        best=max(eligible, key=lambda r: (r['puts_s'], r['run_id'])) if eligible else None)
        candidate = fresh['peaks']['candidate-v7']['best']
        baseline = arms['v7']['best']
        control = arms['v4']['best']
        output[cell] = dict(retained_historical_plus_fresh=arms, candidate_fresh_best=candidate,
                            candidate_vs_retained=dict(throughput=candidate['puts_s'] / baseline['puts_s'],
                                                       p99=candidate['p99_ms'] / baseline['p99_ms']) if candidate and baseline else None,
                            candidate_vs_v4=dict(throughput=candidate['puts_s'] / control['puts_s'],
                                                p99=candidate['p99_ms'] / control['p99_ms']) if candidate and control else None)
    print(json.dumps(dict(cells=output, method='Highest eligible throughput; p99 from that same invocation. Historical cohorts are unequal and provide context, not causal qualification.',
                          historical_index_sha256=sha(ART / (STEM + '-historical-inputs.json')),
                          analyzer_sha256=sha(Path(__file__))), indent=2))


if __name__ == '__main__':
    main()
