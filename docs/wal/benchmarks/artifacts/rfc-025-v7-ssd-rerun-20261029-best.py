#!/usr/bin/env python3
"""Rank matching exact-binary historical and fresh scored runs independently."""
import csv
import hashlib
import json
from pathlib import Path

ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-ssd-rerun-20261029'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(suffix):
    return json.loads((ART / (STEM + suffix)).read_text())


def main():
    plan, matrix, summary = read('-plan.json'), read('-matrix.json'), read('-summary.json')
    identities, history = read('-binaries.json'), read('-historical-inputs.json')
    indexed = {}
    sources = list(history['sources'])
    for cell in summary['cells']:
        name = STEM + '-' + cell
        sources.append(dict(cell=cell, matrix=STEM + '-matrix.json',
                            manifest=name + '-runs.tsv', raw=name + '.jsonl', fresh=True))
    for source in sources:
        if not source.get('fresh'):
            for field in ['matrix', 'manifest', 'raw']:
                assert sha(ART / source[field]) == source[field + '_sha256']
        cfg = json.loads((ART / source['matrix']).read_text())['cells'][source['cell']]
        key = (cfg['bench'], cfg['operations'], cfg['writers'], cfg['batch'] or None, cfg['sample_every'])
        raw = {r['run_id']: (r, hashlib.sha256(line).hexdigest())
               for line in (ART / source['raw']).read_bytes().splitlines() for r in [json.loads(line)]}
        for row in csv.DictReader((ART / source['manifest']).open(), delimiter='\t'):
            if row['phase'] != 'scored':
                continue
            binary_sha = row['binary_sha256']
            if binary_sha not in (identities['retained-v7']['sha256'], identities['candidate-v7']['sha256']):
                continue
            record, digest = raw[row['run_id']]
            assert digest == row['raw_sha256']
            p, result = record['params'], record['result']
            if p['wal_format_version'] == 4 and binary_sha == identities['retained-v4']['sha256']:
                arm = 'retained-v4'
            elif p['wal_format_version'] == 7 and binary_sha == identities['retained-v7']['sha256']:
                arm = 'retained-v7'
            elif p['wal_format_version'] == 7 and binary_sha == identities['candidate-v7']['sha256']:
                arm = 'candidate-v7'
            else:
                continue
            assert p['wal_io_mode'] == 'parallel'
            assert p['threads'] == cfg['writers'] and p['value_size'] == 1024 and p['batch_size'] == cfg['batch']
            assert result['ops'] == cfg['operations'] and record['latency']['sample_every'] == cfg['sample_every']
            assert record['engine_options']['target_sst_size'] == 2147483648 and record['engine_options']['compaction'] == 'none'
            pipeline = result['wal_pipeline']
            marker = pipeline['frontier_write_ns'] / max(pipeline['frontier_writes'], 1) / 1e6
            sync = pipeline['fdatasync_ns'] / max(pipeline['sync_count'], 1) / 1e6
            item = dict(run_id=row['run_id'], arm=arm, puts_s=result['ops_per_sec'],
                        p99_ms=record['latency']['p99_ms'],
                        flagged=marker >= plan['stall_filter']['frontier_mean_ms_at_least'] or sync >= plan['stall_filter']['fdatasync_mean_ms_at_least'],
                        frontier_mean_ms=marker, fdatasync_mean_ms=sync, raw=source['raw'],
                        raw_sha256=digest, fresh=bool(source.get('fresh')))
            bucket = indexed.setdefault(key, {})
            assert item['run_id'] not in bucket
            bucket[item['run_id']] = item
    output = {}
    for cell, fresh in summary['cells'].items():
        cfg = matrix['cells'][cell]
        key = (cfg['bench'], cfg['operations'], cfg['writers'], cfg['batch'] or None, cfg['sample_every'])
        items = indexed.get(key, {}).values()
        arms = {}
        for arm in cfg['arms']:
            scored = [item for item in items if item['arm'] == arm]
            eligible = [item for item in scored if not item['flagged']]
            arms[arm] = dict(scored=len(scored), eligible=len(eligible),
                             best=max(eligible, key=lambda r: (r['puts_s'], r['run_id'])) if eligible else None)
        output[cell] = dict(historical_plus_fresh=arms, fresh_peaks=fresh['peaks'])
    print(json.dumps(dict(cells=output, method='Highest eligible throughput per exact binary and workload; p99 is from the same invocation. Unequal historical cohorts are context rather than causal qualification.',
                          historical_index_sha256=sha(ART / (STEM + '-historical-inputs.json')),
                          analyzer_sha256=sha(Path(__file__))), indent=2))


if __name__ == '__main__':
    main()
