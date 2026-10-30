#!/usr/bin/env python3
"""Rank matching scored v4/v7 runs of the exact retained executable."""
import csv
import hashlib
import json
from pathlib import Path

ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-batch-attribution-20261029'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    plan = json.loads((ART / (STEM + '-plan.json')).read_text())
    inputs = json.loads((ART / (STEM + '-best-inputs.json')).read_text())
    records = {}
    for source in inputs['sources']:
        for field in ['matrix', 'manifest', 'raw']:
            assert sha(ART / source[field]) == source[field + '_sha256']
        cfg = json.loads((ART / source['matrix']).read_text())['cells'][source['cell']]
        if (cfg['bench'], cfg['operations'], cfg['batch'], cfg['sample_every']) != ('wal_batch_concurrent', 750000, 64, 10):
            continue
        if cfg['writers'] not in [1, 8]:
            continue
        raw = {r['run_id']: (r, hashlib.sha256(line).hexdigest())
               for line in (ART / source['raw']).read_bytes().splitlines() for r in [json.loads(line)]}
        for row in csv.DictReader((ART / source['manifest']).open(), delimiter='\t'):
            if row['phase'] != 'scored' or row['binary_sha256'] != plan['baseline']['sha256']:
                continue
            record, digest = raw[row['run_id']]
            assert digest == row['raw_sha256']
            p, q = record['params'], record['result']['wal_pipeline']
            assert p['wal_io_mode'] == 'parallel' and p['wal_format_version'] in [4, 7]
            assert p['value_size'] == 1024 and p['threads'] == cfg['writers'] and p['batch_size'] == 64
            assert record['result']['ops'] == 750000 and record['latency']['sample_every'] == 10
            assert record['engine_options']['target_sst_size'] == 2147483648
            assert record['engine_options']['compaction'] == 'none' and p['seed'] == 42
            marker = q['frontier_write_ns'] / max(q['frontier_writes'], 1) / 1e6
            sync = q['fdatasync_ns'] / q['sync_count'] / 1e6
            assert record['run_id'] not in records
            records[record['run_id']] = dict(run_id=record['run_id'], format=p['wal_format_version'], writers=p['threads'],
                                             puts_s=record['result']['ops_per_sec'], p99_ms=record['latency']['p99_ms'],
                                             flagged=marker >= 1 or sync >= 1.5, frontier_mean_ms=marker,
                                             fdatasync_mean_ms=sync, raw=source['raw'], raw_sha256=digest,
                                             manifest=source['manifest'], current=source['manifest'].startswith(STEM + '-'),
                                             cpu_ms=record['result']['process_cpu_user_ms'] + record['result']['process_cpu_system_ms'],
                                             elapsed_ms=record['result']['measure_elapsed_ms'], pipeline=q)

    def view(current_only):
        result = {}
        for writers in [1, 8]:
            arms = {}
            for fmt in [4, 7]:
                available = [r for r in records.values() if r['writers'] == writers and r['format'] == fmt
                             and (not current_only or r['current'])]
                eligible = [r for r in available if not r['flagged']]
                rank = lambda r: (r['puts_s'], r['run_id'])
                arms['v' + str(fmt)] = dict(scored=len(available), eligible=len(eligible),
                                            best=max(eligible, key=rank) if eligible else None,
                                            raw_best=max(available, key=rank) if available else None)
            old, new = arms['v4']['best'], arms['v7']['best']
            result['batch' + str(writers)] = dict(arms=arms, throughput_ratio=new['puts_s'] / old['puts_s'],
                                                  p99_ratio=new['p99_ms'] / old['p99_ms'])
        return result

    print(json.dumps(dict(method='Highest-throughput eligible scored invocation per exact-binary format/workload; p99 from the same record. Warmups and diagnostics excluded; raw maxima preserved.',
                          current=view(True), historical=view(False), scored_records=len(records),
                          input_index_sha256=sha(ART / (STEM + '-best-inputs.json')),
                          analyzer_sha256=sha(Path(__file__))), indent=2))


if __name__ == '__main__':
    main()
