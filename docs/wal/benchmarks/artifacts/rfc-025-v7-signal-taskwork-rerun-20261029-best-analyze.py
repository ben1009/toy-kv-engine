#!/usr/bin/env python3
"""Select each exact binary's best scored throughput and its associated p99."""
import csv
import hashlib
import json
from pathlib import Path

ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-signal-taskwork-rerun-20261029'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    identity = json.loads((ART / (STEM + '-binaries.json')).read_text())
    by_hash = {item['sha256']: arm for arm, item in identity.items()}
    matrix = json.loads((ART / (STEM + '-matrix.json')).read_text())
    wanted = {(cfg['bench'], cfg['writers'], cfg['operations'], cfg['batch'], cfg['sample_every']):
              cell.removeprefix('ext4-').removesuffix('-confirm') for cell, cfg in matrix['cells'].items()}
    inputs = json.loads((ART / (STEM + '-best-inputs.json')).read_text())
    records = {}
    for source in inputs['sources']:
        for key in ['matrix', 'manifest', 'raw']:
            assert sha(ART / source[key]) == source[key + '_sha256']
        cfg = json.loads((ART / source['matrix']).read_text())['cells'][source['cell']]
        key = cfg['bench'], cfg['writers'], cfg['operations'], cfg['batch'], cfg['sample_every']
        if key not in wanted:
            continue
        cell = wanted[key]
        lines = (ART / source['raw']).read_bytes().splitlines()
        raw = {(r['run_id'], r['measurement']): (r, hashlib.sha256(line).hexdigest())
               for line in lines for r in [json.loads(line)]}
        assert len(raw) == len(lines)
        for row in csv.DictReader((ART / source['manifest']).open(), delimiter='\t'):
            if row['phase'] != 'scored' or row['binary_sha256'] not in by_hash:
                continue
            record, digest = raw[row['run_id'], row['part']]
            assert digest == row['raw_sha256']
            params = record['params']
            if params['wal_format_version'] != 7 or params['wal_io_mode'] != 'parallel':
                continue
            assert record['workload'] == cfg['bench'] and params['threads'] == cfg['writers']
            assert record['result']['ops'] == cfg['operations'] and params['batch_size'] == cfg['batch']
            assert params['value_size'] == 1024 and record['engine_options']['target_sst_size'] == 2147483648
            assert record['engine_options'] == dict(wal=True, serializable=False, value_separation=False,
                                                     compaction='none', target_sst_size=2147483648,
                                                     memtable_limit=2, cache_capacity=8192)
            assert params['seed'] == 42
            assert record['latency']['sample_every'] == cfg['sample_every']
            pipeline = record['result']['wal_pipeline']
            frontier_ms = pipeline['frontier_write_ns'] / max(pipeline['frontier_writes'], 1) / 1e6
            sync_ms = pipeline['fdatasync_ns'] / max(pipeline['sync_count'], 1) / 1e6
            item = dict(run_id=record['run_id'], binary_sha256=row['binary_sha256'],
                        arm=by_hash[row['binary_sha256']], workload=cell, puts_s=record['result']['ops_per_sec'],
                        p99_ms=record['latency']['p99_ms'], frontier_mean_ms=frontier_ms, fdatasync_mean_ms=sync_ms,
                        obvious_stall=frontier_ms >= 1 or sync_ms >= 1.5, data_bytes=pipeline['commit_bytes'],
                        raw=source['raw'], raw_sha256=digest, manifest=source['manifest'],
                        pair=int(row['pair']), current_cohort=source['manifest'].startswith(STEM + '-'))
            index = record['run_id'], row['part'], row['binary_sha256']
            assert index not in records, 'Duplicate scored invocation in frozen input index'
            records[index] = item

    def compare(current_only):
        output = {}
        for cell in wanted.values():
            arms = {}
            for arm in ['baseline', 'candidate']:
                available = [r for r in records.values() if r['workload'] == cell and r['arm'] == arm
                             and (not current_only or r['current_cohort'])]
                eligible = [r for r in available if not r['obvious_stall']]
                rank = lambda r: (r['puts_s'], r['run_id'])
                arms[arm] = dict(scored_runs=len(available), eligible_runs=len(eligible),
                                 excluded_runs=len(available) - len(eligible),
                                 best=max(eligible, key=rank) if eligible else None,
                                 raw_best=max(available, key=rank) if available else None)
            old, new = arms['baseline']['best'], arms['candidate']['best']
            ratios = dict(throughput=new['puts_s'] / old['puts_s'], p99=new['p99_ms'] / old['p99_ms']) if old and new else None
            target = cell == 'point8'
            guards = dict(available=old is not None and new is not None,
                          throughput=ratios is not None and (ratios['throughput'] > 1 if target else ratios['throughput'] >= .95),
                          p99=ratios is not None and (ratios['p99'] < 1 if target else ratios['p99'] <= 1.10),
                          identical_data_bytes=bool(old and new and old['data_bytes'] == new['data_bytes']))
            output[cell] = dict(arms=arms, ratios=ratios, guards=guards, passed=all(guards.values()))
        return dict(cells=output, passed=all(c['passed'] for c in output.values()))

    print(json.dumps(dict(selection='Highest throughput eligible scored run per exact binary/workload; p99 is from that same invocation. Individual stall exclusions; no pair matching or median ranking. Equal scores break by run ID.',
                          current=compare(True), historical=compare(False), unique_scored_records=len(records),
                          input_index_sha256=sha(ART / (STEM + '-best-inputs.json')),
                          maintainer_assessment_sha256=sha(ART / (STEM + '-maintainer-assessment.json')),
                          analyzer_sha256=sha(Path(__file__))), indent=2))


if __name__ == '__main__':
    main()
