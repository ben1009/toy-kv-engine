#!/usr/bin/env python3
"""Verify requested SSD rerun, original captures, pooled comparisons, and peaks."""
import csv
import hashlib
import json
import statistics
from pathlib import Path

ART = Path(__file__).resolve().parent
BASE = 'rfc-025-v7-durability-cycle-cost'
STEM = BASE + '-ssd-rerun-20261026'
IDENTITY = json.loads((ART / (STEM + '-binaries.json')).read_text())
CELLS = {'ext4-batch64': (750000, 64, 1176), 'ext4-point8': (100000, None, 1000)}
ARMS = ('old_v4', 'new_v4', 'old_v7', 'new_v7')
PIPELINE = ('sync_count', 'fdatasync_ns', 'frontier_writes', 'frontier_write_ns',
            'sync_coalesce_wait_ns', 'preallocation_ns', 'commit_groups', 'commit_bytes')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def throughput(record):
    return record['result']['ops_per_sec']


def cpu(record):
    return record['result']['process_cpu_user_ms'] + record['result']['process_cpu_system_ms']


def load(prefix, cell, label):
    stem = prefix + '-' + cell + '-20261026'
    lines = (ART / (stem + '.jsonl')).read_bytes().splitlines()
    records = {}
    for line in lines:
        record = json.loads(line)
        assert record['run_id'] not in records
        records[record['run_id']] = (record, hashlib.sha256(line).hexdigest())
    rows = list(csv.DictReader((ART / (stem + '-runs.tsv')).open(), delimiter='\t'))
    assert len(rows) == len(records) == 24
    assert {row['run_id'] for row in rows} == set(records)
    expected = [('warmup', 0, arm) for arm in ('old_v4', 'old_v7', 'new_v4', 'new_v7')]
    for pair in range(1, 6):
        arms = ('old_v4', 'new_v4', 'old_v7', 'new_v7') if pair % 2 else ('new_v7', 'old_v7', 'new_v4', 'old_v4')
        expected += [('scored', pair, arm) for arm in arms]
    assert [(row['phase'], int(row['pair']), row['arm']) for row in rows] == expected
    operations, batch, samples = CELLS[cell]
    schedule = {}
    for row in rows:
        record, digest = records[row['run_id']]
        arm = row['arm']
        assert digest == row['raw_sha256']
        assert row['binary_sha256'] == IDENTITY['retained' if arm.startswith('old') else 'candidate']['sha256']
        assert row['filesystem'] == 'ext4'
        assert int(row['operations']) == operations and int(row['writers']) == 8
        assert record['workload'] == ('wal_batch_concurrent' if batch else 'wal_concurrent')
        assert record['params']['threads'] == 8
        assert record['params']['value_size'] == 1024
        assert record['params']['batch_size'] == batch
        assert record['params']['wal_io_mode'] == 'parallel'
        assert record['params']['wal_format_version'] == (4 if arm.endswith('v4') else 7)
        assert record['engine_options']['target_sst_size'] == 2147483648
        assert record['result']['ops'] == operations
        assert record['latency']['samples'] == samples
        pipeline = record['result']['wal_pipeline']
        if arm == 'new_v7':
            assert pipeline['v7_durability_cycle_ns'] >= pipeline['fdatasync_ns'] + pipeline['frontier_write_ns']
            assert 0 <= pipeline['v7_durability_cycle_promotions'] <= pipeline['sync_count']
        else:
            assert 'v7_durability_cycle_ns' not in pipeline
        schedule[row['phase'], int(row['pair']), arm] = record
    pairs = [dict(dataset=label, pair=pair, records={arm: schedule['scored', pair, arm] for arm in ARMS})
             for pair in range(1, 6)]
    hashes = {stem + suffix: sha(ART / (stem + suffix)) for suffix in ('.jsonl', '-runs.tsv', '-run.sh')}
    return pairs, list(records), rows, hashes


def compare(pairs):
    changes = []
    for pair in pairs:
        p = pair['records']
        raw = throughput(p['new_v7']) / throughput(p['old_v7'])
        control = throughput(p['new_v4']) / throughput(p['old_v4'])
        changes.append(dict(dataset=pair['dataset'], pair=pair['pair'],
                            v7_throughput_change=raw, v4_throughput_change=control,
                            normalized_v7_v4_change=raw / control,
                            v7_cpu_change=cpu(p['new_v7']) / cpu(p['old_v7']),
                            v7_p50_change=p['new_v7']['latency']['p50_ms'] / p['old_v7']['latency']['p50_ms'],
                            v7_p99_change=p['new_v7']['latency']['p99_ms'] / p['old_v7']['latency']['p99_ms']))
    cycles = [p['records']['new_v7']['result']['wal_pipeline'] for p in pairs]
    count = sum(p['sync_count'] for p in cycles)
    promotions = sum(p['v7_durability_cycle_promotions'] for p in cycles)
    return {
        'pair_count': len(pairs),
        'pairs': changes,
        'median_changes': {key: statistics.median(c[key] for c in changes)
                           for key in changes[0] if key not in ('dataset', 'pair')},
        'per_arm': {
            arm: {
                'median_puts_per_sec': statistics.median(throughput(p['records'][arm]) for p in pairs),
                'minimum_puts_per_sec': min(throughput(p['records'][arm]) for p in pairs),
                'peak_puts_per_sec': max(throughput(p['records'][arm]) for p in pairs),
                'peak_run_id': max((p['records'][arm] for p in pairs), key=throughput)['run_id'],
                'median_cpu_ms': statistics.median(cpu(p['records'][arm]) for p in pairs),
                'median_p50_ms': statistics.median(p['records'][arm]['latency']['p50_ms'] for p in pairs),
                'median_p99_ms': statistics.median(p['records'][arm]['latency']['p99_ms'] for p in pairs),
                'pipeline_medians': {key: statistics.median(p['records'][arm]['result']['wal_pipeline'][key] for p in pairs)
                                     for key in PIPELINE},
            } for arm in ARMS
        },
        'paired_v7_v4': {label: statistics.median(throughput(p['records'][prefix + '_v7']) /
                                                 throughput(p['records'][prefix + '_v4']) for p in pairs)
                          for label, prefix in (('retained', 'old'), ('candidate', 'new'))},
        'candidate_cycle_diagnostics': {
            'sync_cycles': count, 'changed_classifications': promotions,
            'changed_fraction': promotions / count,
            'mean_cycle_us': sum(p['v7_durability_cycle_ns'] for p in cycles) / count / 1000,
            'mean_fdatasync_us': sum(p['fdatasync_ns'] for p in cycles) / count / 1000,
            'mean_frontier_write_us': sum(p['frontier_write_ns'] for p in cycles) / count / 1000,
        },
    }


def main():
    output = {'cells': {}, 'input_sha256': {}, 'run_ids': [],
              'scope': 'Requested independent rerun; preserve original, rerun, and pooled SSD results. No source reinstallation.'}
    prior_peaks_path = ART / (BASE.replace('durability-cycle-cost', 'large-group-hint-cache') + '-20261026-peaks.json')
    prior_peaks = json.loads(prior_peaks_path.read_text())
    output['input_sha256'][prior_peaks_path.name] = sha(prior_peaks_path)
    for cell in CELLS:
        original, original_ids, original_rows, original_hashes = load(BASE, cell, 'original')
        rerun, rerun_ids, rerun_rows, rerun_hashes = load(BASE + '-ssd-rerun', cell, 'requested-rerun')
        output['input_sha256'].update(original_hashes | rerun_hashes)
        output['run_ids'] += original_ids + rerun_ids
        result = {label: compare(pairs) for label, pairs in [('original', original), ('rerun', rerun), ('pooled', original + rerun)]}
        winners = {key: prior_peaks[cell]['peak_scored_references'][key] for key in ('v4_control', 'retained_v7')}
        for label, pairs, rows, prefix in [('original', original, original_rows, BASE),
                                           ('requested-rerun', rerun, rerun_rows, BASE + '-ssd-rerun')]:
            by_id = {row['run_id']: row for row in rows}
            for pair in pairs:
                for arm, record in pair['records'].items():
                    key = 'v4_control' if arm.endswith('v4') else ('retained_v7' if arm.startswith('old') else 'candidate_v7')
                    if key in winners and winners[key]['puts_per_sec'] >= throughput(record):
                        continue
                    row = by_id[record['run_id']]
                    winners[key] = dict(puts_per_sec=throughput(record), run_id=record['run_id'], arm=arm,
                                        binary_sha256=row['binary_sha256'], raw_record_sha256=row['raw_sha256'],
                                        raw=prefix + '-' + cell + '-20261026.jsonl',
                                        manifest=prefix + '-' + cell + '-20261026-runs.tsv', dataset=label)
        result['strongest_matching_scored_references'] = winners
        output['cells'][cell] = result
    assert len(output['run_ids']) == len(set(output['run_ids'])) == 96
    output['counts'] = {'original_scored': 40, 'original_warmup': 8, 'rerun_scored': 40, 'rerun_warmup': 8}
    output['candidate_reinstalled'] = False
    for suffix in ('-binaries.json', '-plan.json', '-environment.json', '-run.py'):
        output['input_sha256'][STEM + suffix] = sha(ART / (STEM + suffix))
    output['analysis_sha256'] = sha(Path(__file__))
    print(json.dumps(output, indent=2))


if __name__ == '__main__':
    main()
