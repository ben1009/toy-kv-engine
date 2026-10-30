#!/usr/bin/env python3
"""Verify serial-preparation and 8/16-writer SSD comparisons."""
import csv
import hashlib
import json
import statistics
import sys
from pathlib import Path

ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-serial-startup-20261027'
PLAN = json.loads((ART / (STEM + '-plan.json')).read_text())
IDENTITY = json.loads((ART / (STEM + '-binaries.json')).read_text())
PIPELINE = ('commit_groups', 'commit_buffers', 'commit_bytes', 'sync_count', 'fdatasync_ns',
            'frontier_writes', 'frontier_bytes', 'frontier_write_ns', 'preallocation_ns',
            'sync_coalesce_wait_calls', 'sync_coalesce_wait_ns', 'wal_sync_wait_ns', 'memtable_insert_ns')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def cpu(record):
    return record['result']['process_cpu_user_ms'] + record['result']['process_cpu_system_ms']


def rate(record):
    return record['result']['ops_per_sec']


def compare(cell):
    cfg = PLAN['matrix'][cell]
    arms = tuple(cfg['arms'])
    warmups = (('base_v7',) if 'base_v7' in arms else ()) + ('old_v4', 'old_v7', 'new_v4', 'new_v7')
    stem = 'rfc-025-v7-serial-startup-initial-' + cell + '-20261027'
    raw = {r['run_id']: (r, hashlib.sha256(line).hexdigest())
           for line in (ART / (stem + '.jsonl')).read_bytes().splitlines() for r in [json.loads(line)]}
    rows = list(csv.DictReader((ART / (stem + '-runs.tsv')).open(), delimiter='\t'))
    audit = {r['run_id']: (r, hashlib.sha256(line).hexdigest())
             for line in (ART / (stem + '-host.jsonl')).read_bytes().splitlines() for r in [json.loads(line)]}
    count = len(arms) * (cfg['pairs'] + 1)
    assert len(raw) == len(rows) == len(audit) == count
    assert set(raw) == set(audit) == {row['run_id'] for row in rows}
    expected = [('warmup', 0, arm) for arm in warmups]
    for pair in range(1, cfg['pairs'] + 1):
        expected += [('scored', pair, arm) for arm in (arms if pair % 2 else tuple(reversed(arms)))]
    assert [(row['phase'], int(row['pair']), row['arm']) for row in rows] == expected
    schedule, host_samples = {}, 0
    for row in rows:
        arm, pair, phase = row['arm'], int(row['pair']), row['phase']
        record, raw_digest = raw[row['run_id']]
        item, audit_digest = audit[row['run_id']]
        identity = IDENTITY['pre_async' if arm == 'base_v7' else ('retained' if arm.startswith('old') else 'candidate')]
        assert row['raw_sha256'] == raw_digest and row['audit_sha256'] == audit_digest
        assert row['binary_sha256'] == identity['sha256'] == item['binary_sha256']
        assert record['params']['wal_format_version'] == (4 if arm.endswith('v4') else 7)
        assert record['params']['wal_io_mode'] == 'parallel'
        assert record['params']['threads'] == cfg['writers']
        assert record['params']['batch_size'] == cfg['batch'] and record['params']['value_size'] == 1024
        assert record['engine_options']['target_sst_size'] == 2147483648
        assert record['workload'] == ('wal_batch_concurrent' if cfg['batch'] else 'wal_concurrent')
        assert record['result']['ops'] == cfg['operations']
        assert record['latency']['sample_every'] == cfg['sample_every']
        assert record['latency']['samples'] == cfg['samples']
        assert item['arm'] == arm and item['pair'] == pair and item['phase'] == phase
        assert item['clean_sampled'] and row['isolation_status'] == 'clean_sampled'
        assert item['returncode'] == 0 and not item['sampling_errors'] and item['during']
        for sample in [item['before'], *item['during'], item['after']]:
            assert not sample['competing']
            host_samples += 1
        key = (phase, pair, arm)
        assert key not in schedule
        schedule[key] = record
    pairs = []
    for pair in range(1, cfg['pairs'] + 1):
        p = {arm: schedule['scored', pair, arm] for arm in arms}
        old, new = p['old_v7'], p['new_v7']
        x = dict(pair=pair,
                 v7_throughput_change=rate(new) / rate(old),
                 v4_throughput_change=rate(p['new_v4']) / rate(p['old_v4']),
                 normalized_v7_v4_change=(rate(new) / rate(p['new_v4'])) / (rate(old) / rate(p['old_v4'])),
                 v7_cpu_change=cpu(new) / cpu(old),
                 v7_p50_change=new['latency']['p50_ms'] / old['latency']['p50_ms'],
                 v7_p99_change=new['latency']['p99_ms'] / old['latency']['p99_ms'],
                 v7_preallocation_wait_change=new['result']['wal_pipeline']['preallocation_ns'] / old['result']['wal_pipeline']['preallocation_ns'],
                 v7_sync_count_change=new['result']['wal_pipeline']['sync_count'] / old['result']['wal_pipeline']['sync_count'],
                 v7_data_bytes_change=new['result']['wal_pipeline']['commit_bytes'] / old['result']['wal_pipeline']['commit_bytes'])
        if 'base_v7' in arms:
            ref = p['base_v7']
            x.update(v7_pre_async_throughput_change=rate(new) / rate(ref),
                     v7_pre_async_p99_change=new['latency']['p99_ms'] / ref['latency']['p99_ms'],
                     v7_pre_async_data_bytes_change=new['result']['wal_pipeline']['commit_bytes'] / ref['result']['wal_pipeline']['commit_bytes'])
        pairs.append(x)
    medians = {key: statistics.median(p[key] for p in pairs) for key in pairs[0] if key != 'pair'}
    per_arm = {}
    for arm in arms:
        records = [schedule['scored', pair, arm] for pair in range(1, cfg['pairs'] + 1)]
        peak = max(records, key=rate)
        per_arm[arm] = dict(median_puts_per_sec=statistics.median(rate(r) for r in records),
                            peak_puts_per_sec=rate(peak), peak_run_id=peak['run_id'],
                            peak_raw_sha256=raw[peak['run_id']][1],
                            median_cpu_ms=statistics.median(cpu(r) for r in records),
                            median_p50_ms=statistics.median(r['latency']['p50_ms'] for r in records),
                            median_p99_ms=statistics.median(r['latency']['p99_ms'] for r in records),
                            pipeline_medians={metric: statistics.median(r['result']['wal_pipeline'][metric] for r in records) for metric in PIPELINE})
    guards = dict(paired_raw_floor=medians['v7_throughput_change'] >= 0.95,
                  paired_normalized_floor=medians['normalized_v7_v4_change'] >= 0.95,
                  arm_median_floor=per_arm['new_v7']['median_puts_per_sec'] / per_arm['old_v7']['median_puts_per_sec'] >= 0.95,
                  p99_ceiling=medians['v7_p99_change'] <= 1.10,
                  sync_count_ceiling=medians['v7_sync_count_change'] <= 1.05,
                  identical_data_bytes=all(p['v7_data_bytes_change'] == 1 for p in pairs))
    if 'base_v7' in arms:
        guards.update(serial_p99_improves=medians['v7_p99_change'] <= 0.95,
                      pre_async_p99_ceiling=medians['v7_pre_async_p99_change'] <= 1.10,
                      pre_async_raw_floor=medians['v7_pre_async_throughput_change'] >= 0.95,
                      identical_pre_async_data_bytes=all(p['v7_pre_async_data_bytes_change'] == 1 for p in pairs))
    return dict(run_ids=list(raw), host_samples=host_samples, pairs=pairs, median_changes=medians,
                per_arm=per_arm, guards=guards, cell_pass=all(guards.values()),
                artifact_sha256={suffix: sha(ART / (stem + suffix)) for suffix in ('.jsonl', '-runs.tsv', '-host.jsonl', '-run.sh')})


if __name__ == '__main__':
    cells = sys.argv[1:] or list(PLAN['matrix'])
    output = {cell: compare(cell) for cell in cells}
    ids = [run for part in output.values() for run in part['run_ids']]
    assert len(ids) == len(set(ids))
    output['experiment_status'] = dict(complete=set(cells) == set(PLAN['matrix']),
                                       captured_cells_pass=all(output[cell]['cell_pass'] for cell in cells),
                                       candidate_eligible_for_retention=set(cells) == set(PLAN['matrix']) and all(output[cell]['cell_pass'] for cell in cells),
                                       original_async_retained_by_maintainer=True)
    output['analysis_sha256'] = sha(Path(__file__))
    output['driver_sha256'] = sha(ART / (STEM + '-run.py'))
    output['identity_sha256'] = sha(ART / (STEM + '-binaries.json'))
    output['plan_sha256'] = sha(ART / (STEM + '-plan.json'))
    print(json.dumps(output, indent=2))
