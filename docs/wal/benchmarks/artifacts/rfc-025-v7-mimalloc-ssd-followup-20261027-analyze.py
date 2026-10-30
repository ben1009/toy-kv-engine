#!/usr/bin/env python3
"""Verify the post-SSD allocator comparison, child RSS, controls and assessments."""
import csv
import hashlib
import json
import statistics
import sys
from pathlib import Path

ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-mimalloc-ssd-followup-20261027'
PLAN = json.loads((ART / (STEM + '-plan.json')).read_text())
IDENTITY = json.loads((ART / (STEM + '-binaries.json')).read_text())
PIPELINE = ('commit_groups', 'commit_buffers', 'commit_bytes', 'sync_count', 'fdatasync_ns',
            'frontier_writes', 'frontier_bytes', 'frontier_write_ns', 'preallocation_ns',
            'wal_sync_wait_ns', 'memtable_insert_ns')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def cpu(record):
    return record['result']['process_cpu_user_ms'] + record['result']['process_cpu_system_ms']


def rate(record):
    return record['result']['ops_per_sec']


def compare(cell):
    cfg = PLAN['matrix'][cell]
    arms = tuple(cfg['arms'])
    warmups = ('base_v7', 'old_v4', 'old_v7', 'new_v4', 'new_v7')
    stem = 'rfc-025-v7-mimalloc-ssd-followup-initial-' + cell + '-20261027'
    lines = (ART / (stem + '.jsonl')).read_bytes().splitlines()
    raw = {r['run_id']: (r, hashlib.sha256(line).hexdigest()) for line in lines for r in [json.loads(line)]}
    rows = list(csv.DictReader((ART / (stem + '-runs.tsv')).open(), delimiter='\t'))
    audit_lines = (ART / (stem + '-host.jsonl')).read_bytes().splitlines()
    audit = {r['run_id']: (r, hashlib.sha256(line).hexdigest()) for line in audit_lines for r in [json.loads(line)]}
    count = len(arms) * (cfg['pairs'] + 1)
    assert len(raw) == len(lines) == len(rows) == len(audit) == len(audit_lines) == count
    assert set(raw) == set(audit) == {row['run_id'] for row in rows}
    expected = [('warmup', 0, arm) for arm in warmups]
    for pair in range(1, cfg['pairs'] + 1):
        expected += [('scored', pair, arm) for arm in (arms if pair % 2 else tuple(reversed(arms)))]
    assert [(row['phase'], int(row['pair']), row['arm']) for row in rows] == expected
    schedule, host_samples = {}, 0
    for row in rows:
        arm, pair, phase = row['arm'], int(row['pair']), row['phase']
        record, digest = raw[row['run_id']]
        item, audit_digest = audit[row['run_id']]
        identity = IDENTITY['baseline' if arm == 'base_v7' else ('retained' if arm.startswith('old') else 'candidate')]
        assert row['raw_sha256'] == digest and row['audit_sha256'] == audit_digest
        assert row['binary_sha256'] == identity['sha256'] == item['binary_sha256']
        assert record['params']['wal_format_version'] == (4 if arm.endswith('v4') else 7)
        assert record['params']['wal_io_mode'] == 'parallel'
        assert record['params']['threads'] == cfg['writers'] and record['params']['batch_size'] == cfg['batch']
        assert record['params']['value_size'] == 1024 and record['engine_options']['target_sst_size'] == 2147483648
        assert record['workload'] == ('wal_batch_concurrent' if cfg['batch'] else 'wal_concurrent')
        assert record['result']['ops'] == cfg['operations']
        assert record['latency']['sample_every'] == cfg['sample_every'] and record['latency']['samples'] == cfg['samples']
        assert item['arm'] == arm and item['pair'] == pair and item['phase'] == phase
        assert item['clean_sampled'] and row['isolation_status'] == 'clean_sampled'
        assert item['returncode'] == 0 and not item['sampling_errors'] and item['during']
        usage = item['child_rusage']
        assert usage['pid'] == item['benchmark_pid'] != item['launcher_pid']
        assert usage['max_rss_kib'] > 0
        encoded_usage = (json.dumps(usage, sort_keys=True) + '\n').encode()
        assert hashlib.sha256(encoded_usage).hexdigest() == item['child_rusage_sha256']
        for sample in [item['before'], *item['during'], item['after']]:
            assert not sample['competing']
            host_samples += 1
        key = (phase, pair, arm)
        assert key not in schedule
        schedule[key] = record

    def rss(record):
        return audit[record['run_id']][0]['child_rusage']['max_rss_kib']

    pairs = []
    for pair in range(1, cfg['pairs'] + 1):
        p = {arm: schedule['scored', pair, arm] for arm in arms}
        x = dict(pair=pair)
        for fmt in ('v4', 'v7'):
            old, new = p['old_' + fmt], p['new_' + fmt]
            x.update({fmt + '_throughput_change': rate(new) / rate(old),
                      fmt + '_cpu_change': cpu(new) / cpu(old),
                      fmt + '_p50_change': new['latency']['p50_ms'] / old['latency']['p50_ms'],
                      fmt + '_p99_change': new['latency']['p99_ms'] / old['latency']['p99_ms'],
                      fmt + '_rss_change': rss(new) / rss(old),
                      fmt + '_sync_count_change': new['result']['wal_pipeline']['sync_count'] / old['result']['wal_pipeline']['sync_count'],
                      fmt + '_data_bytes_change': new['result']['wal_pipeline']['commit_bytes'] / old['result']['wal_pipeline']['commit_bytes']})
        old, base, new = p['old_v7'], p['base_v7'], p['new_v7']
        x.update(system_vs_baseline_throughput_change=rate(old) / rate(base),
                 system_vs_baseline_cpu_change=cpu(old) / cpu(base),
                 system_vs_baseline_p99_change=old['latency']['p99_ms'] / base['latency']['p99_ms'],
                 system_vs_baseline_data_bytes_change=old['result']['wal_pipeline']['commit_bytes'] / base['result']['wal_pipeline']['commit_bytes'],
                 mimalloc_vs_baseline_throughput_change=rate(new) / rate(base),
                 mimalloc_vs_baseline_p99_change=new['latency']['p99_ms'] / base['latency']['p99_ms'],
                 relative_v7_v4_change=(rate(new) / rate(p['new_v4'])) / (rate(old) / rate(p['old_v4'])))
        pairs.append(x)
    medians = {key: statistics.median(p[key] for p in pairs) for key in pairs[0] if key != 'pair'}
    per_arm = {}
    for arm in arms:
        records = [schedule['scored', pair, arm] for pair in range(1, cfg['pairs'] + 1)]
        peak = max(records, key=rate)
        per_arm[arm] = dict(median_puts_per_sec=statistics.median(rate(r) for r in records),
                            peak_puts_per_sec=rate(peak), peak_run_id=peak['run_id'], peak_raw_sha256=raw[peak['run_id']][1],
                            median_cpu_ms=statistics.median(cpu(r) for r in records),
                            median_p50_ms=statistics.median(r['latency']['p50_ms'] for r in records),
                            median_p99_ms=statistics.median(r['latency']['p99_ms'] for r in records),
                            median_peak_rss_kib=statistics.median(rss(r) for r in records),
                            max_peak_rss_kib=max(rss(r) for r in records),
                            pipeline_medians={metric: statistics.median(r['result']['wal_pipeline'][metric] for r in records) for metric in PIPELINE})
    guards = {}
    for fmt in ('v4', 'v7'):
        guards.update({fmt + '_paired_raw_floor': medians[fmt + '_throughput_change'] >= 0.95,
                       fmt + '_arm_median_floor': per_arm['new_' + fmt]['median_puts_per_sec'] / per_arm['old_' + fmt]['median_puts_per_sec'] >= 0.95,
                       fmt + '_cpu_ceiling': medians[fmt + '_cpu_change'] <= 1.05,
                       fmt + '_p99_ceiling': medians[fmt + '_p99_change'] <= 1.10,
                       fmt + '_sync_count_ceiling': medians[fmt + '_sync_count_change'] <= 1.05,
                       fmt + '_identical_data_bytes': all(p[fmt + '_data_bytes_change'] == 1 for p in pairs)})
    control = dict(raw_floor=medians['system_vs_baseline_throughput_change'] >= 0.95,
                   cpu_ceiling=medians['system_vs_baseline_cpu_change'] <= 1.05,
                   p99_ceiling=medians['system_vs_baseline_p99_change'] <= 1.10,
                   identical_data_bytes=all(p['system_vs_baseline_data_bytes_change'] == 1 for p in pairs))
    memory_flags = {fmt: medians[fmt + '_rss_change'] > 1.20 for fmt in ('v4', 'v7')}
    return dict(run_ids=list(raw), host_samples=host_samples, pairs=pairs, median_changes=medians,
                per_arm=per_arm, guards=guards, allocator_cell_pass=all(guards.values()),
                system_control_guards=control, system_control_pass=all(control.values()), memory_review_flags=memory_flags,
                paired_v7_v4={prefix: statistics.median(rate(schedule['scored', pair, prefix + '_v7']) / rate(schedule['scored', pair, prefix + '_v4']) for pair in range(1, cfg['pairs'] + 1)) for prefix in ('old', 'new')},
                artifact_sha256={suffix: sha(ART / (stem + suffix)) for suffix in ('.jsonl', '-runs.tsv', '-host.jsonl', '-run.sh')})


if __name__ == '__main__':
    cells = sys.argv[1:] or list(PLAN['matrix'])
    output = {cell: compare(cell) for cell in cells}
    ids = [run for part in output.values() for run in part['run_ids']]
    assert len(ids) == len(set(ids))
    material_gain = any(output[cell]['median_changes'][fmt + '_throughput_change'] >= 1.02 and output[cell]['median_changes'][fmt + '_cpu_change'] <= 1 for cell in cells for fmt in ('v4', 'v7'))
    output['experiment_status'] = dict(complete=set(cells) == set(PLAN['matrix']),
                                       captured_allocator_cells_pass=all(output[cell]['allocator_cell_pass'] for cell in cells),
                                       captured_system_controls_pass=all(output[cell]['system_control_pass'] for cell in cells),
                                       material_gain=material_gain,
                                       assessment_passed=set(cells) == set(PLAN['matrix']) and all(output[cell]['allocator_cell_pass'] for cell in cells) and material_gain,
                                       memory_review_required=any(any(output[cell]['memory_review_flags'].values()) for cell in cells),
                                       allocator_default_changed=False, ssd_optimizations_preserved=True)
    output['analysis_sha256'] = sha(Path(__file__))
    output['driver_sha256'] = sha(ART / (STEM + '-run.py'))
    output['child_launcher_sha256'] = sha(ART / (STEM + '-child.py'))
    output['identity_sha256'] = sha(ART / (STEM + '-binaries.json'))
    output['plan_sha256'] = sha(ART / (STEM + '-plan.json'))
    print(json.dumps(output, indent=2))
