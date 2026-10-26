#!/usr/bin/env python3
"""Verify native probe identities, audit samples, schedule and fixed guards."""
import csv
import hashlib
import json
import statistics
from pathlib import Path

ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-frontier-link-20261026'
POLICIES = ['syscalls', 'uring_default', 'uring_coop_single', 'uring_defer_single']
CELLS = ['point8', 'batch64x8']
FLAGS = dict(zip(POLICIES, [0, 0, 4352, 12288]))


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    build = json.loads((ART / (STEM + '-build.json')).read_text())
    assert sha(ART / (STEM + '-probe.c')) == build['source_sha256']
    assert sha(ART / (STEM + '-plan.json')) == build['plan_sha256']
    rows = list(csv.DictReader((ART / (STEM + '-runs.tsv')).open(), delimiter='\t'))
    records, audits = {}, {}
    for suffix, destination in [('.jsonl', records), ('-host.jsonl', audits)]:
        for line in (ART / (STEM + suffix)).read_bytes().splitlines():
            record = json.loads(line)
            assert record['run_id'] not in destination
            destination[record['run_id']] = (record, hashlib.sha256(line).hexdigest())
    assert len(rows) == len(records) == len(audits) == 48
    assert {r['run_id'] for r in rows} == set(records) == set(audits)
    expected = []
    for cell in CELLS:
        expected.extend((cell, 'warmup', 0, policy) for policy in POLICIES)
        for round_index in range(1, 6):
            shift = (round_index - 1) % len(POLICIES)
            order = POLICIES[shift:] + POLICIES[:shift]
            if round_index % 2 == 0:
                order = list(reversed(order))
            expected.extend((cell, 'scored', round_index, policy) for policy in order)
    assert [(r['cell'], r['phase'], int(r['round']), r['policy']) for r in rows] == expected
    samples, schedule = 0, {}
    for row in rows:
        r, raw_sha = records[row['run_id']]
        audit, audit_sha = audits[row['run_id']]
        assert raw_sha == row['raw_sha256'] and audit_sha == row['audit_sha256']
        assert row['binary_sha256'] == build['binary_sha256'] == audit['binary_sha256']
        assert (r['cell'], r['phase'], r['round'], r['policy']) == (
            row['cell'], row['phase'], int(row['round']), row['policy'])
        assert all(audit[k] == r[k] for k in ('cell', 'phase', 'round', 'policy'))
        assert audit['clean_sampled'] and audit['returncode'] == 0
        assert not audit['sampling_errors'] and audit['during']
        for sample in [audit['before'], *audit['during'], audit['after']]:
            assert not sample['competing']
            samples += 1
        b = r['benchmark']
        point = r['cell'] == 'point8'
        assert b['policy'] == r['policy'] and b['cell'] == r['cell']
        assert b['flags'] == FLAGS[r['policy']] and b['cycles'] == 500
        assert b['prezero'] == point and b['readback_verified']
        assert b['data_bytes_per_cycle'] == (32768 if point else 557056)
        assert b['image_bytes'] == 500 * (b['data_bytes_per_cycle'] + 4096)
        assert b['data_ns'] + b['frontier_sync_ns'] <= b['elapsed_ns']
        key = r['cell'], r['phase'], r['round'], r['policy']
        assert key not in schedule
        schedule[key] = b
    output = dict(host_samples=samples, scored_records=40, warmups=8, cells={})
    for cell in CELLS:
        pairs = []
        for round_index in range(1, 6):
            old = schedule[cell, 'scored', round_index, 'syscalls']
            pairs.append(dict(round=round_index, policies={
                policy: dict(
                    frontier_sync_change=schedule[cell, 'scored', round_index, policy]['frontier_sync_ns'] / old['frontier_sync_ns'],
                    throughput_change=old['elapsed_ns'] / schedule[cell, 'scored', round_index, policy]['elapsed_ns'],
                    cpu_change=schedule[cell, 'scored', round_index, policy]['cpu_us'] / old['cpu_us'],
                    p99_change=schedule[cell, 'scored', round_index, policy]['cycle_p99_ns'] / old['cycle_p99_ns'])
                for policy in POLICIES[1:]}))
        medians = {policy: {
            metric: statistics.median(pair['policies'][policy][metric] for pair in pairs)
            for metric in ('frontier_sync_change', 'throughput_change', 'cpu_change', 'p99_change')
        } for policy in POLICIES[1:]}
        output['cells'][cell] = dict(
            paired_medians=medians, pairs=pairs,
            per_policy={policy: {
                'median_frontier_sync_us': statistics.median(schedule[cell, 'scored', i, policy]['frontier_sync_ns'] / 500 / 1000 for i in range(1, 6)),
                'median_cycles_per_sec': statistics.median(500e9 / schedule[cell, 'scored', i, policy]['elapsed_ns'] for i in range(1, 6)),
                'median_p99_us': statistics.median(schedule[cell, 'scored', i, policy]['cycle_p99_ns'] / 1000 for i in range(1, 6)),
            } for policy in POLICIES})
    output['guards'] = {policy: all(
        output['cells'][cell]['paired_medians'][policy]['frontier_sync_change'] <= 0.97
        and all(pair['policies'][policy]['throughput_change'] >= 0.95
                and pair['policies'][policy]['p99_change'] <= 1.10
                for pair in output['cells'][cell]['pairs'])
        for cell in CELLS) for policy in POLICIES[1:]}
    output['flag_transitions'] = {
        cell: {left + '_to_' + right: {
            'paired_frontier_sync_change': statistics.median(
                schedule[cell, 'scored', i, right]['frontier_sync_ns'] /
                schedule[cell, 'scored', i, left]['frontier_sync_ns'] for i in range(1, 6)),
            'paired_throughput_change': statistics.median(
                schedule[cell, 'scored', i, left]['elapsed_ns'] /
                schedule[cell, 'scored', i, right]['elapsed_ns'] for i in range(1, 6)),
        } for left, right in [('uring_default', 'uring_coop_single'),
                              ('uring_coop_single', 'uring_defer_single')]}
        for cell in CELLS}
    output['advance_to_engine_candidate'] = any(output['guards'].values())
    output['scope'] = 'Native diagnostic with opaque bytes; not engine throughput or crash qualification.'
    output['artifacts_sha256'] = {suffix: sha(ART / (STEM + suffix)) for suffix in (
        '.jsonl', '-host.jsonl', '-runs.tsv', '-probe.c', '-run.py', '-analyze.py',
        '-plan.json', '-build.json')}
    print(json.dumps(output, indent=2))


if __name__ == '__main__':
    main()
