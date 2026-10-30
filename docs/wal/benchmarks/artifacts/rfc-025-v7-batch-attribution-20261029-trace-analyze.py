#!/usr/bin/env python3
"""Partition batch API paths and report measured wall/thread-CPU components."""
import bisect
import csv
import hashlib
import json
import os
import statistics
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-batch-attribution-20261029'
WORK = ROOT / 'target/wal-opt/20261029-v7-batch-attribution'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def stats(values):
    values = sorted(values)
    if not values:
        return dict(count=0, sum_ms=0)
    n = len(values)
    return dict(count=n, sum_ms=sum(values) / 1e6, mean_us=statistics.mean(values) / 1000,
                p50_us=values[(n - 1) // 2] / 1000, p99_us=values[(n * 99 - 1) // 100] / 1000,
                max_us=values[-1] / 1000)


def analyze(capture):
    for path, digest in capture['files'].items():
        assert sha(ROOT / path) == digest
    path = lambda suffix: ROOT / next(p for p in capture['files'] if p.endswith(suffix))
    record = json.loads(path('.stdout').read_text())
    events = defaultdict(list)
    with path('-events.csv').open() as stream:
        for row in csv.DictReader(stream):
            event, subject, a, b, tid, time, cpu = [int(row[key]) for key in
                                                  ['event', 'subject', 'arg1', 'arg2', 'tid', 'time_ns', 'cpu_ns']]
            events[event].append((subject, a, b, tid, time, cpu))
    n = sum(((capture['operations'] // capture['writers'] + int(t < capture['operations'] % capture['writers'])) + 63) // 64
            for t in range(capture['writers']))
    assert len(events[1]) == len(events[2]) == n
    begin = {e[0]: e for e in events[1]}
    finish = {e[0]: e for e in events[2]}
    first = min(e[4] for e in begin.values())
    last = max(e[4] for e in finish.values())
    inside = lambda e: first <= e[4] <= last
    waits = defaultdict(list)
    for e in events[3]:
        if inside(e):
            waits[e[3]].append(e)
    for tid in waits:
        waits[tid].sort(key=lambda e: e[4])
    wait_times = {tid: [e[4] for e in rows] for tid, rows in waits.items()}
    exits = {e[0]: e for e in events[4] if inside(e)}
    staged, submitted, reaped = [{e[0]: e for e in events[kind] if inside(e)} for kind in [5, 6, 7]]
    assert len(staged) == len(submitted) == len(reaped) == len(exits) == n
    pack_start, pack_end = {}, {}
    for kind, dest in [(13, pack_start), (14, pack_end)]:
        for e in events[kind]:
            if inside(e):
                for ticket in range(e[0], e[1]):
                    assert ticket not in dest
                    dest[ticket] = e
    assert len(pack_start) == len(pack_end) == n
    sync_start, sync_end, published = [{e[0]: e for e in events[kind] if inside(e)} for kind in [31, 32, 10]]
    assert len(sync_start) == len(sync_end) == len(published) == record['result']['wal_pipeline']['sync_count']
    targets = sorted(sync_start)
    assert targets[-1] == n
    f_start, f_end = [{e[0]: e for e in events[kind] if inside(e)} for kind in [33, 34]]
    assert len(f_start) == len(f_end) == (len(targets) if capture['format'] == 7 else 0)

    labels = ['api_to_packer', 'packer_materialize_prepare', 'packer_to_worker', 'stage_to_submit_return',
              'submit_return_to_reap', 'reap_to_frontier_or_sync', 'frontier_write', 'frontier_to_sync',
              'fdatasync', 'sync_to_publish', 'publish_to_wait_return', 'wait_return_to_api_return']
    paths = []
    for key, start in begin.items():
        end = finish[key]
        wait = waits[start[3]][bisect.bisect_left(wait_times[start[3]], start[4])]
        ticket = wait[0]
        returned = exits[ticket]
        assert start[4] <= wait[4] <= returned[4] <= end[4]
        assert start[3] == wait[3] == returned[3] == end[3]
        target = targets[bisect.bisect_right(targets, ticket)]
        sync_a, sync_b, pub = sync_start[target], sync_end[target], published[target]
        fa, fb = (f_start[target], f_end[target]) if capture['format'] == 7 else (sync_a, sync_a)
        points = [start[4], pack_start[ticket][4], pack_end[ticket][4], staged[ticket][4], submitted[ticket][4],
                  reaped[ticket][4], fa[4], fb[4], sync_a[4], sync_b[4], pub[4], returned[4], end[4]]
        assert points == sorted(points), (capture['name'], ticket, points)
        parts = {label: b - a for label, a, b in zip(labels, points, points[1:])}
        latency = end[4] - start[4]
        assert sum(parts.values()) == latency
        cpus = dict(caller_pre_wait=wait[5] - start[5], caller_wait=returned[5] - wait[5],
                    caller_after_wait=end[5] - returned[5])
        assert all(value >= 0 for value in cpus.values())
        paths.append(dict(ticket=ticket, key=key, start_ns=start[4], end_ns=end[4], latency_ns=latency,
                          parts=parts, cpu_ns=cpus, sync_target=target))
    assert {p['ticket'] for p in paths} == set(range(n))
    paths.sort(key=lambda p: p['ticket'])
    out = WORK / (capture['name'] + '-paths.jsonl')
    out.write_text(''.join(json.dumps(p, separators=(',', ':')) + '\n' for p in paths))

    components = {}
    for label, ka, kb in [('packer', 13, 14), ('data_extent_prepare', 15, 16),
                          ('data_framing_crc', 23, 24), ('logical_prefix_sha256', 25, 26),
                          ('canonical_prepare', 27, 28), ('frontier_write', 33, 34),
                          ('owned_pitr_entries', 35, 36), ('publish_batch_build', 41, 42),
                          ('frontier_extent_prepare', 43, 44), ('fdatasync', 31, 32)]:
        starts, ends = defaultdict(list), defaultdict(list)
        for e in events[ka]:
            if inside(e):
                starts[e[3]].append(e)
        for e in events[kb]:
            if inside(e):
                ends[e[3]].append(e)
        assert set(starts) == set(ends)
        wall, cpu = [], []
        for tid in starts:
            aa, bb = sorted(starts[tid], key=lambda e: e[4]), sorted(ends[tid], key=lambda e: e[4])
            assert len(aa) == len(bb)
            for a, b in zip(aa, bb):
                assert b[4] >= a[4] and b[5] >= a[5]
                wall.append(b[4] - a[4])
                cpu.append(b[5] - a[5])
        components[label] = dict(wall=stats(wall), cpu=stats(cpu))

    native = list(csv.DictReader(path('-io.csv').open()))
    native = [row for row in native if first <= int(row['start_ns']) <= int(row['end_ns']) <= last]
    counts = Counter(row['inode'] for row in native if row['op'] == '2')
    inode = counts.most_common(1)[0][0]
    native = [row for row in native if row['inode'] == inode]
    assert all(int(row['error']) == 0 for row in native)
    syncs = sorted([row for row in native if row['op'] == '2'], key=lambda row: int(row['start_ns']))
    markers = sorted([row for row in native if row['kind'] == '2'], key=lambda row: int(row['start_ns']))
    assert len(syncs) == len(targets) and len(markers) == len(f_start)
    for target, row in zip(targets, syncs):
        assert sync_start[target][4] <= int(row['start_ns']) <= int(row['end_ns']) <= sync_end[target][4]
    for target, row in zip(sorted(f_start), markers):
        assert f_start[target][4] <= int(row['start_ns']) <= int(row['end_ns']) <= f_end[target][4]
    preparation = [row for row in native if row['op'] == '1' and row['kind'] != '2']
    native_summary = dict(wal_inode=inode, syncs=len(syncs), frontiers=len(markers),
                          preparation_writes=len(preparation), preparation_bytes=sum(int(row['length']) for row in preparation),
                          preparation_write_wall_ms=sum(int(row['end_ns']) - int(row['start_ns']) for row in preparation) / 1e6,
                          allocation_calls=sum(row['op'] == '3' for row in native))
    role_cpu = {}
    for role, kinds in [('packer', [13, 14]), ('worker', [5, 6, 7]), ('coordinator', [8, 9, 10, 31, 32, 33, 34])]:
        per_tid = defaultdict(list)
        for kind in kinds:
            for e in events[kind]:
                if inside(e):
                    per_tid[e[3]].append(e[5])
        role_cpu[role] = sum(max(values) - min(values) for values in per_tid.values()) / 1e6
    tail = sorted(paths, key=lambda p: p['latency_ns'])[-max(1, n // 100):]
    return dict(name=capture['name'], format=capture['format'], writers=capture['writers'], initialize_large=capture['initialize_large'],
                run_id=capture['run_id'], mapped_calls=n, excluded_from_scoring=True,
                api_latency=stats([p['latency_ns'] for p in paths]),
                stages={label: stats([p['parts'][label] for p in paths]) for label in labels}, components=components,
                caller_cpu={label: stats([p['cpu_ns'][label] for p in paths]) for label in paths[0]['cpu_ns']},
                role_cpu_span_ms=role_cpu, top_1pct_dominant=dict(Counter(max(p['parts'], key=p['parts'].get) for p in tail)),
                native=native_summary, raw_puts_s=record['result']['ops_per_sec'], raw_p99_ms=record['latency']['p99_ms'],
                process_cpu_ms=record['result']['process_cpu_user_ms'] + record['result']['process_cpu_system_ms'],
                pipeline=record['result']['wal_pipeline'], paths_file=str(out.relative_to(ROOT)), paths_sha256=sha(out))


def main():
    captures = json.loads((ART / (STEM + '-captures.json')).read_text())
    print(json.dumps(dict(captures=[analyze(c) for c in captures], analyzer_sha256=sha(Path(__file__)),
                          limits='Diagnostic observations only. Trace locking, timestamps and sampling perturb scheduling. API stages partition each call; concurrent calls and nested components overlap. CPU is measured per thread, not attributed to device service. Worker/coordinator CPU spans omit activity outside the first/last captured event. Native preparation sums overlap other stages and are not additive wall latency.'), indent=2))


if __name__ == '__main__':
    main()
