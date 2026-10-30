#!/usr/bin/env python3
"""Summarize excluded I/O/thread traces without replacing any scored runs."""
import collections
import csv
import hashlib
import json
import math
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-writer-scaling-20261027'
WORK = ROOT / 'target/wal-opt/20261027-v7-writer-scaling'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def percentile(values, q):
    values = sorted(values)
    return values[math.floor((len(values)-1)*q+.5)] if values else None


def durations(rows):
    values = [(r['end_ns']-r['start_ns'])/1e6 for r in rows]
    return dict(count=len(rows), mean_ms=statistics.mean(values) if values else None,
                p99_ms=percentile(values, .99), max_ms=max(values) if values else None,
                total_ms=sum(values))


def main():
    captures = json.loads((ART/(STEM+'-captures.json')).read_text())
    assert len(captures) == 8
    raw = {r['run_id']:(r,hashlib.sha256(line).hexdigest())
           for line in (ART/(STEM+'-runs.jsonl')).read_bytes().splitlines() for r in [json.loads(line)]}
    assert len(raw) == 8
    output = {}
    for meta in captures:
        assert meta['excluded_from_scoring'] and meta['clean_sampled'] and not meta['sampling_errors']
        assert sha(ROOT/meta['binary']['path']) == meta['binary']['sha256']
        for path, digest in meta['files'].items():
            assert sha(ROOT/path) == digest, path
        record, digest = raw[meta['run_id']]
        assert digest == meta['raw_sha256']
        name = meta['name']
        io = [{k:(v if k=='name' else int(v)) for k,v in r.items()}
              for r in csv.DictReader((WORK/(name+'-io.csv')).open())]
        assert all(r['error']==0 and r['result']>=0 for r in io)
        markers = sorted([r for r in io if r['op']==1 and r['kind']==2 and r['generation']>0],
                         key=lambda r:r['start_ns'])
        assert len(markers) == record['result']['wal_pipeline']['frontier_writes']
        assert all(a['ticket_end']<b['ticket_end'] for a,b in zip(markers,markers[1:]))
        inode = markers[0]['inode']
        syncs = [r for r in io if r['op']==2 and r['inode']==inode]
        begin, end = markers[0]['start_ns'], max(r['end_ns'] for r in syncs)
        # Synchronous non-frame writes on this runtime's WAL are initialization
        # writes. Restrict to the active commit window; do not count setup/close.
        active = [r for r in io if r['inode']==inode and begin<=r['start_ns']<=end]
        zeros = [r for r in active if r['op']==1 and r['kind']==0]
        fallocate = [r for r in active if r['op']==3]
        gaps = [r['offset']-r['durable_end'] for r in markers]
        samples = [json.loads(l) for l in (WORK/(name+'-samples.jsonl')).read_text().splitlines()]
        assert len(samples) == meta['samples']
        task_states = collections.defaultdict(collections.Counter)
        syscalls = collections.defaultdict(collections.Counter)
        for sample in samples:
            for task in sample['tasks'].values():
                if task['name'].startswith('wal-'):
                    task_states[task['name']][task['state']+'/'+task['wchan']] += 1
                    syscalls[task['name']][str(task['syscall_number'])] += 1
        disks = [s for s in samples if s.get('disk_stat')]
        delta = [b-a for a,b in zip(disks[0]['disk_stat'],disks[-1]['disk_stat'])]
        output[name] = dict(arm=meta['arm'], writers=meta['writers'], run_id=meta['run_id'],
                            raw_puts_s=record['result']['ops_per_sec'], raw_p99_ms=record['latency']['p99_ms'],
                            markers=durations(markers), fdatasync=durations(syncs), fallocate=durations(fallocate),
                            initialization=dict(**durations(zeros), bytes=sum(r['length'] for r in zeros)),
                            coordinator_fallocate_calls=sum(r['name']=='wal-sync-coordi' for r in fallocate),
                            speculative_tail=dict(mean_bytes=statistics.mean(gaps), max_bytes=max(gaps),
                                                  fraction_nonzero=sum(g>0 for g in gaps)/len(gaps)),
                            wal_thread_states={k:dict(v) for k,v in task_states.items()},
                            wal_thread_syscalls={k:dict(v) for k,v in syscalls.items()},
                            device_sample_window=dict(elapsed_ms=(disks[-1]['monotonic_ns']-disks[0]['monotonic_ns'])/1e6,
                                                      writes=delta[4], write_bytes=delta[6]*512,
                                                      mean_write_ms=delta[7]/delta[4] if delta[4] else None,
                                                      busy_ms=delta[9], flushes=delta[15], flush_ms=delta[16]))
    print(json.dumps(dict(captures=output, excluded_invocations=8,
                          limits='Diagnostic preload and 100ms sampling can perturb timing. Device statistics are host-wide; thread samples miss short waits. Initializer totals count synchronous non-frame WAL writes only within the active FRONTIER window. DATA io_uring requests are not visible in this trace. Scheduler run-delay accounting was disabled; kernel waits establish I/O delay, not an SSD firmware or thermal cause.',
                          analyzer_sha256=sha(Path(__file__))), indent=2))


if __name__ == '__main__':
    main()
