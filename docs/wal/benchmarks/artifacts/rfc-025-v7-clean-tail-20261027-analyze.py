#!/usr/bin/env python3
"""Decompose every sampled batch and retain explicit I/O-stall sensitivity views."""
import bisect
import csv
import hashlib
import json
import math
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-clean-tail-20261027'
WORK = ROOT / 'target/wal-opt/20261027-v7-clean-tail'
STAGES = ['before_frontier_ms','frontier_write_ms','before_sync_ms','fdatasync_ms','after_sync_ms']


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def percentile(values, q):
    values = sorted(values)
    return values[math.floor((len(values)-1)*q+0.5)] if values else None


def stats(rows):
    if not rows:
        return dict(samples=0)
    tail = sorted(rows, key=lambda r:r['latency_ms'], reverse=True)[:max(1, math.ceil(len(rows)*0.01))]
    return dict(samples=len(rows), p50_ms=percentile([r['latency_ms'] for r in rows],.5),
                p90_ms=percentile([r['latency_ms'] for r in rows],.9),
                p99_ms=percentile([r['latency_ms'] for r in rows],.99),
                max_ms=max(r['latency_ms'] for r in rows),
                tail_median_stages_ms={s:statistics.median(r[s] for r in tail) for s in STAGES},
                tail_median_earlier_certificates=statistics.median(r['earlier_certificates'] for r in tail),
                top_samples=tail[:8])


def main():
    captures = json.loads((ART/(STEM+'-captures.json')).read_text())
    assert len(captures) == 4
    raw_lines = (ART/(STEM+'-runs.jsonl')).read_bytes().splitlines()
    raw = {r['run_id']:(r,hashlib.sha256(line).hexdigest()) for line in raw_lines for r in [json.loads(line)]}
    result, pooled = {}, {'baseline':[],'retirement_hint':[]}
    for meta in captures:
        assert meta['excluded_from_scoring'] and meta['clean_sampled'] and not meta['sampling_errors']
        for path,digest in meta['files'].items():
            assert sha(ROOT/path) == digest, path
        record,digest = raw[meta['run_id']]
        assert digest == meta['raw_sha256']
        name=meta['name']
        io=[{k:(v if k=='name' else int(v)) for k,v in r.items()}
            for r in csv.DictReader((WORK/(name+'-io.csv')).open())]
        assert all(r['error']==0 for r in io)
        markers=sorted([r for r in io if r['op']==1 and r['kind']==2 and r['generation']>0],key=lambda r:r['start_ns'])
        assert len(markers)==record['result']['wal_pipeline']['frontier_writes']
        assert all(a['ticket_end']<b['ticket_end'] for a,b in zip(markers,markers[1:]))
        assert markers[-1]['ticket_end']==11728
        syncs=sorted([r for r in io if r['op']==2],key=lambda r:r['start_ns'])
        sync_starts=[r['start_ns']for r in syncs]
        cycles=[]
        for f in markers:
            index=bisect.bisect_left(sync_starts,f['end_ns'])
            s=syncs[index]
            assert s['inode']==f['inode'] and s['tid']==f['tid']
            cycles.append((f,s))
        assert len({s['start_ns']for _,s in cycles})==len(markers)
        key_map={r['first_key']:r for r in json.loads((WORK/(name+'-tickets.json')).read_text())['batches']}
        traces=[json.loads(l)for l in (WORK/(name+'-batch.jsonl')).read_text().splitlines()]
        assert len(traces)==record['latency']['samples']==1184 and len(key_map)==11728
        ends=[f['ticket_end']for f in markers]
        starts=[f['start_ns']for f in markers]
        long_calls={t:[r for r in io if r['end_ns']-r['start_ns']>=t*1_000_000]for t in [1,2,5]}
        rows=[]
        for trace in traces:
            ticket=key_map[trace['first_key']]['ticket']
            f,s=cycles[bisect.bisect_right(ends,ticket)]
            begin,end=trace['start_ns'],trace['end_ns']
            assert begin<=f['start_ns']<=f['end_ns']<=s['start_ns']<=s['end_ns']<=end,(name,trace,ticket,f,s)
            r=dict(client=trace['client'],first_key=trace['first_key'],ticket=ticket,
                   frontier_ticket_end=f['ticket_end'],generation=f['generation'],
                   latency_ms=(end-begin)/1e6,before_frontier_ms=(f['start_ns']-begin)/1e6,
                   frontier_write_ms=(f['end_ns']-f['start_ns'])/1e6,
                   before_sync_ms=(s['start_ns']-f['end_ns'])/1e6,
                   fdatasync_ms=(s['end_ns']-s['start_ns'])/1e6,after_sync_ms=(end-s['end_ns'])/1e6,
                   earlier_certificates=bisect.bisect_left(starts,f['start_ns'])-bisect.bisect_left(starts,begin),
                   speculative_tail_bytes=f['offset']-f['durable_end'])
            assert abs(sum(r[k]for k in STAGES)-r['latency_ms'])<1e-7
            for t,calls in long_calls.items():
                r[f'io_stall_overlap_{t}ms']=any(c['inode']==f['inode']and c['start_ns']<end and c['end_ns']>begin for c in calls)
            rows.append(r)
        filtered={f'no_traced_io_ge_{t}ms':stats([r for r in rows if not r[f'io_stall_overlap_{t}ms']])for t in [1,2,5]}
        c=record['result']['wal_pipeline']
        result[name]=dict(arm=meta['arm'],run_id=meta['run_id'],raw_puts_s=record['result']['ops_per_sec'],
                          raw_reported_p99_ms=record['latency']['p99_ms'],raw_samples=stats(rows),filtered=filtered,
                          counters={k:c[k]for k in ['sync_count','commit_groups','commit_solo_groups','commit_buffers',
                                   'frontier_write_ns','fdatasync_ns','preallocation_ns','sync_coalesce_wait_ns']},
                          trace_sha256=sha(WORK/(name+'-batch.jsonl')),io_sha256=sha(WORK/(name+'-io.csv')))
        pooled[meta['arm']].extend(rows)
    result['pooled_diagnostic_samples']={arm:dict(raw=stats(rows),filtered={f'no_traced_io_ge_{t}ms':stats([r for r in rows if not r[f'io_stall_overlap_{t}ms']])for t in [1,2,5]})for arm,rows in pooled.items()}
    result['limits']='Filtered samples exclude observed synchronous WAL calls, not all asynchronous DATA or scheduling delays. Pooled diagnostics are not clean-run throughput or qualification. Preserve all raw rows; no observation is deleted.'
    result['analyzer_sha256']=sha(Path(__file__))
    print(json.dumps(result,indent=2))


if __name__=='__main__':
    main()
