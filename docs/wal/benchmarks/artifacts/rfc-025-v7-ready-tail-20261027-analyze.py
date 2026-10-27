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
STEM = 'rfc-025-v7-ready-tail-20261027'
WORK = ROOT / 'target/wal-opt/20261027-v7-ready-tail'
STAGES = ['before_data_stage_ms','data_queue_ms','wait_group_and_earlier_data_ms','wait_later_captured_data_ms','prefix_ready_to_capture_ms','capture_to_frontier_ms','frontier_write_ms','before_sync_ms','fdatasync_ms','after_sync_ms']


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
    assert len(captures) == 2
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
        stage_rows=[]
        for path,digest in meta['stage_files'].items():
            assert sha(ROOT/path)==digest
            stage_rows.extend({k:int(v) for k,v in r.items()}for r in csv.DictReader((ROOT/path).open()))
        staged={r['request']:r for r in stage_rows if r['event']==1}
        reaped={r['request']:r for r in stage_rows if r['event']==2}
        captured={r['request']:r for r in stage_rows if r['event']==3}
        assert len(staged)==len(reaped)==11728 and len(captured)==len(markers)
        by_ticket={r['ticket']:r for r in staged.values()}
        assert set(by_ticket)==set(range(11728))
        group_ready={}
        for request,r in staged.items():
            assert request in reaped and reaped[request]['length']==r['length']
            assert r['time_ns']<=reaped[request]['time_ns']
            group_ready[r['group_end']]=max(group_ready.get(r['group_end'],0),reaped[request]['time_ns'])
        prefix_ready={}
        last_ready=0
        for group_end,ready in sorted(group_ready.items()):
            last_ready=max(last_ready,ready)
            prefix_ready[group_end]=last_ready
        data_intervals=[(r['time_ns'],reaped[q]['time_ns'])for q,r in staged.items()]
        for f in markers:
            cap=captured[f['generation']]
            assert cap['ticket']==f['ticket_end'] and cap['offset']==f['offset']
            assert prefix_ready[f['ticket_end']]<=cap['time_ns']<=f['start_ns']
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
            data=by_ticket[ticket]
            data_end=reaped[data['request']]['time_ns']
            own_prefix_ready=prefix_ready[data['group_end']]
            full_prefix_ready=prefix_ready[f['ticket_end']]
            cap=captured[f['generation']]['time_ns']
            assert begin<=data['time_ns']<=data_end<=own_prefix_ready<=full_prefix_ready<=cap<=f['start_ns']
            assert data['offset']==key_map[trace['first_key']]['file_offset']
            r.update(before_data_stage_ms=(data['time_ns']-begin)/1e6,
                     data_queue_ms=(data_end-data['time_ns'])/1e6,
                     wait_group_and_earlier_data_ms=(own_prefix_ready-data_end)/1e6,
                     wait_later_captured_data_ms=(full_prefix_ready-own_prefix_ready)/1e6,
                     prefix_ready_to_capture_ms=(cap-full_prefix_ready)/1e6,
                     capture_to_frontier_ms=(f['start_ns']-cap)/1e6,
                     earlier_captures_after_ready=sum(c['time_ns']>=own_prefix_ready and c['time_ns']<cap for c in captured.values()))
            assert abs(sum(r[k]for k in STAGES)-r['latency_ms'])<1e-7
            for t,calls in long_calls.items():
                r[f'io_stall_overlap_{t}ms']=any(c['inode']==f['inode']and c['start_ns']<end and c['end_ns']>begin for c in calls)
            for threshold in [1,2,5]:
                r[f'data_queue_overlap_{threshold}ms']=any(a<end and b>begin and b-a>=threshold*1_000_000 for a,b in data_intervals)
            rows.append(r)
        filtered={f'no_traced_io_ge_{t}ms':stats([r for r in rows if not r[f'io_stall_overlap_{t}ms']])for t in [1,2,5]}
        filtered.update({f'no_sync_io_or_data_queue_ge_{threshold}ms':stats([r for r in rows if not r[f'io_stall_overlap_{threshold}ms'] and not r[f'data_queue_overlap_{threshold}ms']])for threshold in [1,2,5]})
        c=record['result']['wal_pipeline']
        result[name]=dict(arm=meta['arm'],run_id=meta['run_id'],raw_puts_s=record['result']['ops_per_sec'],
                          raw_reported_p99_ms=record['latency']['p99_ms'],raw_samples=stats(rows),filtered=filtered,
                          counters={k:c[k]for k in ['sync_count','commit_groups','commit_solo_groups','commit_buffers',
                                   'frontier_write_ns','fdatasync_ns','preallocation_ns','sync_coalesce_wait_ns']},
                          trace_sha256=sha(WORK/(name+'-batch.jsonl')),io_sha256=sha(WORK/(name+'-io.csv')))
        pooled[meta['arm']].extend(rows)
    result['pooled_diagnostic_samples']={arm:dict(raw=stats(rows),filtered={f'no_traced_io_ge_{t}ms':stats([r for r in rows if not r[f'io_stall_overlap_{t}ms']])for t in [1,2,5]})for arm,rows in pooled.items()}
    for arm,rows in pooled.items():
        result['pooled_diagnostic_samples'][arm]['filtered'].update({f'no_sync_io_or_data_queue_ge_{threshold}ms':stats([r for r in rows if not r[f'io_stall_overlap_{threshold}ms'] and not r[f'data_queue_overlap_{threshold}ms']])for threshold in [1,2,5]})
    result['limits']='DATA staging-to-reap includes kernel I/O, submission and reaping/scheduling delay. Group-prefix readiness is a CQE-based lower bound preceding group-result publication. The stricter filtered view is diagnostic, not proof that excluded DATA delays are all hardware stalls or that remaining samples are fully stall-free. No operation is deleted.'
    result['analyzer_sha256']=sha(Path(__file__))
    print(json.dumps(result,indent=2))


if __name__=='__main__':
    main()
