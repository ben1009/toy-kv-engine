#!/usr/bin/env python3
"""Map every traced point API call to its physical durability cycle."""
import bisect,csv,hashlib,json,statistics
from collections import Counter,defaultdict
from pathlib import Path
R=Path(__file__).resolve().parents[4];A=Path(__file__).resolve().parent;N='rfc-025-v7-point-handoff-20261029'
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
def stats(values):
    values=sorted(values);n=len(values)
    return dict(count=n,mean_us=statistics.mean(values)/1000,p50_us=values[(n-1)//2]/1000,p95_us=values[(n*95-1)//100]/1000,p99_us=values[(n*99-1)//100]/1000,max_us=values[-1]/1000)
def analyze(capture):
    files=capture['files'];path=lambda suffix:R/next(p for p in files if p.endswith(suffix))
    for p,h in files.items():assert sha(R/p)==h
    record=json.loads(path('.stdout').read_text());ops=capture['operations'];events=defaultdict(list)
    with path('-events.csv').open()as f:
        for r in csv.DictReader(f):
            e,k,a,b,tid,t=[int(r[c])for c in['event','subject','arg1','arg2','tid','time_ns']];events[e].append((k,a,b,tid,t))
    assert len(events[1])==len(events[2])==ops
    start=min(e[-1]for e in events[1]);end=max(e[-1]for e in events[2]);begins={e[0]:e for e in events[1]};ends={e[0]:e for e in events[2]}
    waits=defaultdict(list);exits={e[0]:e for e in events[4]if start<=e[-1]<=end}
    for e in events[3]:
        if start<=e[-1]<=end:waits[e[3]].append(e)
    for tid in waits:waits[tid].sort(key=lambda e:e[-1])
    times={tid:[e[-1]for e in rows]for tid,rows in waits.items()}
    stage={e[0]:e for e in events[5]if start<=e[-1]<=end};submitted={e[0]:e for e in events[6]if start<=e[-1]<=end};reaped={e[0]:e for e in events[7]if start<=e[-1]<=end}
    assert len(stage)==len(submitted)==len(reaped)==ops
    pack_begin={};pack_ready={}
    for kind,target in[(13,pack_begin),(14,pack_ready)]:
        for e in events[kind]:
            if start<=e[-1]<=end:
                for ticket in range(e[0],e[1]):target[ticket]=e
    assert len(pack_begin)==len(pack_ready)==ops
    native=list(csv.DictReader(path('-io.csv').open()));frontiers=[e for e in native if e['kind']=='2'and int(e['ticket_end'])>0];inode=max(frontiers,key=lambda e:int(e['ticket_end']))['inode'];native=[e for e in native if e['inode']==inode];frontiers=[e for e in frontiers if e['inode']==inode];frontiers.sort(key=lambda e:int(e['ticket_end']));boundaries=[int(e['ticket_end'])for e in frontiers]
    assert len(frontiers)==record['result']['wal_pipeline']['frontier_writes'];assert boundaries[-1]==ops and boundaries==sorted(set(boundaries))
    assert all(int(e['error'])==0 for e in native)
    syncs=defaultdict(list)
    for e in native:
        if e['op']=='2':syncs[e['tid']].append(e)
    for tid in syncs:syncs[tid].sort(key=lambda e:int(e['start_ns']))
    sync_starts={tid:[int(e['start_ns'])for e in rows]for tid,rows in syncs.items()}
    published={e[0]:e for e in events[10]if start<=e[-1]<=end};cycles=[]
    for f in frontiers:
        ix=bisect.bisect_left(sync_starts[f['tid']],int(f['end_ns']));sync=syncs[f['tid']][ix]
        pub=published[int(f['ticket_end'])];assert int(sync['end_ns'])<=pub[-1];cycles.append((f,sync,pub))
    labels=['api_to_packer','packer_materialize_prepare','packer_to_worker','stage_to_submit_return','submit_return_to_reap','reap_to_frontier','frontier_write','frontier_to_sync','fdatasync','sync_to_publish','publish_to_wait_return','wait_return_to_api_return']
    rows=[];ticket_ids=set()
    for key,api in begins.items():
        finish=ends[key];tid=api[3];ix=bisect.bisect_left(times[tid],api[-1]);wait=waits[tid][ix];ticket=wait[0];exit=exits[ticket];assert ticket not in ticket_ids;ticket_ids.add(ticket)
        assert api[-1]<=wait[-1]<=exit[-1]<=finish[-1] and wait[3]==exit[3]==tid
        f,sync,pub=cycles[bisect.bisect_right(boundaries,ticket)]
        points=[api[-1],pack_begin[ticket][-1],pack_ready[ticket][-1],stage[ticket][-1],submitted[ticket][-1],reaped[ticket][-1],int(f['start_ns']),int(f['end_ns']),int(sync['start_ns']),int(sync['end_ns']),pub[-1],exit[-1],finish[-1]]
        assert points==sorted(points),(ticket,points)
        parts={label:b-a for label,a,b in zip(labels,points,points[1:])};latency=finish[-1]-api[-1];assert sum(parts.values())==latency
        rows.append(dict(ticket=ticket,key=key,writer_tid=tid,start_ns=api[-1],end_ns=finish[-1],latency_ns=latency,parts=parts,worker_tid=stage[ticket][3],packer_tid=pack_begin[ticket][3],coordinator_tid=int(f['tid'])))
    assert len(ticket_ids)==ops and ticket_ids==set(range(ops));rows.sort(key=lambda r:r['latency_ns']);tail=rows[-max(1,ops//100):];slow=[r for r in rows if r['latency_ns']>=2_000_000]
    roles={'worker':str(rows[0]['worker_tid']),'packer':str(rows[0]['packer_tid']),'coordinator':str(rows[0]['coordinator_tid'])};observations=defaultdict(Counter);samples=[json.loads(l)for l in path('-samples.jsonl').read_text().splitlines()]
    for s in samples:
        if start<=s['monotonic_ns']<=end:
            for role,tid in roles.items():
                if tid in s['tasks']:
                    t=s['tasks'][tid];observations[role][t['state']+':'+t['wchan']]+=1
    output=dict(run_id=capture['run_id'],writers=capture['writers'],operations=ops,mapped_calls=len(rows),latency=stats([r['latency_ns']for r in rows]),stages={label:stats([r['parts'][label]for r in rows])for label in labels},top_1pct=dict(calls=len(tail),stages={label:stats([r['parts'][label]for r in tail])for label in labels},dominant_stage=dict(Counter(max(r['parts'],key=r['parts'].get)for r in tail))),calls_ge_2ms=dict(calls=len(slow),dominant_stage=dict(Counter(max(r['parts'],key=r['parts'].get)for r in slow))),thread_states={role:dict(c)for role,c in observations.items()},pipeline=record['result']['wal_pipeline'],raw_puts_s=record['result']['ops_per_sec'],raw_p99_ms=record['latency']['p99_ms'],trace_counts={str(k):len(v)for k,v in events.items()},files_sha256=files)
    out=R/'target/wal-opt/20261029-v7-point-handoff'/ (str(capture['writers'])+'writers-paths.jsonl');out.write_text(''.join(json.dumps(r,separators=(',',':'))+'\n'for r in rows));output['paths_file']=str(out.relative_to(R));output['paths_sha256']=sha(out)
    return output
captures=json.loads((A/(N+'-captures.json')).read_text());print(json.dumps(dict(captures=[analyze(c)for c in captures],analyzer_sha256=sha(Path(__file__)),limits='Excluded instrumented observations. Stage intervals partition each call; stage percentiles do not sum to an API percentile. DATA submission/reap includes kernel work and scheduling; native synchronous calls identify syscall elapsed time, not hardware-only service. Trace locking and observer overhead can change scheduling. Scheduler statistics were disabled, so runqueue counters cannot establish runnable-delay attribution.'),indent=2))
