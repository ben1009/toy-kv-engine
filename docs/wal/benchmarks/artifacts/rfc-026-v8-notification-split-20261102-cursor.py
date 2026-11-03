from pathlib import Path
import subprocess,json,time,hashlib,os
p=Path(__file__).resolve().parent
bins={'baseline':p/'write-perf-baseline','cursor':p/'write-perf-cursor'}
schedule=[('baseline',8),('cursor',8),('cursor',16),('baseline',16),('cursor',8),('baseline',8),('baseline',16),('cursor',16)]
plan={'schedule':schedule,'binary_sha256':{k:hashlib.sha256(v.read_bytes()).hexdigest() for k,v in bins.items()},'selection':'Highest throughput eligible per arm/writer count; same-run p99, CPU and context switches. Flag p99>10ms or mean fdatasync>1ms; retain all raw samples.','stage':'End-to-end durable-only completion: role-separated notifications plus consuming the already-published cursor without the physical frontier mutex. No digest, DATA grouping or sync policy changes. Same original baseline.','context':'getrusage RUSAGE_SELF deltas only around timed write interval; identical optional probe in both binaries; no hot-path attribution or perf sampling.'}
(p/'cursor-plan.json').write_text(json.dumps(plan,indent=2))
for i,(arm,writers) in enumerate(schedule):
 name=f'cursor-{i:02}-{arm}-{writers}'
 env=dict(os.environ,TOYKV_WAL_CONTEXT=str(p/(name+'-context.json')))
 cmd=[str(bins[arm]),'--bench','wal_batch_concurrent','--wal-batch-size','64','--pitr','--pitr-format','v8','--wal-io-mode','parallel','--num','750000','--threads',str(writers),'--value-size','1024','--target-sst-size','2147483648','--compaction','none','--output','json','--latency-sample-every','10','--path',str(p/(name+'-db'))]
 print('START',name,flush=True)
 with (p/(name+'.json')).open('w') as out,(p/(name+'.stderr')).open('w') as err:
  r=subprocess.run(cmd,stdout=out,stderr=err,env=env,timeout=180)
 if r.returncode:print('FAILED',name,r.returncode,(p/(name+'.stderr')).read_text()[-1000:],flush=True);continue
 d=json.loads((p/(name+'.json')).read_text());q=d['result'];w=q['wal_pipeline'];l=d['latency'];c=json.loads((p/(name+'-context.json')).read_text());mean=w['fdatasync_ns']/w['sync_count']/1e6
 print(json.dumps({'name':name,'puts_s':round(q['ops_per_sec']),'p99_ms':round(l['p99_ms'],3),'cpu_s':round((q['process_cpu_user_ms']+q['process_cpu_system_ms'])/1000,3),'voluntary':c['voluntary'],'involuntary':c['involuntary'],'eventfd_requests':w['worker_eventfd_notifications'],'eventfd_writes':w['worker_eventfd_writes'],'syncs':w['sync_count'],'mean_sync_ms':round(mean,3),'stall_flag':l['p99_ms']>10 or mean>1}),flush=True)
 time.sleep(3)
