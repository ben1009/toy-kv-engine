from pathlib import Path
import subprocess,json,time,hashlib,os
p=Path(__file__).resolve().parent
bins={'baseline':p/'write-perf-cpu-baseline','roles':p/'write-perf-cpu-roles'}
schedule=['baseline','roles','roles','baseline']
plan={'schedule':schedule,'writers':16,'binary_sha256':{k:hashlib.sha256(v.read_bytes()).hexdigest() for k,v in bins.items()},'purpose':'Publication CPU diagnostic only: memtable/MVCC scopes, sample rate 64; no global allocator wrapper, stack sampling or detailed WAL diagnostics. Do not treat as retention qualification.','cooldown_seconds':45,'selection':'Preserve and flag all p99>10ms or mean fdatasync>1ms runs. Compare complete same-run metrics. Inclusive phase estimates, scaled separately per thread; sampling overhead retained in both arms.'}
(p/'cpu-plan.json').write_text(json.dumps(plan,indent=2))
print('COOLDOWN 45 seconds',flush=True);time.sleep(45)
for i,arm in enumerate(schedule):
 name=f'cpu-{i:02}-{arm}-16'
 env=dict(os.environ,TOYKV_WAL_CONTEXT=str(p/(name+'-context.json')),TOYKV_WAL_ATTRIBUTION=str(p/(name+'-phases.json')))
 cmd=[str(bins[arm]),'--bench','wal_batch_concurrent','--wal-batch-size','64','--pitr','--pitr-format','v8','--wal-io-mode','parallel','--num','750000','--threads','16','--value-size','1024','--target-sst-size','2147483648','--compaction','none','--output','json','--latency-sample-every','10','--path',str(p/(name+'-db'))]
 print('START',name,flush=True)
 with (p/(name+'.json')).open('w') as out,(p/(name+'.stderr')).open('w') as err:r=subprocess.run(cmd,stdout=out,stderr=err,env=env,timeout=180)
 if r.returncode:print('FAILED',name,r.returncode,(p/(name+'.stderr')).read_text()[-1000:],flush=True);continue
 d=json.loads((p/(name+'.json')).read_text());q=d['result'];w=q['wal_pipeline'];l=d['latency'];c=json.loads((p/(name+'-context.json')).read_text());a=json.loads((p/(name+'-phases.json')).read_text());ph={}
 for phase in ['memtable_publish','mvcc_publish']:
  j=a['phase_names'].index(phase);v=0
  for t in a['threads']:
   b=t['buckets'][j]
   if b['samples']:v+=b['cpu_ns']*b['calls']/b['samples']/1e9
  ph[phase+'_cpu_s_estimate']=round(v,3)
 mean=w['fdatasync_ns']/w['sync_count']/1e6
 print(json.dumps({'name':name,'puts_s':round(q['ops_per_sec']),'p99_ms':round(l['p99_ms'],3),'cpu_s':round((q['process_cpu_user_ms']+q['process_cpu_system_ms'])/1000,3),'voluntary':c['voluntary'],'eventfd_writes':w['worker_eventfd_writes'],'syncs':w['sync_count'],'mean_sync_ms':round(mean,3),'stall_flag':l['p99_ms']>10 or mean>1,**ph}),flush=True)
 time.sleep(3)
