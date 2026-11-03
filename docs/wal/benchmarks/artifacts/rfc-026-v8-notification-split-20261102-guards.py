from pathlib import Path
import subprocess,json,time,hashlib,os
p=Path(__file__).resolve().parent
bins={'baseline':p/'write-perf-baseline','roles':p/'write-perf-roles'}
cases=[('batch',1),('point',1),('batch',8),('point',8),('batch',16),('point',16)]
schedule=[]
for round_index in range(2):
 for j,(kind,writers) in enumerate(cases if round_index==0 else list(reversed(cases))):
  arms=['baseline','roles'] if (j+round_index)%2==0 else ['roles','baseline']
  for arm in arms:schedule.append((kind,writers,arm))
plan={'schedule':schedule,'binary_sha256':{k:hashlib.sha256(v.read_bytes()).hexdigest() for k,v in bins.items()},'selection':'Highest eligible throughput per workload/writer/arm, with p99 and CPU from the same sample. p99>10ms or mean fdatasync>1ms flagged; preserve all. Qualify throughput/p99 and CPU savings; missing eligible arm means unqualified.','purpose':'Complete production-binary guard cohort after CPU diagnostic showed lower CPU/context switches and nearly unchanged throughput. No source changes or retuning. No per-phase instrumentation.','cooldown_seconds':45,'operations':{'batch':750000,'point_1':5000,'point_8_16':20000}}
if (p/'guards-plan.json').exists():
 assert json.loads((p/'guards-plan.json').read_text())==json.loads(json.dumps(plan))
 print('RESUME original schedule after fixing round-index shadowing in reporting; completed records are preserved.',flush=True)
else:
 (p/'guards-plan.json').write_text(json.dumps(plan,indent=2))
 print('COOLDOWN 45 seconds',flush=True);time.sleep(45)
for i,(kind,writers,arm) in enumerate(schedule):
 name=f'guard-{i:02}-{kind}-{arm}-{writers}'
 if (p/(name+'.json')).exists():
  saved=json.loads((p/(name+'.json')).read_text())
  assert saved['schema']=='kv-engine.write-perf.v1'
  print('PRESERVED',name,flush=True)
  continue
 env=dict(os.environ,TOYKV_WAL_CONTEXT=str(p/(name+'-context.json')))
 cmd=[str(bins[arm]),'--bench','wal_batch_concurrent' if kind=='batch' else 'wal_concurrent','--pitr','--pitr-format','v8','--wal-io-mode','parallel','--num',str(750000 if kind=='batch' else (5000 if writers==1 else 20000)),'--threads',str(writers),'--value-size','1024','--target-sst-size','2147483648','--compaction','none','--output','json','--latency-sample-every','10','--path',str(p/(name+'-db'))]
 if kind=='batch':cmd+=['--wal-batch-size','64']
 print('START',name,flush=True)
 with (p/(name+'.json')).open('w') as out,(p/(name+'.stderr')).open('w') as err:r=subprocess.run(cmd,stdout=out,stderr=err,env=env,timeout=180)
 if r.returncode:print('FAILED',name,r.returncode,(p/(name+'.stderr')).read_text()[-1000:],flush=True);continue
 d=json.loads((p/(name+'.json')).read_text());q=d['result'];w=q['wal_pipeline'];l=d['latency'];mean=w['fdatasync_ns']/w['sync_count']/1e6
 c=json.loads((p/(name+'-context.json')).read_text()) if (p/(name+'-context.json')).exists() else {}
 print(json.dumps({'name':name,'puts_s':round(q['ops_per_sec']),'p99_ms':round(l['p99_ms'],3),'cpu_s':round((q['process_cpu_user_ms']+q['process_cpu_system_ms'])/1000,3),'voluntary':c.get('voluntary'),'eventfd_writes':w['worker_eventfd_writes'],'syncs':w['sync_count'],'mean_sync_ms':round(mean,3),'stall_flag':l['p99_ms']>10 or mean>1}),flush=True)
 time.sleep(3)
