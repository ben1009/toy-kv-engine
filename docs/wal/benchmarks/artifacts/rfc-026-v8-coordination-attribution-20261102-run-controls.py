import pathlib,subprocess,json,time,hashlib,os
p=pathlib.Path(__file__).resolve().parent
binary=(p.parent/'rfc-026-v8-wakeup-profile-20261102/write-perf-timed').resolve()
schedule=['v7','v8','v8','v7']
(p/'controls-plan.json').write_text(json.dumps({'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'schedule':schedule,'threads':16,'operations':750000,'sampling':'none; perf stat enabled only in timed interval','eligibility':'flag p99>10ms or mean fdatasync>1ms; highest eligible throughput; all raw retained'},indent=2))
for i,fmt in enumerate(schedule):
 name=f'control-{i:02}-{fmt}-16';ctl=p/(name+'-control');ctl.mkdir(exist_ok=True)
 for f in ['control','ack']:
  if (ctl/f).exists():(ctl/f).unlink()
  os.mkfifo(ctl/f)
 env=dict(os.environ,KV_PERF_CONTROL_DIR=str(ctl))
 bench=[str(binary),'--bench','wal_batch_concurrent','--wal-batch-size','64','--pitr','--pitr-format',fmt,'--wal-io-mode','parallel','--num','750000','--threads','16','--value-size','1024','--target-sst-size','2147483648','--compaction','none','--output','json','--latency-sample-every','10','--profile','--path',str(p/(name+'-db'))]
 cmd=['perf','stat','-e','task-clock,context-switches,cpu-migrations,page-faults','--delay=-1','--control',f'fifo:{ctl}/control,{ctl}/ack','-x',';','-o',str(p/(name+'-stat.csv')),'--']+bench
 print('START',name,flush=True)
 with (p/(name+'.json')).open('w') as out,(p/(name+'.stderr')).open('w') as err:
  r=subprocess.run(cmd,stdout=out,stderr=err,env=env,timeout=180)
 if r.returncode:print('FAILED',r.returncode,(p/(name+'.stderr')).read_text()[-1000:],flush=True);continue
 d=json.loads((p/(name+'.json')).read_text());q=d['result'];w=q['wal_pipeline'];l=d['latency'];mean=w['fdatasync_ns']/w['sync_count']/1e6
 print(json.dumps({'name':name,'rate':round(q['ops_per_sec']),'p99':round(l['p99_ms'],3),'user_s':round(q['process_cpu_user_ms']/1000,3),'system_s':round(q['process_cpu_system_ms']/1000,3),'syncs':w['sync_count'],'sync_mean_ms':round(mean,3),'stall':l['p99_ms']>10 or mean>1}),flush=True)
 time.sleep(3)
