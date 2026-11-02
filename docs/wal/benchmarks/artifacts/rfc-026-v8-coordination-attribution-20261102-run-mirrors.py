import pathlib,subprocess,json,time,hashlib
p=pathlib.Path(__file__).resolve().parent
bins={'baseline':p.parent/'rfc-026-v8-wakeup-profile-20261102/write-perf-baseline','mirrors':p/'write-perf-status-mirrors'}
schedule=[('baseline',8),('mirrors',8),('mirrors',16),('baseline',16),('mirrors',8),('baseline',8),('baseline',16),('mirrors',16)]
(p/'mirrors-plan.json').write_text(json.dumps({'schedule':schedule,'binary_hashes':{k:hashlib.sha256(v.read_bytes()).hexdigest() for k,v in bins.items()},'selection':'highest eligible throughput with same-run p99; flag p99>10ms or mean fdatasync>1ms; preserve all; normal counters, detailed diagnostics off'},indent=2))
for i,(arm,threads) in enumerate(schedule):
 name=f'mirrors-{i:02}-{arm}-{threads}'
 cmd=[str(bins[arm]),'--bench','wal_batch_concurrent','--wal-batch-size','64','--pitr','--pitr-format','v8','--wal-io-mode','parallel','--num','750000','--threads',str(threads),'--value-size','1024','--target-sst-size','2147483648','--compaction','none','--output','json','--latency-sample-every','10','--path',str(p/(name+'-db'))]
 print('START',name,flush=True)
 with (p/(name+'.json')).open('w') as out,(p/(name+'.stderr')).open('w') as err:
  r=subprocess.run(cmd,stdout=out,stderr=err,timeout=180)
 if r.returncode:print('FAILED',r.returncode,(p/(name+'.stderr')).read_text()[-1000:],flush=True);continue
 d=json.loads((p/(name+'.json')).read_text());q=d['result'];w=q['wal_pipeline'];l=d['latency'];mean=w['fdatasync_ns']/w['sync_count']/1e6
 print(json.dumps({'name':name,'rate':round(q['ops_per_sec']),'p99':round(l['p99_ms'],3),'cpu_s':round((q['process_cpu_user_ms']+q['process_cpu_system_ms'])/1000,3),'syncs':w['sync_count'],'sync_mean_ms':round(mean,3),'stall':l['p99_ms']>10 or mean>1}),flush=True)
 time.sleep(3)
