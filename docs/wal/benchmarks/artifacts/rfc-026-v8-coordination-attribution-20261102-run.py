import pathlib,subprocess,json,time,hashlib,os
p=pathlib.Path(__file__).resolve().parent
binary=p/'write-perf-attribution'
schedule=['v7','v8','v8','v7']
plan={'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'schedule':schedule,'threads':16,'operations':750000,'batch':64,'value_bytes':1024,'perf':'cpu-clock:u 499Hz DWARF4096; timed interval only; output Linux memfd','scopes':'inclusive, 1/64 samples; diagnostic, not throughput qualification'}
(p/'plan.json').write_text(json.dumps(plan,indent=2))
for i,fmt in enumerate(schedule):
 name=f'{i:02}-{fmt}-16';ctl=p/(name+'-control');ctl.mkdir(exist_ok=True)
 for f in ['control','ack']:
  if (ctl/f).exists():(ctl/f).unlink()
  os.mkfifo(ctl/f)
 env=dict(os.environ,KV_PERF_CONTROL_DIR=str(ctl),TOYKV_WAL_ATTRIBUTION=str(p/(name+'-phases.json')))
 fd=os.memfd_create('wal-perf',0)
 bench=[str(binary),'--bench','wal_batch_concurrent','--wal-batch-size','64','--pitr','--pitr-format',fmt,'--wal-io-mode','parallel','--num','750000','--threads','16','--value-size','1024','--target-sst-size','2147483648','--compaction','none','--output','json','--latency-sample-every','10','--profile','--path',str(p/(name+'-db'))]
 cmd=['perf','record','-q','-e','cpu-clock:u','-F','499','--call-graph','dwarf,4096','--delay=-1','--control',f'fifo:{ctl}/control,{ctl}/ack','-o',f'/proc/self/fd/{fd}','--']+bench
 print('START',name,flush=True)
 try:
  with (p/(name+'.json')).open('w') as out,(p/(name+'.stderr')).open('w') as err:
   r=subprocess.run(cmd,stdout=out,stderr=err,env=env,pass_fds=[fd],timeout=180)
  os.lseek(fd,0,0)
  with (p/(name+'.perf')).open('wb') as out:
   while True:
    b=os.read(fd,1024*1024)
    if not b:break
    out.write(b)
  if r.returncode:print('FAILED',r.returncode,(p/(name+'.stderr')).read_text()[-1000:],flush=True);continue
  d=json.loads((p/(name+'.json')).read_text());q=d['result'];w=q['wal_pipeline'];l=d['latency'];mean=w['fdatasync_ns']/w['sync_count']/1e6
  a=json.loads((p/(name+'-phases.json')).read_text());before=a['process_before'];after=a['process_after']
  print(json.dumps({'name':name,'rate':round(q['ops_per_sec']),'p99':round(l['p99_ms'],3),'user_s':round(q['process_cpu_user_ms']/1000,3),'system_s':round(q['process_cpu_system_ms']/1000,3),'voluntary':after['voluntary']-before['voluntary'],'involuntary':after['involuntary']-before['involuntary'],'threads':len(a['threads']),'syncs':w['sync_count'],'sync_mean_ms':round(mean,3),'stall':l['p99_ms']>10 or mean>1}),flush=True)
 except subprocess.TimeoutExpired: print('TIMEOUT',name,flush=True)
 finally:os.close(fd)
 time.sleep(3)
