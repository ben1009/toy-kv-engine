#!/usr/bin/env python3
"""Two excluded diagnostics of kernel helpers in the retained v7 runtime."""
import csv,hashlib,importlib.util,json,os,shutil,subprocess,sys,threading,time
from pathlib import Path
sys.dont_write_bytecode=True
ROOT=Path('/home/liu/proj/toy-kv-engine');ART=ROOT/'docs/wal/benchmarks/artifacts';STEM='rfc-025-v7-async-worker-limit-20261028';WORK=ROOT/'target/wal-opt/20261028-v7-async-worker-limit/candidate-probe'
WORK.mkdir(exist_ok=False)
spec=importlib.util.spec_from_file_location('audit',ART/'rfc-025-v7-large-data-async-dispatch-20261027-run.py');audit=importlib.util.module_from_spec(spec);spec.loader.exec_module(audit)
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
plan=json.loads((ART/(STEM+'-probe-plan.json')).read_text())
for entry in [plan['binary'],plan['trace_library']]:assert sha(ROOT/entry['path'])==entry['sha256']
assert not any(k.startswith('MIMALLOC_')or k=='LD_PRELOAD'for k in os.environ)
raw=ART/(STEM+'-probe-runs.jsonl');captures=ART/(STEM+'-probe-captures.json');assert not raw.exists()and not captures.exists();metadata=[]
time.sleep(plan['initial_seconds'])
for writers in plan['schedule']:
 name=str(writers)+'writers';db=WORK/(name+'-db');trace=WORK/(name+'-io.csv');samples_path=WORK/(name+'-samples.jsonl');assert not db.exists()and not trace.exists()
 command=[str(ROOT/plan['binary']['path']),'--suite','legacy','--preset','smoke','--bench','wal_batch_concurrent','--wal','--wal-io-mode','parallel','--pitr','--pitr-format','v7','--num','750000','--threads',str(writers),'--wal-batch-size','64','--value-size','1024','--target-sst-size','2147483648','--compaction','none','--latency-sample-every','10','--output','json','--path',str(db)]
 before=audit.host_sample();assert not before['competing'];env=os.environ.copy();env.update(LD_PRELOAD=str(ROOT/plan['trace_library']['path']),KV_WAL_IO_TRACE=str(trace));process=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=env)
 stop=threading.Event();samples=[];errors=[]
 def observe():
  while not stop.is_set():
   tasks={}
   try:
    for p in Path('/proc',str(process.pid),'task').glob('*'):
     try:
      stat=(p/'stat').read_text().rpartition(')')[2].split();name=(p/'comm').read_text().strip();tasks[p.name]=dict(name=name,state=stat[0],user_ticks=int(stat[11]),system_ticks=int(stat[12]),wchan=(p/'wchan').read_text().strip())
     except (OSError,IndexError,ValueError):continue
    row=dict(monotonic_ns=time.monotonic_ns(),tasks=tasks)
    if len(samples)%10==0:row['host_audit']=audit.host_sample(process.pid)
    samples.append(row)
   except Exception as e:errors.append(str(e))
   stop.wait(plan['sampler_seconds'])
 thread=threading.Thread(target=observe);thread.start()
 try:stdout,stderr=process.communicate()
 finally:stop.set();thread.join()
 after=audit.host_sample();(WORK/(str(writers)+'writers.stdout')).write_bytes(stdout);(WORK/(str(writers)+'writers.stderr')).write_bytes(stderr);samples_path.write_text(''.join(json.dumps(s,separators=(',',':'))+'\n'for s in samples))
 assert process.returncode==0,(process.returncode,stderr[-1000:]);assert len(stdout.splitlines())==1;r=json.loads(stdout)
 with raw.open('ab')as f:f.write(stdout.strip()+b'\n')
 clean=not errors and not any(s['competing']for s in [before,*[s['host_audit']for s in samples if'host_audit'in s],after]);assert clean,errors
 events=list(csv.DictReader(trace.open()));frontiers=[e for e in events if e['kind']=='2'and int(e['ticket_end'])>0];inode=max(frontiers,key=lambda e:int(e['ticket_end']))['inode'];frontiers=[e for e in frontiers if e['inode']==inode];start=min(int(e['start_ns'])for e in frontiers);end=max(int(e['end_ns'])for e in frontiers);inside=[s for s in samples if start<=s['monotonic_ns']<=end]
 counts=[sum(t['name'].startswith('iou-')for t in s['tasks'].values())for s in inside];active=[sum(t['name'].startswith('iou-')and t['state']in ['R','D']for t in s['tasks'].values())for s in inside]
 metadata.append(dict(writers=writers,run_id=r['run_id'],excluded_from_scoring=True,command=command,binary=plan['binary'],returncode=process.returncode,clean_sampled=clean,sampling_errors=errors,before=before,after=after,samples=len(samples),active_window_samples=len(inside),helper_counts=counts,active_helper_counts=active,max_helpers=max(counts,default=0),max_active_helpers=max(active,default=0),files={str(p.relative_to(ROOT)):sha(p)for p in [trace,samples_path,WORK/(str(writers)+'writers.stdout'),WORK/(str(writers)+'writers.stderr')]}));captures.write_text(json.dumps(metadata,indent=2)+'\n')
 print(writers,'writers: helpers max',metadata[-1]['max_helpers'],'active max',metadata[-1]['max_active_helpers'],'puts/s',round(r['result']['ops_per_sec']),'p99',r['latency']['p99_ms'],flush=True)
 for disposable in [db,Path(str(db)+'.pitr-repository')]:
  if disposable.exists():shutil.rmtree(disposable)
 time.sleep(plan['between_seconds'])
assert len(metadata)==1 and len({m['run_id']for m in metadata})==1
assert 0 < metadata[0]['max_helpers'] <= 4
