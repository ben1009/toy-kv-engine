#!/usr/bin/env python3
"""Two excluded retained-policy point-write traces, emitted after timed work."""
import hashlib,importlib.util,json,os,shutil,subprocess,sys,threading,time
from pathlib import Path
sys.dont_write_bytecode=True
R=Path('/home/liu/proj/toy-kv-engine');A=R/'docs/wal/benchmarks/artifacts';N='rfc-025-v7-point-handoff-20261029';W=R/'target/wal-opt/20261029-v7-point-handoff'
spec=importlib.util.spec_from_file_location('audit',A/'rfc-025-v7-small-completion-priority-20261028-run.py');audit=importlib.util.module_from_spec(spec);spec.loader.exec_module(audit)
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
plan=json.loads((A/(N+'-plan.json')).read_text());binary=json.loads((A/(N+'-binaries.json')).read_text())['diagnostic'];assert sha(R/binary['path'])==binary['sha256']
lib=R/'target/wal-opt/20261027-v7-writer-scaling/io-trace.so'
assert sha(lib)=='7bef6a9cfadc9c803f955fb96e86bd65f5bdebd07aab39184d283832bfe3e46b'
assert not any(k.startswith('MIMALLOC_')or k in ['LD_PRELOAD','KV_WAL_POINT_TRACE']for k in os.environ)
raw=A/(N+'-runs.jsonl');meta=A/(N+'-captures.json');assert not raw.exists()and not meta.exists();captures=[]
time.sleep(30)
for case in plan['schedule']:
 name=str(case['writers'])+'writers';db=W/(name+'-db');trace=W/(name+'-events.csv');io=W/(name+'-io.csv');samples_path=W/(name+'-samples.jsonl');assert not db.exists()and not trace.exists()
 cmd=[str(R/binary['path']),'--suite','legacy','--preset','smoke','--bench','wal_concurrent','--wal','--wal-io-mode','parallel','--pitr','--pitr-format','v7','--num',str(case['operations']),'--threads',str(case['writers']),'--value-size','1024','--target-sst-size','2147483648','--compaction','none','--latency-sample-every',str(case['sample_every']),'--output','json','--path',str(db)]
 before=audit.host_sample();assert not before['competing'];env=os.environ.copy();env.update(LD_PRELOAD=str(lib),KV_WAL_IO_TRACE=str(io),KV_WAL_POINT_TRACE=str(trace))
 process=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=env);stop=threading.Event();samples=[];errors=[]
 def observe():
  last_audit=0
  while not stop.is_set():
   try:
    row=dict(monotonic_ns=time.monotonic_ns(),tasks={})
    for p in Path('/proc',str(process.pid),'task').glob('*'):
     try:
      stat=(p/'stat').read_text().rpartition(')')[2].split();sched=[int(x)for x in(p/'schedstat').read_text().split()]
      row['tasks'][p.name]=dict(name=(p/'comm').read_text().strip(),state=stat[0],user_ticks=int(stat[11]),system_ticks=int(stat[12]),wchan=(p/'wchan').read_text().strip(),schedstat=sched)
     except (OSError,ValueError,IndexError):continue
    if time.monotonic()-last_audit>=1:row['host_audit']=audit.host_sample(process.pid);last_audit=time.monotonic()
    samples.append(row)
   except Exception as e:errors.append(str(e))
   stop.wait(.01)
 thread=threading.Thread(target=observe);thread.start()
 try:stdout,stderr=process.communicate()
 finally:stop.set();thread.join()
 after=audit.host_sample();out=W/(name+'.stdout');err=W/(name+'.stderr');out.write_bytes(stdout);err.write_bytes(stderr);samples_path.write_text(''.join(json.dumps(s,separators=(',',':'))+'\n'for s in samples))
 assert process.returncode==0,(process.returncode,stderr[-3000:]);r=json.loads(stdout);assert r['result']['ops']==case['operations'];tracemeta=json.loads(trace.with_suffix('.meta.json').read_text());assert tracemeta['dropped']==0
 clean=not errors and not any(s['competing']for s in[before,*[s['host_audit']for s in samples if'host_audit'in s],after]);assert clean,errors
 with raw.open('ab')as f:f.write(stdout.strip()+b'\n')
 entry=dict(**case,run_id=r['run_id'],command=cmd,binary=binary,trace_library_sha256=sha(lib),returncode=process.returncode,excluded_from_scoring=True,clean_sampled=clean,sampling_errors=errors,before=before,after=after,events=tracemeta['events'],files={str(p.relative_to(R)):sha(p)for p in[trace,trace.with_suffix('.meta.json'),io,samples_path,out,err]})
 captures.append(entry);meta.write_text(json.dumps(captures,indent=2)+'\n');print('COMPLETE',name,r['run_id'],round(r['result']['ops_per_sec']),r['latency']['p99_ms'],'events',entry['events'],flush=True)
 for disposable in[db,Path(str(db)+'.pitr-repository')]:
  if disposable.exists():shutil.rmtree(disposable)
 time.sleep(15)
assert len(captures)==2 and len({c['run_id']for c in captures})==2
