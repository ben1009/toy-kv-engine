#!/usr/bin/env python3
"""Unscored thread/stack samples, separate from timed comparison cells."""
import csv,hashlib,json,shutil,subprocess
from pathlib import Path
ROOT=Path('/home/liu/proj/toy-kv-engine')
ART=ROOT/'docs/wal/benchmarks/artifacts'
WORK=ROOT/'target/wal-opt/20261026-v7-scheduling-diagnosis'
STEM='rfc-025-v7-critical-path-20261026'
BINARY=ROOT/'target/release/write-perf-v7-fast-sync-wait'
SHA='b2de5e1c73fdf8abccca325dd0ebd4f65bc3dac96dd2b3533df6575204be248d'
SCRATCH=Path('/dev/shm/toy-kv-critical-path-20261026')
assert hashlib.sha256(BINARY.read_bytes()).hexdigest()==SHA
assert not SCRATCH.exists() and not (ART/(STEM+'.jsonl')).exists()
SCRATCH.mkdir()
try:
 with (ART/(STEM+'-runs.tsv')).open('w') as mf:
  writer=csv.writer(mf,delimiter='\t',lineterminator='\n')
  writer.writerow(['cell','binary_sha256','run_id','raw_sha256','command'])
  for cell,version,batch,num in [('v4-batch64',4,True,750000),('v7-batch64',7,True,750000),('v7-point8',7,False,100000)]:
   db=SCRATCH/cell
   data=WORK/(cell+'.data')
   args=[str(BINARY),'--suite','legacy','--preset','smoke','--wal','--bench','wal_batch_concurrent' if batch else 'wal_concurrent','--num',str(num),'--threads','8','--value-size','1024','--target-sst-size','2147483648','--latency-sample-every','10' if batch else '100','--output','json','--wal-io-mode','parallel','--path',str(db)]
   if batch:args+=['--wal-batch-size','64']
   if version==7:args+=['--pitr','--pitr-format','v7']
   command=['perf','record','-e','cpu-clock:u','-F','997','--call-graph','dwarf,8192','-o',str(data),'--',*args]
   print('profiling',cell,flush=True)
   raw=subprocess.check_output(command).strip()
   r=json.loads(raw)
   with (ART/(STEM+'.jsonl')).open('ab') as stream:stream.write(raw+b'\n')
   writer.writerow([cell,SHA,r['run_id'],hashlib.sha256(raw).hexdigest(),subprocess.list2cmdline(command)])
   mf.flush()
   for mode,args in [('flat',['--no-children','--call-graph','none']),('calls',['--children','--call-graph','graph,1.0,caller'])]:
    report=subprocess.check_output(['perf','report','--stdio','--percent-limit','0.5','-i',str(data),*args],text=True)
    (ART/(STEM+'-'+cell+'-'+mode+'.txt')).write_text('\n'.join(line.rstrip() for line in report.splitlines())+'\n')
   with (WORK/(cell+'-samples.txt')).open('w') as out:
    subprocess.run(['perf','script','-i',str(data),'-F','comm,pid,tid,period,ip,sym'],stdout=out,check=True)
   for disposable in [db,Path(str(db)+'.pitr-repository')]:
    if disposable.exists():shutil.rmtree(disposable)
 subprocess.run(['python3', str(ART/(STEM+'-analyze.py')), '--extract'], check=True)
finally:shutil.rmtree(SCRATCH)
