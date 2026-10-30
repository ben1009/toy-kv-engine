#!/usr/bin/env python3
"""Unscored retained SSD profiles before changing v7 extent I/O granularity."""
import csv
import hashlib
import importlib.util
import json
import shutil
import subprocess
import sys
from pathlib import Path
sys.dont_write_bytecode=True
ROOT=Path(__file__).resolve().parents[4]
ART=Path(__file__).resolve().parent
STEM='rfc-025-v7-extent-io-20261026'
WORK=ROOT/'target/wal-opt/20261026-v7-extent-io'
BINARY=ROOT/'target/release/write-perf-v7-validated-logical-batch'
SHA='614f0cb1adde43cabe900eb032c6f884783d658c597471d9d9ddc8faff314aa9'
SCRATCH=WORK/'profile-scratch'
spec=importlib.util.spec_from_file_location('audit',ART/'rfc-025-v7-buffered-isolated-20261026-run.py')
audit=importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)
assert not audit.host_sample()['competing']
assert hashlib.sha256(BINARY.read_bytes()).hexdigest()==SHA
assert not SCRATCH.exists() and not (ART/(STEM+'-profiles.jsonl')).exists()
SCRATCH.mkdir()
try:
    with (ART/(STEM+'-profiles.tsv')).open('w') as manifest:
        writer=csv.writer(manifest,delimiter='\t',lineterminator='\n')
        writer.writerow(['cell','binary_sha256','event','run_id','raw_sha256','command'])
        for cell,version,batch,count in [('v4-point8',4,None,100000),('v7-point8',7,None,100000),('v7-batch64',7,64,750000)]:
            assert not audit.host_sample()['competing']
            db=SCRATCH/cell
            data=WORK/(cell+'.data')
            args=[str(BINARY),'--suite','legacy','--preset','smoke','--wal','--bench','wal_batch_concurrent' if batch else 'wal_concurrent','--num',str(count),'--threads','8','--value-size','1024','--target-sst-size','2147483648','--latency-sample-every','10' if batch else '100','--output','json','--wal-io-mode','parallel','--path',str(db)]
            if batch:args+=['--wal-batch-size',str(batch)]
            if version==7:args+=['--pitr','--pitr-format','v7']
            for event in ['cpu-clock','cpu-clock:u']:
                command=['perf','record','-e',event,'-F','997','--call-graph','dwarf,8192','-o',str(data),'--',*args]
                print('Unscored SSD profile:',cell,event,flush=True)
                result=subprocess.run(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                (WORK/(cell+'-'+event.replace(':','-')+'.stderr')).write_bytes(result.stderr)
                if result.returncode==0:break
                assert not result.stdout.strip(), 'benchmark failed after starting; preserve diagnostic state'
                assert 'Permission' in result.stderr.decode() or 'Access' in result.stderr.decode(),result.stderr.decode()
            assert result.returncode==0,result.stderr.decode()
            raw=result.stdout.strip();record=json.loads(raw)
            with (ART/(STEM+'-profiles.jsonl')).open('ab') as stream:stream.write(raw+b'\n')
            writer.writerow([cell,SHA,event,record['run_id'],hashlib.sha256(raw).hexdigest(),subprocess.list2cmdline(command)])
            manifest.flush()
            for mode,options in [('flat',['--no-children','--call-graph','none']),('calls',['--children','--call-graph','graph,1.0,caller'])]:
                report=subprocess.check_output(['perf','report','--stdio','--percent-limit','0.5','-i',str(data),*options],text=True)
                (ART/(STEM+'-'+cell+'-'+mode+'.txt')).write_text('\n'.join(line.rstrip() for line in report.splitlines())+'\n')
            for disposable in [db,Path(str(db)+'.pitr-repository')]:
                if disposable.exists():shutil.rmtree(disposable)
finally:
    shutil.rmtree(SCRATCH)
