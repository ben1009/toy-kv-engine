#!/usr/bin/env python3
"""Excluded memory diagnostics: native mimalloc statistics plus exact child RSS."""
import csv
import hashlib
import importlib.util
import json
import os
import shutil
import sys
from pathlib import Path
sys.dont_write_bytecode=True
ROOT=Path(__file__).resolve().parents[4]
ART=Path(__file__).resolve().parent
STEM='rfc-025-v7-mimalloc-20261026-memory-probe'
WORK=ROOT/'target/wal-opt/20261026-v7-mimalloc-memory-probe'
SCRATCH=Path('/dev/shm/toy-kv-mimalloc-memory-probe-20261026')
spec=importlib.util.spec_from_file_location('driver',ART/'rfc-025-v7-mimalloc-20261026-run.py')
driver=importlib.util.module_from_spec(spec)
spec.loader.exec_module(driver)
assert not driver.host_sample()['competing']
assert not SCRATCH.exists() and not (ART/(STEM+'.jsonl')).exists()
WORK.mkdir(parents=True,exist_ok=True)
SCRATCH.mkdir()
assert 'MIMALLOC_SHOW_STATS' not in os.environ
os.environ['MIMALLOC_SHOW_STATS']='1'
try:
    with (ART/(STEM+'-runs.tsv')).open('w') as manifest:
        writer=csv.writer(manifest,delimiter='\t',lineterminator='\n')
        writer.writerow(['cell','policy','run_id','binary_sha256','raw_sha256','audit_sha256','stats_sha256','max_rss_kib','command'])
        for cell,batch,count in [('point8',None,100000),('batch64',64,750000)]:
            for policy,binary in [('system','write-perf-allocator-system'),('rust-only','write-perf-allocator-mimalloc'),('aligned','write-perf-allocator-mimalloc-aligned')]:
                tag=cell+'-'+policy
                path=ROOT/'target/release'/binary
                db=SCRATCH/tag
                usage=WORK/(tag+'-usage.json')
                command=[str(path),'--suite','legacy','--preset','smoke','--wal','--pitr','--pitr-format','v7','--wal-io-mode','parallel','--bench','wal_batch_concurrent' if batch else 'wal_concurrent','--num',str(count),'--threads','8','--value-size','1024','--target-sst-size','2147483648','--latency-sample-every','10' if batch else '100','--output','json','--path',str(db)]
                if batch:command+=['--wal-batch-size',str(batch)]
                command=['python3',str(ART/'rfc-025-v7-mimalloc-20261026-child.py'),str(usage),'--',*command]
                raw,stderr,audit,clean=driver.run_audited(command)
                record=json.loads(raw)
                audit.update(run_id=record['run_id'],policy=policy,cell=cell,excluded_from_scoring=True,MIMALLOC_SHOW_STATS='1',child_rusage=json.loads(usage.read_text()),clean_sampled=clean)
                stats=ART/(STEM+'-'+tag+'-stats.txt')
                stats.write_bytes(stderr)
                audit_bytes=json.dumps(audit,sort_keys=True,separators=(',',':')).encode()
                with (ART/(STEM+'.jsonl')).open('ab') as stream:stream.write(raw+b'\n')
                with (ART/(STEM+'-host.jsonl')).open('ab') as stream:stream.write(audit_bytes+b'\n')
                writer.writerow([cell,policy,record['run_id'],driver.sha(path),hashlib.sha256(raw).hexdigest(),hashlib.sha256(audit_bytes).hexdigest(),driver.sha(stats),audit['child_rusage']['max_rss_kib'],str(command)])
                manifest.flush()
                for disposable in [db,Path(str(db)+'.pitr-repository')]:
                    if disposable.exists():shutil.rmtree(disposable)
                assert clean and audit['returncode']==0
                print('Excluded memory diagnostic:',tag,flush=True)
finally:
    os.environ.pop('MIMALLOC_SHOW_STATS',None)
    shutil.rmtree(SCRATCH)
