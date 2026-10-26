#!/usr/bin/env python3
"""Unscored profiles of the retained direct-I/O pipeline, before packer changes."""
import csv
import hashlib
import importlib.util
import json
import shutil
import subprocess
from pathlib import Path
ROOT = Path(__file__).resolve().parents[4]
ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-packer-refill-20261026'
WORK = ROOT / 'target/wal-opt/20261026-v7-packer-refill'
BINARY = ROOT / 'target/release/write-perf-v7-validated-logical-batch'
SHA = '614f0cb1adde43cabe900eb032c6f884783d658c597471d9d9ddc8faff314aa9'
SCRATCH = Path('/dev/shm/toy-kv-packer-refill-20261026-profile')
spec = importlib.util.spec_from_file_location('audit', ART / 'rfc-025-v7-buffered-isolated-20261026-run.py')
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)
assert not audit.host_sample()['competing']
assert hashlib.sha256(BINARY.read_bytes()).hexdigest() == SHA
assert not SCRATCH.exists() and not (ART / (STEM + '-profiles.jsonl')).exists()
WORK.mkdir(parents=True, exist_ok=True)
SCRATCH.mkdir()
try:
    with (ART / (STEM + '-profiles.tsv')).open('w') as manifest:
        writer = csv.writer(manifest, delimiter='\t', lineterminator='\n')
        writer.writerow(['cell','binary_sha256','run_id','raw_sha256','command'])
        for cell, version, batch, count in [('v4-batch64',4,True,750000),('v7-batch64',7,True,750000),('v7-point8',7,False,100000)]:
            assert not audit.host_sample()['competing']
            db = SCRATCH / cell
            data = WORK / (cell + '.data')
            command = [str(BINARY),'--suite','legacy','--preset','smoke','--wal','--bench',
                       'wal_batch_concurrent' if batch else 'wal_concurrent','--num',str(count),
                       '--threads','8','--value-size','1024','--target-sst-size','2147483648',
                       '--latency-sample-every','10' if batch else '100','--output','json',
                       '--wal-io-mode','parallel','--path',str(db)]
            if batch: command += ['--wal-batch-size','64']
            if version == 7: command += ['--pitr','--pitr-format','v7']
            command = ['perf','record','-e','cpu-clock:u','-F','997','--call-graph','dwarf,8192','-o',str(data),'--',*command]
            print('Unscored profile:',cell,flush=True)
            result = subprocess.run(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE,check=True)
            (WORK / (cell + '.stderr')).write_bytes(result.stderr)
            raw = result.stdout.strip()
            record = json.loads(raw)
            with (ART / (STEM + '-profiles.jsonl')).open('ab') as stream: stream.write(raw + b'\n')
            writer.writerow([cell,SHA,record['run_id'],hashlib.sha256(raw).hexdigest(),subprocess.list2cmdline(command)])
            manifest.flush()
            for mode, options in [('flat',['--no-children','--call-graph','none']),('calls',['--children','--call-graph','graph,1.0,caller'])]:
                report = subprocess.check_output(['perf','report','--stdio','--percent-limit','0.5','-i',str(data),*options],text=True)
                (ART / (STEM + '-' + cell + '-' + mode + '.txt')).write_text('\n'.join(line.rstrip() for line in report.splitlines()) + '\n')
            with (WORK / (cell + '-samples.txt')).open('w') as stream:
                subprocess.run(['perf','script','-i',str(data),'-F','comm,pid,tid,period,ip,sym'],stdout=stream,check=True)
            for path in [db,Path(str(db) + '.pitr-repository')]:
                if path.exists(): shutil.rmtree(path)
            assert not audit.host_sample()['competing']
finally:
    shutil.rmtree(SCRATCH)
