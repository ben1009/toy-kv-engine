#!/usr/bin/env python3
"""Excluded sampled-batch/ticket/FRONTIER diagnostic for the original prototype."""
import csv
import hashlib
import importlib.util
import json
import mmap
import os
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[4]
ART = Path(__file__).resolve().parent
STEM = 'rfc-025-v7-clean-tail-20261027'
WORK = ROOT / 'target/wal-opt/20261027-v7-clean-tail'
LIB = ROOT / 'target/wal-opt/20261027-v7-writer-scaling/io-trace.so'
spec = importlib.util.spec_from_file_location('writer_observer', ART / 'rfc-025-v7-writer-scaling-20261027-run.py')
observer_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(observer_module)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_ticket_map(directory, operations):
    mappings = {}
    images = []
    for path in directory.rglob('*.wal'):
        size = path.stat().st_size
        if size < 8192:
            continue
        with path.open('rb') as file:
            with mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as data:
                data.madvise(mmap.MADV_RANDOM)
                offset, found = 4096, 0
                while offset + 4096 <= size:
                    frame = data[offset:offset + 192]
                    if frame[:8] != b'TKVW7FR1':
                        offset += 4096
                        continue
                    kind = int.from_bytes(frame[10:12], 'big')
                    if kind != 1:
                        offset += 4096
                        continue
                    ticket = int.from_bytes(frame[64:72], 'big')
                    index = int.from_bytes(frame[72:76], 'big')
                    count = int.from_bytes(frame[76:80], 'big')
                    assert index == 0 and count > 0
                    assert int.from_bytes(frame[88:96], 'big') == ticket
                    entries = int.from_bytes(frame[116:120], 'big')
                    assert frame[136] == 1 and int.from_bytes(frame[142:146], 'big') == 11
                    key = frame[146:157]
                    assert key[:3] == b'key' and key[3:].isdigit()
                    key_number = int(key[3:])
                    entry = dict(first_key=key_number, ticket=ticket, entries=entries, file_offset=offset)
                    if key_number in mappings:
                        assert mappings[key_number]['ticket'] == ticket
                    else:
                        mappings[key_number] = entry
                    found += 1
                    offset += count * 4096
                if found:
                    images.append(dict(path=str(path.relative_to(ROOT)), inode=path.stat().st_ino, batches=found))
    assert sum(v['entries'] for v in mappings.values()) == operations, (operations, len(mappings), images)
    assert {v['ticket'] for v in mappings.values()} == set(range(len(mappings)))
    return dict(images=images, batches=sorted(mappings.values(), key=lambda x:x['ticket']))


def run_case(arm, name, operations, identity):
    for binary in identity.values():
        assert sha(ROOT / binary['path']) == binary['sha256']
    paths = {key: WORK / (name + suffix) for key, suffix in [('io','-io.csv'),('batch','-batch.jsonl'),
             ('samples','-samples.jsonl'),('map','-tickets.json'),('stdout','.stdout'),('stderr','.stderr')]}
    db = WORK / (name + '-db')
    assert not db.exists() and not any(p.exists() for p in paths.values())
    binary = identity[arm]
    command = [str(ROOT / binary['path']), '--suite','legacy','--preset','smoke','--bench','wal_batch_concurrent',
               '--wal','--wal-io-mode','parallel','--pitr','--pitr-format','v7','--num',str(operations),
               '--threads','16','--wal-batch-size','64','--value-size','1024','--target-sst-size','2147483648',
               '--compaction','none','--latency-sample-every','10','--output','json','--no-cleanup','--path',str(db)]
    before = observer_module.AUDIT.host_sample()
    assert not before['competing'], before
    env = os.environ.copy()
    assert not env.get('LD_PRELOAD') and not any(k.startswith('MIMALLOC_') for k in env)
    env.update(LD_PRELOAD=str(LIB), KV_WAL_IO_TRACE=str(paths['io']), KV_WAL_BATCH_TRACE=str(paths['batch']))
    process = subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stop, samples, errors = threading.Event(), [], []

    def observe():
        i = 0
        while not stop.is_set():
            try:
                samples.append(observer_module.sample(process.pid, i))
            except Exception as error:
                errors.append(str(error))
            i += 1
            stop.wait(0.1)

    observer = threading.Thread(target=observe)
    observer.start()
    try:
        stdout, stderr = process.communicate()
    finally:
        stop.set()
        observer.join()
    after = observer_module.AUDIT.host_sample()
    paths['stdout'].write_bytes(stdout)
    paths['stderr'].write_bytes(stderr)
    paths['samples'].write_text(''.join(json.dumps(s,separators=(',',':'))+'\n' for s in samples))
    assert process.returncode == 0, (name, process.returncode, stderr[-1000:])
    assert len(stdout.splitlines()) == 1
    record = json.loads(stdout)
    mapping = read_ticket_map(db, operations)
    paths['map'].write_text(json.dumps(mapping, separators=(',',':'))+'\n')
    batches = [json.loads(l) for l in paths['batch'].read_text().splitlines()]
    assert len(batches) == record['latency']['samples']
    mapped = {r['first_key']:r for r in mapping['batches']}
    for batch in batches:
        assert batch['first_key'] in mapped and batch['end_ns'] >= batch['start_ns']
    io = list(csv.DictReader(paths['io'].open()))
    assert sum(r['op']=='1' and r['kind']=='2' and int(r['generation'])>0 for r in io) == record['result']['wal_pipeline']['frontier_writes']
    audits = [before,*[s['host_audit'] for s in samples if 'host_audit' in s],after]
    clean = not errors and not any(s['competing'] for s in audits)
    metadata = dict(arm=arm,name=name,operations=operations,run_id=record['run_id'],command=command,
                    excluded_from_scoring=True,binary=binary,preload_sha256=sha(LIB),
                    trace_samples=len(batches),mapped_tickets=len(mapped),host_samples=len(audits),
                    clean_sampled=clean,sampling_errors=errors,before=before,after=after,returncode=process.returncode,
                    raw_sha256=hashlib.sha256(stdout.strip()).hexdigest(),
                    files={str(p.relative_to(ROOT)):sha(p) for p in paths.values()})
    for p in [db, Path(str(db)+'.pitr-repository')]:
        if p.exists():
            shutil.rmtree(p)
    assert clean, 'Captured diagnostic retained; audit stopped further commands'
    print(name,record['run_id'],round(record['result']['ops_per_sec']),record['latency']['p99_ms'],
          'traces',len(batches),'mapped',len(mapped),'clean_sampled',flush=True)
    return metadata,stdout.strip()


def main():
    identity = json.loads((ART / (STEM+'-binaries.json')).read_text())
    assert not (ART/(STEM+'-captures.json')).exists()
    if sys.argv[1:] == ['probe']:
        meta,raw = run_case('baseline','bootstrap-probe',5000,identity)
        (ART/(STEM+'-probe.json')).write_text(json.dumps(meta,indent=2)+'\n')
        (ART/(STEM+'-probe.jsonl')).write_bytes(raw+b'\n')
        return
    assert (ART/(STEM+'-probe.json')).exists()
    captures = []
    time.sleep(30)
    with (ART/(STEM+'-runs.jsonl')).open('xb') as output:
        for i,arm in enumerate(['baseline','retirement_hint','retirement_hint','baseline'],1):
            metadata,raw = run_case(arm,f'case{i}-{arm}',750000,identity)
            captures.append(metadata)
            output.write(raw+b'\n')
            output.flush()
            (ART/(STEM+'-captures.json')).write_text(json.dumps(captures,indent=2)+'\n')
            time.sleep(15)


if __name__ == '__main__':
    main()
