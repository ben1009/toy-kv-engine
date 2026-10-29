#!/usr/bin/env python3
"""Fixed SSD comparison of producer framing plus post-submit hashing."""
import csv
import datetime
import hashlib
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path('/home/liu/proj/toy-kv-engine')
ART = ROOT / 'docs/wal/benchmarks/artifacts'
STEM = 'rfc-025-v7-producer-framing-20261029'
WORK = ROOT / 'target/wal-opt/20261029-v7-producer-framing'
BLOCKED = {'cargo', 'cargo-make', 'cargo-nextest', 'nextest', 'rustc', 'rustdoc',
           'rustfmt', 'clippy-driver', 'sccache', 'cc', 'gcc', 'g++', 'cc1', 'cc1plus',
           'clang', 'clang++', 'ld', 'ld.lld', 'lld', 'collect2', 'cmake', 'ninja',
           'make', 'perf', 'fio', 'stress-ng'}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def host_sample(expected_pid=None):
    output = subprocess.check_output(['ps', '-eo', 'pid=,ppid=,comm=,stat=,pcpu='], text=True)
    competing = []
    for line in output.splitlines():
        fields = line.split()
        pid, ppid = fields[:2]
        state, cpu = fields[-2:]
        name = ' '.join(fields[2:-2])
        if 'Z' in state or int(pid) == expected_pid or (expected_pid is not None and int(ppid) == expected_pid and name.startswith('write-perf')):
            continue
        if name in BLOCKED or name.startswith(('write-perf', 'kv_engine-')):
            competing.append(dict(pid=int(pid), ppid=int(ppid), command=name, state=state, cpu_percent=float(cpu)))
    cpu = Path('/proc/stat').read_text().splitlines()[0].split()[1:]
    return dict(time_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                competing=competing, load_average=list(os.getloadavg()), cpu_jiffies=list(map(int, cpu)))


def run_audited(command):
    before = host_sample()
    if before['competing']:
        raise RuntimeError('Competing processes before launch: ' + json.dumps(before))
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stop = threading.Event()
    samples, errors = [], []

    def observe():
        while not stop.is_set():
            try:
                samples.append(host_sample(process.pid))
            except Exception as error:
                errors.append(str(error))
            stop.wait(1)

    observer = threading.Thread(target=observe, name='wal-host-audit')
    observer.start()
    try:
        stdout, stderr = process.communicate()
    finally:
        stop.set()
        observer.join()
    after = host_sample()
    audit = dict(benchmark_pid=process.pid, returncode=process.returncode,
                 sample_period_seconds=1, before=before, during=samples, after=after,
                 sampling_errors=errors)
    clean = not errors and not any(s['competing'] for s in [before, *samples, after])
    return stdout.strip(), stderr, audit, clean


def main():
    cell = sys.argv[1]
    matrix = json.loads((ART / (STEM + '-matrix.json')).read_text())
    cfg = matrix['cells'][cell]
    identity = json.loads((ART / (STEM + '-binaries.json')).read_text())
    paths = {suffix: ART / (STEM + '-' + cell + suffix) for suffix in ('.jsonl', '-runs.tsv', '-host.jsonl')}
    scratch = WORK / (cell + '-scratch')
    assert not scratch.exists() and not any(p.exists() for p in paths.values()), 'Capture already exists'
    for item in identity.values():
        assert sha(ROOT / item['path']) == item['sha256']
    assert not [key for key in os.environ if key.startswith('MIMALLOC_') or key == 'LD_PRELOAD']
    scratch.mkdir()
    schedule = [('warmup', 0, arm) for arm in cfg['arms']]
    for pair in range(1, cfg['pairs'] + 1):
        schedule += [('scored', pair, arm) for arm in (cfg['arms'] if pair % 2 else list(reversed(cfg['arms'])))]
    invocations = set()
    try:
        with paths['.jsonl'].open('wb') as raw_stream, paths['-runs.tsv'].open('w') as manifest, paths['-host.jsonl'].open('wb') as audits:
            writer = csv.writer(manifest, delimiter='\t', lineterminator='\n')
            writer.writerow(['phase','pair','arm','run_id','part','binary_sha256','raw_sha256','audit_sha256','command'])
            time.sleep(matrix['ssd_initial_cooldown_seconds' if cfg['kind']=='ssd' else 'cpu_initial_cooldown_seconds'])
            for phase, pair, arm in schedule:
                backend = arm
                binary = identity[backend]
                name = phase + '-' + str(pair) + '-' + arm
                db = scratch / (name + '-db')
                usage_path = WORK / (cell + '-' + name + '-rusage.json')
                assert not usage_path.exists()
                command = [str(ROOT / binary['path']), '--suite', 'legacy', '--preset', 'smoke',
                           '--bench',cfg['bench'],'--num',str(cfg['operations']),
                           '--reads',str(cfg['reads']),'--scan-num',str(cfg['scan_num']),
                           '--threads',str(cfg['writers']),'--value-size','1024',
                           '--target-sst-size','2147483648','--compaction','none',
                           '--output','json','--path',str(db)]
                if cfg['batch']:
                    command += ['--wal-batch-size',str(cfg['batch'])]
                if cfg['kind']=='ssd':
                    command += ['--wal','--wal-io-mode','parallel','--latency-sample-every',str(cfg['sample_every'])]
                    if arm.endswith('v7'):
                        command += ['--pitr','--pitr-format','v7']
                wrapped = ['python3',str(ART / (STEM + '-child.py')),str(usage_path),'--',*command]
                stdout, stderr, audit, clean = run_audited(wrapped)
                (WORK / (cell + '-' + name + '.stderr')).write_bytes(stderr)
                (WORK / (cell + '-' + name + '.stdout')).write_bytes(stdout + b'\n')
                if audit['returncode']:
                    (WORK / (cell + '-' + name + '-failed.stdout')).write_bytes(stdout)
                    raise RuntimeError('Benchmark failed: ' + str(audit['returncode']))
                lines = stdout.splitlines()
                records = [json.loads(line) for line in lines]
                run_ids = {r['run_id'] for r in records}
                assert len(run_ids)==1 and run_ids.isdisjoint(invocations)
                run_id = run_ids.pop(); invocations.add(run_id)
                assert len({r['measurement'] for r in records})==len(records)
                usage = json.loads(usage_path.read_text())
                audit.update(launcher_pid=audit['benchmark_pid'],benchmark_pid=usage['pid'],
                             child_rusage=usage,child_rusage_sha256=sha(usage_path),run_id=run_id,
                             phase=phase,pair=pair,arm=arm,binary_sha256=binary['sha256'],clean_sampled=clean)
                encoded_audit = json.dumps(audit,sort_keys=True,separators=(',',':')).encode()
                audits.write(encoded_audit + b'\n'); audits.flush()
                for line, record in zip(lines, records):
                    assert record['workload']==cfg['bench'] and record['params']['value_size']==1024
                    assert record['engine_options']['target_sst_size']==2147483648
                    if cfg['kind']=='ssd':
                        assert record['params']['wal_format_version']==(7 if arm.endswith('v7') else 4)
                        assert record['params']['wal_io_mode']=='parallel' and record['result']['ops']==cfg['operations']
                    else:
                        assert record['measurement'] in cfg['parts']
                        count = record['result'].get('ops') or record['result'].get('entries')
                        assert count==cfg['parts'][record['measurement']]
                        if cfg['bench']=='readrandom':assert record['result']['found']==cfg['reads']
                    raw_stream.write(line + b'\n'); raw_stream.flush()
                    writer.writerow([phase,pair,arm,run_id,record['measurement'],binary['sha256'],
                                     hashlib.sha256(line).hexdigest(),hashlib.sha256(encoded_audit).hexdigest(),
                                     subprocess.list2cmdline(wrapped)])
                    manifest.flush()
                for disposable in (db,Path(str(db)+'.pitr-repository')):
                    if disposable.exists():shutil.rmtree(disposable)
                assert clean, 'Sampled competing activity; capture preserved and matrix stopped'
                print(cell,phase,pair,arm,'clean_sampled',flush=True)
                time.sleep(matrix['ssd_between_seconds' if cfg['kind']=='ssd' else 'cpu_between_seconds'])
    finally:
        shutil.rmtree(scratch)
    for item in identity.values():assert sha(ROOT / item['path'])==item['sha256']


if __name__ == '__main__':
    main()
