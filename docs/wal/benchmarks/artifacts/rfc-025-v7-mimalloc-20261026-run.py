#!/usr/bin/env python3
"""Measure Rust System versus mimalloc with host activity and child memory auditing."""
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
CELLS = {'ext4-point8': (100000, None, 5), 'ext4-batch64': (750000, 64, 5),
         'tmpfs-point8': (100000, None, 3), 'tmpfs-point1': (100000, None, 3),
         'tmpfs-batch64': (750000, 64, 3)}
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
    variant, cell = 'mimalloc', sys.argv[1]
    assert variant == 'mimalloc'
    confirmation = '--confirmation' in sys.argv[2:]
    assert not confirmation or cell.startswith('ext4')
    operations, batch, pairs = CELLS[cell]
    writers = 1 if cell.endswith('point1') else 8
    filesystem = 'ext4' if cell.startswith('ext4') else 'tmpfs'
    tag = 'confirmation' if confirmation else 'initial'
    work = ROOT / ('target/wal-opt/20261026-v7-' + variant + '-' + tag)
    work.mkdir(parents=True, exist_ok=True)
    identity = json.loads((ART / ('rfc-025-v7-' + variant + '-20261026-binaries.json')).read_text())
    stem = 'rfc-025-v7-' + variant + '-' + tag + '-' + cell + '-20261026'
    scratch = (work if filesystem == 'ext4' else Path('/dev/shm')) / (stem + '-scratch')
    raw_path, manifest_path, audit_path = [ART / (stem + suffix) for suffix in ('.jsonl', '-runs.tsv', '-host.jsonl')]
    assert not any(p.exists() for p in (scratch, raw_path, manifest_path, audit_path)), 'refusing to overwrite captures'
    for data in identity.values():
        assert sha(ROOT / data['path']) == data['sha256']
    preflight = host_sample()
    assert not preflight['competing'], preflight
    scratch.mkdir()
    ids = set()
    try:
        with raw_path.open('wb') as raw_stream, manifest_path.open('w') as manifest, audit_path.open('wb') as audits:
            writer = csv.writer(manifest, delimiter='\t', lineterminator='\n')
            writer.writerow(['source_record', 'phase', 'filesystem', 'workload', 'writers', 'operations',
                             'pair', 'arm', 'binary_sha256', 'run_id', 'raw_sha256', 'command',
                             'nvme_temperature_millic', 'isolation_status', 'audit_sha256'])
            schedule = [(0, arm, 'warmup') for arm in ('old_v4', 'old_v7', 'new_v4', 'new_v7')]
            for pair in range(1, pairs + 1):
                arms = ('old_v4', 'new_v4', 'old_v7', 'new_v7') if pair % 2 else ('new_v7', 'old_v7', 'new_v4', 'old_v4')
                schedule += [(pair, arm, 'scored') for arm in arms]
            if filesystem == 'ext4':
                time.sleep(30)
            for pair, arm, phase in schedule:
                data = identity['retained' if arm.startswith('old') else 'candidate']
                version = 4 if arm.endswith('v4') else 7
                name = f'{phase}-p{pair}-{arm}'
                db = scratch / (name + '-db')
                command = [str(ROOT / data['path']), '--suite', 'legacy', '--preset', 'smoke', '--wal',
                           '--bench', 'wal_batch_concurrent' if batch else 'wal_concurrent',
                           '--num', str(operations), '--threads', str(writers), '--value-size', '1024',
                           '--target-sst-size', '2147483648', '--latency-sample-every', '10' if batch else '100',
                           '--output', 'json', '--wal-io-mode', 'parallel', '--path', str(db)]
                if batch:
                    command += ['--wal-batch-size', str(batch)]
                if version == 7:
                    command += ['--pitr', '--pitr-format', 'v7']
                rusage_path = work / (cell + '-' + name + '.rusage.json')
                assert not rusage_path.exists()
                command = ['python3',str(ART / 'rfc-025-v7-mimalloc-20261026-child.py'),str(rusage_path),'--',*command]
                raw, stderr, audit, clean = run_audited(command)
                audit['launcher'] = 'Python fork/exec + wait4 (Linux peak RSS in KiB)'
                audit['child_rusage'] = json.loads(rusage_path.read_text())
                audit['child_rusage_sha256'] = sha(rusage_path)
                (work / (cell + '-' + name + '.stderr')).write_bytes(stderr)
                if audit['returncode']:
                    (work / (cell + '-' + name + '-failed.stdout')).write_bytes(raw)
                record = json.loads(raw)
                assert record['params']['wal_format_version'] == version
                assert record['params']['wal_io_mode'] == 'parallel'
                assert record['params']['threads'] == writers and record['params']['batch_size'] == batch
                assert record['params']['value_size'] == 1024
                assert record['engine_options']['target_sst_size'] == 2147483648
                assert record['result']['ops'] == operations
                assert record['latency']['samples'] == (1176 if batch else 1000)
                assert record['run_id'] not in ids
                ids.add(record['run_id'])
                audit.update(run_id=record['run_id'], phase=phase, pair=pair, arm=arm,
                             binary_sha256=data['sha256'], clean_sampled=clean)
                audit_bytes = json.dumps(audit, sort_keys=True, separators=(',', ':')).encode()
                temperature = 'unavailable'
                for hwmon in Path('/sys/class/hwmon').glob('hwmon*'):
                    try:
                        if (hwmon / 'name').read_text().strip() == 'nvme':
                            temperature = (hwmon / 'temp1_input').read_text().strip()
                            break
                    except (FileNotFoundError, PermissionError):
                        pass
                raw_stream.write(raw + b'\n'); raw_stream.flush()
                audits.write(audit_bytes + b'\n'); audits.flush()
                writer.writerow([name + '.json', phase, filesystem, 'batch64' if batch else 'point' + str(writers),
                                 writers, operations, pair, arm, data['sha256'], record['run_id'],
                                 hashlib.sha256(raw).hexdigest(), subprocess.list2cmdline(command), temperature,
                                 'clean_sampled' if clean else 'contaminated', hashlib.sha256(audit_bytes).hexdigest()])
                manifest.flush()
                for disposable in (db, Path(str(db) + '.pitr-repository')):
                    if disposable.exists():
                        shutil.rmtree(disposable)
                assert audit['returncode'] == 0, audit
                assert clean, 'Competing activity or sampling error detected; run preserved and matrix stopped.'
                print(variant, cell, phase, pair, arm, 'clean_sampled', flush=True)
                time.sleep(10 if filesystem == 'ext4' else 3)
        for data in identity.values():
            assert sha(ROOT / data['path']) == data['sha256']
    finally:
        shutil.rmtree(scratch)


if __name__ == '__main__':
    main()
