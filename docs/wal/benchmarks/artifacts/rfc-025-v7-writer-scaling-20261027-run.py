#!/usr/bin/env python3
"""Excluded, fixed 8/16-writer diagnostics with in-memory syscall/thread capture."""
import datetime
import hashlib
import importlib.util
import json
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
STEM = 'rfc-025-v7-writer-scaling-20261027'
WORK = ROOT / 'target/wal-opt/20261027-v7-writer-scaling'
SPEC = importlib.util.spec_from_file_location('audited_capture', ART / 'rfc-025-v7-frontier-buffer-reuse-20261027-run.py')
AUDIT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(AUDIT)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def task_sample(pid):
    result = {}
    for p in sorted(Path('/proc', str(pid), 'task').glob('*')):
        try:
            stat = (p / 'stat').read_text().rpartition(')')[2].split()
            name = (p / 'comm').read_text().strip()
            row = dict(name=name, state=stat[0], user_ticks=int(stat[11]), system_ticks=int(stat[12]), cpu=int(stat[36]))
            if name.startswith(('wal-', 'write-perf')):
                row['wchan'] = (p / 'wchan').read_text().strip()
                try:
                    row['syscall_number'] = (p / 'syscall').read_text().split()[0]
                except OSError as e:
                    row['syscall_unavailable'] = type(e).__name__
            result[p.name] = row
        except (OSError, IndexError, ValueError):
            continue  # A task may exit between directory and file reads.
    return result


def sample(pid, index):
    value = dict(monotonic_ns=time.monotonic_ns(), tasks=task_sample(pid),
                 disk_stat=list(map(int, Path('/sys/class/block/nvme0n1/stat').read_text().split())),
                 io_pressure=Path('/proc/pressure/io').read_text().strip())
    if index % 10 == 0:
        value['host_audit'] = AUDIT.host_sample(pid)
    return value


def main():
    plan = json.loads((ART / (STEM + '-plan.json')).read_text())
    identity = plan['binary_identities']
    for binary in identity.values():
        assert sha(ROOT / binary['path']) == binary['sha256']
    assert not os.environ.get('LD_PRELOAD')
    assert not any(k.startswith('MIMALLOC_') for k in os.environ)
    raw_path, captures_path = ART / (STEM + '-runs.jsonl'), ART / (STEM + '-captures.json')
    assert not raw_path.exists() and not captures_path.exists()
    topology = {}
    for cpu in Path('/sys/devices/system/cpu').glob('cpu[0-9]*'):
        if not cpu.name[3:].isdigit():
            continue
        values = {}
        for rel in ['topology/core_id', 'topology/thread_siblings_list', 'cpufreq/cpuinfo_max_freq']:
            p = cpu / rel
            if p.exists():
                values[rel] = p.read_text().strip()
        topology[cpu.name] = values
    environment = dict(kernel=subprocess.check_output(['uname', '-srvo'], text=True).strip(),
                       sched_schedstats=Path('/proc/sys/kernel/sched_schedstats').read_text().strip(),
                       tracepoint_probe='perf stat -e syscalls:sys_enter_fdatasync -- true: exit129, no permission to read trace event',
                       topology=topology, trace_library_sha256=sha(WORK / 'io-trace.so'),
                       trace_source_sha256=sha(ART / (STEM + '-io-trace.c')),
                       driver_sha256=sha(Path(__file__)), plan_sha256=sha(ART / (STEM + '-plan.json')),
                       monitor='100ms proc/sysfs sampling; no NVMe SMART or temperature polling')
    (ART / (STEM + '-environment.json')).write_text(json.dumps(environment, indent=2) + '\n')
    captures = []
    time.sleep(plan['configuration']['initial_cooldown_seconds'])
    with raw_path.open('wb') as raw:
        for item in plan['schedule']:
            name = f"round{item['round']}-{item['arm']}-{item['writers']}writers"
            db, io = WORK / (name + '-db'), WORK / (name + '-io.csv')
            stdout_path, stderr_path, samples_path = [WORK / (name + suffix) for suffix in ['.stdout', '.stderr', '-samples.jsonl']]
            assert not any(p.exists() for p in [db, io, stdout_path, stderr_path, samples_path])
            binary = identity[item['arm']]
            command = [str(ROOT / binary['path']), '--suite', 'legacy', '--preset', 'smoke',
                       '--bench', 'wal_batch_concurrent', '--wal', '--wal-io-mode', 'parallel',
                       '--pitr', '--pitr-format', 'v7', '--num', '750000', '--threads', str(item['writers']),
                       '--wal-batch-size', '64', '--value-size', '1024', '--target-sst-size', '2147483648',
                       '--compaction', 'none', '--latency-sample-every', '10', '--output', 'json', '--path', str(db)]
            before = AUDIT.host_sample()
            assert not before['competing'], before
            env = os.environ.copy()
            env.update(LD_PRELOAD=str(WORK / 'io-trace.so'), KV_WAL_IO_TRACE=str(io))
            process = subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stop, samples, errors = threading.Event(), [], []

            def observe():
                i = 0
                while not stop.is_set():
                    try:
                        samples.append(sample(process.pid, i))
                    except Exception as error:
                        errors.append(str(error))
                    i += 1
                    stop.wait(plan['configuration']['sampler_period_seconds'])

            observer = threading.Thread(target=observe)
            observer.start()
            try:
                stdout, stderr = process.communicate()
            finally:
                stop.set()
                observer.join()
            after = AUDIT.host_sample()
            stdout_path.write_bytes(stdout)
            stderr_path.write_bytes(stderr)
            samples_path.write_text(''.join(json.dumps(s, separators=(',', ':')) + '\n' for s in samples))
            assert process.returncode == 0, (name, process.returncode, stderr[-1500:])
            assert len(stdout.splitlines()) == 1
            record = json.loads(stdout)
            assert record['params']['threads'] == item['writers'] and record['params']['wal_format_version'] == 7
            assert record['result']['ops'] == 750000 and record['params']['batch_size'] == 64
            raw.write(stdout.strip() + b'\n')
            raw.flush()
            audits = [before, *[s['host_audit'] for s in samples if 'host_audit' in s], after]
            clean = not errors and not any(a['competing'] for a in audits)
            meta = dict(**item, name=name, excluded_from_scoring=True, run_id=record['run_id'], command=command,
                        binary=binary, preload_library_sha256=environment['trace_library_sha256'],
                        io_trace_environment_key='KV_WAL_IO_TRACE', raw_sha256=hashlib.sha256(stdout.strip()).hexdigest(),
                        samples=len(samples), audit_samples=len(audits), clean_sampled=clean,
                        sampling_errors=errors, before=before, after=after, returncode=process.returncode,
                        files={str(p.relative_to(ROOT)): sha(p) for p in [io, stdout_path, stderr_path, samples_path]})
            captures.append(meta)
            captures_path.write_text(json.dumps(captures, indent=2) + '\n')
            for p in [db, Path(str(db) + '.pitr-repository')]:
                if p.exists():
                    shutil.rmtree(p)
            assert clean, 'Captured diagnostic retained; competing-work audit stopped further runs'
            print(name, record['run_id'], round(record['result']['ops_per_sec']), record['latency']['p99_ms'], 'clean_sampled', flush=True)
            time.sleep(plan['configuration']['between_seconds'])
    assert len(captures) == 8 and len({c['run_id'] for c in captures}) == 8
    for binary in identity.values():
        assert sha(ROOT / binary['path']) == binary['sha256']


if __name__ == '__main__':
    main()
